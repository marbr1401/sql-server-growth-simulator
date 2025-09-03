#!/usr/bin/env python3
"""
SQL Server Database Growth Simulator
Generates RAW data snapshots for SQL Server databases
This script simulates database growth and generates snapshots of database states,
including size, IO statistics, and autogrowth events.
It is designed to work with a configuration manager and unified database manager
to handle server state and synchronization.
"""

import json
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from config_manager import ConfigurationManager
from growth_utils import (
    calculate_next_simulation_period,
    load_server_state,
    save_server_state,
    generate_raw_io_data,
    generate_raw_size_data,
    generate_raw_table_data,
    generate_raw_autogrowth_events
)
from unified_database_manager import auto_sync_if_needed


def setup_logging():
    """Setup minimal logging for presentation"""
    logging.basicConfig(
        level=logging.WARNING,  # Changed from INFO to WARNING
        format='%(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('growth_simulator.log'),
            # Remove StreamHandler to eliminate console logging noise
        ]
    )

def load_configuration_files() -> Dict[str, Any]:
    """Load configuration files"""
    configs = {}
    
    try:
        with open('baseline_templates.json', 'r') as f:
            configs['baseline_templates'] = json.load(f)
        
        with open('table_patterns.json', 'r') as f:
            configs['table_patterns'] = json.load(f)
        
        try:
            with open('data/fake_database_names.json', 'r') as f:
                configs['database_names'] = json.load(f)
        except FileNotFoundError:
            configs['database_names'] = {}
            logging.warning("Database names file not found, using defaults")
        
        return configs
    except Exception as e:
        logging.error(f"Error loading configuration files: {e}")
        raise


def get_database_names_for_server_type(server_type: str, configs: Dict[str, Any]) -> List[str]:
    """Get database names for server type"""
    if 'database_names' in configs and configs['database_names']:
        if server_type in configs['database_names']:
            return configs['database_names'][server_type]
    
    default_names = {
        'oltp_production': ["PrimaryStore_DB", "CustomerCore_DB", "OrderProcessing_DB"],
        'reporting_analytics': ["DataWarehouse_DB", "Analytics_DB", "Reporting_DB"],
        'reference_config': ["ReferenceData_DB", "ConfigStore_DB"]
    }
    
    return default_names.get(server_type, [f'Database_{i+1}' for i in range(3)])


def initialize_database_state(server_num: int, database_name: str, server_type: str) -> Dict[str, Any]:
    """Initialize database state for first run - SIMPLIFIED"""
    
    # Define problem databases
    problem_databases = {
        "Server1/TransactionLog_DB": {"pattern": "no_retention", "start_size": 400},
        "Server2/PrimaryStore_DB": {"pattern": "growing_fast", "start_size": 500},
        "Server2/CustomerCore_DB": {"pattern": "broken_cleanup", "start_size": 450},
        "Server3/DataWarehouse_DB": {"pattern": "archive_failure", "start_size": 600}
    }
    
    db_key = f"Server{server_num}/{database_name}"
    
    # Set base size and growth pattern
    if db_key in problem_databases:
        problem_info = problem_databases[db_key]
        base_size = problem_info["start_size"]
        growth_pattern = problem_info["pattern"]
        if server_type == 'oltp_production':
            table_count = random.randint(15, 25)
        elif server_type == 'reporting_analytics':
            table_count = random.randint(8, 15)
        else:
            table_count = random.randint(5, 10)
    else:
        # Normal databases - stable
        if server_type == 'oltp_production':
            base_size = random.uniform(250.0, 350.0)
            growth_pattern = "stable"
            table_count = random.randint(15, 25)
        elif server_type == 'reporting_analytics':
            base_size = random.uniform(300.0, 500.0)
            growth_pattern = "etl_cycle"
            table_count = random.randint(8, 15)
        else:  # reference_config
            base_size = random.uniform(10.0, 30.0)
            growth_pattern = "static"
            table_count = random.randint(5, 10)
    
    # Initialize tables with minimal data - KEEPING YOUR EXACT CODE
    tables = {}
    for i in range(table_count):
        table_name = f"Table_{i+1:02d}"
        
        if server_type == 'reference_config':
            daily_growth = random.randint(1, 10)
        else:
            daily_growth = random.randint(1000, 10000)
        
        # Determine if table has cleanup
        if server_type == 'oltp_production':
            has_cleanup = random.random() < 0.8
        elif server_type == 'reporting_analytics':
            has_cleanup = random.random() < 0.7
        else:
            has_cleanup = False
        
        tables[table_name] = {
            'name': table_name,
            'rows': random.randint(10000, 1000000),
            'daily_growth': daily_growth,
            'avg_row_bytes': random.randint(128, 512),
            'has_cleanup': has_cleanup
        }
    
    return {
        'database_name': database_name,
        'server_type': server_type,
        'current_size_gb': base_size,
        'data_file_gb': base_size * 0.75,
        'log_file_gb': base_size * 0.25,
        'cumulative_reads': 0,
        'cumulative_writes': 0,
        'growth_pattern': growth_pattern,  # ADDED THIS FIELD
        'tables': tables,
        'last_period': None
    }

def generate_snapshot_data(server_num: int, database_name: str, server_type: str,
                          db_state: Dict[str, Any], period: Dict[str, Any],
                          configs: Dict[str, Any]) -> Dict[str, Any]:
    """Generate snapshot data"""
    
    baseline = configs['baseline_templates']['baseline_templates'][server_type]
    
    # Generate raw IO data
    io_data = generate_raw_io_data(db_state, baseline, period, server_type)
    
    # Generate raw size data  
    size_data = generate_raw_size_data(db_state, baseline, period, server_type)
    
    # Generate raw table data
    tables_data = generate_raw_table_data(db_state, server_type, period)
    
    # Update state for next run
    db_state['current_size_gb'] = size_data['total_gb']
    db_state['data_file_gb'] = size_data['data_file_gb']
    db_state['log_file_gb'] = size_data['log_file_gb']
    db_state['cumulative_reads'] = io_data['cumulative_reads']
    db_state['cumulative_writes'] = io_data['cumulative_writes']
    db_state['last_period'] = period
    
    # Update table states
    for table in tables_data:
        if table['name'] in db_state['tables']:
            db_state['tables'][table['name']]['rows'] = table['rows']
    
    # Create snapshot
    period_end = datetime.fromisoformat(period['period_end'])
    
    snapshot = {
        'timestamp': period_end.isoformat(),
        'server_name': f'Server{server_num}',
        'server_number': server_num,  
        'server_type': server_type,    
        'database_name': database_name,
        'period_start': period['period_start'],
        'period_end': period['period_end'],
        'period_type': period['period_type'],
        
        'size': size_data,
        'io': io_data,
        'tables': tables_data
    }
    
    return snapshot


def save_snapshot(server_num: int, database_name: str, snapshot: Dict[str, Any]):
    """Save snapshot to file"""
    period_end = datetime.fromisoformat(snapshot['period_end'])
    timestamp = period_end.strftime("%Y%m%d_%H%M%S")
    
    server_dir = Path(f"Server{server_num}")
    snapshots_dir = server_dir / "growth_data" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{database_name}_snapshot_{timestamp}.json"
    
    with open(snapshots_dir / filename, 'w') as f:
        json.dump(snapshot, f, indent=2)
    
    # logging.info(f"Saved snapshot: {filename}")


def save_autogrowth_events(server_num: int, database_name: str, events: List[Dict[str, Any]], period: Dict[str, Any]):
    """Save autogrowth events to file"""
    if not events:
        return
        
    period_start = datetime.fromisoformat(period['period_start'])
    timestamp = period_start.strftime("%Y%m%d_%H%M%S")
    
    server_dir = Path(f"Server{server_num}")
    autogrowth_dir = server_dir / "growth_data" / "autogrowth_events"
    autogrowth_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{database_name}_autogrowth_{timestamp}.json"
    
    events_data = {
        'server_name': f'Server{server_num}',
        'server_number': server_num,  
        'database_name': database_name,
        'period_start': period['period_start'],
        'period_end': period['period_end'],
        'events': events
    }
    
    with open(autogrowth_dir / filename, 'w') as f:
        json.dump(events_data, f, indent=2)
    
    # logging.info(f"Saved {len(events)} autogrowth events")


def simulate_server(server_num: int, server_type: str, configs: Dict[str, Any], 
                   simulation_period: Dict[str, Any]) -> List[str]:
    """Simulate all databases for a server"""
    
    snapshots_created = []
    database_names = get_database_names_for_server_type(server_type, configs)
    server_state = load_server_state(server_num)
    
    # Calculate period
    simulation_period = calculate_next_simulation_period(server_state)
    
    for database_name in database_names:
        try:
            # Initialize or load database state
            if database_name not in server_state:
                server_state[database_name] = initialize_database_state(
                    server_num, database_name, server_type
                )
                logging.info(f"Initialized new database: Server{server_num}/{database_name}")
            
            db_state = server_state[database_name]
            
            # Generate snapshot
            snapshot = generate_snapshot_data(
                server_num, database_name, server_type, db_state, simulation_period, configs
            )
            
            # Generate autogrowth events
            baseline = configs['baseline_templates']['baseline_templates'][server_type]
            events = generate_raw_autogrowth_events(
                server_num, database_name, db_state, baseline, simulation_period
            )
            
            # Save data
            save_snapshot(server_num, database_name, snapshot)
            save_autogrowth_events(server_num, database_name, events, simulation_period)
            
            snapshots_created.append(f"Server{server_num}/{database_name}")
            # logging.info(f"Processed {database_name}: {snapshot['size']['total_gb']:.3f} GB")
            
        except Exception as e:
            logging.error(f"Error processing {database_name} on Server{server_num}: {e}")
            continue
    
    # Save updated server state
    save_server_state(server_num, server_state)
    
    return snapshots_created


def main():
    """Main execution function"""
    
    auto_sync_if_needed()
    setup_logging()
    
    print("DATABASE GROWTH SIMULATOR")
    print("=" * 50)
    
    try:
        # Load configuration
        config_manager = ConfigurationManager()
        server_count, server_assignments = config_manager.load_and_validate_configuration()
        
        configs = load_configuration_files()
        
        # Show server configuration clearly
        print(f"Configuration: {server_count} servers")
        print("Server Types:")
        type_counts = {}
        for server_type in server_assignments.values():
            type_counts[server_type] = type_counts.get(server_type, 0) + 1
        
        for server_type, count in type_counts.items():
            print(f"  • {server_type.replace('_', ' ').title()}: {count} server{'s' if count > 1 else ''}")
        
        # Calculate next simulation period
        sample_state = load_server_state(1)
        simulation_period = calculate_next_simulation_period(sample_state)
        
        period_start = datetime.fromisoformat(simulation_period['period_start'])
        period_end = datetime.fromisoformat(simulation_period['period_end'])
        
        print(f"\nSimulation Period: {period_start.strftime('%Y-%m-%d %H:%M')} → {period_end.strftime('%H:%M')}")
        print(f"Period Type: {simulation_period['period_type'].title()}")
        print("=" * 50)
        
        # Process all servers
        total_snapshots = []
        
        for server_num in range(1, server_count + 1):
            server_type = server_assignments[server_num]
            
            snapshots = simulate_server(server_num, server_type, configs, simulation_period)
            total_snapshots.extend(snapshots)
            
            print(f"Server{server_num}: {len(snapshots)} databases processed")
        
        # Clean summary
        print("=" * 50)
        print(f"Complete: {len(total_snapshots)} database snapshots generated")
        
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False
        
    except Exception as e:
        logging.error(f"Simulation failed: {e}")
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)