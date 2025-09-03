"""
Growth Simulation Utility Functions
Generates RAW data for SQL Server databases
This module provides functions to calculate the next simulation period,
generate raw I/O data, size data, table data, and autogrowth events.
It also includes functions to load and save server state from/to JSON files.
It is designed to be used in conjunction with a configuration manager and unified database manager.


Normal databases: 0-5 events x 256-512MB = 0-2.5GB total growth
Problem databases: 10-30 events x 256-512MB = 2.5-15GB total growth
Anomaly database: 180-250 events x 8-32MB = 1.4-8GB total growth (many small increments)

"""

import json
import random
import logging  
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any


def calculate_next_simulation_period(server_state: Dict[str, Any] = None) -> Dict[str, Any]:
    """Calculate the next 12-hour simulation period"""
    
    start_date = datetime(2025, 5, 1, 6, 0, 0)  # Start at 6 AM on may 1, 2025    
    latest_period_end = None
    
    if server_state:
        for db_name, db_state in server_state.items():
            if isinstance(db_state, dict) and 'last_period' in db_state:
                last_period = db_state.get('last_period')
                if last_period and 'period_end' in last_period:
                    try:
                        period_end = datetime.fromisoformat(last_period['period_end'])
                        if latest_period_end is None or period_end > latest_period_end:
                            latest_period_end = period_end
                    except (ValueError, TypeError):
                        continue
    
    if latest_period_end:
        if latest_period_end.hour == 6:  # 6 AM -> 6 PM
            period_start = latest_period_end
            period_end = latest_period_end.replace(hour=18)
            period_type = "day"
        else:  # 6 PM -> 6 AM next day
            period_start = latest_period_end
            period_end = (latest_period_end + timedelta(days=1)).replace(hour=6)
            period_type = "night"
    else:
        # First execution
        period_start = start_date
        period_end = start_date.replace(hour=18)
        period_type = "day"
    
    return {
        'period_start': period_start.isoformat(),
        'period_end': period_end.isoformat(),
        'period_type': period_type
    }


def generate_raw_io_data(db_state: Dict[str, Any], baseline: Dict[str, Any], 
                         period: Dict[str, Any], server_type: str) -> Dict[str, Any]:
    """Generate RAW I/O data - just counts, no calculations"""
    
    io_baselines = baseline['io_baselines']
    period_type = period['period_type']
    
    # Base operations per second
    reads_per_sec = io_baselines['reads_per_second_baseline']
    writes_per_sec = io_baselines['writes_per_second_baseline']
    
    # Apply time patterns (day vs night)
    if period_type == "day":
        if server_type == 'oltp_production':
            reads_per_sec *= 2.8  # More activity during day
            writes_per_sec *= 2.8
        elif server_type == 'reporting_analytics':
            reads_per_sec *= 0.8
            writes_per_sec *= 0.8
        else:  # reference
            reads_per_sec *= 3.0
            writes_per_sec *= 1.2
    else:  # night
        if server_type == 'oltp_production':
            reads_per_sec *= 0.4  # Less activity at night
            writes_per_sec *= 0.4
        elif server_type == 'reporting_analytics':
            reads_per_sec *= 0.5
            writes_per_sec *= 4.0  # Batch jobs at night
        else:  # reference
            reads_per_sec *= 0.8
            writes_per_sec *= 0.8
    
    # Calculate for 12 hours with some randomness
    total_seconds = 12 * 3600
    reads = int(reads_per_sec * total_seconds * random.uniform(0.85, 1.15))
    writes = int(writes_per_sec * total_seconds * random.uniform(0.85, 1.15))
    
    # Convert to GB (8KB per operation)
    read_gb = round(reads * 8192 / (1024**3), 3)
    write_gb = round(writes * 8192 / (1024**3), 3)
    
    # Update cumulative
    db_state['cumulative_reads'] = db_state.get('cumulative_reads', 0) + reads
    db_state['cumulative_writes'] = db_state.get('cumulative_writes', 0) + writes
    
    return {
        'reads': reads,
        'writes': writes,
        'read_gb': read_gb,
        'write_gb': write_gb,
        'cumulative_reads': db_state['cumulative_reads'],
        'cumulative_writes': db_state['cumulative_writes']
    }


def generate_raw_size_data(db_state: Dict[str, Any], baseline: Dict[str, Any],
                           period: Dict[str, Any], server_type: str) -> Dict[str, Any]:
    """Generate RAW size data based on growth pattern"""
    
    current_size = db_state.get('current_size_gb', 50.0)
    growth_pattern = db_state.get('growth_pattern', 'stable')  # Get the pattern
    period_type = period['period_type']
    
    # Pattern-based growth
    if growth_pattern == 'stable':
        # Normal OLTP - cleanup works, minimal net growth
        growth = random.uniform(-0.5, 0.5)
        
    elif growth_pattern == 'no_retention':
        # Server1/TransactionLog_DB - steady growth, no cleanup
        growth = random.uniform(0.5, 2.0)
        
    elif growth_pattern == 'growing_fast':
        # Server2/PrimaryStore_DB - autogrowth issues cause faster growth
        growth = random.uniform(2.0, 5.0)
        
    elif growth_pattern == 'broken_cleanup':
        # Server2/CustomerCore_DB - cleanup fails occasionally
        if random.random() < 0.8:  # 80% of time, grows
            growth = random.uniform(1.0, 3.0)
        else:  # 20% partial cleanup
            growth = random.uniform(-1.0, 0.5)
            
    elif growth_pattern == 'archive_failure':
        # Server3/DataWarehouse_DB - archive job broken, accumulating
        if period_type == 'night':  # ETL loads at night
            growth = random.uniform(5.0, 15.0)
        else:
            growth = random.uniform(0.5, 2.0)  # Slow growth during day
            
    elif growth_pattern == 'etl_cycle':
        # Normal reporting - loads and archives balance
        if period_type == 'night' and random.random() < 0.3:
            growth = random.uniform(5.0, 10.0)  # ETL load
        elif random.random() < 0.2:
            growth = random.uniform(-8.0, -3.0)  # Archive
        else:
            growth = random.uniform(-0.5, 0.5)
            
    elif growth_pattern == 'static':
        # Reference DBs - no growth
        growth = random.uniform(-0.01, 0.01)
    
    else:
        # Fallback - shouldn't happen but just in case
        growth = random.uniform(-0.5, 0.5)
    
    # Apply some variance for realism
    growth *= random.uniform(0.9, 1.1)
    
    new_size = current_size + growth
    
    # Ensure minimum size
    min_size = 10.0 if server_type != 'reference_config' else 5.0
    new_size = max(min_size, new_size)
    
    # Split between data and log
    if server_type == 'oltp_production':
        data_ratio = 0.75
    elif server_type == 'reporting_analytics':
        data_ratio = 0.90
    else:
        data_ratio = 0.95
    
    return {
        'total_gb': round(new_size, 3),
        'data_file_gb': round(new_size * data_ratio, 3),
        'log_file_gb': round(new_size * (1 - data_ratio), 3),
        'file_count': 2 if new_size < 50 else 4
    }

def generate_raw_table_data(db_state: Dict[str, Any], server_type: str, 
                            period: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate REALISTIC table data with varied cleanup patterns"""
    
    tables_data = []
    period_type = period['period_type']
    growth_pattern = db_state.get('growth_pattern', 'stable')
    
    # Initialize table cleanup history if not exists
    if 'table_cleanup_history' not in db_state:
        db_state['table_cleanup_history'] = {}
    
    for table_name, table_state in db_state['tables'].items():
        # Initialize cleanup history for this table
        if table_name not in db_state['table_cleanup_history']:
            db_state['table_cleanup_history'][table_name] = {
                'last_cleanup_success': True,
                'failed_cleanups': 0,
                'cleanup_schedule': (table_name, server_type),
                'periods_since_cleanup': 0
            }
        
        cleanup_history = db_state['table_cleanup_history'][table_name]
        
        # Calculate base growth
        daily_growth = table_state['daily_growth']
        period_growth = int(daily_growth * 0.5)  # Half day
        
        # Apply period pattern
        if server_type == 'oltp_production':
            period_growth = int(period_growth * (1.5 if period_type == "day" else 0.3))
        elif server_type == 'reporting_analytics':
            period_growth = int(period_growth * (0.5 if period_type == "day" else 2.0))
        
        # Add realistic variance
        period_growth = int(period_growth * random.uniform(0.7, 1.4))
        
        # Determine cleanup behavior based on table type and history
        rows_deleted = 0
        cleanup_occurred = False
        cleanup_history['periods_since_cleanup'] += 1
        
        # Realistic cleanup logic
        if table_state.get('has_cleanup', False):
            cleanup_occurred, rows_deleted = _calculate_realistic_cleanup(
                table_name, table_state, cleanup_history, period_growth, 
                growth_pattern, period_type
            )
            
            if cleanup_occurred:
                cleanup_history['periods_since_cleanup'] = 0
                cleanup_history['last_cleanup_success'] = rows_deleted > 0
                cleanup_history['failed_cleanups'] = 0 if rows_deleted > 0 else cleanup_history['failed_cleanups'] + 1
        
        # Ensure minimum row count
        new_rows = max(1000, table_state['rows'] + period_growth - rows_deleted)
        
        # Calculate size
        size_gb = round((new_rows * table_state['avg_row_bytes']) / (1024**3), 3)
        
        tables_data.append({
            'name': table_name,
            'rows': new_rows,
            'size_gb': size_gb,
            'rows_added': period_growth,
            'rows_deleted': rows_deleted,
            'avg_row_bytes': table_state['avg_row_bytes'],
            'has_cleanup': table_state.get('has_cleanup', False),
            'cleanup_occurred': cleanup_occurred
        })
        
        # Update table state
        table_state['rows'] = new_rows
    
    return tables_data


def _determine_cleanup_schedule(table_name: str, server_type: str) -> str:
    """Determine cleanup schedule based on table name patterns"""
    table_lower = table_name.lower()
    
    if any(keyword in table_lower for keyword in ['log', 'audit', 'history']):
        return random.choice(['daily', 'weekly', 'monthly', 'never'])
    elif any(keyword in table_lower for keyword in ['temp', 'staging', 'cache']):
        return random.choice(['hourly', 'daily'])
    elif any(keyword in table_lower for keyword in ['transaction', 'order', 'payment']):
        return random.choice(['weekly', 'monthly', 'quarterly'])
    else:
        # Regular business tables
        return random.choice(['weekly', 'monthly', 'never'])


def _calculate_realistic_cleanup(table_name: str, table_state: Dict[str, Any], 
                               cleanup_history: Dict[str, Any], period_growth: int,
                               growth_pattern: str, period_type: str) -> tuple:
    """Calculate realistic cleanup with more frequent occurrence"""
    
    # Much simpler logic - cleanup happens more frequently
    cleanup_probability = 0.75  # Base 75% chance each period for tables with cleanup
    
    # Adjust based on growth pattern
    if growth_pattern == 'broken_cleanup':
        cleanup_probability = 0.2  # Still broken, less frequent
    elif growth_pattern == 'no_retention':
        cleanup_probability = 0.05  # Almost never
    elif growth_pattern == 'stable':
        cleanup_probability = 0.7  # More aggressive for stable DBs
    
    # Check if cleanup runs
    if random.random() > cleanup_probability:
        return False, 0
    
    # Cleanup runs - determine effectiveness
    current_rows = table_state['rows']
    
    # Calculate how much to delete based on table type
    if 'log' in table_name.lower() or 'audit' in table_name.lower():
        base_cleanup_pct = random.uniform(0.20, 0.50)  # 20-50% for logs
    elif 'temp' in table_name.lower() or 'staging' in table_name.lower():
        base_cleanup_pct = random.uniform(0.70, 0.95)  # 70-95% for temp
    else:
        base_cleanup_pct = random.uniform(0.05, 0.25)  # 5-25% for regular
    
    rows_to_delete = int(current_rows * base_cleanup_pct)
    
    # Sometimes cleanup deletes more than was added (shrinking pattern)
    if random.random() < 0.15:  # 15% chance of aggressive cleanup
        rows_to_delete = int(period_growth * random.uniform(1.2, 2.0))
    # Sometimes exactly matches growth (static pattern)  
    elif random.random() < 0.10:  # 10% chance of perfect balance
        rows_to_delete = period_growth
    # Usually deletes less than growth (managed growth)
    else:
        rows_to_delete = int(period_growth * random.uniform(0.3, 0.9))
    
    # Ensure reasonable bounds
    max_deletion = int(current_rows * 0.5)  # Never delete more than 50%
    rows_to_delete = min(rows_to_delete, max_deletion)
    rows_to_delete = max(0, rows_to_delete)  # Can't delete negative rows
    
    return True, rows_to_delete

def generate_raw_autogrowth_events(server_num: int, database_name: str,
                                   db_state: Dict[str, Any], baseline: Dict[str, Any],
                                   period: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate RAW autogrowth events - correlated with growth pattern"""
    
    events = []
    growth_pattern = db_state.get('growth_pattern', 'stable')
    
    # Check for anomaly database (Server2/PrimaryStore_DB)
    is_anomaly = (server_num == 2 and database_name == "PrimaryStore_DB")
    
    # Determine number of events based on growth pattern
    if is_anomaly:
        num_events = random.randint(180, 250)  # Keep high for anomaly
    elif growth_pattern in ['growing_fast', 'no_retention', 'broken_cleanup']:
        # Problem databases have more autogrowth events
        num_events = random.randint(10, 30)
    elif growth_pattern == 'stable':
        # Stable databases have few events
        num_events = random.randint(0, 5)
    elif growth_pattern == 'archive_failure':
        # Archive failures cause periodic bursts
        num_events = random.randint(5, 20)
    elif growth_pattern == 'etl_cycle':
        # ETL databases have moderate events during loads
        period_type = period['period_type']
        if period_type == 'night':
            num_events = random.randint(20, 40)  # More during ETL
        else:
            num_events = random.randint(5, 15)
    elif growth_pattern == 'static':
        # Reference databases - almost no events
        num_events = random.randint(0, 3)
    else:
        # Default fallback
        server_type = db_state.get('server_type', 'oltp_production')
        if server_type == 'reference_config':
            num_events = random.randint(0, 5)
        elif server_type == 'reporting_analytics':
            num_events = random.randint(15, 30)
        else:  # oltp
            num_events = random.randint(30, 60)
    
    # Generate events
    period_start = datetime.fromisoformat(period['period_start'])
    period_end = datetime.fromisoformat(period['period_end'])
    total_seconds = (period_end - period_start).total_seconds()
    
    for _ in range(num_events):
        # Random time within period
        offset = random.uniform(0, total_seconds)
        event_time = period_start + timedelta(seconds=offset)
        
        # File type
        file_type = "data" if random.random() < 0.7 else "log"
        
        # Increment size
        if is_anomaly:
            increment_mb = random.choice([8, 16, 32]) if random.random() < 0.8 else random.choice([64, 128])
        else:
            if file_type == "data":
                increment_mb = random.choice([256, 512])  # Standard increments only
            else:
                increment_mb = random.choice([64, 128])  # Log files smaller
        
        # Base file size
        if file_type == "data":
            previous_mb = random.randint(5000, 20000)  # 5-20GB data files
        else:
            previous_mb = random.randint(500, 2000)  # 0.5-2GB log files
        
        # Duration based on increment
        if increment_mb <= 32:
            duration_ms = random.randint(100, 500)
        elif increment_mb <= 128:
            duration_ms = random.randint(300, 1000)
        else:
            duration_ms = random.randint(800, 2000)
        
        # Blocking
        blocking = random.random() < 0.7
        
        events.append({
            'timestamp': event_time.isoformat(),
            'file_type': file_type,
            'previous_mb': previous_mb,
            'increment_mb': increment_mb,
            'new_mb': previous_mb + increment_mb,
            'duration_ms': duration_ms,
            'blocking': blocking,
            'io_wait_ms': random.randint(50, 500),
            'blocked_processes': random.randint(1, 10) if blocking else 0
        })
    
    # Sort by timestamp
    events.sort(key=lambda x: x['timestamp'])
    
    return events

def load_server_state(server_num: int) -> Dict[str, Any]:
    """Load server state from file"""
    state_file = Path(f"Server{server_num}/growth_data/server_state.json")
    
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            # Remove metadata if present
            if 'metadata' in state:
                del state['metadata']

            # was used for debugging
            # logging.info(f"Loaded state for Server{server_num}")
            return state
        except Exception as e:
            logging.warning(f"Error loading state for Server{server_num}: {e}")
            return {}
    else:
        return {}


def save_server_state(server_num: int, server_state: Dict[str, Any]):
    """Save server state to file"""
    state_dir = Path(f"Server{server_num}/growth_data")
    state_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(state_dir / "server_state.json", 'w') as f:
            json.dump(server_state, f, indent=2)

        # was used for debugging
        # logging.info(f"Saved state for Server{server_num}")
    except IOError as e:
        logging.error(f"Error saving state for Server{server_num}: {e}")