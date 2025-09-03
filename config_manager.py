"""
Configuration Manager for Database Growth Simulator
Simplified for 12-hour simulation periods (6 AM and 6 PM daily)
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List


class ConfigurationManager:
    def __init__(self, growth_config_path='growth_config.json'):
        """Initialize configuration manager"""
        self.growth_config_path = growth_config_path
        self.logger = logging.getLogger(__name__)
        
        # Load growth configuration
        with open(growth_config_path, 'r') as f:
            self.growth_config = json.load(f)
        
        self.server_count = None
        self.server_type_assignments = {}
        
    def load_and_validate_configuration(self) -> Tuple[int, Dict[int, str]]:
        """Load configurations and return server count and type assignments"""
        
        # Get server count from error log config or use default
        self.server_count = self._get_server_count()
        
        # Get server type assignments
        self.server_type_assignments = self._get_server_type_assignments()
        
        # Validate assignments
        self._validate_server_assignments()
        
        self.logger.info(f"Configuration loaded: {self.server_count} servers")
        # used to log assignments, but too verbose
        # self.logger.info(f"Server type assignments: {self.server_type_assignments}")
        
        return self.server_count, self.server_type_assignments
    
    def _get_server_count(self) -> int:
        """Get server count from error log config or use default"""
        
        integration_config = self.growth_config['server_integration']
        
        if integration_config['read_from_error_log_config']:
            error_log_config_path = integration_config['error_log_config_path']
            
            try:
                if Path(error_log_config_path).exists():
                    with open(error_log_config_path, 'r') as f:
                        error_log_config = json.load(f)
                    
                    # Try different possible keys for server count
                    server_count_keys = [
                        'server_count', 
                        'simulation.server_count',
                        'servers',
                        'total_servers'
                    ]
                    
                    for key in server_count_keys:
                        if '.' in key:
                            # Handle nested keys like 'simulation.server_count'
                            parts = key.split('.')
                            value = error_log_config
                            try:
                                for part in parts:
                                    value = value[part]
                                if isinstance(value, int) and value > 0:
                                    self.logger.info(f"Found server count in error log config: {value}")
                                    return value
                            except (KeyError, TypeError):
                                continue
                        else:
                            # Handle direct keys
                            if key in error_log_config:
                                value = error_log_config[key]
                                if isinstance(value, int) and value > 0:
                                    self.logger.info(f"Found server count in error log config: {value}")
                                    return value
                    
                    self.logger.warning("No valid server count found in error log config, using default")
                
                else:
                    self.logger.warning(f"Error log config file not found: {error_log_config_path}")
            
            except Exception as e:
                self.logger.error(f"Error reading error log config: {e}")
        
        # Use default server count
        default_count = integration_config['default_server_count']
        self.logger.info(f"Using default server count: {default_count}")
        return default_count
    
    def _get_server_type_assignments(self) -> Dict[int, str]:
        """Parse server type assignments from configuration"""
        
        assignments_config = self.growth_config['server_integration']['server_type_assignments']
        default_type = self.growth_config['server_integration']['unassigned_server_default']
        
        server_types = {}
        
        # Parse range assignments like "servers_1_4": "oltp_production"
        for range_spec, server_type in assignments_config.items():
            start_server, end_server = self._parse_server_range(range_spec)
            
            for server_num in range(start_server, end_server + 1):
                if server_num <= self.server_count:
                    server_types[server_num] = server_type
                else:
                    self.logger.warning(f"Server assignment {server_num} exceeds server count {self.server_count}")
        
        # Assign default type to unassigned servers
        for server_num in range(1, self.server_count + 1):
            if server_num not in server_types:
                server_types[server_num] = default_type
                self.logger.info(f"Server{server_num} assigned default type: {default_type}")
        
        return server_types
    
    def _parse_server_range(self, range_spec: str) -> Tuple[int, int]:
        """Parse server range specification like 'servers_1_4' to (1, 4)"""
        
        try:
            # Expected format: "servers_X_Y" where X and Y are numbers
            parts = range_spec.split('_')
            
            if len(parts) >= 3 and parts[0] == 'servers':
                start_server = int(parts[1])
                end_server = int(parts[2])
                
                if start_server <= end_server:
                    return start_server, end_server
                else:
                    raise ValueError(f"Invalid range: start ({start_server}) > end ({end_server})")
            else:
                raise ValueError(f"Invalid range format: {range_spec}")
        
        except (ValueError, IndexError) as e:
            self.logger.error(f"Error parsing server range '{range_spec}': {e}")
            raise ValueError(f"Invalid server range specification: {range_spec}")
    
    def _validate_server_assignments(self):
        """Validate server assignments"""
        
        # Check that all server numbers are valid
        for server_num in self.server_type_assignments.keys():
            if server_num < 1 or server_num > self.server_count:
                raise ValueError(f"Invalid server number: {server_num} (valid range: 1-{self.server_count})")
        
        # Check that all assigned server types are valid
        valid_types = ['oltp_production', 'reporting_analytics', 'reference_config']
        for server_num, server_type in self.server_type_assignments.items():
            if server_type not in valid_types:
                raise ValueError(f"Invalid server type '{server_type}' for Server{server_num}")
        
        # Log assignment summary
        type_counts = {}
        for server_type in self.server_type_assignments.values():
            type_counts[server_type] = type_counts.get(server_type, 0) + 1
        
        self.logger.info("Server type distribution:")
        for server_type, count in type_counts.items():
            self.logger.info(f"  {server_type}: {count} servers")
    
    def get_database_configuration(self, server_type: str) -> Dict[str, Any]:
        """Get database configuration for a specific server type"""
        return self.growth_config['database_configuration'][server_type]
    
    def get_execution_schedule(self) -> Dict[str, Any]:
        """Get execution schedule configuration"""
        return self.growth_config['execution_schedule']
    
    def get_execution_settings(self) -> Dict[str, Any]:
        """Get execution settings"""
        return self.growth_config['execution_settings']
    
    def get_output_configuration(self) -> Dict[str, Any]:
        """Get output configuration"""
        return self.growth_config['output_configuration']
    
    def get_servers_by_type(self, server_type: str) -> List[int]:
        """Get list of server numbers for a specific server type"""
        return [
            server_num for server_num, assigned_type 
            in self.server_type_assignments.items() 
            if assigned_type == server_type
        ]
    
    def create_server_directories(self):
        """Create server directories if they don't exist"""
        for server_num in range(1, self.server_count + 1):
            server_dir = Path(f"Server{server_num}")
            server_dir.mkdir(exist_ok=True)
            
            growth_dir = server_dir / "growth_data"
            growth_dir.mkdir(exist_ok=True)
            
            (growth_dir / "snapshots").mkdir(exist_ok=True)
            (growth_dir / "autogrowth_events").mkdir(exist_ok=True)
        
        self.logger.info(f"Created directories for {self.server_count} servers")


class SimulationScheduler:
    """Simple scheduler for 12-hour simulation periods"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.simulation_mode = config.get('simulation_mode', {})
        self.start_date = datetime.fromisoformat(
            self.simulation_mode.get('schedule_start_date', '2025-05-01')
        ).replace(hour=6, minute=0, second=0, microsecond=0)
    
    def get_next_simulation_period(self, current_state: Dict[str, Any] = None) -> Dict[str, Any]:
        """Get the next 12-hour simulation period"""
        
        if current_state and 'last_simulation_period' in current_state:
            # Continue from last period
            last_period = current_state['last_simulation_period']
            last_end = datetime.fromisoformat(last_period['period_end'])
            
            # Next period starts where last one ended
            if last_end.hour == 6:  # Last ended at 6 AM, next is 6 AM to 6 PM
                period_start = last_end
                period_end = last_end.replace(hour=18)
                period_type = "day"
            else:  # Last ended at 6 PM, next is 6 PM to 6 AM next day
                period_start = last_end
                period_end = (last_end + timedelta(days=1)).replace(hour=6)
                period_type = "night"
        else:
            # First execution - start from configured start date
            period_start = self.start_date
            period_end = self.start_date.replace(hour=18)
            period_type = "day"
        
        return {
            'period_start': period_start.isoformat(),
            'period_end': period_end.isoformat(),
            'period_type': period_type,
            'duration_hours': 12.0,
            'period_label': f"{period_start.strftime('%Y-%m-%d')} {period_start.strftime('%H:%M')}-{period_end.strftime('%H:%M')}"
        }
    
    def validate_configuration_files(self) -> bool:
        """Validate that all required configuration files exist"""
        
        required_files = [
            'growth_config.json',
            'baseline_templates.json',
            'table_patterns.json'
        ]
        
        missing_files = []
        
        for file_path in required_files:
            if not Path(file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            print("Missing configuration files:")
            for file_path in missing_files:
                print(f"  - {file_path}")
            return False
        
        print("All required configuration files are present")
        return True