#!/usr/bin/env python3
"""
Unified Database Name Manager
Synchronizes database names between Error Log Simulator and Growth Simulator
Ensures both tools use the same databases for realistic monitoring simulation
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any


class DatabaseNameManager:
    """Manages unified database names across both simulators"""
    
    def __init__(self, growth_config_path='growth_config.json'):
        self.growth_config_path = growth_config_path
        self.logger = logging.getLogger(__name__)
        
        # File paths
        self.fake_db_names_json = Path("data/fake_database_names.json")
        self.error_log_db_names_txt = Path("data/database_names.txt")
        
    def sync_database_names(self) -> bool:
        """Synchronize database names from JSON source to TXT file"""
        
        try:
            # Load the hierarchical JSON (source of truth)
            if not self.fake_db_names_json.exists():
                self.logger.error(f"Source JSON file not found: {self.fake_db_names_json}")
                return False
            
            with open(self.fake_db_names_json, 'r') as f:
                hierarchical_db_names = json.load(f)
            
            # Load growth config to understand server assignments
            server_assignments = self._get_server_assignments()
            
            # Generate flat list based on actual server assignments
            unified_db_list = self._generate_unified_database_list(
                hierarchical_db_names, server_assignments
            )
            
            # Write to TXT file for error log simulator
            self._write_txt_file(unified_db_list)
            
            # Log the synchronization
            self.logger.info(f"✅ Synchronized {len(unified_db_list)} database names")
            self._log_database_distribution(hierarchical_db_names, server_assignments)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to sync database names: {e}")
            return False
    
    def _get_server_assignments(self) -> Dict[int, str]:
        """Get server type assignments from growth config"""
        
        try:
            with open(self.growth_config_path, 'r') as f:
                growth_config = json.load(f)
            
            # Parse server assignments
            server_integration = growth_config.get('server_integration', {})
            assignments_config = server_integration.get('server_type_assignments', {})
            default_type = server_integration.get('unassigned_server_default', 'reference_config')
            
            # Get server count
            server_count = self._get_server_count(growth_config, server_integration)
            
            # Build server assignments
            server_assignments = {}
            
            # Parse range assignments
            for range_spec, server_type in assignments_config.items():
                start_server, end_server = self._parse_server_range(range_spec)
                for server_num in range(start_server, end_server + 1):
                    if server_num <= server_count:
                        server_assignments[server_num] = server_type
            
            # Assign default type to unassigned servers
            for server_num in range(1, server_count + 1):
                if server_num not in server_assignments:
                    server_assignments[server_num] = default_type
            
            return server_assignments
            
        except Exception as e:
            self.logger.error(f"Error reading server assignments: {e}")
            return {1: 'oltp_production', 2: 'reporting_analytics', 3: 'reference_config'}
    
    def _get_server_count(self, growth_config: Dict[str, Any], 
                         server_integration: Dict[str, Any]) -> int:
        """Get server count from various sources"""
        
        # Try to read from error log config
        if server_integration.get('read_from_error_log_config'):
            error_log_config_path = server_integration.get('error_log_config_path', 'config.json')
            
            try:
                if Path(error_log_config_path).exists():
                    with open(error_log_config_path, 'r') as f:
                        error_config = json.load(f)
                    
                    # Check nested structure
                    if 'simulation' in error_config and 'server_count' in error_config['simulation']:
                        return error_config['simulation']['server_count']
                        
            except Exception as e:
                self.logger.warning(f"Could not read error log config: {e}")
        
        # Fallback to default
        return server_integration.get('default_server_count', 3)
    
    def _parse_server_range(self, range_spec: str) -> tuple:
        """Parse server range like 'servers_1_4' to (1, 4)"""
        parts = range_spec.split('_')
        if len(parts) >= 3 and parts[0] == 'servers':
            return int(parts[1]), int(parts[2])
        raise ValueError(f"Invalid range format: {range_spec}")
    
    def _generate_unified_database_list(self, hierarchical_db_names: Dict[str, List[str]], 
                                      server_assignments: Dict[int, str]) -> List[str]:
        """Generate unified database list based on server assignments"""
        
        unified_databases = set()  # Use set to avoid duplicates
        
        # For each server, add databases based on its type
        for server_num, server_type in server_assignments.items():
            
            if server_type in hierarchical_db_names:
                type_databases = hierarchical_db_names[server_type]
                
                # Add all databases of this type
                for db_name in type_databases:
                    unified_databases.add(db_name)
                
                self.logger.debug(f"Server{server_num} ({server_type}): {len(type_databases)} databases")
        
        # Convert to sorted list for consistency
        return sorted(list(unified_databases))
    
    def _write_txt_file(self, database_list: List[str]):
        """Write unified database list to TXT file"""
        
        # Ensure data directory exists
        self.error_log_db_names_txt.parent.mkdir(exist_ok=True)
        
        # Write to TXT file
        with open(self.error_log_db_names_txt, 'w') as f:
            for db_name in database_list:
                f.write(f"{db_name}\n")
        
        self.logger.info(f"📝 Updated {self.error_log_db_names_txt} with {len(database_list)} databases")
    
    def _log_database_distribution(self, hierarchical_db_names: Dict[str, List[str]], 
                                 server_assignments: Dict[int, str]):
        """Log database distribution for verification"""
        
        # Count servers by type
        type_counts = {}
        for server_type in server_assignments.values():
            type_counts[server_type] = type_counts.get(server_type, 0) + 1
        
        self.logger.info("📊 Database Distribution:")
        for server_type, count in type_counts.items():
            db_count = len(hierarchical_db_names.get(server_type, []))
            self.logger.info(f"  {server_type}: {count} servers × {db_count} databases each")
        
        self.logger.info("🖥️ Server Assignments:")
        for server_num, server_type in sorted(server_assignments.items()):
            self.logger.info(f"  Server{server_num}: {server_type}")
    
    def validate_synchronization(self) -> bool:
        """Validate that both files contain consistent database names"""
        
        try:
            # Load JSON file
            with open(self.fake_db_names_json, 'r') as f:
                json_data = json.load(f)
            
            # Load TXT file
            if not self.error_log_db_names_txt.exists():
                self.logger.warning("TXT file doesn't exist yet")
                return False
            
            with open(self.error_log_db_names_txt, 'r') as f:
                txt_databases = set(line.strip() for line in f if line.strip())
            
            # Generate expected databases from JSON
            server_assignments = self._get_server_assignments()
            expected_databases = set(self._generate_unified_database_list(json_data, server_assignments))
            
            # Compare
            missing_in_txt = expected_databases - txt_databases
            extra_in_txt = txt_databases - expected_databases
            
            if missing_in_txt or extra_in_txt:
                self.logger.warning("❌ Synchronization validation failed:")
                if missing_in_txt:
                    self.logger.warning(f"  Missing in TXT: {missing_in_txt}")
                if extra_in_txt:
                    self.logger.warning(f"  Extra in TXT: {extra_in_txt}")
                return False
            
            self.logger.info("✅ Database synchronization validated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    
    def get_databases_for_server(self, server_num: int) -> List[str]:
        """Get specific databases that should exist on a server"""
        
        try:
            # Get server type
            server_assignments = self._get_server_assignments()
            server_type = server_assignments.get(server_num, 'reference_config')
            
            # Load hierarchical database names
            with open(self.fake_db_names_json, 'r') as f:
                hierarchical_db_names = json.load(f)
            
            return hierarchical_db_names.get(server_type, [])
            
        except Exception as e:
            self.logger.error(f"Error getting databases for Server{server_num}: {e}")
            return []
    
    def create_integration_report(self) -> str:
        """Create a report showing the integration between both simulators"""
        
        try:
            server_assignments = self._get_server_assignments()
            
            with open(self.fake_db_names_json, 'r') as f:
                hierarchical_db_names = json.load(f)
            
            report = []
            report.append("=" * 70)
            report.append("DATABASE INTEGRATION REPORT")
            report.append("=" * 70)
            report.append("")
            
            # Server assignments summary
            report.append("SERVER ASSIGNMENTS:")
            for server_num in sorted(server_assignments.keys()):
                server_type = server_assignments[server_num]
                databases = hierarchical_db_names.get(server_type, [])
                report.append(f"  Server{server_num}: {server_type} ({len(databases)} databases)")
            
            report.append("")
            
            # Database distribution
            report.append("DATABASE TYPES:")
            for server_type, databases in hierarchical_db_names.items():
                report.append(f"  {server_type}:")
                for db in databases[:3]:  # Show first 3
                    report.append(f"    - {db}")
                if len(databases) > 3:
                    report.append(f"    ... and {len(databases) - 3} more")
                report.append("")
            
            # Integration status
            report.append("INTEGRATION STATUS:")
            if self.validate_synchronization():
                report.append("  ✅ Database names synchronized")
                report.append("  ✅ Both simulators use same databases")
                report.append("  ✅ Monitoring dashboard will show consistent data")
            else:
                report.append("  ❌ Database names not synchronized")
                report.append("  ❌ Run sync_database_names() to fix")
            
            report.append("")
            report.append("=" * 70)
            
            return "\n".join(report)
            
        except Exception as e:
            return f"Error generating report: {e}"


def auto_sync_if_needed() -> bool:
    """Automatically sync if TXT file is missing or outdated"""
    
    manager = DatabaseNameManager()
    
    # Check if sync is needed
    txt_file = Path("data/database_names.txt")
    json_file = Path("data/fake_database_names.json")
    
    if not txt_file.exists() or not json_file.exists():
        logging.info("🔄 Auto-syncing database names (missing files)")
        return manager.sync_database_names()
    
    # Check if JSON is newer than TXT
    json_time = json_file.stat().st_mtime
    txt_time = txt_file.stat().st_mtime
    
    if json_time > txt_time:
        logging.info("🔄 Auto-syncing database names (JSON updated)")
        return manager.sync_database_names()
    
    # Validate existing sync
    if not manager.validate_synchronization():
        logging.info("🔄 Auto-syncing database names (validation failed)")
        return manager.sync_database_names()
    
    return True


def main():
    """Main function to sync database names"""
    
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    manager = DatabaseNameManager()
    
    print("🔄 Synchronizing Database Names Between Simulators")
    print("=" * 60)
    
    # Perform synchronization
    success = manager.sync_database_names()
    
    if success:
        print("\n📊 Integration Report:")
        print(manager.create_integration_report())
        
        # Validation
        print("\n🔍 Validating synchronization...")
        if manager.validate_synchronization():
            print("✅ SUCCESS: Both simulators now use the same database names!")
            print("🎯 Your monitoring dashboard will show consistent data.")
        else:
            print("❌ Validation failed. Please check the logs.")
    else:
        print("❌ Synchronization failed. Please check the configuration.")


if __name__ == "__main__":
    main()