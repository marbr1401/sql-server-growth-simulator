#!/usr/bin/env python3
"""
Enhanced Setup Script for SQL Server Database Growth Simulation
Creates directory structure, validates configuration, and initializes the project
"""

import os
import json
from pathlib import Path
from datetime import datetime


def create_directory_structure():
    """Create the basic directory structure for the project"""
    
    directories = [
        "data",
        "logs", 
        "backups"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Created directory: {directory}")


def create_server_directories_from_config():
    """Create server directories based on configuration"""
    
    server_count = 10  # Default
    
    try:
        # Try to read from growth config first
        if Path('growth_config.json').exists():
            with open('growth_config.json', 'r') as f:
                growth_config = json.load(f)
            
            integration_config = growth_config.get('server_integration', {})
            
            if integration_config.get('read_from_error_log_config'):
                error_log_config_path = integration_config.get('error_log_config_path', 'config.json')
                
                # Try to read from error log config
                if Path(error_log_config_path).exists():
                    with open(error_log_config_path, 'r') as f:
                        error_log_config = json.load(f)
                    
                    # Try different possible keys for server count
                    server_count_keys = ['server_count', 'simulation.server_count', 'servers', 'total_servers']
                    
                    for key in server_count_keys:
                        if '.' in key:
                            parts = key.split('.')
                            value = error_log_config
                            try:
                                for part in parts:
                                    value = value[part]
                                if isinstance(value, int) and value > 0:
                                    server_count = value
                                    print(f"📖 Found server count in error log config: {server_count}")
                                    break
                            except (KeyError, TypeError):
                                continue
                        else:
                            if key in error_log_config and isinstance(error_log_config[key], int):
                                server_count = error_log_config[key]
                                print(f"📖 Found server count in error log config: {server_count}")
                                break
                else:
                    server_count = integration_config.get('default_server_count', 10)
                    print(f"⚠️ Error log config not found, using default: {server_count}")
            else:
                server_count = integration_config.get('default_server_count', 10)
                print(f"📖 Using default server count from growth config: {server_count}")
        
    except Exception as e:
        print(f"⚠️ Error reading configuration, using default server count (10): {e}")
        server_count = 10
    
    # Create server directories
    for server_num in range(1, server_count + 1):
        server_dir = Path(f"Server{server_num}")
        server_dir.mkdir(exist_ok=True)
        
        # Create growth_data subdirectories
        growth_dir = server_dir / "growth_data"
        growth_dir.mkdir(exist_ok=True)
        
        (growth_dir / "snapshots").mkdir(exist_ok=True)
        (growth_dir / "trends").mkdir(exist_ok=True)
        
        print(f"✓ Created server directory: Server{server_num}")
    
    return server_count


def create_sample_error_log_config():
    """Create sample error log config if it doesn't exist"""
    
    error_log_config_path = Path("config.json")
    
    if not error_log_config_path.exists():
        sample_config = {
            "simulation": {
                "server_count": 10,
                "log_interval_seconds": 5,
                "log_interval_variation": 2,
                "max_runtime_minutes": 0,
                "timezone_offset": "+03:00"
            },
            "error_types": {
                "startup": {"weight": 5, "enabled": True},
                "deadlock": {"weight": 15, "enabled": True},
                "login_failed": {"weight": 25, "enabled": True},
                "timeout": {"weight": 20, "enabled": True},
                "io_error": {"weight": 10, "enabled": True}
            },
            "output": {
                "encoding": "utf-16le",
                "log_rotation": {"enabled": False, "max_size_mb": 10, "max_files": 5}
            }
        }
        
        with open(error_log_config_path, 'w') as f:
            json.dump(sample_config, f, indent=2)
        
        print(f"✓ Created sample error log config: {error_log_config_path}")
    else:
        print(f"✓ Error log config already exists: {error_log_config_path}")


def create_sample_error_logs(server_count):
    """Create sample ERRORLOG files to simulate existing error log simulator"""
    
    sample_error_entries = [
        "2025-08-02 08:49:47.02 Server     Microsoft SQL Server 2022 (RTM-GDR) (KB5058712) - 16.0.1140.6 (X64)",
        "2025-08-02 08:49:47.05 Server     UTC adjustment: 3:00",
        "2025-08-02 08:49:47.06 Server     Server process ID is 6256.",
        "2025-08-02 08:49:52.34 spid42s    SQL Server is now ready for client connections.",
        "2025-08-02 08:54:30.34 spid35s    Deadlock encountered .... Printing deadlock information"
    ]
    
    for server_num in range(1, server_count + 1):
        server_dir = Path(f"Server{server_num}")
        server_dir.mkdir(exist_ok=True)
        
        errorlog_file = server_dir / "ERRORLOG"
        
        if not errorlog_file.exists():
            with open(errorlog_file, 'w', encoding='utf-16le') as f:
                for entry in sample_error_entries:
                    f.write(entry + '\n')
            
            print(f"✓ Created sample ERRORLOG for Server{server_num}")


def validate_configuration_files():
    """Validate that all required configuration files exist and are valid"""
    
    required_files = [
        'growth_config.json',
        'baseline_templates.json',
        'table_patterns.json',
        'data/fake_database_names.json'
    ]
    
    missing_files = []
    invalid_files = []
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
        else:
            # Validate JSON syntax
            try:
                with open(file_path, 'r') as f:
                    json.load(f)
                print(f"✓ Valid configuration file: {file_path}")
            except json.JSONDecodeError as e:
                invalid_files.append((file_path, str(e)))
                print(f"❌ Invalid JSON in {file_path}: {e}")
    
    if missing_files:
        print(f"\n❌ Missing configuration files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        return False
    
    if invalid_files:
        print(f"\n❌ Invalid configuration files:")
        for file_path, error in invalid_files:
            print(f"   - {file_path}: {error}")
        return False
    
    print(f"\n✅ All configuration files are present and valid")
    return True


def validate_server_type_assignments():
    """Validate server type assignments in growth configuration"""
    
    try:
        with open('growth_config.json', 'r') as f:
            config = json.load(f)
        
        server_integration = config.get('server_integration', {})
        assignments = server_integration.get('server_type_assignments', {})
        
        if not assignments:
            print("⚠️ No server type assignments found in configuration")
            return True
        
        print(f"\n📋 Server Type Assignments:")
        
        total_assigned_servers = 0
        for range_spec, server_type in assignments.items():
            try:
                # Parse range like "servers_1_4"
                parts = range_spec.split('_')
                if len(parts) >= 3 and parts[0] == 'servers':
                    start_server = int(parts[1])
                    end_server = int(parts[2])
                    
                    server_count = end_server - start_server + 1
                    total_assigned_servers += server_count
                    
                    print(f"   {range_spec}: {server_type} ({server_count} servers)")
                else:
                    print(f"   ⚠️ Invalid range format: {range_spec}")
                    
            except (ValueError, IndexError) as e:
                print(f"   ❌ Error parsing range {range_spec}: {e}")
                return False
        
        print(f"   Total assigned servers: {total_assigned_servers}")
        return True
        
    except Exception as e:
        print(f"❌ Error validating server assignments: {e}")
        return False


def test_configuration_integration():
    """Test configuration integration between error log and growth configs"""
    
    print(f"\n🔧 Testing Configuration Integration:")
    
    try:
        from config_manager import ConfigurationManager
        
        config_manager = ConfigurationManager()
        server_count, server_assignments = config_manager.load_and_validate_configuration()
        
        print(f"✅ Configuration integration successful")
        print(f"   Total servers: {server_count}")
        print(f"   Server assignments: {len(server_assignments)} servers assigned")
        
        # Display assignment summary
        assignment_summary = {}
        for server_type in server_assignments.values():
            assignment_summary[server_type] = assignment_summary.get(server_type, 0) + 1
        
        print(f"   Assignment summary:")
        for server_type, count in assignment_summary.items():
            print(f"     - {server_type}: {count} servers")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration integration test failed: {e}")
        return False


def create_readme():
    """Create enhanced README file with usage instructions"""
    
    readme_content = """# SQL Server Database Growth Simulation

## Overview
Enhanced database growth simulator that integrates with existing error log simulator configuration and supports flexible server assignments and execution schedules.

## Features
- **Flexible Server Configuration**: Reads existing error log simulator config or uses configurable defaults
- **Configurable Server Types**: Assign any servers to OLTP, Reporting, or Reference types
- **Flexible Execution Schedule**: Hourly, daily, twice-daily, weekly, or custom execution patterns
- **Realistic Table Growth Patterns**: Simulates retention policy issues and cleanup effectiveness
- **Comprehensive Output**: JSON snapshots and trend analysis for monitoring tool integration

## Quick Start

1. **Setup Project**
   ```bash
   python setup_project.py
   ```

2. **Review Configuration**
   - Edit `growth_config.json` for server assignments and execution schedule
   - Edit `baseline_templates.json` for server performance baselines
   - Edit `table_patterns.json` for table growth and retention patterns

3. **Run Simulation**
   ```bash
   python growth_simulator.py
   ```

4. **Check Output**
   - Snapshots: `Server*/growth_data/snapshots/`
   - Trends: `Server*/growth_data/trends/`

## Configuration

### Server Type Assignment
Configure which servers are assigned to which types in `growth_config.json`:

```json
{
  "server_integration": {
    "server_type_assignments": {
      "servers_1_4": "oltp_production",      // Servers 1-4 are OLTP
      "servers_5_7": "reporting_analytics",  // Servers 5-7 are Reporting
      "servers_8_10": "reference_config"     // Servers 8-10 are Reference
    }
  }
}
```

### Execution Schedule
Configure how often the simulation runs:

```json
{
  "execution_schedule": {
    "frequency": "twice_daily",           // Options: hourly, daily, twice_daily, weekly, custom_interval, custom_times
    "execution_times": ["06:00", "18:00"], // Specific times for daily/twice_daily
    "timezone": "UTC+03:00"
  }
}
```

## Server Types

### OLTP Production Servers
- **Characteristics**: Heavy transactional workload, retention policy issues
- **Databases**: 3 per server (PrimaryStore_DB, CustomerCore_DB, OrderProcessing_DB)
- **Tables**: 50-100 per database
- **Retention Patterns**: 20% no retention, 50% good cleanup, 30% insufficient cleanup

### Reporting/Analytics Servers  
- **Characteristics**: Batch processing, data lifecycle management
- **Databases**: 2 per server (ReportWarehouse_DB, BusinessMetrics_DB)
- **Tables**: 20-50 per database
- **Retention Patterns**: 15% no retention, 65% good cleanup, 20% insufficient cleanup

### Reference/Configuration Servers
- **Characteristics**: High read activity, minimal growth, no cleanup required
- **Databases**: 2 per server (ConfigRegistry_DB, Dictionary_Lookup)
- **Tables**: 10-30 per database
- **Patterns**: Reference and lookup tables only, no retention policies needed

## Output Format

### Snapshots
Each execution creates detailed snapshots with:
- Database size metrics (total, data, log files)
- I/O statistics (reads, writes, efficiency ratios)
- Table-level metrics (row counts, sizes, retention patterns)
- Index usage statistics
- Retention effectiveness analysis

### Trends
Long-term trend files track:
- Database size growth over time
- I/O performance patterns
- Table growth trajectories
- Retention policy effectiveness

## Integration with Error Log Simulator

This growth simulator is designed to work alongside the existing error log simulator:
- **Independent execution**: Runs separately with own schedule
- **Shared server structure**: Uses same Server1/, Server2/, etc. directories
- **Coordinated patterns**: Growth patterns match server types for realistic environments
- **Configuration integration**: Reads existing error log config for server count

## Troubleshooting

### Common Issues
1. **Missing configuration files**: Run `python setup_project.py` first
2. **Invalid server assignments**: Check server ranges don't exceed actual server count
3. **Permission errors**: Ensure write access to Server directories
4. **Configuration integration errors**: Verify error log config format

### Log Files
- Check `growth_simulator.log` for detailed execution logs
- Review configuration validation output during setup

## Development and Testing

### Test Configuration
```bash
python -c "from config_manager import ConfigurationManager; cm = ConfigurationManager(); print(cm.load_and_validate_configuration())"
```

### Run with Different Configurations
```bash
# Development environment
cp growth_config.json growth_config_dev.json
# Edit growth_config_dev.json with development settings
python growth_simulator.py  # Will use growth_config.json by default
```

## Architecture

The enhanced simulator uses a modular architecture:
- **config_manager.py**: Configuration integration and validation
- **growth_simulator.py**: Main simulation engine with flexible server support
- **growth_utils.py**: Calculation utilities for growth metrics
- **baseline_templates.json**: Server performance baselines
- **table_patterns.json**: Table growth and retention patterns

This design ensures maximum flexibility while maintaining realistic simulation of SQL Server environments.
"""
    
    with open('README.md', 'w') as f:
        f.write(readme_content)
    
    print("✓ Created enhanced README.md")


def initialize_enhanced_project():
    """Initialize the complete enhanced project structure"""
    
    print("Enhanced SQL Server Database Growth Simulation - Setup")
    print("=" * 70)
    print(f"Setup started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Create basic directory structure
    print(f"\n📁 1. Creating directory structure...")
    create_directory_structure()
    
    # Create sample error log config if needed
    print(f"\n📄 2. Setting up error log configuration...")
    create_sample_error_log_config()
    
    # Create server directories
    print(f"\n🖥️ 3. Creating server directories...")
    server_count = create_server_directories_from_config()
    
    # Create sample error logs
    print(f"\n📋 4. Creating sample ERRORLOG files...")
    create_sample_error_logs(server_count)
    
    # Validate configuration files
    print(f"\n✅ 5. Validating configuration files...")
    config_valid = validate_configuration_files()
    
    if not config_valid:
        print(f"\n❌ Setup incomplete due to missing or invalid configuration files.")
        return False
    
    # Validate server assignments
    print(f"\n🔧 6. Validating server type assignments...")
    assignments_valid = validate_server_type_assignments()
    
    # Test configuration integration
    print(f"\n🔗 7. Testing configuration integration...")
    integration_valid = test_configuration_integration()
    
    # Create README
    print(f"\n📖 8. Creating documentation...")
    create_readme()
    
    # Final summary
    print(f"\n{'='*70}")
    if config_valid and assignments_valid and integration_valid:
        print("🎉 Enhanced setup completed successfully!")
        print(f"\n📊 Configuration Summary:")
        print(f"   Total Servers: {server_count}")
        print(f"   Configuration Files: ✅ Valid")
        print(f"   Server Assignments: ✅ Valid") 
        print(f"   Integration Test: ✅ Passed")
        print(f"\n🚀 Ready to run:")
        print("   python growth_simulator.py")
    else:
        print("⚠️ Setup completed with warnings.")
        print("Please review the issues above before running the simulator.")
    
    print(f"{'='*70}")
    
    return config_valid and assignments_valid and integration_valid


def main():
    """Main setup function"""
    success = initialize_enhanced_project()
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)