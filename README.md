# SQL Server Database Growth Simulator

A standalone Python simulator that generates realistic SQL Server database growth patterns, including database size changes, I/O statistics, table-level metrics, and autogrowth events. Designed to create test data for monitoring tools and capacity planning systems.

## Features

- **Realistic Growth Patterns**: Simulates different database growth scenarios including stable, problematic, and anomalous patterns
- **Multiple Server Types**: Supports OLTP Production, Reporting/Analytics, and Reference/Configuration server patterns
- **Table-Level Simulation**: Tracks individual table growth with cleanup job simulation
- **Autogrowth Events**: Generates realistic autogrowth events with configurable patterns
- **I/O Statistics**: Simulates read/write operations based on workload patterns
- **12-Hour Simulation Periods**: Each run simulates 12 hours of database activity (day/night cycles)
- **State Persistence**: Maintains continuous growth progression across multiple runs

## Requirements

- Python 3.7 or higher
- No external dependencies (uses Python standard library only)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/marbr1401/sql-server-growth-simulator.git
cd sql-server-growth-simulator
```
2. Run the setup script:
```bash
python setup_project.py
```
## Quick Start
1. **Run the simulator**:
```bash
python growth_simulator.py
```
2. **Check generated data**:
   - Snapshots: `Server*/growth_data/snapshots/`
   - Autogrowth events: `Server*/growth_data/autogrowth_events/`
   - Server state: `Server*/growth_data/server_state.json`

## How It Works
The simulator follows these steps each execution:
1. **Load Configuration**: Reads server assignments and growth patterns from `growth_config.json`
2. **Calculate Period**: Determines the next 12-hour period to simulate (day: 6AM-6PM or night: 6PM-6AM)
3. **For Each Server**:
   - Load or initialize database states from `server_state.json`
   - Generate size changes based on growth patterns (stable/growing/problematic)
   - Simulate I/O operations (reads/writes) based on server type and time period
   - Create table-level metrics with cleanup job simulation
   - Generate autogrowth events if configured (normal or anomaly patterns)
4. **Save Output**: 
   - Write JSON snapshots to `Server*/growth_data/snapshots/`
   - Save autogrowth events to `Server*/growth_data/autogrowth_events/`
5. **Persist State**: Update `server_state.json` for continuous progression

Each run advances the simulation by 12 hours, maintaining realistic growth patterns over time.
## Configuration
### Server Types
The simulator supports three server types with different growth characteristics:
| Server Type | Databases | Tables per DB | Growth Pattern |
|------------|-----------|---------------|----------------|
| OLTP Production | 3 | 15-25 | Heavy transactional, retention issues |
| Reporting/Analytics | 3 | 8-15 | Batch processing, ETL cycles |
| Reference/Config | 2 | 5-10 | Minimal growth, read-heavy |

### Key Configuration Files
- **growth_config.json**: Main configuration for server assignments and simulation settings
- **baseline_templates.json**: Defines baseline metrics for each server type
- **table_patterns.json**: Table growth and cleanup patterns
- **data/fake_database_names.json**: Database names for each server type

### Configuring Server Assignments
Edit `growth_config.json` to assign servers to types:
```json
{
  "server_integration": {
    "server_type_assignments": {
      "servers_1_2": "oltp_production",
      "servers_3_3": "reporting_analytics"
    },
    "default_server_count": 3,
    "unassigned_server_default": "reference_config"
  }
}
```
Simulation Patterns
Growth Patterns
The simulator includes several predefined growth patterns:

stable: Normal growth with effective cleanup
no_retention: Steady growth without cleanup
growing_fast: Rapid growth due to configuration issues
broken_cleanup: Intermittent cleanup failures
archive_failure: Failed archive jobs causing accumulation
etl_cycle: Normal ETL load/archive cycles
static: Minimal or no growth (reference data)

Anomaly Simulation
One database can be configured for autogrowth anomalies (many small increments):
```json
"Server2/PrimaryStore_DB": {
  "anomaly_scenario": "excessive_autogrowth",
  "frequency_multiplier": 10,
  "small_increment_bias": 0.8,
  "preferred_increments_mb": [8, 16, 32]
}
```
Output Format
Snapshot JSON Structure
```json
{
  "timestamp": "2025-05-01T18:00:00",
  "server_name": "Server1",
  "database_name": "CustomerCore_DB",
  "size": {
    "total_gb": 350.123,
    "data_file_gb": 262.592,
    "log_file_gb": 87.531,
    "file_count": 4
  },
  "io": {
    "reads": 186432000,
    "writes": 93216000,
    "read_gb": 1440.0,
    "write_gb": 720.0
  },
  "tables": [...]
}
```
```json
### Autogrowth Event Structure{
  "timestamp": "2025-05-01T14:23:45",
  "file_type": "data",
  "previous_mb": 10240,
  "increment_mb": 256,
  "new_mb": 10496,
  "duration_ms": 1250,
  "blocking": true,
  "io_wait_ms": 450,
  "blocked_processes": 5
}
```
## Utility Scripts

- **setup_project.py**: Initial project setup and directory creation
- **fix_simulator_state.py**: Diagnose and repair simulation state issues
- **fix_autogrowth_generation.py**: Fix autogrowth generation configuration
- **unified_database_manager.py**: Synchronize database names across simulators

## Directory Structure
sql-server-growth-simulator/
├── growth_simulator.py          # Main simulator
├── growth_utils.py              # Core utility functions
├── config_manager.py            # Configuration management
├── growth_config.json           # Main configuration
├── baseline_templates.json      # Server baselines
├── table_patterns.json          # Table patterns
├── data/
│   └── fake_database_names.json # Database names
└── Server*/                     # Generated server directories
└── growth_data/
├── snapshots/           # JSON snapshots
├── autogrowth_events/   # Autogrowth events
└── server_state.json    # Persistent state

Troubleshooting
Common Issues
1. No autogrowth events generated:
     - Run python fix_autogrowth_generation.py
     - Verify autogrowth_simulation.enabled is true in config

2. Simulation stuck on same date:
    - Run python fix_simulator_state.py
    - Option 3: Fix simulation period progression

3. Missing database names:
    - Ensure data/fake_database_names.json exists
    - Run setup script again

Reset Simulation
To start fresh from May 1, 2025:
python fix_simulator_state.py
# Choose option 4: Force reset to May 1st

## Use Cases

- **Testing Monitoring Tools**: Generate realistic data for testing database monitoring dashboards
- **Capacity Planning**: Simulate growth patterns to test capacity planning models
- **Alert Testing**: Create anomaly scenarios to test alerting systems
- **Performance Testing**: Generate I/O patterns for performance testing tools
- **Training**: Create realistic scenarios for DBA training

## License

MIT License - See LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit pull requests.

## Author

marbr1401

## Acknowledgments

This simulator was created as a standalone tool for generating realistic SQL Server database growth patterns for testing and development purposes.
