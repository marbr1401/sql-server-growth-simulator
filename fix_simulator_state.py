#!/usr/bin/env python3
"""
Diagnostic and Repair Script for SQL Server Database Growth Simulator
Fixes state management issues and ensures proper progression
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

def setup_logging():
    """Setup logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def diagnose_server_states():
    """Diagnose current server states"""
    
    print("🔍 DIAGNOSING SERVER STATES")
    print("=" * 50)
    
    issues_found = []
    
    # Check for server directories
    server_dirs = sorted([d for d in Path('.').iterdir() if d.is_dir() and d.name.startswith('Server')])
    
    if not server_dirs:
        issues_found.append("No server directories found")
        print("❌ No server directories found")
        return issues_found
    
    print(f"📁 Found {len(server_dirs)} server directories")
    
    for server_dir in server_dirs:
        server_num = int(server_dir.name.replace('Server', ''))
        print(f"\n🖥️ {server_dir.name}:")
        
        # Check growth_data directory
        growth_dir = server_dir / "growth_data"
        if not growth_dir.exists():
            issues_found.append(f"{server_dir.name}: Missing growth_data directory")
            print(f"   ❌ Missing growth_data directory")
            continue
        
        # Check server state file
        state_file = growth_dir / "server_state.json"
        if not state_file.exists():
            issues_found.append(f"{server_dir.name}: No server state file")
            print(f"   ⚠️ No server state file found")
            continue
        
        # Analyze state file
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            if not state:
                issues_found.append(f"{server_dir.name}: Empty state file")
                print(f"   ⚠️ Empty state file")
                continue
            
            print(f"   📊 Databases in state: {len(state)}")
            
            # Check each database state
            for db_name, db_state in state.items():
                if not isinstance(db_state, dict):
                    continue
                    
                last_period = db_state.get('last_simulation_period')
                if last_period:
                    period_end = last_period.get('period_end', 'Unknown')
                    print(f"      - {db_name}: Last period ended {period_end}")
                else:
                    print(f"      - {db_name}: No simulation periods recorded")
            
        except Exception as e:
            issues_found.append(f"{server_dir.name}: Error reading state file: {e}")
            print(f"   ❌ Error reading state file: {e}")
        
        # Check snapshots
        snapshots_dir = growth_dir / "snapshots"
        if snapshots_dir.exists():
            snapshots = list(snapshots_dir.glob("*.json"))
            print(f"   📸 Snapshots found: {len(snapshots)}")
            
            if snapshots:
                # Show latest snapshot
                latest_snapshot = max(snapshots, key=lambda x: x.stat().st_mtime)
                print(f"      Latest: {latest_snapshot.name}")
        else:
            print(f"   📸 No snapshots directory")
    
    print(f"\n{'='*50}")
    if issues_found:
        print(f"⚠️ Issues found: {len(issues_found)}")
        for issue in issues_found:
            print(f"   - {issue}")
    else:
        print("✅ No critical issues found")
    
    return issues_found


def create_database_names_file():
    """Create the database names file"""
    
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    db_names_file = data_dir / "fake_database_names.json"
    
    database_names = {
        "oltp_production": [
            "CustomerDB_PROD",
            "OrderManagement_DB", 
            "InventoryCore_DB",
            "PaymentProcessor_DB",
            "TransactionEngine_DB",
            "UserAccount_DB"
        ],
        "reporting_analytics": [
            "DataWarehouse_MAIN",
            "BusinessIntelligence_DB",
            "ReportingEngine_DB",
            "AnalyticsStore_DB",
            "MetricsRepository_DB"
        ],
        "reference_config": [
            "ConfigurationStore_DB",
            "ReferenceData_DB",
            "SystemRegistry_DB",
            "DictionaryLookup_DB"
        ]
    }
    
    with open(db_names_file, 'w') as f:
        json.dump(database_names, f, indent=2)
    
    print(f"✅ Created database names file: {db_names_file}")


def reset_server_state(server_num: int, keep_existing: bool = True):
    """Reset or clean a server's state"""
    
    server_dir = Path(f"Server{server_num}")
    if not server_dir.exists():
        print(f"❌ Server{server_num} directory not found")
        return
    
    growth_dir = server_dir / "growth_data"
    growth_dir.mkdir(exist_ok=True)
    
    state_file = growth_dir / "server_state.json"
    
    if not keep_existing or not state_file.exists():
        # Create empty state
        with open(state_file, 'w') as f:
            json.dump({}, f, indent=2)
        print(f"✅ Reset state for Server{server_num}")
    else:
        print(f"📋 Keeping existing state for Server{server_num}")


def fix_simulation_periods():
    """Fix simulation period progression issues"""
    
    print("\n🔧 FIXING SIMULATION PERIODS")
    print("=" * 50)
    
    server_dirs = sorted([d for d in Path('.').iterdir() if d.is_dir() and d.name.startswith('Server')])
    
    for server_dir in server_dirs:
        server_num = int(server_dir.name.replace('Server', ''))
        state_file = server_dir / "growth_data" / "server_state.json"
        
        if not state_file.exists():
            continue
        
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            print(f"🖥️ Checking Server{server_num}...")
            
            # Find databases stuck on May 1st
            may_1_databases = []
            for db_name, db_state in state.items():
                if isinstance(db_state, dict) and 'last_simulation_period' in db_state:
                    last_period = db_state['last_simulation_period']
                    if last_period and '2025-05-01' in last_period.get('period_end', ''):
                        may_1_databases.append(db_name)
            
            if may_1_databases:
                print(f"   ⚠️ Found {len(may_1_databases)} databases stuck on May 1st")
                
                # Calculate what the next period should be
                start_date = datetime(2025, 5, 1, 6, 0, 0)
                current_time = datetime.now()
                
                # Calculate how many 12-hour periods should have passed
                time_diff = current_time - start_date
                periods_passed = int(time_diff.total_seconds() / (12 * 3600))
                
                # Calculate the target period end time
                target_period_start = start_date + timedelta(hours=12 * periods_passed)
                target_period_end = target_period_start + timedelta(hours=12)
                
                period_type = "day" if target_period_start.hour == 6 else "night"
                
                target_period = {
                    'period_start': target_period_start.isoformat(),
                    'period_end': target_period_end.isoformat(),
                    'period_type': period_type,
                    'duration_hours': 12.0,
                    'period_label': f"{target_period_start.strftime('%Y-%m-%d')} {target_period_start.strftime('%H:%M')}-{target_period_end.strftime('%H:%M')}"
                }
                
                print(f"   📅 Target period: {target_period['period_label']}")
                
                # Update all stuck databases
                for db_name in may_1_databases:
                    state[db_name]['last_simulation_period'] = target_period
                
                # Save updated state
                with open(state_file, 'w') as f:
                    json.dump(state, f, indent=2)
                
                print(f"   ✅ Updated {len(may_1_databases)} databases to current period")
            else:
                print(f"   ✅ No databases stuck on May 1st")
                
        except Exception as e:
            print(f"   ❌ Error processing Server{server_num}: {e}")


def force_reset_to_may_1():
    """Force reset all servers back to May 1st start"""
    
    print("\n🔄 FORCE RESET TO MAY 1ST")
    print("=" * 50)
    
    confirm = input("This will reset ALL simulation progress to May 1st, 2025. Continue? (y/N): ")
    if confirm.lower() != 'y':
        print("❌ Reset cancelled")
        return
    
    server_dirs = sorted([d for d in Path('.').iterdir() if d.is_dir() and d.name.startswith('Server')])
    
    for server_dir in server_dirs:
        server_num = int(server_dir.name.replace('Server', ''))
        growth_dir = server_dir / "growth_data"
        growth_dir.mkdir(exist_ok=True)
        
        # Reset state file
        state_file = growth_dir / "server_state.json"
        with open(state_file, 'w') as f:
            json.dump({}, f, indent=2)
        
        print(f"✅ Reset Server{server_num} to initial state")
    
    print("🎯 All servers reset to May 1st, 2025 starting point")


def check_configuration_files():
    """Check if all required configuration files exist"""
    
    print("\n📋 CHECKING CONFIGURATION FILES")
    print("=" * 50)
    
    required_files = [
        'growth_config.json',
        'baseline_templates.json', 
        'table_patterns.json',
        'config_manager.py',
        'growth_utils.py'
    ]
    
    optional_files = [
        'data/fake_database_names.json'
    ]
    
    missing_required = []
    missing_optional = []
    
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"✅ {file_path}")
        else:
            missing_required.append(file_path)
            print(f"❌ {file_path}")
    
    for file_path in optional_files:
        if Path(file_path).exists():
            print(f"✅ {file_path}")
        else:
            missing_optional.append(file_path)
            print(f"⚠️ {file_path} (will create)")
    
    if missing_required:
        print(f"\n❌ Missing required files: {missing_required}")
        return False
    
    if missing_optional:
        print(f"\n📝 Will create missing optional files")
        if 'data/fake_database_names.json' in missing_optional:
            create_database_names_file()
    
    return True


def show_next_simulation_info():
    """Show what the next simulation run should generate"""
    
    print("\n🎯 NEXT SIMULATION INFO")
    print("=" * 50)
    
    # Check Server1 as reference
    server1_state_file = Path("Server1/growth_data/server_state.json")
    
    if not server1_state_file.exists():
        print("📅 Next run will start from: May 1, 2025 06:00-18:00 (Day Period)")
        return
    
    try:
        with open(server1_state_file, 'r') as f:
            state = json.load(f)
        
        if not state:
            print("📅 Next run will start from: May 1, 2025 06:00-18:00 (Day Period)")
            return
        
        # Find the latest period from any database
        latest_period_end = None
        
        for db_name, db_state in state.items():
            if isinstance(db_state, dict) and 'last_simulation_period' in db_state:
                last_period = db_state['last_simulation_period']
                if last_period:
                    period_end = datetime.fromisoformat(last_period['period_end'])
                    if latest_period_end is None or period_end > latest_period_end:
                        latest_period_end = period_end
        
        if latest_period_end:
            # Calculate next period
            if latest_period_end.hour == 6:  # Last ended at 6 AM
                next_start = latest_period_end
                next_end = latest_period_end.replace(hour=18)
                period_type = "Day"
            else:  # Last ended at 6 PM
                next_start = latest_period_end
                next_end = (latest_period_end + timedelta(days=1)).replace(hour=6)
                period_type = "Night"
            
            print(f"📅 Next run will simulate: {next_start.strftime('%Y-%m-%d %H:%M')} → {next_end.strftime('%Y-%m-%d %H:%M')}")
            print(f"🌅 Period Type: {period_type}")
        else:
            print("📅 Next run will start from: May 1, 2025 06:00-18:00 (Day Period)")
    
    except Exception as e:
        print(f"❌ Error reading state: {e}")
        print("📅 Next run will start from: May 1, 2025 06:00-18:00 (Day Period)")


def main():
    """Main diagnostic and repair function"""
    
    setup_logging()
    
    print("🔧 SQL SERVER GROWTH SIMULATOR - DIAGNOSTIC & REPAIR")
    print("=" * 70)
    
    while True:
        print("\nSelect an option:")
        print("1. 🔍 Diagnose current state")
        print("2. 📋 Check configuration files")
        print("3. 🔧 Fix simulation period progression")
        print("4. 🔄 Force reset to May 1st start")
        print("5. 📁 Create missing database names file")
        print("6. 🎯 Show next simulation info")
        print("7. 🚀 Run growth simulator")
        print("0. ❌ Exit")
        
        choice = input("\nEnter choice (0-7): ").strip()
        
        if choice == '0':
            print("👋 Exiting...")
            break
        elif choice == '1':
            diagnose_server_states()
        elif choice == '2':
            check_configuration_files()
        elif choice == '3':
            fix_simulation_periods()
        elif choice == '4':
            force_reset_to_may_1()
        elif choice == '5':
            create_database_names_file()
        elif choice == '6':
            show_next_simulation_info()
        elif choice == '7':
            print("\n🚀 Running growth simulator...")
            try:
                import subprocess
                result = subprocess.run(['python', 'growth_simulator.py'], 
                                      capture_output=True, text=True)
                print(result.stdout)
                if result.stderr:
                    print("Errors:", result.stderr)
            except Exception as e:
                print(f"❌ Error running simulator: {e}")
        else:
            print("❌ Invalid choice")


if __name__ == "__main__":
    main()