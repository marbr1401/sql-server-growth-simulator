
#!/usr/bin/env python3
"""
Fix Autogrowth Generation Issue
This script will identify and fix why autogrowth events are not being generated
"""

import json
import logging
from pathlib import Path
from datetime import datetime

def check_autogrowth_config_loading():
    """Check if autogrowth config is being loaded correctly"""
    
    print("🔧 CHECKING AUTOGROWTH CONFIG LOADING")
    print("="*50)
    
    try:
        # Test loading like the AutogrowthEventGenerator does
        with open('baseline_templates.json', 'r') as f:
            baseline_config = json.load(f)
        
        print("✅ baseline_templates.json loaded")
        
        # Check if autogrowth_simulation exists in baseline_templates
        autogrowth_config = baseline_config.get('autogrowth_simulation', {})
        if autogrowth_config:
            print("✅ autogrowth_simulation found in baseline_templates.json")
            enabled = autogrowth_config.get('enabled', False)
            print(f"   Enabled: {enabled}")
        else:
            print("❌ autogrowth_simulation NOT found in baseline_templates.json")
            print("   This is likely the problem!")
        
        # Check growth_config.json
        with open('growth_config.json', 'r') as f:
            growth_config = json.load(f)
        
        growth_autogrowth = growth_config.get('autogrowth_simulation', {})
        if growth_autogrowth:
            print("✅ autogrowth_simulation found in growth_config.json")
            enabled = growth_autogrowth.get('enabled', False)
            print(f"   Enabled: {enabled}")
            
            db_specific = growth_autogrowth.get('database_specific_config', {})
            print(f"   Database-specific configs: {list(db_specific.keys())}")
        else:
            print("❌ autogrowth_simulation NOT found in growth_config.json")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking config: {e}")
        return False


def check_which_databases_exist():
    """Check which databases actually exist vs configured"""
    
    print(f"\n📋 CHECKING ACTUAL VS CONFIGURED DATABASES")
    print("="*50)
    
    # Get configured database names from growth_config.json
    try:
        with open('growth_config.json', 'r') as f:
            config = json.load(f)
        
        db_config = config.get('database_configuration', {})
        
        print("🔧 Configured database names:")
        for server_type, type_config in db_config.items():
            db_names = type_config.get('database_names', [])
            print(f"   {server_type}: {db_names}")
        
    except Exception as e:
        print(f"❌ Error reading growth_config.json: {e}")
    
    # Get database names from external file
    try:
        with open('data/fake_database_names.json', 'r') as f:
            external_names = json.load(f)
        
        print(f"\n📁 External database names:")
        for server_type, db_names in external_names.items():
            print(f"   {server_type}: {db_names[:3]}...")  # Show first 3
        
    except Exception as e:
        print(f"❌ Error reading external database names: {e}")
    
    # Get actual database names from snapshots
    actual_databases = {}
    
    for server_dir in Path('.').glob('Server*'):
        server_num = int(server_dir.name.replace('Server', ''))
        snapshots_dir = server_dir / 'growth_data' / 'snapshots'
        
        if not snapshots_dir.exists():
            continue
        
        snapshots = list(snapshots_dir.glob('*.json'))
        if not snapshots:
            continue
        
        latest_snapshot = max(snapshots, key=lambda x: x.stat().st_mtime)
        
        try:
            with open(latest_snapshot, 'r') as f:
                data = json.load(f)
            
            db_name = data.get('database_name', 'Unknown')
            server_type = data.get('server_type', 'Unknown')
            
            if server_num not in actual_databases:
                actual_databases[server_num] = []
            
            actual_databases[server_num].append((db_name, server_type))
            
        except Exception:
            continue
    
    print(f"\n📸 Actual databases from snapshots:")
    for server_num, db_list in actual_databases.items():
        print(f"   Server{server_num}:")
        for db_name, server_type in db_list:
            print(f"      {db_name} ({server_type})")
    
    return actual_databases


def fix_autogrowth_config():
    """Fix the autogrowth configuration"""
    
    print(f"\n🔧 FIXING AUTOGROWTH CONFIGURATION")
    print("="*50)
    
    # The issue is likely that AutogrowthEventGenerator is looking for config in baseline_templates.json
    # but the autogrowth config is in growth_config.json
    
    try:
        # Load current baseline_templates.json
        with open('baseline_templates.json', 'r') as f:
            baseline_config = json.load(f)
        
        # Load autogrowth config from growth_config.json
        with open('growth_config.json', 'r') as f:
            growth_config = json.load(f)
        
        autogrowth_config = growth_config.get('autogrowth_simulation', {})
        
        if not autogrowth_config:
            print("❌ No autogrowth config found in growth_config.json")
            return False
        
        # Add autogrowth config to baseline_templates.json
        baseline_config['autogrowth_simulation'] = autogrowth_config
        
        # Write back to baseline_templates.json
        with open('baseline_templates.json', 'w') as f:
            json.dump(baseline_config, f, indent=2)
        
        print("✅ Added autogrowth_simulation to baseline_templates.json")
        print("   This should fix the autogrowth generation issue")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing config: {e}")
        return False


def update_anomaly_database():
    """Update anomaly database to match actual databases"""
    
    print(f"\n🚨 UPDATING ANOMALY DATABASE CONFIGURATION")
    print("="*50)
    
    # Get actual databases
    actual_databases = check_which_databases_exist()
    
    # Find a good candidate for anomaly (Server2, OLTP production)
    server2_databases = actual_databases.get(2, [])
    
    if not server2_databases:
        print("❌ No Server2 databases found")
        return False
    
    # Pick the first database on Server2
    anomaly_db_name = server2_databases[0][0]
    anomaly_key = f"Server2/{anomaly_db_name}"
    
    print(f"🎯 Setting anomaly database to: {anomaly_key}")
    
    try:
        # Update growth_config.json
        with open('growth_config.json', 'r') as f:
            config = json.load(f)
        
        # Update the database_specific_config
        autogrowth_config = config.get('autogrowth_simulation', {})
        old_config = autogrowth_config.get('database_specific_config', {})
        
        # Remove old configs and add new one
        new_db_config = {}
        if old_config:
            # Use the first old config as template
            old_key = list(old_config.keys())[0]
            new_db_config[anomaly_key] = old_config[old_key]
        else:
            # Create new anomaly config
            new_db_config[anomaly_key] = {
                "anomaly_scenario": "excessive_autogrowth",
                "frequency_multiplier": 10,
                "small_increment_bias": 0.8,
                "preferred_increments_mb": [8, 16, 32],
                "description": "Simulates misconfigured autogrowth causing frequent small increments"
            }
        
        autogrowth_config['database_specific_config'] = new_db_config
        config['autogrowth_simulation'] = autogrowth_config
        
        # Write back
        with open('growth_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Updated anomaly database configuration")
        print(f"   Anomaly database: {anomaly_key}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating anomaly config: {e}")
        return False


def test_fixed_configuration():
    """Test if the configuration fixes work"""
    
    print(f"\n🧪 TESTING FIXED CONFIGURATION")
    print("="*50)
    
    try:
        # Test importing and creating AutogrowthEventGenerator
        from growth_utils import AutogrowthEventGenerator
        
        # Load baseline templates (should now have autogrowth config)
        with open('baseline_templates.json', 'r') as f:
            baseline_config = json.load(f)
        
        generator = AutogrowthEventGenerator(baseline_config)
        
        print(f"✅ AutogrowthEventGenerator created successfully")
        print(f"   Enabled: {generator.enabled}")
        
        if generator.enabled:
            print(f"   Database-specific configs: {list(generator.db_specific_config.keys())}")
            return True
        else:
            print(f"❌ Still not enabled")
            return False
        
    except Exception as e:
        print(f"❌ Error testing: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main fix function"""
    
    print("🔧 AUTOGROWTH GENERATION FIX UTILITY")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Check current config loading
    config_ok = check_autogrowth_config_loading()
    
    # Step 2: Check database name mismatches
    actual_databases = check_which_databases_exist()
    
    # Step 3: Fix autogrowth config location
    fix_ok = fix_autogrowth_config()
    
    # Step 4: Update anomaly database
    anomaly_ok = update_anomaly_database()
    
    # Step 5: Test the fixes
    test_ok = test_fixed_configuration()
    
    print(f"\n" + "="*70)
    print(f"🎯 FIX RESULTS")
    print(f"="*70)
    
    if fix_ok and anomaly_ok and test_ok:
        print(f"✅ All fixes applied successfully!")
        print(f"\n🚀 Next steps:")
        print(f"   1. Run: python growth_simulator.py")
        print(f"   2. Run: python test_autogrowth_events.py")
        print(f"   3. Check for autogrowth events in snapshots")
        
        return True
    else:
        print(f"❌ Some fixes failed:")
        print(f"   Config fix: {'✅' if fix_ok else '❌'}")
        print(f"   Anomaly update: {'✅' if anomaly_ok else '❌'}")
        print(f"   Test: {'✅' if test_ok else '❌'}")
        
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)