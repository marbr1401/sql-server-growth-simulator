#!/usr/bin/env python3
"""
Setup script for SQL Server Database Growth Simulation
Creates necessary directory structure and initializes server directories
"""

import os
import json
from pathlib import Path


def create_directory_structure():
    """Create the basic directory structure for the project"""
    
    directories = [
        "data",
        "logs", 
        "backups"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"Created directory: {directory}")


def create_server_directories(server_count=6):
    """Create server directories based on configuration"""
    
    for server_num in range(1, server_count + 1):
        server_dir = Path(f"Server{server_num}")
        server_dir.mkdir(exist_ok=True)
        
        # Create growth_data subdirectories
        growth_dir = server_dir / "growth_data"
        growth_dir.mkdir(exist_ok=True)
        
        (growth_dir / "snapshots").mkdir(exist_ok=True)
        (growth_dir / "trends").mkdir(exist_ok=True)
        
        print(f"Created server directory: Server{server_num}")


def create_sample_error_log():
    """Create sample ERRORLOG files to simulate existing error log simulator"""
    
    sample_error_entries = [
        "2025-08-02 08:49:47.02 Server     Microsoft SQL Server 2022 (RTM-GDR) (KB5058712) - 16.0.1140.6 (X64)",
        "2025-08-02 08:49:47.05 Server     UTC adjustment: 3:00",
        "2025-08-02 08:49:47.06 Server     Server process ID is 6256.",
        "2025-08-02 08:49:52.34 spid42s    SQL Server is now ready for client connections.",
        "2025-08-02 08:54:30.34 spid35s    Deadlock encountered .... Printing deadlock information"
    ]
    
    try:
        # Read configuration to get server count
        with open('growth_config.json', 'r') as f:
            config = json.load(f)
        server_count = config['server_configuration']['total_servers']
    except:
        server_count = 6
    
    for server_num in range(1, server_count + 1):
        server_dir = Path(f"Server{server_num}")
        server_dir.mkdir(exist_ok=True)
        
        errorlog_file = server_dir / "ERRORLOG"
        
        if not errorlog_file.exists():
            with open(errorlog_file, 'w', encoding='utf-16le') as f:
                for entry in sample_error_entries:
                    f.write(entry + '\n')
            
            print(f"Created sample ERRORLOG for Server{server_num}")


def validate_configuration_files():
    """Validate that all required configuration files exist"""
    
    required_files = [
        'growth_config.json',
        'baseline_templates.json',
        'table_patterns.json',
        'data/fake_database_names.json'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print("\nMissing configuration files:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        print("\nPlease create these files")
    else:
        print("All required configuration files are present. Setup successful!")