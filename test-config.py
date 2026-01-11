#!/usr/bin/env python3
"""
Config validation tool - similar to 'nginx -t'
Usage: ./test-config.py [config_file]
"""

import sys
import os
import yaml

def validate_config(config_file='backup_config.yaml'):
    """Validate config file and show summary"""

    print(f"Testing config file: {config_file}")
    print("-" * 50)

    # Check file exists
    if not os.path.exists(config_file):
        print(f"❌ FAIL: Config file not found")
        return False

    # Parse YAML directly first
    try:
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"❌ FAIL: Invalid YAML syntax")
        print(f"   Error: {e}")
        return False
    except Exception as e:
        print(f"❌ FAIL: Cannot read config file")
        print(f"   Error: {e}")
        return False

    if config_data is None:
        print("❌ FAIL: Config file is empty")
        return False

    # Import after YAML validation
    from backup_config import BackupConfig

    # Try to load config
    try:
        config = BackupConfig(config_file)
    except Exception as e:
        print(f"❌ FAIL: Cannot load config")
        print(f"   Error: {e}")
        return False

    # Validate server settings
    if not config.server:
        print("❌ FAIL: Missing server settings")
        return False

    if config.server.backup_interval <= 0:
        print(f"❌ FAIL: Invalid backup_interval: {config.server.backup_interval}")
        return False

    # Validate API settings
    if not config.api:
        print("❌ FAIL: Missing API settings")
        return False

    if not config.api.url:
        print("❌ FAIL: Missing API URL")
        return False

    # Validate datasets
    if not config.datasets:
        print("⚠️  WARNING: No datasets configured")

    for dataset in config.datasets:
        if not dataset.local_dataset:
            print(f"❌ FAIL: Dataset missing local_dataset")
            return False

        for dest in dataset.destinations:
            if not dest.is_local_only() and not dest.remote_host:
                print(f"❌ FAIL: Dataset {dataset.local_dataset} has destination without remote_host")
                return False

    # Show summary
    print("✅ Config syntax is valid")
    print()
    print(f"Server:")
    print(f"  Backup interval: {config.server.backup_interval}s")
    print(f"  Remote sync: {config.server.remote_sync}")
    print(f"  Retention: {config.server.keep_frequent}f {config.server.keep_hourly}h {config.server.keep_daily}d")
    print()
    print(f"API: {config.api.url}")
    print()
    print(f"Datasets: {len(config.datasets)}")
    for ds in config.datasets:
        enabled_dests = len(ds.get_enabled_destinations())
        status = "✓" if ds.active else "✗"
        print(f"  [{status}] {ds.local_dataset} → {enabled_dests} destination(s)")

    print()
    print("✅ Config test successful")
    return True


if __name__ == '__main__':
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'backup_config.yaml'

    success = validate_config(config_file)
    sys.exit(0 if success else 1)
