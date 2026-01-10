#!/usr/bin/env python3
"""
Backup Configuration Manager
Handles loading and parsing of unified backup configuration (server + datasets)
"""

import yaml
import os
from typing import List, Dict, Optional, Any
from datetime import datetime


class ServerConfig:
    """Server-wide backup settings"""
    def __init__(self, config_data: Dict[str, Any]):
        server = config_data.get('server', {})

        # Backup intervals and schedules
        self.backup_interval = server.get('backup_interval', 600)

        schedule = server.get('schedule', {})
        self.days = schedule.get('days', '1111111')
        self.hours = schedule.get('hours', '111111111111111111111111')

        # Retention policies
        retention = server.get('retention', {})
        self.keep_frequent = retention.get('frequent', 4)
        self.keep_hourly = retention.get('hourly', 12)
        self.keep_daily = retention.get('daily', 7)
        self.keep_weekly = retention.get('weekly', 4)
        self.keep_monthly = retention.get('monthly', 6)
        self.keep_yearly = retention.get('yearly', 3)

        # Remote sync settings
        remote_sync = server.get('remote_sync', {})
        self.remote_sync = remote_sync.get('enabled', False)
        self.remote_sync_interval = remote_sync.get('interval', 86400)
        self.remote_sync_days = remote_sync.get('days', '1111111')
        self.remote_sync_hours = remote_sync.get('hours', '111111111111111111111111')

    def get_cleanup_tags(self) -> Dict[str, int]:
        """Get retention policy dictionary for cleanup"""
        return {
            'frequent': self.keep_frequent,
            'hourly': self.keep_hourly,
            'daily': self.keep_daily,
            'weekly': self.keep_weekly,
            'monthly': self.keep_monthly,
            'yearly': self.keep_yearly
        }


class ZFSAPIConfig:
    """ZFS API connection settings"""
    def __init__(self, config_data: Dict[str, Any]):
        api = config_data.get('zfs_api', {})
        self.url = api.get('url', 'http://localhost:8545')
        self.timeout = api.get('timeout', 30)


class BackupDestination:
    """Represents a single backup destination"""
    def __init__(self, remote_host: Optional[str] = None, 
                 remote_dataset: Optional[str] = None, 
                 enabled: bool = True):
        self.remote_host = remote_host
        self.remote_dataset = remote_dataset
        self.enabled = enabled
        self.last_sync_time = None
        self.current_task_id = None  # Track ongoing migration
        
    def is_local_only(self) -> bool:
        """True if this is local snapshots only (no remote sync)"""
        return self.remote_host is None
        
    def get_target_dataset(self, local_dataset: str) -> str:
        """Get the target dataset name (uses local name if remote_dataset is None)"""
        return self.remote_dataset if self.remote_dataset else local_dataset
        
    def __repr__(self):
        if self.is_local_only():
            return "LocalOnly"
        return f"{self.remote_host}:{self.get_target_dataset('')}"


class BackupDataset:
    """Represents a local dataset with its backup destinations"""
    def __init__(self, local_dataset: str, destinations: List[BackupDestination] = None):
        self.local_dataset = local_dataset
        self.destinations = destinations or []
        self.active = True
        self.snapshots = {}
        self.last_snapshot_time = None
        
    def has_remote_destinations(self) -> bool:
        """True if this dataset has any remote sync destinations"""
        return any(not dest.is_local_only() and dest.enabled for dest in self.destinations)
        
    def get_enabled_destinations(self) -> List[BackupDestination]:
        """Get only enabled destinations"""
        return [dest for dest in self.destinations if dest.enabled]
        
    def __repr__(self):
        dest_count = len(self.get_enabled_destinations())
        return f"BackupDataset({self.local_dataset}, {dest_count} destinations)"


class BackupConfig:
    """Main configuration manager - loads unified YAML config"""
    def __init__(self, config_file: str = "backup_config.yaml"):
        self.config_file = config_file
        self.server: ServerConfig = None
        self.api: ZFSAPIConfig = None
        self.datasets: List[BackupDataset] = []
        self.load_config()
        
    def load_config(self) -> None:
        """Load unified configuration from YAML file with atomic reload"""
        if not os.path.exists(self.config_file):
            print(f"Warning: Config file {self.config_file} not found. Using defaults.")
            # Set default configurations
            self.server = ServerConfig({})
            self.api = ZFSAPIConfig({})
            return

        # Keep backup of current config in case reload fails
        old_server = self.server
        old_api = self.api
        old_datasets = self.datasets

        try:
            with open(self.config_file, 'r') as f:
                config_data = yaml.safe_load(f)

            # Validate config_data is not None or empty
            if config_data is None:
                raise ValueError("Config file is empty or invalid YAML")

            # Load to temporary variables first (don't overwrite yet)
            new_server = ServerConfig(config_data)
            new_api = ZFSAPIConfig(config_data)
            new_datasets = []

            # Parse all datasets
            for dataset_config in config_data.get('datasets', []):
                local_dataset = dataset_config.get('local_dataset')
                if not local_dataset:
                    continue

                destinations = []
                for dest_config in dataset_config.get('destinations', []):
                    destination = BackupDestination(
                        remote_host=dest_config.get('remote_host'),
                        remote_dataset=dest_config.get('remote_dataset'),
                        enabled=dest_config.get('enabled', True)
                    )
                    destinations.append(destination)

                backup_dataset = BackupDataset(local_dataset, destinations)
                new_datasets.append(backup_dataset)

            # Preserve runtime state from old config (last_sync_time, current_task_id)
            if old_datasets:
                for new_ds in new_datasets:
                    # Find matching old dataset
                    old_ds_list = [ds for ds in old_datasets if ds.local_dataset == new_ds.local_dataset]
                    if old_ds_list:
                        old_ds = old_ds_list[0]
                        # Match destinations and preserve runtime state
                        for new_dest in new_ds.destinations:
                            for old_dest in old_ds.destinations:
                                # Match destinations by remote_host and remote_dataset
                                if (new_dest.remote_host == old_dest.remote_host and
                                    new_dest.remote_dataset == old_dest.remote_dataset):
                                    # Preserve runtime state
                                    new_dest.last_sync_time = old_dest.last_sync_time
                                    new_dest.current_task_id = old_dest.current_task_id
                                    break

            # Only if everything succeeded, replace old config atomically
            self.server = new_server
            self.api = new_api
            self.datasets = new_datasets

        except Exception as e:
            # On any error, keep old config and log warning
            print(f"WARNING: Failed to reload config from {self.config_file}: {e}")
            print(f"         Keeping previous configuration ({len(old_datasets) if old_datasets else 0} datasets)")

            # Restore old config if it was partially overwritten
            if old_server:
                self.server = old_server
            if old_api:
                self.api = old_api
            if old_datasets:
                self.datasets = old_datasets

            # Set defaults only if this is the first load and nothing exists
            if not self.server:
                self.server = ServerConfig({})
            if not self.api:
                self.api = ZFSAPIConfig({})
            
    def get_datasets_for_backup(self) -> List[BackupDataset]:
        """Get all datasets that should be backed up"""
        return [ds for ds in self.datasets if ds.active]
        
    def get_datasets_with_remote_sync(self) -> List[BackupDataset]:
        """Get datasets that have remote sync destinations"""
        return [ds for ds in self.datasets if ds.has_remote_destinations()]
        
    def find_dataset(self, local_dataset: str) -> List[BackupDataset]:
        """Find all backup configs for a given local dataset"""
        return [ds for ds in self.datasets if ds.local_dataset == local_dataset]
        
    def reload_config(self) -> None:
        """Reload configuration from file"""
        self.load_config()
        
    def print_config_summary(self) -> None:
        """Print a summary of the current configuration"""
        print("=" * 70)
        print("ZFS AUTOBACKUP CONFIGURATION SUMMARY")
        print("=" * 70)

        # Server settings
        print("\n[SERVER SETTINGS]")
        print(f"  Backup interval:     {self.server.backup_interval}s")
        print(f"  Schedule days:       {self.server.days}")
        print(f"  Schedule hours:      {self.server.hours}")
        print(f"\n[RETENTION POLICIES]")
        print(f"  Frequent:  {self.server.keep_frequent}")
        print(f"  Hourly:    {self.server.keep_hourly}")
        print(f"  Daily:     {self.server.keep_daily}")
        print(f"  Weekly:    {self.server.keep_weekly}")
        print(f"  Monthly:   {self.server.keep_monthly}")
        print(f"  Yearly:    {self.server.keep_yearly}")
        print(f"\n[REMOTE SYNC]")
        print(f"  Enabled:    {self.server.remote_sync}")
        print(f"  Interval:   {self.server.remote_sync_interval}s")
        print(f"  Days:       {self.server.remote_sync_days}")
        print(f"  Hours:      {self.server.remote_sync_hours}")

        # API settings
        print(f"\n[ZFS API]")
        print(f"  URL:        {self.api.url}")
        print(f"  Timeout:    {self.api.timeout}s")

        # Dataset configurations
        print(f"\n[DATASETS] ({len(self.datasets)} configured)")
        print("-" * 70)
        for dataset in self.datasets:
            print(f"\nDataset: {dataset.local_dataset}")
            if not dataset.destinations:
                print("  -> Local snapshots only")
            else:
                for i, dest in enumerate(dataset.destinations):
                    status = "ENABLED" if dest.enabled else "DISABLED"
                    if dest.is_local_only():
                        print(f"  -> [{status}] Local snapshots only")
                    else:
                        target = dest.get_target_dataset(dataset.local_dataset)
                        print(f"  -> [{status}] {dest.remote_host}:{target}")
        print("\n" + "=" * 70)


# Example usage and testing
if __name__ == "__main__":
    # Test with the unified config
    config = BackupConfig("backup_config.yaml")
    config.print_config_summary()

    print("\n[ADDITIONAL INFO]")
    print(f"Datasets with remote sync: {len(config.get_datasets_with_remote_sync())}")
    for ds in config.get_datasets_with_remote_sync():
        print(f"  {ds.local_dataset}: {len(ds.get_enabled_destinations())} destination(s)")