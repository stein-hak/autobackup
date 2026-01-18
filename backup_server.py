#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sys
import os
import time
from collections import OrderedDict

sys.path.append('/opt/lib/')
from zfs_api_client import zfs
from datetime import datetime, timedelta
from backup_config import BackupConfig
from threading import Thread

# zfs instance is already created in zfs_api_client.py and imported above
# No need to instantiate again


def remote_sync(backup_dataset, destination):
    """Simplified remote sync using API migration with task tracking"""
    local_dataset = backup_dataset.local_dataset
    remote_host = destination.remote_host
    remote_dataset = destination.get_target_dataset(local_dataset)
    
    print('Performing remote sync for %s to %s:%s' % (local_dataset, remote_host, remote_dataset))
    
    try:
        # Use API for migration - it returns a task_id for tracking
        result = zfs.adaptive_send(
            dataset=local_dataset,
            recurse=True,
            verbose=False,
            remote_host=remote_host,
            remote_dataset=remote_dataset
        )
        
        # Check if migration was started successfully
        if result.task_id:
            print('Remote sync started for %s to %s:%s (task: %s)' % 
                  (local_dataset, remote_host, remote_dataset, result.task_id))
            destination.current_task_id = result.task_id
            return True
        else:
            print('Remote sync failed to start for %s to %s:%s' % 
                  (local_dataset, remote_host, remote_dataset))
            return False
            
    except Exception as e:
        print('Remote sync for %s to %s:%s failed: %s' %
              (local_dataset, remote_host, remote_dataset, e))
        return False


def update_destination_sync_times(backup_dataset):
    """
    Update last_sync_time for each destination based on holds.
    Parses sync holds to find the latest sync time per remote host.
    """
    dataset = backup_dataset.local_dataset

    try:
        # Get all holds on all snapshots for this dataset
        holds = zfs.get_holds(dataset)

        # Parse holds to find latest sync time per host
        # holds format: {"snapshot_name": ["hold_tag1", "hold_tag2"]}
        sync_times = {}  # {hostname: datetime}

        for snap_name, hold_list in holds.items():
            for hold_tag in hold_list:
                try:
                    # Parse hold format: sync_YYYY-MM-DD-HH-MM-SS_hostname
                    if hold_tag.startswith('sync_'):
                        parts = hold_tag.split('_')
                        if len(parts) >= 3:
                            timestr = parts[1]  # YYYY-MM-DD-HH-MM-SS
                            host = parts[2]      # hostname/IP

                            # Parse the timestamp
                            try:
                                sync_time = datetime.strptime(timestr, '%Y-%m-%d-%H-%M-%S')
                            except ValueError:
                                # Try alternate format without seconds
                                try:
                                    sync_time = datetime.strptime(timestr, '%Y-%m-%d-%H-%M')
                                except ValueError:
                                    continue

                            # Keep only the latest sync time for each host
                            if host not in sync_times or sync_time > sync_times[host]:
                                sync_times[host] = sync_time
                except Exception as e:
                    # Skip malformed hold tags
                    continue

        # Update destination objects with discovered sync times
        # Only update if hold time is newer than current in-memory value
        for destination in backup_dataset.destinations:
            if destination.remote_host and destination.remote_host in sync_times:
                hold_time = sync_times[destination.remote_host]
                # Only update if we don't have a time, or hold is newer
                if not destination.last_sync_time or hold_time > destination.last_sync_time:
                    destination.last_sync_time = hold_time

    except Exception as e:
        print(f'Error updating destination sync times for {dataset}: {e}')


def get_backup_snapshots(local_dataset):
    """
    Get backup snapshots and determine last snapshot time.
    Returns: (snapshots_dict, last_snapshot_time)
    """
    snapshots = {}
    last_snapshot_time = None

    try:
        # Get all snapshots for the dataset via API
        all_snaps = zfs.get_snapshots(local_dataset)
        backup_snapshots = []

        # Filter for backup snapshots (format: type_backup_YYYY-MM-DD-HH-MM)
        for snap in all_snaps:
            if '_backup_' in snap:
                backup_snapshots.append(snap)

        if backup_snapshots:
            # Determine last snapshot time from the latest backup snapshot
            latest_snap = backup_snapshots[-1]
            try:
                # Parse snapshot name: frequent_backup_2026-01-10-12-30
                parts = latest_snap.split('_')
                if len(parts) >= 3:
                    timestr = parts[2]  # YYYY-MM-DD-HH-MM
                    last_snapshot_time = datetime.strptime(timestr, '%Y-%m-%d-%H-%M')
            except Exception as e:
                print(f'Warning: Could not parse timestamp from snapshot {latest_snap}: {e}')

            # Group snapshots by type (frequent, hourly, daily, weekly, monthly, yearly)
            for snap in backup_snapshots:
                try:
                    snap_type = snap.split('_')[0]  # Get the first part (type)
                    if snap_type not in snapshots:
                        snapshots[snap_type] = []
                    snapshots[snap_type].append(snap)
                except:
                    pass

    except Exception as e:
        print(f'Error getting backup snapshots for {local_dataset}: {e}')

    return snapshots, last_snapshot_time


class backup_fs():
    def __init__(self, fs, type, active, remote_sync=False,remote_sync_hosts={}):
        self.fs = fs
        self.type = type
        self.active = active
        self.inotify_events = []
        self.snapshots = {}
        self.snap_usage = 0
        self.last_snapshot_time = None
        self.remote_sync = remote_sync
        self.remote_sync_hosts = remote_sync_hosts
        self.last_remote_sync_time = {}
        zfs.set(fs, 'snapdir', 'visible')

    def datetime_from_snapshot(self, snap):
        ret = None
        try:
            tag = snap.split('_')[1]
            timestr = snap.split('_')[2]
            if tag == 'backup':
                ret = datetime.strptime(timestr, '%Y-%m-%d-%H-%M')
        except:
            pass

        return ret

    def host_datetime_from_hold(self,hold):
        if 'sync' in hold:
            parts = hold.split('_')
            timestr = hold.split('_')[1]
            host = hold.split('_')[2]
            try:
                time = datetime.strptime(timestr, '%Y-%m-%d-%H-%M-%S')
            except:
                try:
                    time = datetime.strptime(timestr, '%Y-%m-%d-%H-%M')
                except:
                    time = datetime.strptime(timestr, '%Y-%m-%d-%M-%S')

            return (host,time)
        else:
            return (None,None)

    def get_remote_sync(self):
        # determine last remote sync time for every host from latest hold
        if self.remote_sync and self.remote_sync_hosts:
            holds = zfs.get_holds(self.fs)
            for snap in holds.values():
                for hold in snap:
                    host, time = self.host_datetime_from_hold(hold)
                    if host:
                        self.last_remote_sync_time[host] = time

        # if remote sync to host never took place - assume last remote sync time None
        for host in self.remote_sync_hosts.keys():
            if host not in self.last_remote_sync_time.keys():
                self.last_remote_sync_time[host] = None

    def get_backup_snapshots(self):
        # get all auto snapshots and determine last backup time from latest
        self.snapshots = {}
        snapshots = zfs.get_snapshots(self.fs)
        backup_snapshots = []
        for snap in snapshots:
            if self.datetime_from_snapshot(snap):
                backup_snapshots.append(snap)

        if backup_snapshots:
            self.last_snapshot_time = self.datetime_from_snapshot(backup_snapshots[-1])

            for snap in backup_snapshots:
                type = snap.split('_')[0]
                if not self.snapshots.get(type):
                    self.snapshots[type] = []
                    self.snapshots[type].append(snap)
                else:
                    if snap not in self.snapshots[type]:
                        self.snapshots[type].append(snap)





    def auto_snap(self, tag):
        rc, name = zfs.snapshot_auto(self.fs, tag, tag1='backup')


class backup_server(Thread):
    def __init__(self, config_file='backup_config.yaml'):
        Thread.__init__(self)

        # Load unified YAML configuration
        self.backup_config = BackupConfig(config_file)

        # Server settings (from unified config)
        self.days = self.backup_config.server.days
        self.hours = self.backup_config.server.hours
        self.backup_interval = self.backup_config.server.backup_interval

        # Retention settings
        self.keep_frequent = self.backup_config.server.keep_frequent
        self.keep_hourly = self.backup_config.server.keep_hourly
        self.keep_daily = self.backup_config.server.keep_daily
        self.keep_weekly = self.backup_config.server.keep_weekly
        self.keep_monthly = self.backup_config.server.keep_monthly
        self.keep_yearly = self.backup_config.server.keep_yearly

        # Remote sync settings
        self.remote_sync = self.backup_config.server.remote_sync
        self.remote_sync_days = self.backup_config.server.remote_sync_days
        self.remote_sync_hours = self.backup_config.server.remote_sync_hours
        self.remote_sync_interval = self.backup_config.server.remote_sync_interval

        # Cleanup tags for snapshot retention
        self.cleanup_tags = self.backup_config.server.get_cleanup_tags()

        self.interrupt = False
        self.active_migrations = {}  # Track running migrations: {task_id: (destination, backup_dataset, start_time)}
        self.last_progress_log = {}  # Track when we last logged progress: {task_id: timestamp}

    def reload_config(self):
        """Reload backup configuration from file and update active migration references"""
        old_count = len(self.backup_config.datasets)
        self.backup_config.reload_config()
        new_count = len(self.backup_config.datasets)

        # Update active_migrations to reference new destination objects
        for task_id, (old_dest, backup_dataset, start_time) in list(self.active_migrations.items()):
            # Find the new destination object that matches the old one
            for new_ds in self.backup_config.datasets:
                if new_ds.local_dataset == backup_dataset.local_dataset:
                    for new_dest in new_ds.destinations:
                        if (new_dest.remote_host == old_dest.remote_host and
                            new_dest.remote_dataset == old_dest.remote_dataset):
                            # Update reference to new destination object
                            self.active_migrations[task_id] = (new_dest, new_ds, start_time)
                            break
                    break

        # Only log if config changed
        if old_count != new_count:
            print(f"Config reloaded: {old_count} -> {new_count} datasets")

    def check_schedule(self):
        now = datetime.now()
        hour = int(now.strftime('%H'))
        weekday = int(now.strftime('%w')) - 1
        if weekday < 0:  # change us weekdays to russian
            weekday = 6

        if int(self.days[weekday]) and int(self.hours[hour]):
            return True
        else:
            return False

    def check_remote_sync_schedule(self):
        if self.remote_sync:
            now = datetime.now()
            hour = int(now.strftime('%H'))
            weekday = int(now.strftime('%w')) - 1
            if weekday < 0:  # change us weekdays to russian
                weekday = 6

            if int(self.remote_sync_days[weekday]) and int(self.remote_sync_hours[hour]):
                return True
            else:
                return False
        else:
            return False


    def cleanup_dataset(self, backup_dataset):
        """Cleanup old snapshots and sync holds for a dataset"""
        dataset = backup_dataset.local_dataset
        
        # Clean up old sync holds - keep only latest per host
        holds = zfs.get_holds(dataset)
        hosts = {}
        for snap in holds.keys():
            tags = holds[snap]
            for tag in tags:
                try:
                    if tag.startswith('sync_'):  # API format: sync_YYYY-MM-DD-HH-MM-SS_hostname
                        parts = tag.split('_')
                        if len(parts) >= 3:
                            host = parts[2]
                            if host not in hosts:
                                hosts[host] = OrderedDict()
                            hosts[host][snap] = tag
                except:
                    pass

        # Keep only latest sync hold per remote host
        for host in hosts.keys():
            for snap in list(hosts[host].keys())[:-1]:
                zfs.release(dataset, snap, hosts[host][snap])

        # Remove old backup snapshots based on retention policy
        zfs.autoremove(dataset=dataset, tags=self.cleanup_tags)

    def remote_sync_async(self):
        """Handle async migrations - start new ones and check status of running ones"""
        # Check status of running migrations
        completed_tasks = []
        completed_datasets = set()  # Track datasets that completed in this cycle
        now = datetime.now()

        for task_id, (destination, backup_dataset, start_time) in self.active_migrations.items():
            try:
                status = zfs.get_migration_status(task_id)

                if status and status.get("status") in ["completed", "failed", "cancelled"]:
                    print(f'Migration {task_id} for {backup_dataset.local_dataset} to {destination.remote_host}: {status.get("status")}')
                    completed_tasks.append(task_id)
                    completed_datasets.add(backup_dataset.local_dataset)  # Track this dataset

                    # Clear current_task_id regardless of success/failure
                    destination.current_task_id = None

                    if status.get("status") == "completed":
                        # Set sync time immediately to prevent re-triggering
                        destination.last_sync_time = datetime.now()

                    # Clean up progress tracking
                    if task_id in self.last_progress_log:
                        del self.last_progress_log[task_id]

                elif status and status.get("status") == "running":
                    # Log progress periodically (every 60 seconds)
                    last_log = self.last_progress_log.get(task_id)
                    if not last_log or (now - last_log).total_seconds() >= 60:
                        progress = status.get("progress", {})
                        percentage = progress.get("percentage", 0)
                        rate_mbps = progress.get("rate_mbps", 0)
                        eta_seconds = progress.get("eta_seconds", 0)

                        eta_min = eta_seconds // 60
                        eta_sec = eta_seconds % 60

                        print(f'Migration {backup_dataset.local_dataset} -> {destination.remote_host}: '
                              f'{percentage:.1f}% complete, {rate_mbps:.2f} Mbps, ETA: {eta_min}m {eta_sec}s')

                        self.last_progress_log[task_id] = now

            except Exception as e:
                print(f'Error checking migration {task_id}: {e}')
                completed_tasks.append(task_id)
                completed_datasets.add(backup_dataset.local_dataset)  # Track this dataset
                # Clear task_id on error too
                destination.current_task_id = None
                if task_id in self.last_progress_log:
                    del self.last_progress_log[task_id]

        # Remove completed migrations
        for task_id in completed_tasks:
            del self.active_migrations[task_id]
        
        # Start new migrations for datasets that need syncing
        for backup_dataset in self.backup_config.get_datasets_with_remote_sync():
            # Update sync times from holds (skip if we just completed a migration for this dataset)
            if backup_dataset.local_dataset not in completed_datasets:
                update_destination_sync_times(backup_dataset)
            else:
                print(f'Skipping update_destination_sync_times for {backup_dataset.local_dataset} (just completed)')

            # Check via API if any migration is already running for this dataset
            if zfs.has_running_migration(backup_dataset.local_dataset):
                print(f'Skipping {backup_dataset.local_dataset}: migration already running')
                continue  # Skip this dataset - already has a migration running

            for destination in backup_dataset.get_enabled_destinations():
                if destination.is_local_only():
                    print(f'Skipping destination: local-only')
                    continue  # Skip local-only destinations

                # Check if migration is needed
                if destination.last_sync_time:
                    time_diff = (datetime.now() - destination.last_sync_time).total_seconds()
                    needs_sync = time_diff >= self.remote_sync_interval
                    print(f'Check {backup_dataset.local_dataset} -> {destination.remote_host}: last_sync={destination.last_sync_time}, time_diff={time_diff:.0f}s, interval={self.remote_sync_interval}s, needs_sync={needs_sync}')
                else:
                    needs_sync = True
                    print(f'Check {backup_dataset.local_dataset} -> {destination.remote_host}: last_sync=None, needs_sync=True')

                if needs_sync:
                    success = remote_sync(backup_dataset, destination)
                    if success and destination.current_task_id:
                        self.active_migrations[destination.current_task_id] = (destination, backup_dataset, datetime.now())
                        break  # Only start one migration per dataset per cycle




    def run(self):
        while not self.interrupt:
            try:
                self.reload_config()

                for backup_dataset in self.backup_config.get_datasets_for_backup():
                    schedule_ok = self.check_schedule()
                    if not schedule_ok:
                        continue

                    # Get backup snapshots and last snapshot time
                    snapshots, last_snapshot_time = get_backup_snapshots(backup_dataset.local_dataset)
                    backup_dataset.snapshots = snapshots
                    backup_dataset.last_snapshot_time = last_snapshot_time

                    # Cleanup old snapshots
                    self.cleanup_dataset(backup_dataset)
                now = datetime.utcnow()

                # Create new snapshot if needed
                if backup_dataset.active:
                    tag = None
                    if not backup_dataset.last_snapshot_time:
                        tag = 'frequent'
                    else:
                        if backup_dataset.last_snapshot_time.strftime('%Y') != now.strftime('%Y'):
                            tag = 'yearly'
                        elif backup_dataset.last_snapshot_time.strftime('%m') != now.strftime('%m'):
                            tag = 'monthly'
                        elif backup_dataset.last_snapshot_time.strftime('%W') != now.strftime('%W'):
                            tag = 'weekly'
                        elif backup_dataset.last_snapshot_time.strftime('%d') != now.strftime('%d'):
                            tag = 'daily'
                        elif backup_dataset.last_snapshot_time.strftime('%H') != now.strftime('%H'):
                            tag = 'hourly'
                        elif now - backup_dataset.last_snapshot_time > timedelta(seconds=self.backup_interval):
                            tag = 'frequent'

                    if tag:
                        # Create snapshot - only log this action
                        try:
                            zfs.set(backup_dataset.local_dataset, 'snapdir', 'visible')
                            rc, name = zfs.snapshot_auto(backup_dataset.local_dataset, tag, tag1='backup')
                            if rc == 0:
                                print(f"Created {tag} snapshot: {backup_dataset.local_dataset}@{name}")
                            else:
                                print(f"ERROR: Failed to create {tag} snapshot for {backup_dataset.local_dataset} (rc={rc})")
                        except Exception as e:
                            print(f"ERROR: Exception creating snapshot for {backup_dataset.local_dataset}: {e}")

                if self.check_remote_sync_schedule():
                    print(f'Remote sync schedule check: OK, checking {len(self.backup_config.get_datasets_with_remote_sync())} datasets')
                    self.remote_sync_async()
                # else:
                #     print(f'Remote sync schedule check: SKIP (enabled={self.remote_sync})')

            except Exception as e:
                # Check if it's an API connection issue
                api_available = zfs.health_check()
                if not api_available:
                    print(f'ERROR: ZFS API is not responding at {self.backup_config.api.url}')
                    print(f'       Backup cycle skipped. Will retry in {self.backup_interval / 10:.0f}s...')
                else:
                    print(f'ERROR: Backup cycle failed: {e}')
                    print(f'       Will retry in {self.backup_interval / 10:.0f}s...')
                # Continue running - don't crash on errors

            time.sleep(self.backup_interval / 10)


if __name__ == '__main__':
    # Get config file from environment variable or use default
    config_file = os.environ.get('CONFIG_FILE', 'backup_config.yaml')

    print(f"Autobackup started: {config_file}")
    back = backup_server(config_file)
    back.run()
