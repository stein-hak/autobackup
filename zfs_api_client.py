#!/usr/bin/env python3
"""
ZFS JSON-RPC API Client
Provides same interface as zfs class but uses JSON-RPC API calls
"""

import requests
import json
from datetime import datetime


class ZFSAPIClient:
    """JSON-RPC client that mimics the zfs class interface for backup_server.py"""
    
    def __init__(self, api_url="http://localhost:8545"):
        self.api_url = api_url
        self.session = requests.Session()
        
    def _call_api(self, method, params=None):
        """Make JSON-RPC API call"""
        if params is None:
            params = {}
            
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        
        try:
            response = self.session.post(self.api_url, json=payload, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            if "error" in result:
                raise Exception(f"API Error: {result['error']}")
                
            return result.get("result")
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"API Connection Error: {e}")

    def health_check(self):
        """Check if ZFS API is accessible"""
        try:
            response = self.session.get(f"{self.api_url.rstrip('/')}/health", timeout=5)
            return response.status_code == 200
        except:
            return False

    def set(self, dataset, property, value):
        """Set dataset property"""
        params = {
            "dataset": dataset,
            "property": property,
            "value": value
        }
        result = self._call_api("dataset_set_property", params)
        return 0 if result else 1
    
    def get_holds(self, dataset, recurse=False):
        """Get snapshot holds for dataset - returns dict of {snapshot: [holds]}"""
        # Get all snapshots for the dataset first
        snapshots = self.get_snapshots(dataset)

        holds_dict = {}
        for snap in snapshots:
            try:
                params = {
                    "dataset": dataset,
                    "snapshot": snap
                }
                result = self._call_api("snapshot_holds_list", params)
                if result and isinstance(result, dict) and 'holds' in result:
                    holds = result['holds']
                    if holds:  # Only add if there are actual holds
                        holds_dict[snap] = holds
            except:
                # If no holds on this snapshot, skip it
                pass

        return holds_dict
    
    def get_snapshots(self, dataset):
        """Get list of snapshots for dataset"""
        params = {"dataset": dataset}
        result = self._call_api("snapshot_list", params)
        # API returns {"dataset": "...", "snapshots": [...]}
        if result and isinstance(result, dict) and 'snapshots' in result:
            return result['snapshots']
        return result if result else []
    
    def snapshot_auto(self, dataset, tag, tag1=None, recurse=False):
        """Create auto-named snapshot - exact replica of zfs.py logic"""
        now = datetime.utcnow()
        name = tag

        if tag1:
            name += '_'
            name += str(tag1)
        name += '_'

        time_s = now.strftime('%Y-%m-%d-%H-%M')

        name += time_s

        # Create snapshot via API
        params = {
            "dataset": dataset,
            "name": name,
            "recursive": recurse
        }

        try:
            self._call_api("snapshot_create", params)
            rc = 0
        except Exception as e:
            # Error will be logged by caller
            rc = 1

        return rc, name
    
    def get(self, dataset=None, property='all'):
        """Get dataset properties"""
        if dataset:
            params = {
                "dataset": dataset,
                "property": property
            }
            result = self._call_api("dataset_get_properties", params)
            return result
        else:
            # Get all datasets with property
            datasets = self._call_api("dataset_list", {})
            result = {}
            for ds in datasets:
                try:
                    props = self._call_api("dataset_get_properties", {"dataset": ds, "property": property})
                    if props:
                        result[ds] = props
                except:
                    result[ds] = None
            return result
    
    def type(self, dataset):
        """Get dataset type"""
        params = {"dataset": dataset}
        try:
            props = self._call_api("dataset_get_properties", params)
            if props and 'type' in props:
                dataset_type = props['type']
                # Check if it's a clone by looking for origin property
                origin = props.get('origin', '-')
                if origin != '-':
                    return [dataset_type, 'clone']
                else:
                    return [dataset_type, 'original']
            return None
        except:
            return None
    
    def release(self, dataset, snapshot, tag, recurse=False):
        """Release snapshot hold"""
        params = {
            "dataset": dataset,
            "snapshot": snapshot,
            "tag": tag,
            "recursive": recurse
        }
        try:
            self._call_api("snapshot_release", params)
            return 0
        except:
            return 1
    
    def autoremove(self, dataset, keep=2, tag=None, recurse=False, tags={}):
        """Remove old snapshots - exact replica of zfs.py logic"""
        snaps = self.get_snapshots(dataset)

        if tag:
            new_snaps = []
            for snap in snaps:
                if tag in snap:
                    new_snaps.append(snap)
            snaps = new_snaps

            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        elif tags:
            new_snaps = []
            for tag in tags.keys():
                keep = tags[tag]
                tag_snaps = []
                for snap in snaps:
                    if tag in snap:
                        tag_snaps.append(snap)

                if tag_snaps and len(tag_snaps) > keep:
                    del tag_snaps[len(tag_snaps) - keep:]
                    new_snaps.extend(tag_snaps)

            snaps = new_snaps

        else:
            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        for snap in snaps:
            try:
                params = {
                    "dataset": dataset,
                    "snapshot": snap,
                    "recursive": recurse
                }
                self._call_api("snapshot_destroy", params)
            except:
                pass

    def is_zfs(self, dataset):
        """Check if dataset exists"""
        try:
            params = {"dataset": dataset}
            result = self._call_api("dataset_get_properties", params)
            return result is not None
        except:
            return False

    def snapshot(self, dataset, snap, recurse=False):
        """Create snapshot"""
        params = {
            "dataset": dataset,
            "snapshot": snap,
            "recursive": recurse
        }
        try:
            self._call_api("snapshot_create", params)
            return 0
        except:
            return 1

    def hold(self, dataset, snapshot, tag, recurse=False):
        """Place hold on snapshot"""
        params = {
            "dataset": dataset,
            "snapshot": snapshot,
            "tag": tag,
            "recursive": recurse
        }
        try:
            self._call_api("snapshot_hold", params)
            return 0
        except:
            return 1

    def destroy(self, dataset, recurse=False):
        """Destroy dataset or snapshot"""
        if '@' in dataset:
            ds, snap = dataset.split('@')
            params = {
                "dataset": ds,
                "snapshot": snap,
                "recursive": recurse
            }
            try:
                self._call_api("snapshot_destroy", params)
                return 0
            except:
                return 1
        else:
            params = {
                "dataset": dataset,
                "recursive": recurse
            }
            try:
                self._call_api("dataset_destroy", params)
                return 0
            except:
                return 1

    def engociate_inc_send(self, dataset, recv_snapshots=[]):
        """No longer needed - API handles negotiation automatically"""
        snapshots = self.get_snapshots(dataset)
        if snapshots:
            return snapshots[0], snapshots[-1]
        return None, None

    def start_migration(self, dataset, remote_host, remote_dataset=None, snap=None, 
                       recurse=True, compression=None, verbose=False):
        """Start an async migration and return task_id"""
        try:
            # Use provided snapshot or get latest if none specified
            if snap:
                source_snap = snap
            else:
                snapshots = self.get_snapshots(dataset)
                if not snapshots:
                    if verbose:
                        print(f"No snapshots found for {dataset}")
                    return None
                source_snap = snapshots[-1]
            
            # API expects source as dataset@snapshot
            source = f"{dataset}@{source_snap}"
            destination = remote_dataset or dataset
            
            params = {
                "source": source,
                "destination": destination,
                "remote": remote_host,
                "recursive": recurse
            }
            
            if compression:
                params["compression"] = compression
            
            if verbose:
                print(f"Starting API migration: {source} -> {remote_host}:{destination}")
            
            # Returns {"task_id": "uuid", "status": "pending", "created_at": "..."}
            result = self._call_api("migration_create", params)
            return result.get("task_id") if result else None
            
        except Exception as e:
            if verbose:
                print(f"Failed to start migration: {e}")
            return None

    def get_migration_status(self, task_id):
        """Check status of a running migration"""
        try:
            params = {"task_id": task_id}
            result = self._call_api("migration_get", params)
            return result
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def has_running_migration(self, dataset):
        """Check if dataset has any running migrations via API"""
        try:
            result = self._call_api("migration_list", {})
            if result and 'tasks' in result:
                for task in result['tasks']:
                    if task.get('status') in ['pending', 'running']:
                        # Check if source dataset matches
                        params = task.get('params', {})
                        source = params.get('source', '')
                        # Extract dataset from source (format: dataset@snapshot)
                        if '@' in source:
                            task_dataset = source.split('@')[0]
                            if task_dataset == dataset:
                                return True
            return False
        except Exception as e:
            # On error, assume no migration running
            return False

    def adaptive_send(self, dataset=None, snap=None, snap1=None, recurse=True, 
                     compression=None, verbose=False, limit=0, time=0, out_fd=None, 
                     resume_token=None, remote_host=None, remote_dataset=None):
        """
        Legacy interface - starts migration and returns task_id for tracking
        """
        class MigrationResult:
            def __init__(self, task_id=None):
                self.stdout = None
                self.returncode = 0 if task_id else 1
                self.task_id = task_id  # Store task_id for tracking
            
            def communicate(self):
                return None, None
        
        # Start the migration using the provided snapshot
        task_id = self.start_migration(
            dataset=dataset,
            remote_host=remote_host, 
            remote_dataset=remote_dataset,
            snap=snap,  # Use the specific snapshot if provided
            recurse=recurse,
            compression=compression,
            verbose=verbose
        )
        
        return MigrationResult(task_id=task_id)


# Create instance to match original usage pattern
zfs = ZFSAPIClient()