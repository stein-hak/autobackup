# autobackup
Usage: backup_server.py

Requirements: paramiko

To enable autobackup for dataset rpool/example:

zfs set control:autobackup=active rpool/example

To enable remote sync for dataset first copy ssh key to enable passwordless login

ssh-copy-id root@backup-server

And set zfs property as follows:

zfs set control:remote_sync=backup-server rpool/example

Frequency of remote sync and number of backup snapshots stored are defined in backup_server.conf
