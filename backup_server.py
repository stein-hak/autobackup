#!/usr/bin/python3
# -*- coding: utf-8 -*-
import sys

sys.path.append('/opt/lib/')
from zfs import zfs
from datetime import datetime, timedelta
from config_parser import my_conf
from threading import Thread

import time
from collections import OrderedDict
from ssh_new import ssh_send
from multiprocessing import Pool
from functools import partial
from event import send_event


zfs = zfs()


def remote_sync(host,fs):
    ret = {}
    last_remote_sync_time = fs.last_remote_sync_time[host]
    recv_dataset = fs.remote_sync_hosts[host]
    sender = ssh_send(host=host,dataset=fs.fs,verbose=False,sync=True,recv_dataset=recv_dataset)
    print('Performing remote sync for %s to %s' % (fs.fs,host))
    if sender.state == 1:
        try:
            sender.send_snapshot()
        except:
            pass

    fs.get_remote_sync()
    if last_remote_sync_time != fs.last_remote_sync_time[host]:
        ret[host] = (1,fs.fs)
        print('Remote sync for %s to %s successfull' % (fs.fs,host))
    else:
        print('Remote sync for %s to %s failed' % (fs.fs, host))
        ret[host] = (0,fs.fs)

    return ret




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
    def __init__(self):
        Thread.__init__(self)
        self.conf = my_conf('/opt/autobackup/backup_server.conf')
        self.days = self.conf.get('general', 'days', '1111111')
        self.hours = self.conf.get('general', 'hours', '111111111111111111111111')
        self.backup_interval = self.conf.getint('general', 'backup_interval', 600)
        self.keep_frequent = self.conf.getint('general', 'keep_frequent', 4)
        self.keep_hourly = self.conf.getint('general', 'keep_hourly', 12)
        self.keep_daily = self.conf.getint('general', 'keep_daily', 7)
        self.keep_weekly = self.conf.getint('general', 'keep_weekly', 4)
        self.keep_monthly = self.conf.getint('general', 'keep_monthly', 6)
        self.keep_yearly = self.conf.getint('general', 'keep_yearly', 3)

        self.remote_sync = self.conf.getboolean('general','remote_sync',False)
        self.remote_sync_days = self.conf.get('general', 'remote_sync_days', '1111111')
        self.remote_sync_hours = self.conf.get('general', 'remot_sync_hours', '111111111111111111111111')
        self.remote_sync_interval = self.conf.getint('general','remote_sync_interval',86400)

        self.cleanup_tags = {'frequent': self.keep_frequent, 'hourly': self.keep_hourly, 'daily': self.keep_daily,
                             'monthly': self.keep_monthly, 'yearly': self.keep_yearly,'weekly':self.keep_weekly}

        self.fs_to_backup = {}
        self.interrupt = False
        self.pool = None
        self.remote_sync_workers = {}

    def reload_fs(self):
        all = zfs.get(property='control:autobackup')
        for i in all.keys():
            fs = i
            value = all[i]
            try:
                type = zfs.type(fs)[0]
            except:
                type = None
            if type and type != 'snapshot':
                if fs not in self.fs_to_backup.keys():
                    if value:
                        if value == 'active':
                            remote_sync = zfs.get(fs,'control:remote_sync')
                            remote_sync_hosts = {}
                            for i in remote_sync.split(','):
                                if len(i.split(':')) > 1:
                                    host = i.split(':')[0]
                                    dataset = i.split(':')[1]
                                else:
                                    host = i
                                    dataset = None
                                remote_sync_hosts[host] = dataset
                            
                            if remote_sync_hosts and self.remote_sync:
                                back_fs = backup_fs(fs=fs, type=type, active=True,remote_sync=True,remote_sync_hosts=remote_sync_hosts)
                            else:
                                back_fs = backup_fs(fs=fs,type=type, active=True)

                            back_fs.get_backup_snapshots()
                            self.fs_to_backup[fs] = back_fs
                        elif value == 'passive':
                            back_fs = backup_fs(fs=fs, type=type, active=False)
                            back_fs.get_backup_snapshots()
                            self.fs_to_backup[fs] = back_fs

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


    def cleanup_time(self, fs):
        holds = zfs.get_holds(fs.fs)
        hosts = {}
        for snap in holds.keys():
            tags = holds[snap]
            for tag in tags:
                try:
                    parts = tag.split('_')
                    if parts[0] == 'sync': #and datetime.strptime(parts[1], '%Y-%m-%d-%M-%S'):
                        host = parts[2]
                        if host not in hosts.keys():
                            hosts[host] = OrderedDict()
                            hosts[host][snap] = tag
                except:
                    pass

        for host in hosts.keys(): # Keep only one sync hold per remote host
            for snap in list(hosts[host].keys())[:-1]:
                zfs.release(fs,snap,hosts[host][snap])

        zfs.autoremove(dataset=fs.fs, tags=self.cleanup_tags)

    def remote_sync_mp(self):
        fs_to_sync = {}
        if not self.remote_sync_workers:
            for fs in list(self.fs_to_backup.values()):
                if fs.remote_sync and fs.remote_sync_hosts:
                    fs.get_remote_sync()
                    for host in fs.remote_sync_hosts:
                        if not fs.last_remote_sync_time[host] or (datetime.now() - fs.last_remote_sync_time[host] >= timedelta(seconds=self.remote_sync_interval)):
                            if not fs_to_sync.get(fs):
                                fs_to_sync[fs] = [host]
                            else:
                                fs_to_sync[fs].append(host)

            if not fs_to_sync:
                if self.pool:
                    self.pool.close()
                    self.pool = None


            else:
                if not self.pool:
                    self.pool = Pool(2)
                for fs in fs_to_sync.keys():
                    sync = partial(remote_sync,fs=fs)
                    map = self.pool.map_async(sync,fs_to_sync[fs])
                    self.remote_sync_workers[fs] = map
        else:
            for fs in list(self.remote_sync_workers.keys()):
                if self.remote_sync_workers[fs].ready():
                    ret = self.remote_sync_workers[fs].get()
                    del self.remote_sync_workers[fs]




    def run(self):
        while not self.interrupt:
            self.reload_fs()

            for fs in self.fs_to_backup.values():
                if self.check_schedule():
                    fs.get_backup_snapshots()
                    self.cleanup_time(fs)
                    now = datetime.utcnow()

                    if fs.active:
                        tag = None
                        if not fs.last_snapshot_time:
                            tag = 'frequent'
                        else:
                            if fs.last_snapshot_time.strftime('%Y') != now.strftime('%Y'):
                                tag = 'yearly'
                            elif fs.last_snapshot_time.strftime('%m') != now.strftime('%m'):
                                tag = 'monthly'
                            elif fs.last_snapshot_time.strftime('%W') != now.strftime('%W'):
                                tag = 'weekly'
                            elif fs.last_snapshot_time.strftime('%d') != now.strftime('%d'):
                                tag = 'daily'
                            elif fs.last_snapshot_time.strftime('%H') != now.strftime('%H'):
                                tag = 'hourly'
                            elif now - fs.last_snapshot_time > timedelta(seconds=self.backup_interval):
                                tag = 'frequent'

                        if tag:
                            fs.auto_snap(tag=tag)

            if self.check_remote_sync_schedule():
                self.remote_sync_mp()



            time.sleep(self.backup_interval / 10)


if __name__ == '__main__':
    back = backup_server()
    back.run()
