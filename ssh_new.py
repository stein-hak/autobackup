#!/usr/bin/env python

from subprocess import Popen,PIPE
from zfs import zfs, zpool
from compression import uncompress_ssh_recv
from ping import fping
from datetime import datetime
from execute import execute as execute_local
from collections import OrderedDict
from ssh import ssh



class ssh_send(ssh):
	def __init__(self, host, dataset=None,recv_dataset=None,snapshot=None,compress_type='auto',limit=0,time=0,recurse=True, verbose=False, user='root', passwd='ntt',oneshot=False,create_snapshot=False,update_bootfs=False,sync=False,reverse=False):
		ssh.__init__(self,host)

		self.dataset = dataset
		self.recv_dataset = recv_dataset
		self.snapshot = snapshot
		self.compress_type=compress_type
		self.limit=limit
		self.time=time
		self.recurse = recurse
		self.verbose=verbose
		self.oneshot=oneshot
		self.create_snapshot = create_snapshot
		self.host=host
		self.update_bootfs=update_bootfs
		self.sync= sync
		self.backup_stream=False
		self.reverse=reverse

		self.zfs = zfs()
		self.zpool = zpool()

		self.get_auto_compression()

		if self.update_bootfs:
			print('Updating bootfs for server %s' % self.host)
			if not self.dataset:
				bootfs_raw = self.zpool.get(zpool=None,property='bootfs')
				if len(bootfs_raw.keys()) == 1:
					self.dataset = bootfs_raw.values()[0].split('@')[0]
				else:
					if 'syspool' in bootfs_raw.keys():
						self.dataset = bootfs_raw['syspool'].split('@')[0]
					elif 'archive' in bootfs_raw.keys():
						self.dataset = bootfs_raw['archive'].split('@')[0]

				print('Local bootfs dataset %s' % self.dataset)

			if not self.recv_dataset:
				bootfs_raw = self.get_remote_bootfs()
				if len(bootfs_raw.keys()) == 1:
					self.recv_dataset = bootfs_raw.values()[0].split('@')[0]
				else:
					if 'syspool' in bootfs_raw.keys():
						self.recv_dataset = bootfs_raw['syspool'].split('@')[0]
					elif 'archive' in bootfs_raw.keys():
						self.recv_dataset = bootfs_raw['archive'].split('@')[0]

				print('Remote bootfs dataset %s' % self.recv_dataset)

		if self.zfs.is_zfs(self.dataset):
			self.state = 1
			backup = self.zfs.get(self.dataset,'control:autobackup')
			if backup and backup == 'active':
				if self.verbose:
					print('Setting up backup stream')
				self.backup_stream = True
				self.sync = True
				if not self.recv_dataset:
					self.recv_dataset = self.dataset




	def execute(self, cmd):
		stdin, stdout, stderr = self.client.exec_command(cmd)
		out = stdout.read().decode('utf-8')

		return out


	def get_auto_compression(self):
		if self.compress_type == 'auto':
			out, err , rc = execute_local(['zstd', '-h'])
			out = self.execute('type zstd')
			if rc == 0 and out:
				print('Found auto compression type: zstd')
				self.compress_type = 'zstd'
			else:
				out, err, rc = execute_local(['lz4c', '-h'])
				out = self.execute('type lz4c')
				if rc == 0 and out:
					print('Found auto compression type: lz4')
					self.compress_type = 'lz4'
				else:
					print('No compression type found, using uncompressed stream')
					self.compress_type = None


	def is_zfs(self, dataset):
		out = self.execute('zfs list -H -p ' + dataset)
		list = []
		if not out:
			return False
		else:
			for i in out.split('\t'):
				list.append(i)
			if list and list[0] == dataset:
				return True
			else:
				return False

	def get_snapshots(self,dataset):
		result = []
		out =  self.execute('zfs list -t snapshot -H -o name ')
		for i in out.splitlines():
			if dataset == i.split('@')[0]:
				result.append(i.split('@')[1])

		return result

	def destroy(self,dataset):
		result = []
		out = self.execute('zfs destroy -r %s' % dataset)
		return out

	def get_remote_bootfs(self):
		list = {}
		out = self.execute('zpool get bootfs -H -p')
		print(out)
		for i in out.splitlines():
			if i.split('\t')[2] != '-':
				list[i.split('\t')[0]] = i.split('\t')[2]
		return list

	def zfs_set_remote(self,dataset,property,value):
		cmd = 'zfs set %s=%s %s' % (property,value,dataset)
		out = self.execute(cmd)
		return out

	def zfs_hold_remote(self,dataset,tag,snapshot):
		cmd = 'zfs hold %s %s@%s' % (tag,dataset,snapshot)
		out = self.execute(cmd)
		return out

	def zfs_get_remote(self,dataset,property):
		cmd = 'zfs get -H -p %s %s' % (property, dataset)
		out = self.execute(cmd)
		if out.rstrip().split('\t')[2] != '-':
			value = out.rstrip().split('\t')[2]
		else:
			value = None
		return value

	def get_remote_send(self,dataset,snapshot,compression=None):
		pass


	


	def cleanup_sync(self):
		holds = self.zfs.get_holds(self.dataset)
		host_holds = OrderedDict()

		for snap in holds.keys():

			tags = holds[snap]
			for tag in tags:
				try:
					parts = tag.split('_')
					if parts[0] == 'sync' and parts[2] == self.host:
						#sync_date = datetime.strptime(parts[1], '%Y-%m-%d-%M-%S')
						host_holds[snap] = tag
				except:
					pass

		if self.verbose and host_holds:
			print ('Found %i sync points for host %s' % (len(host_holds.keys()), self.host))
			last_snap = host_holds.keys()[-1]
			print ('Latest was snapshot %s ' % (last_snap))

		for i in list(host_holds.keys())[:-1]:
			self.zfs.release(self.dataset,i,host_holds[i])


	def send_snapshot(self):
		if self.is_zfs(self.recv_dataset):
			resume_token = self.zfs_get_remote(self.recv_dataset,'receive_resume_token')
			if resume_token:
				if self.verbose:
					print('Resuming interrupted send')
				send = self.zfs.adaptive_send(dataset=self.dataset, recurse=self.recurse,
				                              verbose=self.verbose, compression=self.compress_type,
				                              limit=self.limit, time=self.time,resume_token=resume_token)
				recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type,
				                           dataset=self.recv_dataset)

				recv.communicate()
				rc = recv.returncode
				if rc != 0:
					print('Resuming interrupted send failed. Trying basic')
					recv_snaps = self.get_snapshots(self.recv_dataset)
					if recv_snaps:
						snap, snap1 = self.zfs.engociate_inc_send(self.dataset, recv_snaps)


						if snap != snap1:
								if self.verbose:
									print('Performing incremental send from %s to %s' % (snap,snap1))



								send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1, recurse=self.recurse,
															 verbose=self.verbose, compression=self.compress_type,
															 limit=self.limit, time=self.time)

								recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, dataset=self.recv_dataset)

								recv.communicate()

			else:

				recv_snaps = self.get_snapshots(self.recv_dataset)


				if recv_snaps:
					snap, snap1 = self.zfs.engociate_inc_send(self.dataset, recv_snaps)

					if snap != snap1:
							if self.verbose:
								print('Performing incremental send from %s to %s' % (snap,snap1))



							send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1, recurse=self.recurse,
														 verbose=self.verbose, compression=self.compress_type,
														 limit=self.limit, time=self.time)

							recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, dataset=self.recv_dataset)

							recv.communicate()

							if self.sync:
								tag = "sync" + '_'+ datetime.now().strftime('%Y-%m-%d-%H-%M-%S')+'_'+ self.host
								self.zfs.hold(self.dataset,snapshot=snap1,tag=tag)
								self.zfs_hold_remote(self.recv_dataset,snapshot=snap1,tag=tag)
								self.cleanup_sync()

							if self.update_bootfs:
								pool = self.recv_dataset.split('/')[0]
								bootfs = self.recv_dataset + '@' + snap1
								cmd = 'zpool set bootfs=%s %s' % (bootfs,pool)
								out = self.execute(cmd)

							if self.backup_stream:
								self.zfs_set_remote(self.recv_dataset,'control:autobackup','passive')




					else:
						if self.verbose:
							print('Target dataset is up to date on %s' % snap)

						if self.create_snapshot:
							now = datetime.now()
							date = now.strftime('%y%m%d')
							hour = now.strftime('%H')
							minsec = now.strftime('%M%S')
							snap1 = 'migrate-%s-%s-%s' % (date, hour, minsec)
							if self.verbose:
								print('Creating snapshot for send %s' % snap1)
							self.zfs.snapshot(self.dataset, snap1, self.recurse)
							if self.verbose:
								print('Performing incremental send from %s to %s' % (snap,snap1))
							send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, snap1=snap1,
														  recurse=self.recurse,
														  verbose=self.verbose, compression=self.compress_type,
														  limit=self.limit, time=self.time)
							recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type,
													   dataset=self.recv_dataset)

							recv.communicate()

							if self.sync:
								tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
								self.zfs.hold(self.dataset, snapshot=snap1, tag=tag)
								self.zfs_hold_remote(self.recv_dataset, snapshot=snap1, tag=tag)
								self.cleanup_sync()

							if self.backup_stream:
								self.zfs_set_remote(self.recv_dataset,'control:autobackup','passive')

				else:
					if self.verbose:
						print('Unable to setup incremental stream for zfs send. Recv dataset has no snapshots')
					pass

		else:
			if self.snapshot and self.snapshot in self.zfs.get_snapshots(self.dataset):
				if self.verbose:
					print('Sending full snapshot %s' % self.snapshot)
				send = self.zfs.adaptive_send(dataset=self.dataset, snap=self.snapshot, recurse=self.recurse, verbose=self.verbose,
											 compression=self.compress_type,
											 limit=self.limit, time=self.time)

				recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type, dataset=self.recv_dataset)

				recv.communicate()
				if self.backup_stream:
					self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')
			else:
				if not self.create_snapshot and self.zfs.get_snapshots(self.dataset):
					snap_list = self.zfs.get_snapshots(self.dataset)
					if snap_list:
						snap = snap_list[-1]
						if self.verbose:
							print('Sending full snapshot %s' % snap)
						send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap, recurse=self.recurse,
													  verbose=self.verbose,
													  compression=self.compress_type,
													  limit=self.limit, time=self.time)

						recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type,
												   dataset=self.recv_dataset)

						recv.communicate()

						if self.sync:
							tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
							self.zfs.hold(self.dataset, snapshot=snap, tag=tag)
							self.zfs_hold_remote(self.recv_dataset, snapshot=snap, tag=tag)
							self.cleanup_sync()

						if self.backup_stream:
							self.zfs_set_remote(self.recv_dataset,'control:autobackup','passive')

				else:
					now = datetime.now()
					date = now.strftime('%y%m%d')
					hour = now.strftime('%H')
					minsec = now.strftime('%M%S')
					snap_name = 'migrate-%s-%s-%s' % (date,hour,minsec)
					if self.verbose:
						print('Creating snapshot for send %s' % snap_name)
					self.zfs.snapshot(self.dataset,snap_name,self.recurse)

					if self.verbose:
						print('Sending full snapshot %s' % snap_name)

					send = self.zfs.adaptive_send(dataset=self.dataset, snap=snap_name, recurse=self.recurse,
												  verbose=self.verbose,
												  compression=self.compress_type,
												  limit=self.limit, time=self.time)

					recv = uncompress_ssh_recv(send.stdout, host=self.host, type=self.compress_type,
											   dataset=self.recv_dataset)

					recv.communicate()

					if self.oneshot:
						if self.verbose:
							print('Oneshot send. Cleaning up snapshot %s' % snap_name)
						self.zfs.destroy(self.dataset+'@'+snap_name,recurse=self.recurse)
						self.destroy(self.recv_dataset+'@'+snap_name)

					elif self.sync:
						tag = "sync" + '_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '_' + self.host
						self.zfs.hold(self.dataset, snapshot=snap_name, tag=tag)
						self.zfs_hold_remote(self.recv_dataset, snapshot=snap_name, tag=tag)
						self.cleanup_sync()

					if self.backup_stream:
						self.zfs_set_remote(self.recv_dataset, 'control:autobackup', 'passive')




	def __kill__(self):
		self.client.close()


if __name__ == '__main__':
	recv_dataset = 'archive/ubuntu-zfs'
	send_dataset = 'archive/ubuntu-zfs'
	host = '192.168.1.245'
	snap = '019.018.02.18'
	recurse=False
	verbose=True
	limit = 0
	time = 0
	comp = 'lz4'

	new = ssh_send(host,dataset=send_dataset,snapshot=snap,limit=limit,time=time,recv_dataset=recv_dataset,verbose=verbose)


	if new.state == 1:
		new.send_snapshot()

#print new.send_snapshot('archive/base_head@soft_1004')
