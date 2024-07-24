#!/usr/bin/env python
import paramiko
from ping import fping
import os
from scp import SCPClient


class ssh:
	def __init__(self, host, user='root', passwd='mttadmin',deploy_key=True):
		#paramiko.Transport._preferred_ciphers = ('arcfour128',)
		self.state = 0
		self.host = host
		self.user=user

		alive, dead = fping([host])
		if host in alive:
			self.client = paramiko.SSHClient()
			self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

			try:
				self.client.connect(hostname=host, username=user, key_filename='/root/.ssh/authorized_keys')
				self.client.get_transport().window_size = 3 * 1024 * 1024
				self.state=1
				#self.scp_client = SCPClient(self.client.get_transport())
			except:
				print('Key authorization failed, trying password')
				try:
					self.client.connect(hostname=host, username=user,password=passwd)
					self.client.get_transport().window_size = 3 * 1024 * 1024
					self.state=1
					#self.scp_client = SCPClient(self.client.get_transport())
					if deploy_key == True:
						print('Auto deploying key for user %s' % user)
						ret = self.copy_key()
						if ret == 1:
							print('Key deployed successfull')
						else:
							print('Error deploying key')
				except:
					print('Password autorization failed. exiting')
					self.state = 0
		else:
			self.state = 0


	def execute(self, cmd):
		out = []
		stdin, stdout, stderr = self.client.exec_command(cmd)
		for line in stdout.readlines():
			out.append(line.decode())

		return out
	
	def put(self,file_name,dest=None):
		if dest:
			self.scp_client.put(file_name,remote_path = dest)
		else:
			self.scp_client.put(file_name)

	def copy_key(self):
		try:
			key = open(os.path.expanduser('~/.ssh/id_rsa.pub')).read()
		except:
			key = None
			print('No public gey generated. Use ssh-keygen for user')

		if key:
			self.execute('mkdir -p ~/.ssh/')
			self.execute('echo "%s" >> ~/.ssh/authorized_keys' % key)
			self.execute('chmod 644 ~/.ssh/authorized_keys')
			self.execute('chmod 700 ~/.ssh/')
			return 1
		else:
			return 0



	def __kill__(self):
		self.client.close()
		self.scp_client.close()


if __name__ == '__main__':
	new = ssh('192.168.19.9')
	new.put('/root/config_txt_squash.txt','/run/boot/config.txt')
