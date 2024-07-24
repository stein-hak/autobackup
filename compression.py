import sys
import os
from subprocess import Popen, PIPE


def compressor(fd,type='gzip',level=6,shell=False,out_fd=None):
    ret = None
    cmd = []
    if type =='gzip':
        cmd = ['gzip','-c']

    elif type == 'lz4':
        level = 1
        cmd = ['lz4c','-c']

    elif type == 'bzip2':
        cmd = ['bzip2','-c']

    elif type == 'xz':
        cmd = ['xz', '-c']

    elif type == 'zstd':
        cmd = ['zstd']


    if level and cmd:
        cmd.append('-' + str(level))

    if shell:
        ret = ''
        for i in cmd:
            ret += i + ' '

    else:
        ret = cmd
    
    if ret:
        if not out_fd:
            p = Popen(ret, stdin=fd, stdout=PIPE, shell=shell)
        else:
            p = Popen(ret, stdin=fd,stdout=out_fd,shell=shell)
        
        return p
    
    else:
        return None
    

def uncompressor(fd, type='gzip',shell=False,out_fd=None):
    ret = None
    cmd = []
    if type == 'gzip':
        cmd = ['gzip', '-d']

    elif type == 'lz4':
        cmd = ['lz4c', '-d']

    elif type == 'bzip2':
        cmd = ['bzip2', '-d']

    elif type == 'xz':
        cmd = ['xz', '-d']

    elif type == 'zstd':
        cmd = ['zstd','-d','-c']

    if shell:
        ret = ''
        for i in cmd:
            ret += i + ' '

    else:
        ret = cmd
        
    if ret:
        print(ret)
        if not out_fd:

            p = Popen(ret, bufsize=128 * 1024 * 1024, stdin=fd, stdout=PIPE, shell=shell)

        else:
            p = Popen(ret, bufsize=128 * 1024 * 1024, stdin=fd, stdout=out_fd, shell=shell)

        return p

    else:
        return None



def uncompressor_file(file,type='gzip',shell=False):
    ret = None
    cmd = []
    if os.path.isfile(file):
        if type == 'gzip':
            cmd = ['zcat', '-c', file ]

        elif type == 'lz4':
            cmd = ['lz4cat', '-c', file]

        elif type == 'bzip2':
            cmd = ['bzcat', '-c', file]

        elif type == 'xz':
            cmd = ['xzcat', '-c', file]

        elif type == 'zstd':
            cmd = ['xzcat','-c',file]

        if shell:
            ret = ''
            for i in cmd:
                ret += i + ' '

        else:
            ret = cmd

        if ret:
            p = Popen(ret, bufsize=128 * 1024 * 1024, stdout=PIPE, shell=shell)

            return p

        else:
            return None

def uncompress_ssh_recv(fd,host,dataset,type=None,force=False):
    cmd ='ssh -oStrictHostKeyChecking=no root@%s ' % host

    if type == 'lz4':
        cmd += "'lz4c -d | zfs recv -s -F %s'" % dataset

    elif type == 'gzip':
        cmd += "'gzip -d | zfs recv -s -F %s'" % dataset

    elif type == 'bzip2':
        cmd += "'bzip2 -d | zfs recv -s -F %s'" % dataset

    elif type == 'xz':
        cmd += "'xz -d | zfs recv -s -F %s'" % dataset

    elif type == 'zstd':
        cmd += "'zstd -d -c | zfs recv -s -F %s'" % dataset

    elif type == None:
        cmd += "'zfs recv -s -F %s'" % dataset

    else:
        cmd += "'zfs recv -s -F %s'" % dataset


    if cmd:
        p = Popen(cmd,stdin=fd,shell=True)
        return p

    else:
        pass


def ssh_recv_local(fd,dataset,type=None,force=False):
    cmd = ['zfs','recv','-F',dataset]
    if cmd:
        p = Popen(cmd,stdin=fd)
        return p

    else:
        pass



def compress_ssh_remote_send(host,dataset,snapshot,type=None):
    cmd = "ssh root@%s 'zfs send %s@%s " % (host,dataset,snapshot)

    if type == 'lz4':
        cmd += "| lz4c -c '"

    elif type == 'gzip':
        cmd += "| gzip -c '"

    elif type == 'bzip2':
        cmd += "| bzip2 -c '"

    elif type == 'xz':
        cmd += "| xz -c '"

    elif type == 'zstd':
        cmd += "| zstd -c '"

    else:
        cmd += "'"


    if cmd:
        p = Popen(cmd,stdout=PIPE,shell=True)
        return p

    else:
        pass





if __name__ == '__main__':
    p = compress_ssh_remote_send(host='192.168.0.242',dataset='archive/images/win7',snapshot='migrate-200309-15-0307',type='lz4')