# Copyright (c) 2014, Robert Escriva
# All rights reserved.

import time

from ygor import Experiment
from ygor import Host
from ygor import HostSet
from ygor import Parameter
from ygor import Utility


class MongoDB(Experiment):

    REPL_FACTOR = Parameter(2)

    CONFIGS = HostSet('configs')
    DAEMONS = HostSet('daemons')
    ROUTERS = HostSet('routers')

    @Utility
    def clean(self):
        # ensure everything is dead
        self.CONFIGS.run(('pkill', '-9', 'mongod'), status=None)
        self.DAEMONS.run(('pkill', '-9', 'mongod'), status=None)
        self.ROUTERS.run(('pkill', '-9', 'mongos'), status=None)
        # erase the data
        self.CONFIGS.run(('rm', '-rf', 'config.log'), status=None)
        self.CONFIGS.run(('rm', '-rf', 'config-data'), status=None)
        self.ROUTERS.run(('rm', '-rf', 'router.log'), status=None)
        self.DAEMONS.run(('rm', '-rf', 'daemon.log'), status=None)
        self.DAEMONS.run(('rm', '-rf', 'daemon-data'), status=None)

    @Utility
    def start(self):
        assert(len(self.CONFIGS) == 3)
        assert(len(self.ROUTERS) == len(self.DAEMONS))
        assert(len(self.DAEMONS) % self.REPL_FACTOR.as_int() == 0)
        self.CONFIGS.run(('mkdir', '-p', 'config-data'))
        self.CONFIGS.run(('mongod', '--configsvr', '--fork',
                          '--logpath', 'config.log',
                          '--dbpath', 'config-data', '--port', '27019'))
        config = ','.join(['%s:27019' % x.location for x in self.CONFIGS.hosts])
        self.ROUTERS.run(('mongos', '--configdb', config, '--fork',
                          '--logpath', 'router.log'))
        def replSet(x):
            return 'rs%d' % (x / self.REPL_FACTOR.as_int())
        self.DAEMONS.run(('mkdir', '-p', 'daemon-data'))
        self.DAEMONS.run_many(('mongod', '--fork',
                               '--port', '27018',
                               '--replSet', HostSet.Index(replSet),
                               '--logpath', 'daemon.log',
                               '--dbpath', 'daemon-data'))
        def primary(x):
            return x % self.REPL_FACTOR.as_int() == 0
        self.DAEMONS.run_many(('echo-pipe', 'rs.initiate()', 'mongo', '--port', '27018'),
                               cond=primary)
        if self.REPL_FACTOR.as_int() > 0:
            time.sleep(15)
            for i in range(1, self.REPL_FACTOR.as_int()):
                def replNode(x):
                    assert(x % self.REPL_FACTOR.as_int() == 0)
                    return 'rs.add("%s:27018")' % self.DAEMONS.hosts[x + i].location
                self.DAEMONS.run_many(('echo-pipe', HostSet.Index(replNode), 'mongo', '--port', '27018'),
                                       cond=primary)
        for i in range(len(self.DAEMONS) / self.REPL_FACTOR.as_int()):
            time.sleep(15)
            def shardName(x):
                return 'sh.addShard("rs%d/%s:27018")' % \
                    (i, self.DAEMONS.hosts[i * self.REPL_FACTOR.as_int()].location)
            self.ROUTERS.run_many(('echo-pipe', HostSet.Index(shardName), 'mongo', '--port', '27017',),
                                  number=1)

    @Utility
    def reset(self):
        self.clean()
        self.start()


"""
def initiate_mongo_replicaset(host, setname):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USERNAME)
    stdin, stdout, stderr = ssh.exec_command(
        '''echo 'rs.initiate()' | %s %s:27018/admin''' \
        % (MONGO, host))
    stdin.flush()
    return (ssh, stdin, stdout, stderr)

def add_shard(host, setname):
    print 'Adding shard %s/%s' % (setname, host)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=USERNAME)
    stdin, stdout, stderr = ssh.exec_command(
        '''echo 'db.runCommand( { addshard : "%s/%s:27018" } );' \
           | %s %s:27017/admin''' \
        % (setname, host, MONGO, host))
    stdin.flush()
    print_stdout_stderr(stdout, stderr)
    ssh.close()
    print

"""
