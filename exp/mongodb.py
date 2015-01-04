# Copyright (c) 2014, Robert Escriva
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of this project nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
