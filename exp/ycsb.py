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


import os.path

from ygor import Experiment
from ygor import Host
from ygor import HostSet
from ygor import Parameter
from ygor import Environment
from ygor import Utility


class YCSBCore(Experiment):

    # Experimental Setup
    FIELD_COUNT     = Parameter(10)
    FIELD_LENGTH    = Parameter(100)
    RECORD_COUNT    = Parameter(1000000)
    OPERATION_COUNT = Parameter(1000000)
    DATABASE        = Parameter('org.hyperdex.ycsb.HyperDex')
    THREADS         = Parameter(64)

    JAVA_ARGS       = Environment('')
    WORKLOADS       = Environment('workload')
    HYPERDEX_HOST   = Environment('localhost')
    HYPERDEX_PORT   = Environment('1982')

    CLIENTS = HostSet('clients')

    def loadA(self):
        self._load('workloada')

    def runA(self):
        self._run('workloada')

    def runB(self):
        self._run('workloadb')

    def runC(self):
        self._run('workloadc')

    def runF(self):
        self._run('workloadf')

    def runD(self):
        self._run('workloadd')

    def loadE(self):
        self._load('workloade')

    def runE(self):
        self._run('workloade')

    def _build_common_cmdline(self, workload):
        cmdline = ['java']
        if str(self.JAVA_ARGS) != '':
            cmdline += [self.JAVA_ARGS]
        cmdline += ['com.yahoo.ycsb.Client']
        cmdline += ['-db', self.DATABASE]
        cmdline += ['-P', '%s'%os.path.join(str(self.WORKLOADS), workload)]
        cmdline += ['-p', 'workload=com.yahoo.ycsb.workloads.CoreWorkload']
        cmdline += ['-p', 'fieldcount=%d'%self.FIELD_COUNT.as_int()]
        cmdline += ['-p', 'fieldlength=%d'%self.FIELD_LENGTH.as_int()]
        cmdline += ['-p', 'recordcount=%d'%self.RECORD_COUNT.as_int()]
        cmdline += ['-p', 'hyperdex.host=%s'%self.HYPERDEX_HOST]
        cmdline += ['-p', 'hyperdex.port=%s'%self.HYPERDEX_PORT]
        cmdline += ['-threads', self.THREADS]
        return cmdline

    def _load(self, workload):
        cmdline = self._build_common_cmdline(workload)
        cmdline += ['-load']
        interval = self.RECORD_COUNT.as_int() / len(self.CLIENTS)
        def start(x):
            return 'insertstart=%d'%(x * interval)
        cmdline += ['-p', HostSet.Index(start)]
        cmdline += ['-p', 'insertcount=%d'%interval]
        self.CLIENTS.run_many(cmdline)
        self.CLIENTS.collect('ycsb.dat.bz2', 'ycsb.dat.bz2')

    def _run(self, workload):
        cmdline = self._build_common_cmdline(workload)
        cmdline += ['-t']
        cmdline += ['-p', 'operationcount=%d'%(self.OPERATION_COUNT.as_int() / len(self.CLIENTS))]
        self.CLIENTS.run_many(cmdline)
        self.CLIENTS.collect('ycsb.dat.bz2', 'ycsb.dat.bz2')
