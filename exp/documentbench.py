# Portions Copyright (c) 2014, Robert Escriva
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
from ygor import HostSet
from ygor import Parameter
from ygor import Environment
from ygor import Utility


class DocumentBench(Experiment):

    HOSTS = HostSet('hosts')

    SYSTEM     = Parameter('hyperdex')
    WORKLOAD   = Parameter('put')
    DOCUMENTS  = Parameter(500000)
    OPERATIONS = Parameter(1000000)

    # Connect to the cluster
    CLUSTER_HOST = Environment('127.0.0.1')
    CLUSTER_PORT = Environment('1982')

    def cluster(self):
        output = HostSet.Index(lambda x: 'benchmark-{0}.dat.bz2'.format(x))
        CLIENTS = 50
        args = ('documentbench',
                '--host', self.CLUSTER_HOST,
                '--port', self.CLUSTER_PORT,
                '--system', self.SYSTEM,
                '--output', output,
                '--documents', self.DOCUMENTS)
        if str(self.WORKLOAD) == 'load':
            per_host = (self.DOCUMENTS.as_int() / CLIENTS)
            args += ('--action', 'load',
                     '--load-start', HostSet.Index(lambda x: per_host * x),
                     '--load-limit', HostSet.Index(lambda x: per_host * (x + 1)))
        else:
            args += ('--action', 'run', '--operation', self.WORKLOAD,
                     '--operations', self.OPERATIONS.as_int() / CLIENTS)
        self.HOSTS.run_many(args, number=CLIENTS)
        self.HOSTS.collect('benchmark.dat.bz2', output, number=CLIENTS)
