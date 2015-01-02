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


class HyperDex(Experiment):

    THREADS = Parameter(1)

    COORDINATOR = Host('coordinator')
    COORDINATORS = HostSet('coordinators')
    DAEMONS = HostSet('daemons')

    @Utility
    def clean(self):
        # ensure everything is dead
        self.COORDINATOR.run(('pkill', '-9', 'replicant'), status=None)
        self.COORDINATOR.run(('pkill', '-9', 'hyperdex'), status=None)
        self.COORDINATORS.run(('pkill', '-9', 'replicant'), status=None)
        self.COORDINATORS.run(('pkill', '-9', 'hyperdex'), status=None)
        self.DAEMONS.run(('pkill', '-9', 'replicant'), status=None)
        self.DAEMONS.run(('pkill', '-9', 'hyperdex'), status=None)
        # erase the data
        self.COORDINATOR.run(('rm', '-rf', 'coord-data'), status=None)
        self.COORDINATORS.run(('rm', '-rf', 'coord-data'), status=None)
        self.DAEMONS.run(('rm', '-rf', 'daemon-data'), status=None)

    @Utility
    def start(self):
        self.COORDINATOR.run(('mkdir', '-p', 'coord-data'))
        self.COORDINATOR.run(('hyperdex', 'coordinator',
                              '--data', 'coord-data'))
        time.sleep(1)
        self.COORDINATORS.run(('mkdir', '-p', 'coord-data'))
        self.COORDINATORS.run(('hyperdex', 'coordinator',
                               '-c', self.COORDINATOR.location,
                               '--data', 'coord-data'))
        time.sleep(1)
        self.DAEMONS.run(('mkdir', '-p', 'daemon-data'))
        self.DAEMONS.run(('hyperdex', 'daemon',
                          '-c', self.COORDINATOR.location,
                          '-t', self.THREADS,
                          '--data', 'daemon-data'))

    @Utility
    def reset(self):
        self.clean()
        self.start()
