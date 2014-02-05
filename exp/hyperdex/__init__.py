# Copyright (c) 2014, Robert Escriva
# All rights reserved.

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
    def stop(self):
        self.DAEMONS.run(('pkill', 'hyperdex'), status=None)
        time.sleep(1)
        self.COORDINATORS.run(('pkill', 'replicant'), status=None)
        time.sleep(1)
        self.COORDINATOR.run(('pkill', 'replicant'), status=None)

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
        self.stop()
        self.clean()
        self.start()
