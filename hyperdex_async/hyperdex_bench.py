import sys
import functools
import time

from functools import partial
from hyperdex.mongo import HyperDatabase, HyperSpace 

from common.timer import Timer
from common.random_access import get_random_accesses
from common.docbuilder import create_document
from common.globals import NumCalls, NumWarmup, NumDocuments, EntryPrefix
from bulk_insert import BulkInserter

class HyperdexBench:
        def setup(self, addr, port):
                self.db = HyperDatabase(addr, port)
                self.space = self.db.bench
    
        def prepare(self):
                generator = partial(create_document, EntryPrefix)
                bulk_inserter = BulkInserter(NumDocuments, self.space, generator)
                bulk_inserter.run()

        def clean(self):
                self.space.clear()

        def warmup(self):
                pendingOps = []
                accesses = get_random_accesses(NumWarmup)

                for i in accesses:
                        p = self.do_bench_call(i)
                        pendingOps.append(p)

                while len(pendingOps) > 0:
                        pendingOps[0].wait()
                        pendingOps.pop(0)

        def run(self):
                pendingOps = []
                accesses = get_random_accesses(NumCalls)

                timer = Timer()
                timer.start()

                for i in accesses:
                        p = self.do_bench_call(i)
                        pendingOps.append(p)

                while len(pendingOps) > 0:
                        pendingOps[0].wait()
                        pendingOps.pop(0)

                return timer.stop()
