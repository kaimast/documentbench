#! /usr/bin/python

from hyperdex_async.hyperdex_bench import HyperdexBench
from common.globals import EntryPrefix

# Run
class AtomicAdd(HyperdexBench):
        def do_bench_call(self, i):
                return self.space.async_atomic_add(EntryPrefix + str(i), {'elem50' : 10})

        def name(self): 
                return "Atomic Add"
