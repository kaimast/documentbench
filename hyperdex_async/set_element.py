#! /usr/bin/python

from hyperdex_async.hyperdex_bench import HyperdexBench
from common.globals import EntryPrefix

class SetElement(HyperdexBench):
        def do_bench_call(self, i):
                return self.space.async_set(EntryPrefix + str(i), {'new_elem' : 42})

        def name(self): 
                return "Set Element"
