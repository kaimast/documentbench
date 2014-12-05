#! /usr/bin/python

from mongo_bulk.mongo_bulk_bench import MongoBulkBench
from common.globals import EntryPrefix

# Run
class AtomicAdd(MongoBulkBench):
        def do_bench_call(self, i):
                self.bulk_op.find({'_id' : EntryPrefix + str(i)}).update({'$inc' : {'elem50' : 10}})

        def name(self): 
                return "Atomic Add"
