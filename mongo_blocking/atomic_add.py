#! /usr/bin/python

from mongo_blocking.mongo_blocking_bench import MongoBlockingBench
from common.globals import EntryPrefix

# Run
class AtomicAdd(MongoBlockingBench):
        def do_bench_call(self, i):
                self.db.bench.update({'_id' :  EntryPrefix + str(i)}, {'$inc' : {'elem50' : 10}},  w=1)

        def name(self): 
                return "Atomic Add"
