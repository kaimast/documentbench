#! /usr/bin/python

from mongo_blocking.mongo_blocking_bench import MongoBlockingBench
from common.globals import EntryPrefix

# Run
class SetElement(MongoBlockingBench):
        def do_bench_call(self, i):
                self.db.bench.update({'_id': EntryPrefix + str(i)}, {'$set' : {'newelem' : 10}}, w=1)

        def name(self): 
                return "Set Element"
