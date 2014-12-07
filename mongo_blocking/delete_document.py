#! /usr/bin/python

from mongo_blocking.mongo_blocking_bench import MongoBlockingBench
from common.globals import EntryPrefix

# Run
class DeleteDocument(MongoBlockingBench):
        def do_bench_call(self, i):
                self.db.bench.remove({'_id', EntryPrefix + str(i)},  w=1)

        def name(self): 
                return "Delete Document"
