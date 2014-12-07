#! /usr/bin/python

from mongo_bulk.mongo_bulk_bench import MongoBulkBench
from common.globals import EntryPrefix

# Run
class DeleteDocument(MongoBulkBench):
        def do_bench_call(self, i):
                self.bulk_op.delete({'_id' : EntryPrefix + str(i)})

        def name(self): 
                return "Delete Document"
