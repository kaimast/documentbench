#! /usr/bin/python

from mongo_bulk.mongo_bulk_bench import MongoBulkBench
from common.globals import NewEntryPrefix

# Run
class PutDocument(MongoBulkBench):
        def do_bench_call(self, i):
                doc = create_document(NewEntryPrefix, i)
                bulk_op.insert(doc)

        def name(self): 
                return "Put Document"
