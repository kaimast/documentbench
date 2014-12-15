#! /usr/bin/python

from mongo_bulk.mongo_bulk_bench import MongoBulkBench
from common.docbuilder import create_document
from common.globals import NewEntryPrefix
from common.docbuilder import create_document

# Run
class PutDocument(MongoBulkBench):
        def do_bench_call(self, i):
                doc = create_document(NewEntryPrefix, i)

		# There are no save operations in bulk
                self.bulk_op.find({'_id': doc['_id']}).upsert().replace_one(doc)

        def name(self): 
                return "Put Document"
