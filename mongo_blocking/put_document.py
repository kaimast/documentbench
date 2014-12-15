#! /usr/bin/python

from mongo_blocking.mongo_blocking_bench import MongoBlockingBench
from common.globals import NewEntryPrefix
from common.docbuilder import create_document

# Run
class PutDocument(MongoBlockingBench):
        def do_bench_call(self, i):
		# Use save so we overwrite if document already exists
		# This can happen because i is random
                self.db.bench.save(create_document(NewEntryPrefix, i), w=1)

        def name(self): 
		return "Put Document"
