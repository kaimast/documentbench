import sys
import functools
import time
import pymongo

from functools import partial

from common.timer import Timer
from common.random_access import get_random_accesses
from common.docbuilder import create_document
from common.globals import NumCalls, NumWarmup, NumDocuments, EntryPrefix

class MongoBulkBench:
        def setup(self, addr, port, writes):
                self.client = pymongo.MongoClient(addr, port)
                self.db = self.client.test
                self.writes = writes
    
        def prepare(self):
                insert_bulk = self.db.bench.initialize_unordered_bulk_op()

                for i in range(NumDocuments):
                        insert_bulk.insert(create_document(EntryPrefix, i))
               
                insert_bulk.execute( { 'w' : self.writes } )


        def clean(self):
                self.db.bench.remove({})

        def warmup(self):
                accesses = get_random_accesses(NumWarmup)

                self.bulk_op = self.db.bench.initialize_unordered_bulk_op()

                for i in accesses:
                        self.do_bench_call(i)

	        self.bulk_op.execute({'w' : self.writes })

        def run(self):
                accesses = get_random_accesses(NumCalls)

                timer = Timer()
                timer.start()

                self.bulk_op = self.db.bench.initialize_unordered_bulk_op()

                for i in accesses:
                        self.do_bench_call(i)

	        self.bulk_op.execute({'w' : self.writes })

                return timer.stop()
