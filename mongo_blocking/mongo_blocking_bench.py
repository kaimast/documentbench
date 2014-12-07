import sys
import functools
import time
import pymongo

from functools import partial

from common.timer import Timer
from common.random_access import get_random_accesses
from common.docbuilder import create_document
from common.globals import NumCalls, NumWarmup, NumDocuments, EntryPrefix

class MongoBlockingBench:
        def setup(self, addr, port):
                self.client = pymongo.MongoClient(addr, port)
                self.db = self.client.test
    
        def prepare(self):
                for i in range(NumDocuments):
                	self.db.bench.insert(create_document(EntryPrefix, i), w=1)

        def clean(self):
                self.db.bench.remove({})

        def warmup(self):
                accesses = get_random_accesses(NumWarmup)

                for i in accesses:
                        self.do_bench_call(i)

        def run(self):
                accesses = get_random_accesses(NumCalls)

                timer = Timer()
                timer.start()

                for i in accesses:
                        self.do_bench_call(i)

                return timer.stop()
