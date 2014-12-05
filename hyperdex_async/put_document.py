#! /usr/bin/python

from hyperdex_async.hyperdex_bench import HyperdexBench
from common.docbuilder import create_document
from common.globals import NewEntryPrefix

class PutDocument(HyperdexBench):
        def do_bench_call(self, i):
                doc = create_document(NewEntryPrefix, i)
                return self.space.async_insert(doc)

        def name(self): 
                return "Put Document"
