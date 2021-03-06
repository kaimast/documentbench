#! /usr/bin/python

from hyperdex_async.hyperdex_bench import HyperdexBench
from common.docbuilder import create_document
from common.globals import EntryPrefix

class DeleteDocument(HyperdexBench):
        def do_bench_call(self, i):
                return self.space.async_remove({'_id' : EntryPrefix + str(i)})

        def name(self): 
                return "Delete Document"
