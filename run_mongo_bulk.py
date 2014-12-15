from mongo_bulk.atomic_add import AtomicAdd
from mongo_bulk.set_element import SetElement
from mongo_bulk.put_document import PutDocument
from mongo_bulk.delete_document import DeleteDocument

from common.globals import NumRuns
from sys import argv
from time import sleep

if len(argv) != 4:
     raise ValueError('Wrong amount of arguments given')

address = argv[1]
port = int(argv[2])
writes = int(argv[3])

def run_bench(bench):
     print bench.name()

     bench.setup(address, port, writes)
     bench.clean()

     for n in range(0, NumRuns, 1):
        bench.prepare()
        sleep(5)
        
        bench.warmup()
        result = bench.run()
        print str(result)

        bench.clean()
        sleep(5)   

run_bench(AtomicAdd())
run_bench(SetElement())
run_bench(PutDocument())
run_bench(DeleteDocument())
