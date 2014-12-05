from numpy.random import zipf
from common.globals import NumDocuments

def get_random_accesses(amount):
        return zipf(NumDocuments, amount)
