import numpy

def create_document(prefix, count):
        doc = {'_id' : prefix + str(count)}
        num_elems = numpy.random.poisson(1000)

        for i in range(num_elems):
                doc['elem' + str(i)] = i

        return doc
