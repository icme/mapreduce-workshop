#!/usr/bin/env dumbo

import sys
import os
import numpy
import util
import struct
import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class ManySmallMatrices(dumbo.backends.common.MapRedBase):
    def __init__(self, test_type):
        self.test_type = test_type
        self.n = 10
        self.iterations = 10

    def __call__(self, data):
        for key, value in data:
            for x in xrange(self.iterations):
                Q = numpy.linalg.qr(numpy.random.randn(self.n, self.n))[0]

                if self.test_type == 0:
                    for i, row in enumerate(Q):
                        yield i, row
                
                elif self.test_type == 1:
                    yield 0, Q

                elif self.test_type == 2:
                    yield 0, [entry for row in Q for entry in row]

                elif self.test_type == 3:
                    flat = [entry for row in Q for entry in row]
                    yield 0, struct.pack('d'*len(flat), *flat)

def runner(job):
    test_type = gopts.getintkey('test_type')
    options=[('numreducetasks', '0'), ('nummaptasks', '150')]
    job.additer(mapper=ManySmallMatrices(test_type),
                reducer=dumbo.lib.identityreducer,
                opts=options)

def starter(prog):
    mypath =  os.path.dirname(__file__)
    
    # set the global opts
    gopts.prog = prog

    gopts.getintkey('test_type', 0)

    mat = prog.delopt('mat')
    if not mat:
        return "'mat' not specified'"

    prog.addopt('input',mat)    

    prog.addopt('memlimit','2g')
    prog.addopt('file',os.path.join(mypath,'util.py'))    

    nonumpy = prog.delopt('use_system_numpy')
    if nonumpy is None:
        print >> sys.stderr, 'adding numpy egg: %s'%(str(nonumpy))
        prog.addopt('libegg', 'numpy')

    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-speed_test'% (matname,))

    prog.addopt('overwrite','yes')
    prog.addopt('jobconf','mapred.output.compress=true')

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
