#!/usr/bin/env dumbo

"""
Chol.py
===========

Implement a Cholesky QR algorithm using dumbo and numpy.

Assumes that matrix is stored as a typedbytes vector and that
the user knows how many columns are in the matrix.
"""

import sys
import os
import time
import random
import struct

import numpy
import numpy.linalg

import util

import dumbo
import dumbo.backends.common

# create the global options structure
gopts = util.GlobalOptions()

class ComputeNorm(dumbo.backends.common.MapRedBase):
    def __init__(self,ncols=10):
        self.data = range(ncols)
        self.ncols = ncols
    
    def array2list(self,row):
        return [float(val) for val in row]

    def close(self):
        AtA = numpy.mat(self.data)
        I = numpy.eye(self.ncols)
        B = AtA - I
        norm = numpy.linalg.norm(B,2)
        yield "norm is: ", norm

    def __call__(self,data):
        for key,values in data:
            for value in values:
                self.data[key] = list(struct.unpack('d'*self.ncols, value))
                
        for key,val in self.close():
            yield key, val

class AtA(base.MatrixHandler):
    def __init__(self, blocksize=3, isreducer=False, ncols=10):
        base.MatrixHandler.__init__(self)        
        self.blocksize=blocksize
        self.isreducer=isreducer
        self.data = []
        self.ncols = ncols
        self.A_curr = None
        self.row = None
        self.TEST_TYPE = 'left'
        #self.TEST_TYPE = 'right'
        #self.TEST_TYPE = 'last'
    
    def compress(self):
        if len(self.data) == 0:
            return
        t0 = time.time()
        A_mat = numpy.mat(self.data)
        A_flush = A_mat.T*A_mat
        dt = time.time() - t0
        self.counters['numpy time (millisecs)'] += int(1000*dt)
        self.data = []

    def close(self):
        self.compress()
        if self.A_curr is not None:
            for ind, row in enumerate(self.A_curr.getA()):
                r = self.array2list(row)
                yield ind, struct.pack('d'*len(r),*r)

    def collect(self, key, value):
        if self.TEST_TYPE == 'left':
            self.data.append(value)

        elif self.TEST_TYPE = 'right':
            self.data.append(value)
            if len(self.data) > self.blocksize:
                self.compress()

        elif self.TEST_TYPE = 'last':
            self.data.append(value)
            if len(self.data) > self.blocksize:
                self.compress()
            
    def __call__(self,data):
        if not self.isreducer:
            self.collect_data(data)
        else:
            for key,values in data:
                self.collect_data(values, key)            

        for key,val in self.close():
            yield key, val
    
def runner(job):
    blocksize = gopts.getintkey('blocksize')
    schedule = gopts.getstrkey('reduce_schedule')

    schedule = schedule.split(',')
    for i,part in enumerate(schedule):
        if i==0:
            mapper = AtA(blocksize=blocksize,isreducer=False)
            reducer = AtA(blocksize=blocksize,isreducer=True)
        else:
            nreducers = int(part)
            mapper = base.ID_MAPPER
            reducer = ComputeNorm()
        job.additer(mapper=mapper, reducer=reducer, opts=[('numreducetasks',str(nreducers))])
    

def starter(prog):
    gopts.prog = prog
    gopts.getintkey('blocksize',3)
    gopts.getstrkey('reduce_schedule','1')

    mat = base.starter_helper(prog)
    if not mat: return "'mat' not specified"

    matname,matext = os.path.splitext(mat)
    output = prog.getopt('output')
    if not output:
        prog.addopt('output','%s-qrr%s'%(matname,matext))

    gopts.save_params()

if __name__ == '__main__':
    dumbo.main(runner, starter)
