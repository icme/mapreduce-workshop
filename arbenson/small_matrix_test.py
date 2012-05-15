#!/usr/bin/env python

"""
Driver for running the many-small-matrices test.

usage: python small_matrix_test.py [TEST_TYPE]

TEST_TYPE is an integer \in {0, 1, 2, 3} indicating which test type to run
0: simple yield on each row
1: simple yield on each matrix
2: simple yield on flattened matrix
3: packed yield on flattened matrix

e.g. to run test type 1, do:

python small_matrix_test.py 1

Austin R. Benson
ICME MapReduce Workshop
May 2012
"""

import sys
import os
import time

def usage():
    print "usage: python small_matrix_test.py [TEST_TYPE]"
    sys.exit(-1)

try:
    test_type = int(sys.argv[1])
    if test_type not in [0, 1, 2, 3]:
        raise Exception
except:
    usage()

copy_cmd = 'hadoop fs -copyFromLocal data/Simple_1M.tmat Simple_1M.tmat'
os.system(copy_cmd)

test_cmd = 'dumbo start speed_test.py \
-mat Simple_1M.tmat -output small_matrix_test_%d \
-use_system_numpy -test_type %d -hadoop icme' % (test_type, test_type)

t0 = time.time()
os.system(test_cmd)
dt = time.time() - t0

print 'test took %d seconds' % dt
