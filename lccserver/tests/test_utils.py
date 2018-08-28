#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''test_utils.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This tests the lccserver.utils module.

'''

import lccserver.utils as lcu
import multiprocessing as mp
from concurrent.futures import as_completed
import time
import random

def initializer_func(test_string):
    '''This is the initializer function that places a string at process global
    scope.

    '''

    global i_am_global
    i_am_global = test_string


def worker_func(input_param):
    '''
    This sleeps for random seconds between 1 and 3, then returns.

    '''
    global i_am_global
    time.sleep(random.randrange(1,3))
    return "%s|%s" % (i_am_global, input_param)


def test_ProcExecutor():
    '''
    This tests our local ProcExecutor instance to see if it works correctly.

    '''
    ncpus = mp.cpu_count()
    print('CPUS: %s' % ncpus)

    executor = lcu.ProcExecutor(
        ncpus,
        initializer=initializer_func,
        initargs=('glob glob glob',)
    )

    print('executor up OK: %r' % executor)

    tasks = [x for x in 'abcdefghijklmnopqrstuvwxyz']

    print('tasks: %r' % tasks)

    futures = [executor.submit(worker_func, x) for x in tasks]
    results = []

    print('submitted all tasks')

    for f in as_completed(futures):
        results.append(f.result())

    assert len(results) == len(tasks), "Number of results == number of tasks"

    for r in results:
        rx = r.split('|')
        assert rx[0] == 'glob glob glob', "The proc-global var is present"
        assert rx[1] in tasks, "Actual func args was passed in successfully"


if __name__ == '__main__':
    test_ProcExecutor()
