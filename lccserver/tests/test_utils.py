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
import sqlite3
import tempfile
import os

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
        max_workers=ncpus,
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

    executor.shutdown()



#############################################
## MORE INVOLVED PROCESSPOOLEXECUTOR TESTS ##
#############################################

def database_initializer(database_fpath):
    global db_connection
    db_connection = sqlite3.connect(database_fpath)


def database_worker(task):
    global db_connection

    cursor = db_connection.cursor()
    query, params = task

    time.sleep(random.randrange(1,2))

    cursor.execute(query, (params,))
    row = cursor.fetchone()
    return params, row[0]


def test_background_sqlite3():
    '''This tests if the persistent DB connections work correctly in background
    workers.

    '''

    temp_fd, temp_fname = tempfile.mkstemp()

    conn = sqlite3.connect(temp_fname)
    cursor = conn.cursor()

    # from https://github.com/jalapic/engsoccerdata
    # /blob/master/data-raw/teamnames.csv
    data = [
        (1,"England","Sutton United","Sutton United"),
        (2,"England","Aberdare Athletic","Aberdare Athletic"),
        (3,"England","Accrington","Accrington"),
        (4,"England","Accrington F.C.","Accrington"),
        (5,"England","AFC Bournemouth","AFC Bournemouth"),
        (6,"England","AFC Wimbledon","AFC Wimbledon"),
        (7,"England","Aldershot","Aldershot Tn."),
        (8,"England","Arsenal","Arsenal"),
        (9,"England","Aston Villa","Aston Villa"),
    ]

    queries_params_results = [
        ('select country from team_names where serial = ?',
         4,
         'England'),
        ('select team_name from team_names where team_name = ?',
         'Arsenal',
         'Arsenal'),
        ('select alt_team_name from team_names where team_name = ?',
         'Aldershot',
         'Aldershot Tn.'),
        ('select serial from team_names where team_name = ?',
         'Aston Villa',
         9),
    ]*5

    param_result_dict = {x[1]:x[2] for x in queries_params_results}

    cursor.execute(
        "create table team_names ("
        "serial integer, country text, team_name text, alt_team_name text"
        ")"
    )
    cursor.executemany("insert into team_names values (?,?,?,?)", data)
    conn.commit()
    conn.close()

    ncpus = mp.cpu_count()
    print('CPUS: %s' % ncpus)

    executor = lcu.ProcExecutor(
        max_workers=2,
        initializer=database_initializer,
        initargs=(temp_fname,)
    )

    print('executor up OK: %r' % executor)

    tasks = [(x[0], x[1]) for x in queries_params_results]

    futures = [executor.submit(database_worker,task) for task in tasks]

    results = []

    for f in as_completed(futures):
        results.append(f.result())

    for res in results:
        input_param, returned = res
        assert returned == param_result_dict[input_param], "Result matches"

    executor.shutdown()
    os.remove(temp_fname)


if __name__ == '__main__':
    test_ProcExecutor()
    test_background_sqlite3()
