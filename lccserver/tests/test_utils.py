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
import os.path

################
## Basic test ##
################

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


#################################################################
## Test using multiprocessing.current_process() to store stuff ##
#################################################################

def initializer_func_procstorage(test_string):
    '''This is the initializer function that places a string at process global
    scope.

    '''

    thisproc = mp.current_process()
    thisproc.local_proc_store = test_string


def worker_func_procstorage(input_param):
    '''
    This sleeps for random seconds between 1 and 3, then returns.

    '''
    thisproc = mp.current_process()
    time.sleep(random.randrange(1,3))
    return "%s|%s" % (thisproc.local_proc_store, input_param)


def test_ProcExecutor_procstorage():
    '''
    This tests our local ProcExecutor instance to see if it works correctly.

    '''
    ncpus = mp.cpu_count()
    print('CPUS: %s' % ncpus)

    executor = lcu.ProcExecutor(
        max_workers=ncpus,
        initializer=initializer_func_procstorage,
        initargs=('glob glob glob',)
    )

    print('executor up OK: %r' % executor)

    tasks = [x for x in 'abcdefghijklmnopqrstuvwxyz']

    print('tasks: %r' % tasks)

    futures = [executor.submit(worker_func_procstorage, x) for x in tasks]
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


################################################################
## Test using the finalizer arg to do stuff when workers exit ##
################################################################

from datetime import datetime
import glob


def initializer_func_finalizer(test_string):
    '''This is the initializer function that places a string at process global
    scope.

    '''

    thisproc = mp.current_process()
    thisproc.local_proc_store = test_string


def worker_func_finalizer(input_param):
    '''
    This sleeps for random seconds between 1 and 3, then returns.

    '''
    thisproc = mp.current_process()
    time.sleep(random.randrange(1,3))
    return "%s|%s" % (thisproc.local_proc_store, input_param)


def finalizer_func():
    '''
    This gets called right before the worker is ready to exit.

    '''
    thisproc = mp.current_process()
    with open('worker-done-%s.txt' % thisproc.name,'w') as outfd:
        outfd.write("Yay! I'm done!\n")
        outfd.write('worker shutdown called at: %s\n' %
                    datetime.utcnow().isoformat())


def test_ProcExecutor_finalizer():
    '''
    This tests our local ProcExecutor instance to see if it works correctly.

    '''
    ncpus = mp.cpu_count()
    print('CPUS: %s' % ncpus)

    executor = lcu.ProcExecutor(
        max_workers=ncpus,
        initializer=initializer_func_finalizer,
        initargs=('glob glob glob',),
        finalizer=finalizer_func,
    )

    print('executor up OK: %r' % executor)

    tasks = [x for x in 'abcdefghijklmnopqrstuvwxyz']

    print('tasks: %r' % tasks)

    futures = [executor.submit(worker_func_finalizer, x) for x in tasks]
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
    with open('executor-shutdown.txt','w') as outfd:
        outfd.write('executor shutdown at: %s\n' %
                    datetime.utcnow().isoformat())

    # now we'll check if the result files were generated correctly
    worker_results = glob.glob(os.path.join(os.getcwd(),
                                            'worker-done-*.txt'))

    assert len(worker_results) == ncpus, "All workers cleaned up correctly"

    for wrkres in worker_results:
        with open(wrkres,'r') as infd:
            assert "Yay! I'm done!" in infd.read(), "Clean up result OK"
        os.remove(wrkres)

    assert os.path.exists(os.path.join(os.getcwd(), 'executor-shutdown.txt'))
    os.remove(os.path.join(os.getcwd(), 'executor-shutdown.txt'))



#############################################
## More involved ProcessPoolExecutor tests ##
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
        max_workers=4,
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


###################################################################
## Testing sqlite3 background workers with process-local storage ##
###################################################################

def database_initializer_procstorage(database_fpath):
    thisproc = mp.current_process()
    thisproc.db_connection = sqlite3.connect(database_fpath)


def database_worker_procstorage(task):
    thisproc = mp.current_process()

    cursor = thisproc.db_connection.cursor()
    query, params = task

    time.sleep(random.randrange(1,2))

    cursor.execute(query, (params,))
    row = cursor.fetchone()
    return params, row[0]


def test_background_sqlite3_procstorage():
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
        max_workers=4,
        initializer=database_initializer_procstorage,
        initargs=(temp_fname,)
    )

    print('executor up OK: %r' % executor)

    tasks = [(x[0], x[1]) for x in queries_params_results]

    futures = [
        executor.submit(database_worker_procstorage,task) for task in tasks
    ]

    results = []

    for f in as_completed(futures):
        results.append(f.result())

    for res in results:
        input_param, returned = res
        assert returned == param_result_dict[input_param], "Result matches"

    executor.shutdown()
    os.remove(temp_fname)


#############################################
## Testing database closure on worker exit ##
#############################################

def database_initializer_finalizer(database_fpath):
    thisproc = mp.current_process()
    thisproc.db_connection = sqlite3.connect(database_fpath)


def database_worker_finalizer(task):
    thisproc = mp.current_process()

    cursor = thisproc.db_connection.cursor()
    query, params = task

    time.sleep(random.randrange(1,2))

    cursor.execute(query, (params,))
    row = cursor.fetchone()
    return params, row[0]


def database_closer_finalizer():

    thisproc = mp.current_process()
    thisproc.db_connection.close()

    try:
        thisproc.db_connection.cursor()
    except sqlite3.ProgrammingError as e:
        with open('worker-done-%s.txt' % thisproc.name,'w') as outfd:
            outfd.write(
                'database closed successfully: %r at %s\n' %
                (e, datetime.utcnow().isoformat())
            )



def test_background_sqlite3_finalizer():
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
        max_workers=4,
        initializer=database_initializer_finalizer,
        initargs=(temp_fname,),
        finalizer=database_closer_finalizer
    )

    print('executor up OK: %r' % executor)

    tasks = [(x[0], x[1]) for x in queries_params_results]

    futures = [
        executor.submit(database_worker_finalizer,task) for task in tasks
    ]

    results = []

    for f in as_completed(futures):
        results.append(f.result())

    for res in results:
        input_param, returned = res
        assert returned == param_result_dict[input_param], "Result matches"

    executor.shutdown()
    os.remove(temp_fname)
    with open('executor-shutdown.txt','w') as outfd:
        outfd.write('executor shutdown at: %s\n' %
                    datetime.utcnow().isoformat())

    # now we'll check if the result files were generated correctly
    worker_results = glob.glob(os.path.join(os.getcwd(),
                                            'worker-done-*.txt'))

    assert len(worker_results) == 4, "All workers cleaned up correctly"

    for wrkres in worker_results:
        with open(wrkres,'r') as infd:
            assert "closed database" in infd.read(), "Clean up result OK"
        os.remove(wrkres)

    assert os.path.exists(os.path.join(os.getcwd(), 'executor-shutdown.txt'))
    os.remove(os.path.join(os.getcwd(), 'executor-shutdown.txt'))
