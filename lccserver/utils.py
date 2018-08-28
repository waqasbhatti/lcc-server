#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''utils.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains various utility functions and classes for lcc-server.

'''

#############
## LOGGING ##
#############

import logging
from datetime import datetime
from traceback import format_exc

# setup a logger
LOGGER = None
LOGMOD = __name__
DEBUG = False

def set_logger_parent(parent_name):
    globals()['LOGGER'] = logging.getLogger('%s.%s' % (parent_name, LOGMOD))

def LOGDEBUG(message):
    if LOGGER:
        LOGGER.debug(message)
    elif DEBUG:
        print('[%s - DBUG] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGINFO(message):
    if LOGGER:
        LOGGER.info(message)
    else:
        print('[%s - INFO] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGERROR(message):
    if LOGGER:
        LOGGER.error(message)
    else:
        print('[%s - ERR!] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGWARNING(message):
    if LOGGER:
        LOGGER.warning(message)
    else:
        print('[%s - WRN!] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGEXCEPTION(message):
    if LOGGER:
        LOGGER.exception(message)
    else:
        print(
            '[%s - EXC!] %s\nexception was: %s' % (
                datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                message, format_exc()
            )
        )


#############
## IMPORTS ##
#############

import sys

if sys.version_info[:2] < (3,7):

    import multiprocessing
    import concurrent.futures.process

    def _process_worker(self,
                        call_queue,
                        result_queue,
                        initializer,
                        initargs):
        '''This just adds in an initialization step to the
        concurrent.futures.process _process_worker function.

        '''
        if initializer is not None:
            try:
                initializer(*initargs)
            except Exception as e:
                # here, I've chosen to ignore the exception in the initializer
                # assume we don't do something stupid and send in an initializer
                # that immediately breaks
                pass

        # call the original worker to finish this
        concurrent.futures.process._process_worker(call_queue, result_queue)

    # ProcessPoolExecutor that can accept initializer and initialargs.
    # This is super-useful for running database connections in a background
    # process. This was added to Python 3.7:
    #
    # https://github.com/python/cpython/pull/4241/
    # files#diff-d24fedf7a1cf058e9e4166d89f2bb378
    #
    # but we add in the changes here so we can run stuff on Python < 3.7.
    class ProcExecutor(concurrent.futures.process.ProcessPoolExecutor):
        '''This is a subclass of the ProcessPoolExecutor that adds init args.

        '''

        def __init__(self,
                     max_workers=None,
                     initializer=None,
                     initargs=()):

            if initializer is not None and not callable(initializer):
                raise TypeError("initializer must be a callable")
            self._initializer = initializer
            self._initargs = initargs

            super().__init__(max_workers=max_workers)


    def _adjust_process_count(self):

        for _ in range(len(self._processes), self._max_workers):

            p = multiprocessing.Process(
                target=_process_worker,
                args=(self._call_queue,
                      self._result_queue,
                      self._initializer,
                      self._initargs))
            p.start()
            self._processes[p.pid] = p


# if we're on 3.7, return the usual executor
else:
    from concurrent.futures import ProcessPoolExecutor
    ProcExecutor = ProcessPoolExecutor
