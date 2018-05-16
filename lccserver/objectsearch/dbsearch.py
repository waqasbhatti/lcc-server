#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''{{ name }}.py - {{ author }} ({{ email }}) - {{ month }} {{ year }}
License: MIT - see the LICENSE file for the full text.

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

import os
import sqlite3


#####################################
## UTILITY FUNCTIONS FOR DATABASES ##
#####################################

def sqlite_join_collections(basedir, lcclist):
    '''This returns an instance of sqlite3 connection with all sqlite DBs
    corresponding to the collections in lcclist attached to it.

    Useful for cross-collection searches.

    Also returns:

    - the set of columns that are common to all collections
    - the set of indexed cols and FTS indexed cols available
    - the reformed database identifiers (e.g. hatnet-kepler -> hatnet_kepler)

    '''

    # open the index database
    indexdbf = os.path.join(basedir, 'lcc-index.sqlite')
    indexdb = sqlite3.connect(
        indexdbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = indexdb.cursor()

    # get the info we need
    query = ("select ")



###########################
## PARSING SEARCH PARAMS ##
###########################


###################
## SQLITE SEARCH ##
###################


def sqlite_fulltext_search(basedir, lcclist, ftsquery):
    '''
    This searches the columns for full text.

    '''


def sqlite_column_search(basedir, lcclist, columns, conditions):
    '''
    This runs an arbitrary column search.

    '''


def sqlite_sql_search(basedir, lcclist, sqlstatement):
    '''
    This runs an arbitrary column search.

    '''


###################
## KDTREE SEARCH ##
###################

def kdtree_conesearch(basedir, lcclist, searchparams):
    '''
    This does a cone-search using searchparams over all lcc in lcclist.

    - do an overlap between footprint of lcc and cone size
    - figure out which lccs to use
    - run kdtree search for each of these and concat the results

    '''
