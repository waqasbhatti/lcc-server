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

def sqlite_get_collections(basedir,
                           lcclist,
                           require_ispublic=True):
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
    query = ("select collection_id, object_catalog_path, kdtree_pkl_path, "
             "columnlist, indexedcols, ftsindexedcols, name, "
             "description, project, ispublic, datarelease, "
             "ra_min, ra_max, decl_min, decl_max, nobjects from lcc_index "
             "where collection_id in (?)")

    if require_ispublic:
        query = query + ' and ispublic = 1'

    db_lcclist = ','.join(lcclist)

    cur.execute(query, (db_lcclist,))

    results = cur.fetchall()

    indexdb.close()

    # if we got the databases, then proceed
    if results and len(results) > 0:

        results = zip(list(*results))

        (collection_id, object_catalog_path,
         kdtree_pkl_path, columnlist,
         indexedcols, ftsindexedcols, name,
         description, project,
         ispublic, datarelease,
         minra, maxra, mindecl, maxdecl, nobjects) = results

        dbnames = [x.replace('-','_') for x in collection_id]

        columns_available = ','.join(columnlist)
        columns_available = list(set(columns_available.split(',')))

        indexed_cols_available = ','.join(indexedcols)
        indexed_cols_available = list(set(indexed_cols_available.split(',')))

        ftsindexed_cols_available = ','.join(ftsindexedcols)
        ftsindexed_cols_available = list(
            set(ftsindexed_cols_available.split(','))
        )

        # this is the connection we will return
        newconn = sqlite3.connect(
            ':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        newcur = newconn.cursor()

        for dbn, catpath in zip(dbnames, object_catalog_path):
            newcur.execute("attach database '%s' as %s" % (catpath, dbn))

        outdict = {
            'connection':newconn,
            'cursor':newcur,
            'databases':dbnames,
            'columns':columns_available,
            'indexedcols':indexed_cols_available,
            'ftscols':ftsindexed_cols_available,
            'info':{'collection_id':collection_id,
                    'object_catalog_path':object_catalog_path,
                    'kdtree_pkl_path':kdtree_pkl_path,
                    'columnlist':columnlist,
                    'indexedcols':indexedcols,
                    'ftsindexedcols':ftsindexedcols,
                    'name':name,
                    'description':description,
                    'project':project,
                    'ispublic':ispublic,
                    'datarelease':datarelease,
                    'minra':minra,
                    'maxra':maxra,
                    'mindecl':mindecl,
                    'maxdecl':maxdecl,
                    'nobjects':nobjects}
        }

        return outdict

    else:

        LOGERROR('could not find any information '
                 'about the requested LCC collections')
        return None



###########################
## PARSING SEARCH PARAMS ##
###########################


###################
## SQLITE SEARCH ##
###################


def sqlite_fulltext_search(basedir,
                           lcclist,
                           ftsquerystr,
                           columns,
                           require_ispublic=True):
    '''
    This searches the columns for full text.

    '''

    dbinfo = sqlite_get_collections(basedir,
                                    lcclist,
                                    require_ispublic=require_ispublic)


    db = dbinfo['database']
    cur = dbinfo['cursor']

    available_lcc = dbinfo['databases']

    # now we have to execute the FTS query for all of the attached databases.
    for lcc in available_lcc:

        q = ("select %s from %s where %s")



def sqlite_column_search(basedir,
                         lcclist,
                         columns,
                         conditions,
                         require_ispublic=True):
    '''
    This runs an arbitrary column search.

    '''


def sqlite_sql_search(basedir,
                      lcclist,
                      sqlstatement,
                      require_ispublic=True):
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
