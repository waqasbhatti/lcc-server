#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''dbsearch.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - May 2018
License: MIT - see the LICENSE file for the full text.

This contains the implementations of database search for LCC server.

This has:

- full text search
- cone search
- column search based on arbitrary conditions
- cross-matching to input data based on coordinates and distance or columns

TODO:

- arbitrary SQL statement search (this will likely be some sort of ADQL search,
  we need an SQL parser to do this correctly because we need to recognize custom
  functions, etc.)

FIXME:

- for column search and extraconditions, we parse the SQL ourselves to validate
  it and remove harmful stuff. This is likely not as safe as the actual SQL
  parameter substitution code in SQLite. How to get around this?


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
import os.path
import sqlite3
import re
import pickle
import json
from tornado.escape import squeeze

import numpy as np

from astrobase.coordutils import make_kdtree, conesearch_kdtree, \
    xmatch_kdtree, great_circle_dist


#####################################
## UTILITY FUNCTIONS FOR DATABASES ##
#####################################

def sqlite_get_collections(basedir,
                           lcclist=None,
                           require_ispublic=True,
                           return_connection=True):
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
             "ra_min, ra_max, decl_min, decl_max, nobjects, "
             "lcformat_key, lcformat_desc_path, catalog_columninfo_json "
             "from lcc_index "
             "{lccspec}")

    # handle the case where lcclist is not provided, we'll use any LCC available
    # in the database
    if lcclist is not None:

        # we need to do this because we're mapping from directory names on the
        # filesystem that may contain hyphens (although they really shouldn't)
        # and database names in the sqlite3 table which can't have hyphens
        query = query.format(
            lccspec="where replace(collection_id,'-','_') in (?)"
        )
        db_lcclist = ','.join(lcclist)

        if require_ispublic:
            query = query + ' and ispublic = 1'

        cur.execute(query, (db_lcclist,))

    # otherwise, we'll provide a list of LCCs
    else:

        query = query.format(lccspec='')

        if require_ispublic:
            query = query + ' where ispublic = 1'

        cur.execute(query)

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
         minra, maxra, mindecl, maxdecl,
         nobjects, lcformatkey, lcformatdesc, columnjson) = results

        dbnames = [x.replace('-','_') for x in collection_id]

        columns_available = ','.join(columnlist)
        columns_available = list(set(columns_available.split(',')))

        indexed_cols_available = ','.join(indexedcols)
        indexed_cols_available = list(set(indexed_cols_available.split(',')))

        ftsindexed_cols_available = ','.join(ftsindexedcols)
        ftsindexed_cols_available = list(
            set(ftsindexed_cols_available.split(','))
        )

        if return_connection:

            # this is the connection we will return
            newconn = sqlite3.connect(
                ':memory:',
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            newconn.row_factory = sqlite3.Row
            newcur = newconn.cursor()

            for dbn, catpath in zip(dbnames, object_catalog_path):
                newcur.execute("attach database '%s' as %s" % (catpath, dbn))

        else:

            newconn = None
            newcur = None


        outdict = {
            'connection':newconn,
            'cursor':newcur,
            'databases':dbnames,
            'columns':columns_available,
            'indexedcols':indexed_cols_available,
            'ftscols':ftsindexed_cols_available,
            'info':{
                'collection_id':collection_id,
                'db_collection_id':[d.replace('-','_') for d in collection_id],
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
                'nobjects':nobjects,
                'lcformatkey':lcformatkey,
                'lcformatdesc':lcformatdesc,
                'columnjson': [json.loads(c) for c in columnjson]
            }
        }

        return outdict

    else:

        LOGERROR('could not find any information '
                 'about the requested LCC collections')
        return None



def sqlite_list_collections(basedir,
                            require_ispublic=True):
    '''
    This just lists the collections in basedir.

    '''

    return sqlite_get_collections(basedir,
                                  require_ispublic=require_ispublic,
                                  return_connection=False)



###########################
## PARSING SEARCH PARAMS ##
###########################

SQLITE_ALLOWED_WORDS = ['and','between','in',
                        'is','isnull','like','not',
                        'notnull','null','or',
                        '=','<','>','<=','>=','!=','%']

SQLITE_ALLOWED_ORDERBY = ['asc','desc']

SQLITE_ALLOWED_LIMIT = ['limit']



def validate_sqlite_filters(filterstring,
                            columnlist=None,
                            allowedsqlwords=SQLITE_ALLOWED_WORDS,
                            otherkeywords=None):
    '''This validates the sqlitecurve filter string.

    This MUST be valid SQL but not contain any commands.

    '''

    # first, lowercase, then squeeze to single spaces
    stringelems = squeeze(filterstring).lower()

    # replace shady characters
    stringelems = filterstring.replace('(','')
    stringelems = stringelems.replace(')','')
    stringelems = stringelems.replace(',','')
    stringelems = stringelems.replace("'",'"')
    stringelems = stringelems.replace('\n',' ')
    stringelems = stringelems.replace('\t',' ')
    stringelems = squeeze(stringelems)

    # split into words
    stringelems = stringelems.split(' ')
    stringelems = [x.strip() for x in stringelems]

    # get rid of all numbers
    stringwords = []
    for x in stringelems:
        try:
            _ = float(x)
        except ValueError as e:
            stringwords.append(x)

    # get rid of everything within quotes
    stringwords2 = []
    for x in stringwords:
        if not(x.startswith('"') and x.endswith('"')):
            stringwords2.append(x)
    stringwords2 = [x for x in stringwords2 if len(x) > 0]

    # check the filterstring words against the allowed words
    wordset = set(stringwords2)

    # generate the allowed word set for these LC columns
    if columnlist is not None:
        allowedcolumnlist = columnlist
    else:
        allowedcolumnlist = []

    allowedwords = allowedsqlwords + allowedcolumnlist

    # this allows us to handle other stuff like ADQL operators
    if otherkeywords is not None:
        allowedwords = allowedwords + otherkeywords

    checkset = set(allowedwords)

    validatecheck = list(wordset - checkset)

    # if there are words left over, then this filter string is suspicious
    if len(validatecheck) > 0:

        # check if validatecheck contains an elem with % in it
        LOGWARNING("provided SQL filter string '%s' "
                   "contains non-allowed keywords: %s" % (filterstring,
                                                          validatecheck))
        return None

    else:
        return filterstring



###################
## SQLITE SEARCH ##
###################

def sqlite_fulltext_search(basedir,
                           ftsquerystr,
                           getcolumns=None,
                           extraconditions=None,
                           lcclist=None,
                           require_ispublic=True,
                           raiseonfail=False):
    '''This searches the specified collections for a full-text match.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. This is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    getcolumns is a list that specifies which columns to return after the query
    is complete.

    extraconditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, extraconditions will be disabled.

    lcclist is the list of light curve collection IDs to search in. If this is
    None, all light curve collections are searched.

    require_ispublic sets if the query is restricted to public light curve
    collections only.

    '''

    # connect to all the specified databases
    dbinfo = sqlite_get_collections(basedir,
                                    lcclist=lcclist,
                                    require_ispublic=require_ispublic)
    db = dbinfo['connection']
    cur = dbinfo['cursor']

    # get the available databases and columns
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            db.close()
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc

    # we have some default columns that we'll always get

    # if we have some columns that the user provided, get them and append
    # default cols
    if getcolumns is not None:

        # get the requested columns together
        columnstr = ', '.join('a.%s' % (c,) for c in getcolumns)

        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, a.ra as db_ra, '
              'a.decl as db_decl, a.lcfname as db_lcfname')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname']


    # otherwise, if there are no columns, use the default ones
    else:

        columnstr = ('a.objectid as db_oid, a.ra as db_ra, '
                     'a.decl as db_decl, a.lcfname as db_lcfname')

        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname']

    # this is the query that will be used for FTS
    q = ("select {columnstr} from {collection_id}.object_catalog a join "
         "{collection_id}.catalog_fts b on (a.rowid = b.rowid) where "
         "catalog_fts MATCH ?{extraconditions} "
         "order by bm25(catalog_fts)")

    # handle the extra conditions
    if extraconditions is not None and len(extraconditions) > 0:

        # validate this string
        extraconditions = validate_sqlite_filters(extraconditions,
                                                  columnlist=available_columns)


    # now we have to execute the FTS query for all of the attached databases.
    results = {}

    for lcc in uselcc:

        dbindex = available_lcc.index(lcc)
        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]

        lcc_columnspec = dbinfo['info']['columnjson'][dbindex]
        lcc_collid = dbinfo['info']['collection_id'][dbindex]

        # update the lcc_columnspec with the extra columns we always return
        lcc_columnspec['db_oid'] = lcc_columnspec['objectid']
        lcc_columnspec['db_oid']['title'] = 'database object ID'

        lcc_columnspec['db_ra'] = lcc_columnspec['ra']
        lcc_columnspec['db_ra']['title'] = 'database &alpha;'

        lcc_columnspec['db_decl'] = lcc_columnspec['decl']
        lcc_columnspec['db_decl']['title'] = 'database &delta;'

        lcc_columnspec['db_lcfname'] = lcc_columnspec['lcfname']
        lcc_columnspec['db_lcfname']['title'] = 'database LC filename'

        # we should return all FTS indexed columns regardless of whether the
        # user selected them or not
        collection_ftscols = (
            dbinfo['info']['ftsindexedcols'][dbindex].split(',')
        )

        # add these to the default columns we return
        rescolumns.extend(collection_ftscols)

        # add them to the SQL column statement too
        columnstr = (
            columnstr + ', ' +
            ', '.join(['a.%s' % x for x in collection_ftscols])
        )

        try:

            # if we have extra filters, apply them
            if extraconditions is not None and len(extraconditions) > 0:

                extraconditionstr = ' and (%s)' % extraconditions

            else:

                extraconditionstr = ''


            # format the query
            # FIXME: this isn't using sqlite safe param substitution
            # does it matter?
            thisq = q.format(columnstr=columnstr,
                             collection_id=lcc,
                             extraconditions=extraconditionstr)

            # execute the query
            LOGINFO('query = %s' % thisq)

            cur.execute(thisq, (ftsquerystr,))
            rows = cur.fetchall()

            if rows and len(rows) > 0:

                rows = [dict(x) for x in rows]

            else:

                rows = []

            # put the results into the right place
            results[lcc] = {'result':rows,
                            'query':thisq.replace('?',"'%s'" % ftsquerystr),
                            'success':True}
            results[lcc]['nmatches'] = len(results[lcc]['result'])

            msg = ('executed FTS query: "%s" successfully '
                   'for collection: %s, matching nrows: %s' %
                   (ftsquerystr, lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)

            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

        except Exception as e:

            msg = ('failed to execute FTS query: "%s" '
                   'for collection: %s, exception: %s' %
                   (ftsquerystr, lcc, e))

            LOGEXCEPTION(msg)
            results[lcc] = {'result':[],
                            'query':q,
                            'nmatches':0,
                            'message':msg,
                            'success':False}

            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise


    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {'ftsquerystr':ftsquerystr,
                       'getcolumns':rescolumns,
                       'lcclist':lcclist,
                       'extraconditions':extraconditions}
    results['search'] = 'sqlite_fulltext_search'


    db.close()
    return results



def sqlite_column_search(basedir,
                         getcolumns=None,
                         conditions=None,
                         sortby=None,
                         limit=None,
                         lcclist=None,
                         require_ispublic=True,
                         raiseonfail=False):
    '''This runs an arbitrary column search.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. this is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    getcolumns is a list that specifies which columns to return after the query
    is complete.

    require_ispublic sets if the query is restricted to public light curve
    collections only.

    '''

    # connect to all the specified databases
    dbinfo = sqlite_get_collections(basedir,
                                    lcclist=lcclist,
                                    require_ispublic=require_ispublic)
    db = dbinfo['connection']
    cur = dbinfo['cursor']

    # get the available databases and columns
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            db.close()
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # we have some default columns that we'll always get
    # if we have some columns to get, get them and append default cols
    if getcolumns is not None:

        # get the requested columns together
        columnstr = ', '.join('a.%s' % c for c in getcolumns)
        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, a.ra as db_ra, '
              'a.decl as db_decl, a.lcfname as db_lcfname')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname']

    # otherwise, if there are no columns, use the default ones
    else:

        columnstr = ('a.objectid as db_oid, a.ra as db_ra, '
                     'a.decl as db_decl, a.lcfname as db_lcfname')

        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname']

    # this is the query that will be used
    q = ("select {columnstr} from {collection_id}.object_catalog a "
         "{wherecondition} {sortcondition} {limitcondition}")

    # validate the column conditions
    if conditions is not None:

        wherecondition = validate_sqlite_filters(conditions,
                                                 columnlist=available_columns)

        if not wherecondition:
            wherecondition = ''
        else:
            wherecondition = 'where %s' % wherecondition

    else:

        wherecondition = ''


    # validate the sortby condition
    if sortby is not None:

        # validate the sort condition
        sortcondition = validate_sqlite_filters(
            sortby,
            columnlist=available_columns,
            allowedsqlwords=SQLITE_ALLOWED_ORDERBY
        )

        if not sortcondition:
            sortcondition = ''
        else:
            sortcondition = 'order by %s' % sortcondition

    else:

        sortcondition = ''


    # validate the limit condition
    if limit is not None:

        # validate the sort condition
        limitcondition = validate_sqlite_filters(
            str(limit),
            columnlist=available_columns,
            allowedsqlwords=SQLITE_ALLOWED_LIMIT
        )

        if not limitcondition:
            limitcondition = ''
        else:
            limitcondition = 'limit %s' % limitcondition

    else:

        limitcondition = ''


    # finally, run the queries for each collection
    results = {}

    for lcc in uselcc:

        dbindex = available_lcc.index(lcc)
        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]

        lcc_columnspec = dbinfo['info']['columnjson'][dbindex]
        lcc_collid = dbinfo['info']['collection_id'][dbindex]

        # update the lcc_columnspec with the extra columns we always return
        lcc_columnspec['db_oid'] = lcc_columnspec['objectid']
        lcc_columnspec['db_oid']['title'] = 'database object ID'

        lcc_columnspec['db_ra'] = lcc_columnspec['ra']
        lcc_columnspec['db_ra']['title'] = 'database &alpha;'

        lcc_columnspec['db_decl'] = lcc_columnspec['decl']
        lcc_columnspec['db_decl']['title'] = 'database &delta;'

        lcc_columnspec['db_lcfname'] = lcc_columnspec['lcfname']
        lcc_columnspec['db_lcfname']['title'] = 'database LC filename'


        thisq = q.format(columnstr=columnstr,
                         collection_id=lcc,
                         wherecondition=wherecondition,
                         sortcondition=sortcondition,
                         limitcondition=limitcondition)

        try:

            LOGINFO('query = %s' % thisq)
            cur.execute(thisq)
            rows = cur.fetchall()

            if rows and len(rows) > 0:
                rows = [dict(x) for x in rows]
            else:
                rows = []

            # put the results into the right place
            results[lcc] = {'result':rows,
                            'query':thisq,
                            'success':True}
            results[lcc]['nmatches'] = len(results[lcc]['result'])

            msg = ('executed query successfully for collection: %s'
                   ', matching nrows: %s' %
                   (lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid


        except Exception as e:

            msg = ('failed to execute query for '
                   'collection: %s, exception: %s' % (lcc, e))

            LOGEXCEPTION(msg)
            results[lcc] = {'result':[],
                            'query':thisq,
                            'nmatches':0,
                            'message':msg,
                            'success':False}
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise

    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {'getcolumns':rescolumns,
                       'conditions':conditions,
                       'sortby':sortby,
                       'limit':limit,
                       'lcclist':lcclist}
    results['search'] = 'sqlite_column_search'

    db.close()
    return results



def sqlite_sql_search(basedir,
                      sqlstatement,
                      lcclist=None,
                      require_ispublic=True,
                      raiseonfail=False):
    '''This runs an arbitrary SQL statement search.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. this is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    columns is a list that specifies which columns to return after the query is
    complete.

    extraconditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, extraconditions will be disabled.

    require_ispublic sets if the query is restricted to public light curve
    collections only.

    FIXME: this will require an sql parser to do it right.

    '''


#################
## CONE SEARCH ##
#################

def sqlite_kdtree_conesearch(basedir,
                             center_ra,
                             center_decl,
                             radius_arcmin,
                             maxradius_arcmin=60.0,
                             getcolumns=None,
                             extraconditions=None,
                             lcclist=None,
                             require_ispublic=True,
                             conesearchworkers=1,
                             raiseonfail=False):
    '''This does a cone-search using searchparams over all lcc in lcclist.

    - do an overlap between footprint of lcc and cone size
    - figure out which lccs to use
    - run kdtree search for each of these and concat the results

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. this is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    getcolumns is a list that specifies which columns to return after the query
    is complete. NOTE: If this is None, this query is executed just as a 'check
    query' to see if there are any matching objects for conesearchparams in the
    LCC.

    extraconditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, extraconditions will be disabled.

    require_ispublic sets if the query is restricted to public light curve
    collections only.

    '''
    # connect to all the specified databases
    dbinfo = sqlite_get_collections(basedir,
                                    lcclist=lcclist,
                                    require_ispublic=require_ispublic)
    db = dbinfo['connection']
    cur = dbinfo['cursor']

    # get the available databases and columns
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            db.close()
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # get the requested columns together
    if getcolumns is not None:
        columnstr = ', '.join('a.%s' % c for c in getcolumns)

        # we add some columns that will always be present to use in sorting and
        # filtering
        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, b.objectid as kdtree_oid, '
              'a.ra as db_ra, a.decl as db_decl, '
              'b.ra as kdtree_ra, b.decl as kdtree_decl, '
              'a.lcfname as db_lcfname')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'kdtree_oid',
                                       'db_ra',
                                       'db_decl',
                                       'kdtree_ra',
                                       'kdtree_decl',
                                       'db_lcfname',
                                       'dist_arcsec']

    # otherwise, if there are no columns, get the default set of columns for a
    # 'check' cone-search query
    else:

        columnstr = ('a.objectid as db_oid, b.objectid as kdtree_oid, '
                     'a.ra as db_ra, a.decl as db_decl, '
                     'b.ra as kdtree_ra, b.decl as kdtree_decl, '
                     'a.lcfname as db_lcfname')

        rescolumns = ['db_oid',
                      'kdtree_oid',
                      'db_ra',
                      'db_decl',
                      'kdtree_ra',
                      'kdtree_decl',
                      'db_lcfname',
                      'dist_arcsec']


    # this is the query that will be used to query the database only
    q = ("select {columnstr} from {collection_id}.object_catalog a "
         "join _temp_objectid_list b on (a.objectid = b.objectid) "
         "{extraconditions} order by b.objectid asc")

    # handle the extra conditions
    if extraconditions is not None and len(extraconditions) > 0:

        # validate this string
        extraconditions = validate_sqlite_filters(extraconditions,
                                                  columnlist=available_columns)


    # now go through each LCC
    # - load the kdtree
    # - run the cone-search
    # - see if we have any results.
    #   - if we do, get the appropriate columns from the LCC in the same order
    #     as that of the results from the cone search. then add in the distance
    #     info from the cone-search results
    #   - if we don't, return null result for this LCC
    results = {}

    for lcc in uselcc:

        # get the kdtree path
        dbindex = available_lcc.index(lcc)

        kdtree_fpath = dbinfo['info']['kdtree_pkl_path'][dbindex]
        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]

        lcc_columnspec = dbinfo['info']['columnjson'][dbindex]
        lcc_collid = dbinfo['info']['collection_id'][dbindex]

        # update the lcc_columnspec with the extra columns we always return
        lcc_columnspec['db_oid'] = lcc_columnspec['objectid'].copy()
        lcc_columnspec['db_oid']['title'] = 'database object ID'

        lcc_columnspec['kdtree_oid'] = lcc_columnspec['objectid'].copy()
        lcc_columnspec['kdtree_oid']['title'] = (
            'object ID in kd-tree for spatial queries'
        )

        lcc_columnspec['db_ra'] = lcc_columnspec['ra'].copy()
        lcc_columnspec['db_ra']['title'] = 'database &alpha;'

        lcc_columnspec['db_decl'] = lcc_columnspec['decl'].copy()
        lcc_columnspec['db_decl']['title'] = 'database &delta;'

        lcc_columnspec['kdtree_ra'] = lcc_columnspec['ra'].copy()
        lcc_columnspec['kdtree_ra']['title'] = 'kd-tree &alpha;'

        lcc_columnspec['kdtree_decl'] = lcc_columnspec['decl'].copy()
        lcc_columnspec['kdtree_decl']['title'] = 'kd-tree &delta;'

        lcc_columnspec['db_lcfname'] = lcc_columnspec['lcfname'].copy()
        lcc_columnspec['db_lcfname']['title'] = 'database LC file path'

        # this is the extra spec for dist_arcsec
        lcc_columnspec['dist_arcsec'] = {
            'title': 'distance [arcsec]',
            'format': '%.3f',
            'description':'distance from search center in arcsec',
            'dtype':'<f8',
            'index':True,
            'ftsindex':False,
        }

        # if we can't find the kdtree, we can't do anything. skip this LCC
        if not os.path.exists(kdtree_fpath):

            msg = 'cannot find kdtree for LCC: %s, skipping...' % lcc
            LOGERROR(msg)

            results[lcc] = {'result':[],
                            'query':(center_ra, center_decl, radius_arcmin),
                            'nmatches':0,
                            'message':msg,
                            'success':False}
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            continue

        # otherwise, continue as normal

        with open(kdtree_fpath, 'rb') as infd:
            kdtreedict = pickle.load(infd)

        kdt = kdtreedict['kdtree']

        # make sure never to exceed maxradius_arcmin
        if radius_arcmin > maxradius_arcmin:
            radius_arcmin = maxradius_arcmin

        searchradiusdeg = radius_arcmin/60.0


        # do the conesearch and get the appropriate kdtree indices
        kdtinds = conesearch_kdtree(kdt,
                                    center_ra,
                                    center_decl,
                                    searchradiusdeg,
                                    conesearchworkers=conesearchworkers)

        # if we returned nothing, that means we had no matches
        if not kdtinds:

            msg = 'no matches in kdtree for LCC: %s, skipping...' % lcc
            LOGERROR(msg)

            results[lcc] = {'result':[],
                            'query':(center_ra, center_decl, radius_arcmin),
                            'nmatches':0,
                            'message':msg,
                            'success':False}
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            continue

        # get the objectids associated with these indices
        matching_objectids = kdtreedict['objectid'][np.atleast_1d(kdtinds)]
        matching_ras = kdtreedict['ra'][np.atleast_1d(kdtinds)]
        matching_decls = kdtreedict['decl'][np.atleast_1d(kdtinds)]

        # sort them so we have a stable order we can use later in the sql
        # statement
        objectid_sortind = np.argsort(matching_objectids)
        matching_objectids = matching_objectids[objectid_sortind]
        matching_ras = matching_ras[objectid_sortind]
        matching_decls = matching_decls[objectid_sortind]

        try:

            # if we have extra filters, apply them
            if extraconditions is not None and len(extraconditions) > 0:

                extraconditionstr = 'where (%s)' % extraconditions

            else:

                extraconditionstr = ''

            # now, we'll get the corresponding info from the database
            thisq = q.format(columnstr=columnstr,
                             collection_id=lcc,
                             extraconditions=extraconditionstr)


            # first, we need to add a temporary table that contains the object
            # IDs of the kdtree results.
            create_temptable_q = (
                "create table _temp_objectid_list "
                "(objectid text, ra double precision, decl double precision, "
                "primary key (objectid))"
            )
            insert_temptable_q = (
                "insert into _temp_objectid_list values (?, ?, ?)"
            )

            LOGINFO('creating temporary match table for '
                    '%s matching kdtree results...' % matching_objectids.size)
            cur.execute(create_temptable_q)
            cur.executemany(insert_temptable_q,
                            [(x,y,z) for (x,y,z) in
                             zip(matching_objectids,
                                 matching_ras,
                                 matching_decls)])

            # now run our query
            LOGINFO('query = %s' % thisq)
            cur.execute(thisq)

            # get the results
            rows = [dict(x) for x in cur.fetchall()]

            # remove the temporary table
            cur.execute('drop table _temp_objectid_list')

            LOGINFO('table-kdtree match complete, generating result rows...')

            # for each row of the results, add in the objectid, ra, decl if
            # they're not already present in the requested columns. also add in
            # the distance from the center of the cone search
            for row in rows:

                obj = row['db_oid']
                ra = row['db_ra']
                decl = row['db_decl']

                # figure out the distances from the search center
                searchcenter_distarcsec = great_circle_dist(
                    center_ra,
                    center_decl,
                    ra,
                    decl
                )

                if 'objectid' not in row:
                    row['objectid'] = obj
                if 'ra' not in row:
                    row['ra'] = ra
                if 'decl' not in row:
                    row['decl'] = decl
                if 'dist_arcsec' not in row:
                    row['dist_arcsec'] = searchcenter_distarcsec


            # make sure to resort the rows in the order of the distances
            rows = sorted(rows, key=lambda row: row['dist_arcsec'])

            # generate the output dict key
            results[lcc] = {'result':rows,
                            'query':thisq,
                            'success':True}

            results[lcc]['nmatches'] = len(rows)
            msg = ('executed query successfully for collection: %s'
                   ', matching nrows: %s' %
                   (lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid


        except Exception as e:

            msg = ('failed to execute query for collection: %s, exception: %s' %
                   (lcc, e))
            LOGEXCEPTION(msg)

            results[lcc] = {
                'result':[],
                'query':q,
                'nmatches':0,
                'message':msg,
                'success':False,
            }
            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise


    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {'center_ra':center_ra,
                       'center_decl':center_decl,
                       'radius_arcmin':radius_arcmin,
                       'getcolumns':rescolumns,
                       'extraconditions':extraconditions,
                       'lcclist':lcclist}

    results['search'] = 'sqlite_kdtree_conesearch'

    db.close()
    return results



###################
## XMATCH SEARCH ##
###################

def sqlite_xmatch_search(basedir,
                         inputdata,
                         xmatch_dist_arcsec=3.0,
                         xmatch_closest_only=False,
                         inputmatchcol=None,
                         dbmatchcol=None,
                         getcolumns=None,
                         extraconditions=None,
                         lcclist=None,
                         require_ispublic=True,
                         max_matchradius_arcsec=30.0,
                         raiseonfail=False):
    '''This does an xmatch between the input and LCC databases.

    - xmatch using coordinates and kdtrees
    - xmatch using an arbitrary column in the input and any column in the LCCs

    inputdata is a dict that has a form like the following:

    {'data':{'col1':[list of col1 items],
             'col2':[list of col2 items],
             'col3':[list of col3 items],
             'col4':[list of col4 items],
             'colN':[list of colN items]},
     'columns':['col1','col2','col3','col4','colN'],
     'types':['int','float','float','str','bool'],
     'colobjectid':name of the objectid column (if None, we'll fake objectids),
     'colra':'name of right ascension column if present' or None,
     'coldec':'name of declination column if present' or None}

    if inputmatchcol is None
       and dbmatchcol is None
       and inputdata['xmatch_dist_arcsec'] is not None -> do coord xmatch

    if one of 'colra', 'coldec' is None, a coordinate xmatch is not possible.

    otherwise, inputmatchcol and dbmatchcol should both not be None and be names
    of columns in the input data dict and an available column in the light curve
    collections specified for use in the xmatch search.

    '''
    # connect to all the specified databases
    dbinfo = sqlite_get_collections(basedir,
                                    lcclist=lcclist,
                                    require_ispublic=require_ispublic)
    db = dbinfo['connection']
    cur = dbinfo['cursor']

    # get the available databases and columns
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            db.close()
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # get the requested columns together
    if getcolumns is not None:

        columnstr = ', '.join('b.%s' % c for c in getcolumns)

        columnstr = ', '.join(
            [columnstr,
             ('b.objectid as db_oid, '
              'b.ra as db_ra, b.decl as db_decl, '
              'b.lcfname as db_lcfname')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname']

    # otherwise, if there are no columns, get the default set of columns for a
    # 'check' cone-search query
    else:
        columnstr = ('b.objectid as db_oid, '
                     'b.ra as db_ra, b.decl as db_decl, '
                     'b.lcfname as db_lcfname')

        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname']

    # make sure we have everything we need from the inputdata

    # if the input data doesn't have an objectid column, we'll make a fake one
    if inputdata['colobjectid'] is None:

        inputdata['columns'].append('in_objectid')
        inputdata['types'].append('str')
        inputdata['colobjectid'] = 'in_objectid'

        inputdata['data']['in_objectid'] = [
            'object-%s' % x
            for x in range(len(input['data'][inputdata['columns'][0]]))
        ]

    # decide if we're doing an xmatch via coords...
    if (inputdata['colra'] is not None and
        inputdata['coldec'] is not None and
        xmatch_dist_arcsec is not None):

        xmatch_type = 'coord'

        # we'll generate a kdtree here for cross-matching
        xmatch_ra = np.atleast_1d(inputdata['data'][inputdata['colra']])
        xmatch_decl = np.atleast_1d(inputdata['data'][inputdata['coldec']])
        xmatch_col = None

        if xmatch_dist_arcsec > max_matchradius_arcsec:
            xmatch_dist_arcsec = max_matchradius_arcsec
            LOGWARNING('match radius %.3f > max possible, setting to %.3f'
                       % (xmatch_dist_arcsec, max_matchradius_arcsec))

        # add the dist_arcsec column to rescolumns
        rescolumns.append('dist_arcsec')

    # or if we're doing an xmatch by table column...
    elif ((inputdata['colra'] is None or inputdata['coldec'] is None) and
          (inputmatchcol is not None and dbmatchcol is not None)):

        xmatch_type = 'column'
        xmatch_col = (
            squeeze(
                inputmatchcol
            ).replace(' ','_').replace('-','_').replace('.','_')
        )


    # or if we're doing something completely nonsensical
    else:

        LOGERROR("column xmatch mode selected but one "
                 "of inputmatchcol/dbmatchcol is not provided, "
                 "can't continue")
        db.close()
        return None


    ###########################################
    ## create a temporary xmatch table first ##
    ###########################################

    # prepare the input data for inserting into a temporary xmatch table
    datatable = [inputdata['data'][x] for x in inputdata['columns']]

    # convert to tuples per row for use with sqlite.cursor.executemany()
    datatable = list(zip(*datatable))

    col_defs = []
    col_names = []

    for col, coltype in zip(inputdata['columns'], inputdata['types']):

        # normalize the column name and add it to a tracking list
        thiscol_name = col.replace(' ','_').replace('-','_').replace('.','_')
        col_names.append(thiscol_name)

        thiscol_type = coltype

        if thiscol_type == 'str':
            col_defs.append('%s text' % thiscol_name)
        elif thiscol_type == 'int':
            col_defs.append('%s integer' % thiscol_name)
        elif thiscol_type == 'float':
            col_defs.append('%s double precision' % thiscol_name)
        elif thiscol_type == 'bool':
            col_defs.append('%s integer' % thiscol_name)
        else:
            col_defs.append('%s text' % thiscol_name)

    column_and_type_list = ', '.join(col_defs)

    # this is the SQL to create the temporary xmatch table
    create_temptable_q = (
        "create table _temp_xmatch_table "
        "({column_and_type_list}, primary key ({objectid_col}))"
    ).format(column_and_type_list=column_and_type_list,
             objectid_col=inputdata['colobjectid'])

    # this is the SQL to make an index on the match column in the xmatch table
    index_temptable_q = (
        "create index xmatch_index on _temp_xmatch_table({xmatch_colname})"
    ).format(xmatch_colname=xmatch_col)

    # this is the SQL to insert all of the input data columns into the xmatch
    # table
    insert_temptable_q = (
        "insert into _temp_xmatch_table values ({placeholders})"
    ).format(placeholders=', '.join(['?']*len(col_names)))

    # 1. create the temporary xmatch table
    cur.execute(create_temptable_q)

    # 2. insert the input data columns
    cur.executemany(insert_temptable_q, datatable)

    # 3. index the xmatch column
    if xmatch_type == 'column':
        cur.execute(index_temptable_q)

    # this is the column string to be used in the query to join the LCC and
    # xmatch tables
    xmatch_columnstr = (
        '%s, %s' % (columnstr, ', '.join('a.%s' % x for x in col_names))
    )

    ###########################################
    ## figure out the xmatch type and run it ##
    ###########################################

    # handle the extra conditions
    if extraconditions is not None and len(extraconditions) > 0:

        # validate this string
        extraconditions = validate_sqlite_filters(extraconditions,
                                                  columnlist=available_columns)


    # handle xmatching by coordinates
    if xmatch_type == 'coord':

        results = {}

        q = (
            "select {columnstr} from {collection_id}.object_catalog b "
            "where b.objectid in ({placeholders}) {extraconditionstr} "
            "order by b.objectid"
        )

        # go through each LCC
        for lcc in uselcc:

            # get the kdtree path
            dbindex = available_lcc.index(lcc)

            kdtree_fpath = dbinfo['info']['kdtree_pkl_path'][dbindex]
            lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
            lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]

            lcc_columnspec = dbinfo['info']['columnjson'][dbindex]
            lcc_collid = dbinfo['info']['collection_id'][dbindex]

            # update the lcc_columnspec with the extra columns we always return
            lcc_columnspec['in_objectid'] = lcc_columnspec['objectid'].copy()
            lcc_columnspec['in_objectid']['title'] = 'input object ID'

            lcc_columnspec['in_oid'] = lcc_columnspec['objectid'].copy()
            lcc_columnspec['in_oid']['title'] = 'input object ID'

            lcc_columnspec['db_oid'] = lcc_columnspec['objectid'].copy()
            lcc_columnspec['db_oid']['title'] = 'database object ID'

            lcc_columnspec['in_ra'] = lcc_columnspec['ra'].copy()
            lcc_columnspec['in_ra']['title'] = 'input &alpha;'

            lcc_columnspec['in_decl'] = lcc_columnspec['decl'].copy()
            lcc_columnspec['in_decl']['title'] = 'input &delta;'

            lcc_columnspec['db_ra'] = lcc_columnspec['ra'].copy()
            lcc_columnspec['db_ra']['title'] = 'database &alpha;'

            lcc_columnspec['db_decl'] = lcc_columnspec['decl'].copy()
            lcc_columnspec['db_decl']['title'] = 'database &delta;'

            lcc_columnspec['db_lcfname'] = lcc_columnspec['lcfname'].copy()
            lcc_columnspec['db_lcfname']['title'] = 'database LC filename'

            # this is the extra spec for dist_arcsec
            lcc_columnspec['dist_arcsec'] = {
                'title': 'distance [arcsec]',
                'format': '%.3f',
                'description':'distance from search center in arcsec',
                'dtype':'<f8',
                'index':True,
                'ftsindex':False,
            }

            # if we can't find the kdtree, we can't do anything. skip this LCC
            if not os.path.exists(kdtree_fpath):

                msg = 'cannot find kdtree for LCC: %s, skipping...' % lcc
                LOGERROR(msg)

                results[lcc] = {'result':[],
                                'query':'xmatch',
                                'nmatches':0,
                                'message':msg,
                                'success':False}
                results[lcc]['lcformatkey'] = lcc_lcformatkey
                results[lcc]['lcformatdesc'] = lcc_lcformatdesc
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid

                continue


            # if we found the lcc's kdtree, load it and do the xmatch now
            with open(kdtree_fpath, 'rb') as infd:
                kdtreedict = pickle.load(infd)

            # load this LCC's ra, decl, and objectids
            lcc_ra = kdtreedict['ra']
            lcc_decl = kdtreedict['decl']
            lcc_objectids = kdtreedict['objectid']

            # this is the distance to use for xmatching
            xmatch_dist_deg = xmatch_dist_arcsec/3600.0

            # generate a kdtree for the inputdata coordinates
            input_coords_kdt = make_kdtree(xmatch_ra, xmatch_decl)

            # run the xmatch. NOTE: here, the extra, extdecl are the LCC ra,
            # decl because we want to get potentially multiple matches in the
            # LCC to each input coordinate
            kdt_matchinds, lcc_matchinds = xmatch_kdtree(
                input_coords_kdt,
                lcc_ra, lcc_decl,
                xmatch_dist_deg,
                closestonly=xmatch_closest_only
            )

            # now, we'll go through each of the xmatches, get their requested
            # information from the database

            # note that the query string below assumes that we only have less
            # than 999 matches per input objectid (this is the max number of
            # input arguments to sqlite3). This should be a reasonable
            # assumption if the match radius isn't too large. We should enforce
            # a match radius of no more than a few arcseconds to make sure this
            # is true.

            this_lcc_results = []

            # if we have extra filters, apply them
            if extraconditions is not None and len(extraconditions) > 0:

                extraconditionstr = 'and (%s)' % extraconditions

            else:

                extraconditionstr = ''

            # for each object ind in the input list that has a possible match,
            # look at the list of matched object inds in the LC collection
            for input_objind, matched_lcc_objind in zip(kdt_matchinds,
                                                        lcc_matchinds):

                matching_lcc_objectids = lcc_objectids[matched_lcc_objind]

                # get the appropriate column info for all of these matched
                # objects from the database
                placeholders = ','.join(['?']*matching_lcc_objectids.size)
                thisq = q.format(columnstr=columnstr,
                                 collection_id=lcc,
                                 placeholders=placeholders,
                                 extraconditionstr=extraconditionstr)

                cur.execute(thisq, tuple(matching_lcc_objectids))

                rows = [dict(x) for x in cur.fetchall()]

                # add in the information from the input data
                inputdata_row = datatable[input_objind]
                inputdata_dict = {}

                for icol, item in zip(col_names, inputdata_row):

                    ircol = 'in_%s' % icol
                    inputdata_dict[ircol] = item

                # for each row add in the input object's info
                for x in rows:
                    x.update(inputdata_dict)

                # add in the distance from the input object
                for row in rows:

                    in_lcc_dist = great_circle_dist(
                        row['db_ra'],
                        row['db_decl'],
                        row['in_%s' % inputdata['colra']],
                        row['in_%s' % inputdata['coldec']]
                    )
                    row['dist_arcsec'] = in_lcc_dist

                # we'll order the results of this objectid search by distance
                # from the input object.
                rows = sorted(rows, key=lambda row: row['dist_arcsec'])
                this_lcc_results.extend(rows)

            #
            # done with this LCC, add in the results to the results dict
            #
            results[lcc] = {'result':this_lcc_results,
                            'query':thisq,
                            'success':True}
            results[lcc]['nmatches'] = len(this_lcc_results)
            msg = ("executed query successfully for collection: %s, "
                   "matching nrows: %s" % (lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)

            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

        #
        # done with all LCCs
        #

        # at the end, add in some useful info
        results['databases'] = available_lcc
        results['columns'] = available_columns

        results['args'] = {'inputdata':inputdata,
                           'xmatch_dist_arcsec':xmatch_dist_arcsec,
                           'xmatch_closest_only':xmatch_closest_only,
                           'inputmatchcol':inputmatchcol,
                           'dbmatchcol':dbmatchcol,
                           'getcolumns':rescolumns,
                           'extraconditions':extraconditions,
                           'lcclist':lcclist}
        results['search'] = 'sqlite_xmatch_search'

        # delete the temporary xmatch table
        cur.execute('drop table _temp_xmatch_table')

        db.close()
        return results


    elif xmatch_type == 'column':

        # this will be a straightforward table join using the inputmatchcol and
        # the dbmatchcol
        results = {}

        # if we have extra filters, apply them
        if extraconditions is not None and len(extraconditions) > 0:

            extraconditionstr = 'where (%s)' % extraconditions

        else:

            extraconditionstr = ''

        # we use a left outer join because we want to keep all the input columns
        # and notice when there are no database matches
        q = (
            "select {columnstr} from "
            "_temp_xmatch_table a "
            "left outer join "
            "{collection_id}.object_catalog b on "
            "(a.{input_xmatch_col} = b.{db_xmatch_col}) {extraconditionstr} "
            "order by a.{input_xmatch_col} asc"
        )

        # go through each LCC
        for lcc in uselcc:

            dbindex = available_lcc.index(lcc)
            lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
            lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]

            lcc_columnspec = dbinfo['info']['columnjson'][dbindex]
            lcc_collid = dbinfo['info']['collection_id'][dbindex]

            # update the lcc_columnspec with the extra columns we always return
            lcc_columnspec['in_oid'] = lcc_columnspec['objectid'].copy()
            lcc_columnspec['in_oid']['title'] = 'input object ID'

            lcc_columnspec['db_oid'] = lcc_columnspec['objectid'].copy()
            lcc_columnspec['db_oid']['title'] = 'database object ID'

            lcc_columnspec['in_ra'] = lcc_columnspec['ra'].copy()
            lcc_columnspec['in_ra']['title'] = 'input &alpha;'

            lcc_columnspec['in_decl'] = lcc_columnspec['decl'].copy()
            lcc_columnspec['in_decl']['title'] = 'input &delta;'

            lcc_columnspec['db_ra'] = lcc_columnspec['ra'].copy()
            lcc_columnspec['db_ra']['title'] = 'database &alpha;'

            lcc_columnspec['db_decl'] = lcc_columnspec['decl'].copy()
            lcc_columnspec['db_decl']['title'] = 'database &delta;'

            lcc_columnspec['db_lcfname'] = lcc_columnspec['lcfname'].copy()
            lcc_columnspec['db_lcfname']['title'] = 'database LC filename'

            # execute the xmatch statement
            thisq = q.format(columnstr=xmatch_columnstr,
                             collection_id=lcc,
                             input_xmatch_col=xmatch_col,
                             db_xmatch_col=dbmatchcol,
                             extraconditionstr=extraconditionstr)

            try:

                cur.execute(thisq)

                # put the results into the right place
                results[lcc] = {'result':cur.fetchall(),
                                'query':thisq,
                                'success':True}
                results[lcc]['nmatches'] = len(results[lcc]['result'])

                msg = ('executed query successfully for collection: %s'
                       ', matching nrows: %s' %
                       (lcc, results[lcc]['nmatches']))
                results[lcc]['message'] = msg
                LOGINFO(msg)

                results[lcc]['lcformatkey'] = lcc_lcformatkey
                results[lcc]['lcformatdesc'] = lcc_lcformatdesc
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid


            except Exception as e:


                msg = ('failed to execute query for '
                       'collection: %s, exception: %s' % (lcc, e))

                LOGEXCEPTION(msg)
                results[lcc] = {'result':[],
                                'query':thisq,
                                'nmatches':0,
                                'message':msg,
                                'success':False}
                results[lcc]['lcformatkey'] = lcc_lcformatkey
                results[lcc]['lcformatdesc'] = lcc_lcformatdesc
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid

                if raiseonfail:
                    raise
        #
        # done with all LCCs
        #

        # at the end, add in some useful info
        results['databases'] = available_lcc
        results['columns'] = available_columns

        results['args'] = {'inputdata':inputdata,
                           'xmatch_dist_arcsec':xmatch_dist_arcsec,
                           'xmatch_closest_only':xmatch_closest_only,
                           'inputmatchcol':inputmatchcol,
                           'dbmatchcol':dbmatchcol,
                           'getcolumns':rescolumns,
                           'extraconditions':extraconditions,
                           'lcclist':lcclist}
        results['search'] = 'sqlite_xmatch_search'

        # delete the temporary xmatch table
        cur.execute('drop table _temp_xmatch_table')

        db.close()
        return results