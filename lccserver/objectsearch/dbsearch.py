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
import re


#####################################
## UTILITY FUNCTIONS FOR DATABASES ##
#####################################

def sqlite_get_collections(basedir,
                           lcclist=None,
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
             "{lccspec}")

    # handle the case where lcclist is not provided, we'll use any LCC available
    # in the database
    if lcclist is not None:

        query = query.format(lccspec='where collection_id in (?)')
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
        newconn.row_factory = sqlite3.Row
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

SQLITE_ALLOWED_WORDS = ['and','between','in',
                        'is','isnull','like','not',
                        'notnull','null','or',
                        '=','<','>','<=','>=','!=','%']

SQLITE_ALLOWED_ORDERBY = ['asc','desc']

SQLITE_ALLOWED_LIMIT = ['limit']

# this is from Tornado's source (MIT License):
# http://www.tornadoweb.org/en/stable/_modules/tornado/escape.html#squeeze
def squeeze(value):
    """Replace all sequences of whitespace chars with a single space."""
    return re.sub(r"[\x00-\x20]+", " ", value).strip()



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
                   "contains non-allowed keywords" % filterstring)
        return None

    else:
        return filterstring



###################
## SQLITE SEARCH ##
###################

def sqlite_fulltext_search(basedir,
                           ftsquerystr,
                           getcolumns,
                           extraconditions=None,
                           lcclist=None,
                           require_ispublic=True):
    '''This searches the specified collections for a full-text match.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. This is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    columns is a list that specifies which columns to return after the query is
    complete.

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


    # get the requested columns together
    columnstr = ', '.join('a.%s' % c for c in getcolumns)

    # this is the query that will be used for FTS
    q = ("select {columnstr} from {collection_id}.object_catalog a join "
         "{collection_id}.catalog_fts b on (a.rowid = b.rowid) where "
         "catalog_fts match ? {extraconditions} "
         "order by bm25(catalog_fts)")

    # handle the extra conditions
    if extraconditions is not None:

        # validate this string
        extraconditions = validate_sqlite_filters(extraconditions,
                                                  columnlist=available_columns)


    # now we have to execute the FTS query for all of the attached databases.
    results = {}

    for lcc in uselcc:

        try:

            # if we have extra filters, apply them
            if extraconditions is not None:

                extraconditionstr = 'and (%s)' % extraconditions

            else:

                extraconditionstr = ''

            # format the query
            thisq = q.format(columnstr=columnstr,
                             collection_id=lcc,
                             ftsquerystr=ftsquerystr,
                             extraconditions=extraconditionstr)

            # execute the query
            cur.execute(thisq, (ftsquerystr,))

            # put the results into the right place
            results[lcc] = {'result':cur.fetchall(),
                            'query':thisq.replace('?',"'%s'" % ftsquerystr),
                            'success':True}
            results[lcc]['nmatches'] = len(results[lcc]['result'])

            msg = ('executed FTS query: "%s" successfully '
                   'for collection: %s, matching nrows: %s' %
                   (ftsquerystr, lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)

        except Exception as e:

            msg = ('failed to execute FTS query: "%s" '
                   'for collection: %s, exception: %s' %
                   (ftsquerystr, lcc, e))

            LOGEXCEPTION(msg)
            results[lcc] = {'result':[],
                            'query':thisq.replace('?',"'%s'" % ftsquerystr),
                            'nmatches':0,
                            'message':msg,
                            'success':False}


    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {'ftsquerystr':ftsquerystr,
                       'getcolumns':getcolumns,
                       'lcclist':lcclist,
                       'extraconditions':extraconditions}


    db.close()
    return results



def sqlite_column_search(basedir,
                         getcolumns,
                         conditions=None,
                         sortby=None,
                         limit=None,
                         lcclist=None,
                         require_ispublic=True):
    '''This runs an arbitrary column search.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. this is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    columns is a list that specifies which columns to return after the query is
    complete.

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
    columnstr = ', '.join('a.%s' % c for c in getcolumns)

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

        thisq = q.format(columnstr=columnstr,
                         collection_id=lcc,
                         wherecondition=wherecondition,
                         sortcondition=sortcondition,
                         limitcondition=limitcondition)

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

        except Exception as e:

            msg = ('failed to execute query for '
                   'collection: %s, exception: %s' % (lcc, e))

            LOGEXCEPTION(msg)
            results[lcc] = {'result':[],
                            'query':thisq,
                            'nmatches':0,
                            'message':msg,
                            'success':False}

    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {'getcolumns':getcolumns,
                       'conditions':conditions,
                       'sortby':sortby,
                       'limit':limit,
                       'lcclist':lcclist}

    db.close()
    return results



def sqlite_sql_search(basedir,
                      sqlstatement,
                      lcclist=None,
                      require_ispublic=True):
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


###################
## KDTREE SEARCH ##
###################

def sqlite_kdtree_conesearch(basedir,
                             conesearchparams,
                             getcolumns=None,
                             extraconditions=None,
                             lcclist=None,
                             require_ispublic=True):
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
    columnstr = ', '.join('a.%s' % c for c in getcolumns)

    # this is the query that will be used
    q = ("select {columnstr} from {collection_id}.object_catalog a "
         "{wherecondition} {sortcondition} {limitcondition}")
