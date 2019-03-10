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

'''

#############
## LOGGING ##
#############

import logging
from lccserver import log_sub, log_fmt, log_date_fmt

DEBUG = False
if DEBUG:
    level = logging.DEBUG
else:
    level = logging.INFO
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=level,
    style=log_sub,
    format=log_fmt,
    datefmt=log_date_fmt,
)

LOGDEBUG = LOGGER.debug
LOGINFO = LOGGER.info
LOGWARNING = LOGGER.warning
LOGERROR = LOGGER.error
LOGEXCEPTION = LOGGER.exception

#############
## IMPORTS ##
#############

import os
import os.path
import sqlite3
import pickle
import json
from functools import reduce, partial
import re
from urllib.parse import quote_plus


from tornado.escape import squeeze, xhtml_unescape
import numpy as np
import requests

from astrobase.coordutils import (
    make_kdtree, conesearch_kdtree,
    xmatch_kdtree, great_circle_dist
)
from astrobase.coordutils import (
    hms_to_decimal, dms_to_decimal,
    hms_str_to_tuple, dms_str_to_tuple
)

# for updating checkplots with SIMBAD and SESAME lookup results
try:
    from astrobase.checkplot.pkl_io import (
        _read_checkplot_picklefile, _write_checkplot_picklefile
    )
except Exception as e:
    from astrobase.checkplot import (
        _read_checkplot_picklefile, _write_checkplot_picklefile
    )

from ..authnzerver.authdb import check_user_access


###########################
## SQLITE UTIL FUNCTIONS ##
###########################

def sqlite3_to_memory(dbfile, dbname,
                      autocommit=False,
                      authorizer=None,
                      authorizer_target=None):
    '''This returns a connection and cursor to the SQLite3 file dbfile.

    The connection is actually made to an in-memory database, and the database
    in dbfile is attached to it. This keeps the original file mostly free of any
    temporary tables we make.

    '''

    # this is the connection we will return
    if autocommit:
        newconn = sqlite3.connect(
            ':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            isolation_level=None
        )
    else:
        newconn = sqlite3.connect(
            ':memory:',
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )

    newconn.row_factory = sqlite3.Row
    newcur = newconn.cursor()

    # turn the cache size up
    newcur.execute('pragma cache_size=-8192')

    # attach the main database to the in-memory database
    newcur.execute("attach database '%s' as %s" % (dbfile, dbname))

    # hook up the authorizer if provided
    if authorizer and authorizer_target:
        callback = partial(authorizer, enforce_dbname=authorizer_target)
        newconn.set_authorizer(callback)

    return newconn, newcur


# https://www.sqlite.org/c3ref/c_alter_table.html
# ******************************************* 3rd ************ 4th ***********/
# define SQLITE_CREATE_INDEX          1   /* Index Name      Table Name      */
# define SQLITE_CREATE_TABLE          2   /* Table Name      NULL            */
# define SQLITE_CREATE_TEMP_INDEX     3   /* Index Name      Table Name      */
# define SQLITE_CREATE_TEMP_TABLE     4   /* Table Name      NULL            */
# define SQLITE_CREATE_TEMP_TRIGGER   5   /* Trigger Name    Table Name      */
# define SQLITE_CREATE_TEMP_VIEW      6   /* View Name       NULL            */
# define SQLITE_CREATE_TRIGGER        7   /* Trigger Name    Table Name      */
# define SQLITE_CREATE_VIEW           8   /* View Name       NULL            */
# define SQLITE_DELETE                9   /* Table Name      NULL            */
# define SQLITE_DROP_INDEX           10   /* Index Name      Table Name      */
# define SQLITE_DROP_TABLE           11   /* Table Name      NULL            */
# define SQLITE_DROP_TEMP_INDEX      12   /* Index Name      Table Name      */
# define SQLITE_DROP_TEMP_TABLE      13   /* Table Name      NULL            */
# define SQLITE_DROP_TEMP_TRIGGER    14   /* Trigger Name    Table Name      */
# define SQLITE_DROP_TEMP_VIEW       15   /* View Name       NULL            */
# define SQLITE_DROP_TRIGGER         16   /* Trigger Name    Table Name      */
# define SQLITE_DROP_VIEW            17   /* View Name       NULL            */
# define SQLITE_INSERT               18   /* Table Name      NULL            */
# define SQLITE_PRAGMA               19   /* Pragma Name     1st arg or NULL */
# define SQLITE_READ                 20   /* Table Name      Column Name     */
# define SQLITE_SELECT               21   /* NULL            NULL            */
# define SQLITE_TRANSACTION          22   /* Operation       NULL            */
# define SQLITE_UPDATE               23   /* Table Name      Column Name     */
# define SQLITE_ATTACH               24   /* Filename        NULL            */
# define SQLITE_DETACH               25   /* Database Name   NULL            */
# define SQLITE_ALTER_TABLE          26   /* Database Name   Table Name      */
# define SQLITE_REINDEX              27   /* Index Name      NULL            */
# define SQLITE_ANALYZE              28   /* Table Name      NULL            */
# define SQLITE_CREATE_VTABLE        29   /* Table Name      Module Name     */
# define SQLITE_DROP_VTABLE          30   /* Table Name      Module Name     */
# define SQLITE_FUNCTION             31   /* NULL            Function Name   */
# define SQLITE_SAVEPOINT            32   /* Operation       Savepoint Name  */
# define SQLITE_COPY                  0   /* No longer used */
# define SQLITE_RECURSIVE            33   /* NULL            NULL            */

def sqlite3_readonly_authorizer(operation,      # opcode from table above
                                arg2,           # '3rd' in table above
                                arg3,           # '4th' in table above
                                target_dbname,  # database name
                                context,        # usually blank
                                enforce_dbname=None):
    '''This is an authorizer that enforces only SELECT queries against
    enforce_dbname.object_catalog.

    docs.python.org/3/library/sqlite3.html#sqlite3.Connection.set_authorizer

    '''

    if ((target_dbname and (target_dbname == enforce_dbname)) and
        ('object_catalog' in arg2 or 'object_catalog' in arg3) and
        (operation not in (sqlite3.SQLITE_SELECT,
                           sqlite3.SQLITE_READ,
                           sqlite3.SQLITE_TRANSACTION))):
        # print(
        #     'opcode: %s, arg 1: %s, arg 2: %s, database: %s denied' % (
        #         operation,
        #         arg2,
        #         arg3,
        #         target_dbname
        #     )
        # )
        return sqlite3.SQLITE_DENY
    else:
        # print(
        #     'opcode: %s, arg 1: %s, arg 2: %s, database: %s OK' % (
        #         operation,
        #         arg2,
        #         arg3,
        #         target_dbname
        #     )
        # )
        return sqlite3.SQLITE_OK



############################
## PARSING FILTER STRINGS ##
############################

SQLITE_ALLOWED_WORDS_FOR_FILTERS = [
    'and','between','in',
    'is','isnull','like','not',
    'notnull','null','or',
    '=','<','>','<=','>=','!=','%'
]

SQLITE_ALLOWED_ORDERBY_FOR_FILTERS = ['asc','desc']

SQLITE_ALLOWED_LIMIT_FOR_FILTERS = ['limit']

SQLITE_DISALLOWED_COLUMNS_FOR_FILTERS = [
    'object_is_public',
    'collection_visibility',
    'object_visibility',
    'dataset_visibility',
    'collection_owner',
    'object_owner',
    'dataset_owner',
    'collection_sharedwith',
    'object_sharedwith',
    'dataset_sharedwith',
]

SQLITE_DISALLOWED_STRINGS_FOR_FILTERS = [
    ';'
    '--',
    '||',
    'drop',
    'delete',
    'update',
    'insert',
    'alter',
    'create',
    'pragma',
    'attach',
    'analyze',
    'cascade',
    'commit',
    'conflict',
    'autoincrement',
    'database',
    'detach',
    'index',
    'glob',
    'exclusive',
    'into',
    'recursive',
    'rename',
    'begin',
    'rollback',
    'savepoint',
    'trigger',
    'vacuum',
    'view',
    'virtual',
]


def validate_sqlite_filters(
        filterstring,
        columnlist=None,
        disallowed_strings=(
            SQLITE_DISALLOWED_STRINGS_FOR_FILTERS +
            SQLITE_DISALLOWED_COLUMNS_FOR_FILTERS
        ),
        allowedsqlwords=SQLITE_ALLOWED_WORDS_FOR_FILTERS,
        otherkeywords=None
):
    '''This validates the sqlitecurve filter string.

    This MUST be valid SQL but not contain any commands.

    '''

    # fail immediately if any of the disallowed_strings are in the query string
    for dstr in disallowed_strings:
        if dstr in filterstring:
            LOGERROR('found disallowed string: %s in query, '
                     'bailing out immediately' % dstr)
            return None

    #
    # otherwise, continue as usual
    #

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
            float(x)
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

        # at the end, check again for disallowed words
        reconstructed_filterstring = ' '.join(stringelems)

        for dstr in disallowed_strings:
            if dstr in reconstructed_filterstring:
                LOGERROR('found disallowed string: %s in reconstructed query, '
                         'bailing out immediately' % dstr)
                return None

        # if all succeeded, return the filterstring to indicate it's ok
        return filterstring


############################################
## PARSING FREE-FORM SQL and ADQL QUERIES ##
############################################

# inspired by:
# https://github.com/simonw/datasette/blob/master/datasette/utils.py
SQLITE_ALLOWED_QUERY_STARTERS = [
    'select',
    'explain query plan select',
]

# FIXME: we need to add:
# - [ ] absolutely paranoid input SQL parsing
# - [ ] an SQL parser using https://sqlparse.readthedocs.io/en/latest
# - [ ] use that to parse the SQL into an AST
# - [ ] find ADQL spatial terms in the AST and translate them to kdtree searches
# - [ ] re-assemble the AST without the ADQL terms into a pure SQL column query
# - [ ] run the kd-tree and column queries and match results based on db_oid
#       (the existing sqlite_kdtree_conesearch can probably do most of this if
#       we give it the parsed filter string and add in the sort col/order spec
#       and limit/offset spec)



#################################
## SQLITE COLLECTION FUNCTIONS ##
#################################

def sqlite_get_collections(basedir,
                           lcclist=None,
                           return_sorted=False,
                           intended_action='view',
                           return_connection=True,
                           incoming_userid=2,
                           incoming_role='anonymous'):
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
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )

    indexdb.row_factory = sqlite3.Row
    cur = indexdb.cursor()

    # get the info we need
    query = ("select collection_id, object_catalog_path, kdtree_pkl_path, "
             "columnlist, indexedcols, ftsindexedcols, name, "
             "description, project, datarelease, last_updated, citation, "
             "ra_min, ra_max, decl_min, decl_max, nobjects, "
             "lcformat_key, lcformat_desc_path, lcformat_magcols, "
             "catalog_columninfo_json, "
             "collection_owner, collection_visibility, collection_sharedwith "
             "from lcc_index "
             "{lccspec}")

    # handle the case where lcclist is provided
    if lcclist:

        sanitized_lcclist = []

        # scan through and get rid of some troubling stuff
        for lcc in lcclist:
            if lcc is not None:
                thislcc = squeeze(lcc).strip().replace(' ','')
                if len(thislcc) > 0 and thislcc != 'all':
                    sanitized_lcclist.append(thislcc)

        # we need to do this because we're mapping from directory names on the
        # filesystem that may contain hyphens (although they really shouldn't)
        # and database names in the sqlite3 table which can't have hyphens
        if len(sanitized_lcclist) > 0:

            lccspec = ("where replace(collection_id,'-','_') in (%s)" %
                       (','.join(['?' for x in sanitized_lcclist])))
            params = sanitized_lcclist
            query = query.format(
                lccspec=lccspec
            )
            if return_sorted:
                query = '%s order by collection_id asc' % query

            cur.execute(query, params)

        # if no collection made it through sanitation, we'll search all of them
        else:
            lccspec = ''
            query = query.format(
                lccspec=''
            )
            if return_sorted:
                query = '%s order by collection_id asc' % query
            cur.execute(query)


    # otherwise, we'll use any LCC available in the database
    else:

        query = query.format(lccspec='')
        cur.execute(query)

    xresults = cur.fetchall()
    indexdb.close()

    # if we got the databases, then proceed
    if xresults and len(xresults) > 0:

        # filter the results using access control

        results = [
            x for x in xresults if
            check_user_access(
                userid=incoming_userid,
                role=incoming_role,
                action=intended_action,
                target_name='collection',
                target_owner=x['collection_owner'],
                target_visibility=x['collection_visibility'],
                target_sharedwith=x['collection_sharedwith']
            )
        ]

        if len(results) > 0:

            results = list(zip(*list(results)))

            (collection_id, object_catalog_path,
             kdtree_pkl_path, columnlist,
             indexedcols, ftsindexedcols, name,
             description, project, datarelease,
             last_updated, citation,
             minra, maxra, mindecl, maxdecl,
             nobjects,
             lcformatkey, lcformatdesc, lcmagcols,
             columnjson,
             collection_owner,
             collection_visibility,
             collection_sharedwith) = results

            dbnames = [x.replace('-','_') for x in collection_id]

            # this is the intersection of all columns available across all
            # collections.  effectively defines the cross-collection columns
            # available for use in queries
            collection_columns = [set(x.split(',')) for x in columnlist]
            columns_available = reduce(lambda x,y: x.intersection(y),
                                       collection_columns)

            # the same, but for columns that have indexes on them
            indexed_columns = [set(x.split(',')) for x in indexedcols]
            indexed_cols_available = reduce(lambda x,y: x.intersection(y),
                                            indexed_columns)

            # the same but for columns that have FTS indexes on them
            fts_columns = [set(x.split(',')) for x in ftsindexedcols]
            ftsindexed_cols_available = reduce(lambda x,y: x.intersection(y),
                                               fts_columns)

            if return_connection:

                # this is the connection we will return
                newconn = sqlite3.connect(
                    ':memory:',
                    detect_types=(
                        sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                    )
                )
                newconn.row_factory = sqlite3.Row
                newcur = newconn.cursor()

                for dbn, catpath in zip(dbnames, object_catalog_path):
                    newcur.execute(
                        "attach database '%s' as %s" % (catpath, dbn)
                    )

            else:

                newconn = None
                newcur = None


            outdict = {
                'connection':newconn,
                'cursor':newcur,
                'databases':dbnames,
                'columns':list(columns_available),
                'indexedcols':list(indexed_cols_available),
                'ftscols':list(ftsindexed_cols_available),
                'info':{
                    'collection_id':collection_id,
                    'db_collection_id':[
                        d.replace('-','_') for d in collection_id
                    ],
                    'object_catalog_path':object_catalog_path,
                    'kdtree_pkl_path':kdtree_pkl_path,
                    'columnlist':columnlist,
                    'indexedcols':indexedcols,
                    'ftsindexedcols':ftsindexedcols,
                    'collection_owner':collection_owner,
                    'collection_visibility':collection_visibility,
                    'collection_sharedwith':collection_sharedwith,
                    'name':name,
                    'description':description,
                    'project':project,
                    'datarelease':datarelease,
                    'last_updated':last_updated,
                    'citation':citation,
                    'minra':minra,
                    'maxra':maxra,
                    'mindecl':mindecl,
                    'maxdecl':maxdecl,
                    'nobjects':nobjects,
                    'lcformatkey':lcformatkey,
                    'lcformatdesc':lcformatdesc,
                    'lcmagcols':lcmagcols,
                    'columnjson': [json.loads(c) for c in columnjson]
                }
            }

            return outdict

        # if no collections match the incoming user_id, role, then return
        # nothing
        else:
            LOGERROR('no viewable collections found for '
                     'incoming_userid = %s, incoming_role = %s' %
                     (incoming_userid, incoming_role))
            return None

    else:

        LOGERROR('could not find any information '
                 'about the requested LCC collections')
        return None



def sqlite_list_collections(basedir,
                            incoming_userid=2,
                            return_sorted=True,
                            incoming_role='anonymous'):
    '''
    This just lists the collections in basedir.

    '''

    return sqlite_get_collections(basedir,
                                  return_sorted=True,
                                  incoming_userid=incoming_userid,
                                  incoming_role=incoming_role,
                                  return_connection=False)


###################
## SIMBAD SEARCH ##
###################

# single object coordinate search
# ra dec radius
COORD_DEGSEARCH_REGEX = re.compile(
    r'^(\d{1,3}\.{0,1}\d*) ([+\-]?\d{1,2}\.{0,1}\d*) ?(\d{1,2}\.{0,1}\d*)?$'
)
COORD_HMSSEARCH_REGEX = re.compile(
    r'^(\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*) '
    r'([+\-]?\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*) ?'
    r'(\d{1,2}\.{0,1}\d*)?$'
)


def parse_coordstring(coordstring):
    '''
    This function parses a coordstring of the form:

    <ra> <dec> <radiusarcmin>

    '''
    searchstr = squeeze(coordstring).strip()

    # try all the regexes and see if one of them works
    degcoordtry = COORD_DEGSEARCH_REGEX.match(searchstr)
    hmscoordtry = COORD_HMSSEARCH_REGEX.match(searchstr)

    # try HHMMSS first because we get false positives on some HH MM SS items in
    # degcoordtry
    if hmscoordtry:

        ra, dec, radius = hmscoordtry.groups()
        ra_tuple, dec_tuple = hms_str_to_tuple(ra), dms_str_to_tuple(dec)

        ra_hr, ra_min, ra_sec = ra_tuple
        dec_sign, dec_deg, dec_min, dec_sec = dec_tuple

        # make sure the coordinates are all legit
        if ((0 <= ra_hr < 24) and
            (0 <= ra_min < 60) and
            (0 <= ra_sec < 60) and
            (0 <= dec_deg < 90) and
            (0 <= dec_min < 60) and
            (0 <= dec_sec < 60)):

            ra_decimal = hms_to_decimal(ra_hr, ra_min, ra_sec)
            dec_decimal = dms_to_decimal(dec_sign, dec_deg, dec_min, dec_sec)

            paramsok = True
            searchrad = float(radius)/60.0 if radius else 5.0/60.0
            radeg, decldeg, radiusdeg = ra_decimal, dec_decimal, searchrad

        else:

            paramsok = False
            radeg, decldeg, radiusdeg = None, None, None

    elif degcoordtry:

        ra, dec, radius = degcoordtry.groups()

        try:
            ra, dec = float(ra), float(dec)
            if ((abs(ra) < 360.0) and (abs(dec) < 90.0)):
                if ra < 0:
                    ra = 360.0 + ra
                paramsok = True
                searchrad = float(radius)/60.0 if radius else 5.0/60.0
                radeg, decldeg, radiusdeg = ra, dec, searchrad

            else:
                paramsok = False
                radeg, decldeg, radiusdeg = None, None, None

        except Exception as e:

            LOGGER.error('could not parse search string: %s' % coordstring)
            paramsok = False
            radeg, decldeg, radiusdeg = None, None, None


    else:

        paramsok = False
        radeg, decldeg, radiusdeg = None, None, None

    return paramsok, radeg, decldeg, radiusdeg



def parse_sesame_response(
        sesame_response,
        object_name,
):
    '''Parses the SESAME results to a dict containing the name and coords.

    '''

    if 'Nothing found' in sesame_response:
        LOGWARNING('SIMBAD SESAME name resolution '
                   'did not succeed for object name: %s' %
                   object_name)
        return None

    else:

        lines = sesame_response.split('\n')
        coordline = [x.strip() for x in lines if x.startswith('%J')]
        if len(coordline) > 0:
            coords = coordline[0]
        else:
            LOGWARNING('SIMBAD SESAME did not return '
                       'any coords for object name: %s' %
                       object_name)
            return None

        coords = coords.replace('%J','').split(' = ')[0]
        coords_ok, ra, decl, radius = parse_coordstring(coords)

        if not coords_ok:

            LOGWARNING('could not understand returned SESAME '
                       'coord string: %s for object name: %s' %
                       (coords, object_name))
            return None

        else:

            #
            # go ahead and get the rest of the SIMBAD info
            #
            simbad_main_id = [x.strip() for x
                              in lines if x.startswith('%I.0')]
            if len(simbad_main_id) > 0:
                simbad_main_id = (
                    simbad_main_id[0].replace('%I.0','').strip()
                )
            else:
                simbad_main_id = None

            simbad_other_ids = [x.strip() for x
                                in lines if
                                (x.startswith('%I') and '%I.0' not in x)]
            if len(simbad_other_ids) > 0:
                simbad_other_ids = [
                    x.replace('%I','').strip() for x in simbad_other_ids
                ]
                simbad_other_ids = (
                    '; '.join(simbad_other_ids) + '; %s' % object_name
                )
            else:
                simbad_other_ids = object_name

            simbad_object_type = [x.strip() for x
                                  in lines if x.startswith('%C.0')]
            simbad_star_class = [x.strip() for x
                                 in lines if x.startswith('%S')]
            if len(simbad_object_type) > 0:
                simbad_object_type = (
                    simbad_object_type[0].replace('%C.0','').strip()
                )
            else:
                simbad_object_type = None

            if len(simbad_star_class) > 0:
                simbad_star_class = (
                    simbad_star_class[0].replace('%S','').strip()
                )
                simbad_star_class = simbad_star_class.split('=')[0]
            else:
                simbad_star_class = None

            if simbad_object_type and simbad_star_class:
                simbad_best_objtype = '%s; %s' % (simbad_object_type,
                                                  simbad_star_class)
            elif simbad_object_type:
                simbad_best_objtype = simbad_object_type
            elif simbad_star_class:
                simbad_best_objtype = simbad_star_class
            else:
                simbad_best_objtype = None

            return {
                'name':object_name,
                'ra':ra,
                'decl':decl,
                'simbad_best_mainid':simbad_main_id,
                'simbad_best_allids':simbad_other_ids,
                'simbad_best_objtype':simbad_best_objtype,
            }



def sesame_query(
        object_name,
        timeout=5.0,
        mirrors=(
            'http://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame',
            'https://cdsweb.u-strasbg.fr/cgi-bin/nph-sesame',
        ),
        raiseonfail=False
):
    '''This talks to SIMBAD to resolve an object name to its coords.

    '''

    reqok = False

    for mirror in mirrors:

        req_url = mirror + '/-oI/SNV?' + quote_plus(object_name)

        try:

            resp = requests.get(req_url, timeout=timeout)
            resp.raise_for_status()

            reqok = True
            break

        except Exception as e:

            # if this request failed, try the next mirror
            LOGGER.warning(
                'SIMBAD mirror: %s not responding, trying another...'
                % mirror
            )

            if raiseonfail:
                raise

            continue


    # if the request succeeded,
    if reqok:
        parsed = parse_sesame_response(resp.text, object_name)
        LOGINFO('SESAME lookup results = %r' % parsed)
        resp.close()
        return parsed

    else:
        LOGWARNING('SIMBAD SESAME server requests did not succeed '
                   'for object name: %s' % object_name)
        return None



def parse_simbad_response(simbad_response):
    '''
    This returns the contents of the simbad response in a dict.

    '''

    if 'No astronomical object found' in simbad_response:
        LOGERROR('no SIMBAD info available for this object')
        return None

    elif 'Object' in simbad_response and 'Coordinates' in simbad_response:

        lines = [x for x in simbad_response.split('\n')
                 if ((len(x) > 0) and
                     ('------' not in x) and
                     ('======' not in x))]

        # get the main object ID and best object type
        main_objectid_and_type = [
            x.split('---')[:2] for x in lines if x.startswith('Object')
        ]

        if len(main_objectid_and_type) > 0:

            main_objectid_and_type = main_objectid_and_type[0]

            main_objectid = main_objectid_and_type[0].replace(
                'Object',''
            ).strip()

            if (len(main_objectid_and_type) > 1 and
                'OID' not in main_objectid_and_type[1]):
                best_objtype = main_objectid_and_type[1].strip()

            else:
                best_objtype = None

        else:
            main_objectid = None
            best_objtype = None

        # get the spectral type
        spectral_type = [x.replace('Spectral type: ','') for x in lines
                         if (x.startswith('Spectral type:') and '~' not in x)]
        if len(spectral_type) > 0:
            spectral_type = spectral_type[0]
        else:
            spectral_type = None

        if best_objtype is None and spectral_type is not None:
            best_objtype = spectral_type
        elif best_objtype is None and spectral_type is None:
            best_objtype = None
        elif best_objtype is not None and spectral_type is None:
            best_objtype = best_objtype
        else:
            best_objtype = '%s; %s' % (best_objtype, spectral_type)

        # get the other identifiers if present
        identifier_line = [x.strip() for x in lines
                           if x.startswith('Identifiers')]

        if len(identifier_line) > 0:
            nidentifiers = identifier_line[0].split()[1].replace(
                '(',''
            ).replace(')','').replace(':','')
            try:
                nidentifiers = int(nidentifiers)
            except Exception as e:
                nidentifiers = 0

            if nidentifiers > 0:

                # let's grab everything after the identifier line and below and
                # stuff it into the identifiers column (it's effectively an FTS
                # document now)
                identifier_index = [lines.index(x) for x in lines
                                    if x.startswith('Identifiers')]

                identifier_lines = [squeeze(x) for x in
                                    lines[identifier_index[0]+1:] if
                                    (('Bibcodes' not in x) and
                                     ('Measures' not in x) and
                                     ('PLX' not in x) and
                                     ('Notes' not in x))]

                all_identifiers = '; '.join(identifier_lines)

            else:

                all_identifiers = None

        else:
            all_identifiers = None


        return {
            'simbad_best_mainid': main_objectid,
            'simbad_best_objtype': best_objtype,
            'simbad_best_allids': all_identifiers,
            'n_allids':nidentifiers
        }


    else:
        LOGERROR('could not parse SIMBAD response for this object')
        return None



def updatedb_with_simbad_info(basedir,
                              objectid,
                              collection,
                              current_info,
                              simbad_results,
                              incoming_userid=2,
                              incoming_role='anonymous'):
    '''
    This updates the SIMBAD info in the DB with the new results.

    FIXME: the frontend should also update the checkplot with the new info.

    '''

    dbinfo = sqlite_get_collections(
        basedir,
        lcclist=[collection],
        return_connection=True,
        incoming_userid=incoming_userid,
        incoming_role=incoming_role
    )
    db, cur = dbinfo['connection'], dbinfo['cursor']

    cur.execute('begin')

    #
    # we'll only update the columns if they're null and the new values are not
    #
    updated = False

    if (simbad_results['simbad_best_mainid'] is not None and
        current_info['simbad_best_mainid'] is None):

        query = (
            "update object_catalog set "
            "simbad_best_mainid = ? "
            "where objectid = ?"
        )
        params = (simbad_results['simbad_best_mainid'], objectid)
        cur.execute(query, params)
        updated = True
        LOGINFO('updated simbad_best_mainid')

    if (simbad_results['simbad_best_objtype'] is not None and
        current_info['simbad_best_objtype'] is None):
        query = (
            "update object_catalog set "
            "simbad_best_objtype = ? "
            "where objectid = ?"
        )
        params = (simbad_results['simbad_best_objtype'], objectid)
        cur.execute(query, params)
        updated = True
        LOGINFO('updated simbad_best_objtype')

    if (simbad_results['simbad_best_allids'] is not None and
        current_info['simbad_best_allids'] is None):
        query = (
            "update object_catalog set "
            "simbad_best_allids = ? "
            "where objectid = ?"
        )
        params = (simbad_results['simbad_best_allids'], objectid)
        cur.execute(query, params)
        updated = True
        LOGINFO('updated simbad_best_allids')

    if updated:

        query = (
            "update object_catalog set "
            "simbad_best_distarcsec = ? "
            "where objectid = ?"
        )
        params = (5.0, objectid)
        cur.execute(query, params)

    db.commit()
    db.close()

    LOGINFO('database updated for objectid: %s '
            'in collection: %s with SIMBAD info' %
            (objectid, collection))

    return updated



def sqlite_simbad_objectsearch(
        basedir,
        objectid,
        collection,
        radius_arcmin=0.08,
        timeout=5.0,
        mirrors=(
            'http://simbad.u-strasbg.fr/simbad/sim-coo',
        ),
        raiseonfail=False,
        incoming_userid=2,
        incoming_role='anonymous',
        force_update=False,
        updatedb_from_simbad=True,
        update_checkplot_pickle=True,
):
    '''This does a basic coordinate search on SIMBAD and updates missing info.

    The purpose is to fill or update SIMBAD information for objects after the
    initial LCC-Server ingestion, especially if the SIMBAD queries failed or
    were not tried in that stage.

    '''

    # look up this object in the collection
    res = sqlite_column_search(
        basedir,
        getcolumns=['simbad_best_mainid',
                    'simbad_best_objtype',
                    'simbad_best_allids',
                    'simbad_best_distarcsec'],
        conditions="(objectid = '%s')" % objectid,
        lcclist=[collection],
        limit=1,
        raiseonfail=raiseonfail,
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
    )

    if (res and
        res[collection]['result'] and
        res[collection]['result'][0]['db_oid'] == objectid):

        ra = res[collection]['result'][0]['db_ra']
        decl = res[collection]['result'][0]['db_decl']

    else:

        LOGERROR('could not look up %s, collection: %s in the DB' %
                 (objectid, collection))
        return res

    # check if we can return all info from the database already
    if (res[collection]['result'][0]['simbad_best_mainid'] is not None and
        res[collection]['result'][0]['simbad_best_objtype'] is not None and
        res[collection]['result'][0]['simbad_best_allids'] is not None and
        not force_update):

        LOGWARNING('all SIMBAD info already present in the DB for this object')
        return res

    #
    # otherwise, we'll look up the info from SIMBAD
    #

    params = {'Coord':'%.3f %.3f' % (ra, decl),
              'Radius':radius_arcmin,
              'Radius.unit':'arcmin',
              'output.format':'ASCII',
              'output.max':1}

    reqok = False
    for mirror in mirrors:

        req_url = mirror

        try:

            resp = requests.get(req_url, params, timeout=timeout)
            resp.raise_for_status()
            reqok = True
            break

        except Exception as e:

            # if this request failed, try the next mirror
            LOGGER.warning(
                'SIMBAD mirror: %s not responding, trying another...'
                % mirror
            )

            if raiseonfail:
                raise

            continue

    # if the request succeeded,
    if reqok:

        parsed = parse_simbad_response(resp.text)
        LOGINFO('SIMBAD lookup results = %r' % parsed)
        resp.close()

        if updatedb_from_simbad and parsed is not None:

            current_info = {
                'simbad_best_mainid': (
                    res[collection]['result'][0]['simbad_best_mainid']
                ),
                'simbad_best_objtype': (
                    res[collection]['result'][0]['simbad_best_objtype']
                ),
                'simbad_best_allids': (
                    res[collection]['result'][0]['simbad_best_allids']
                ),
            }

            updated = updatedb_with_simbad_info(basedir,
                                                objectid,
                                                collection,
                                                current_info,
                                                parsed,
                                                incoming_userid=incoming_userid,
                                                incoming_role=incoming_role)

            if update_checkplot_pickle:

                cplist = [os.path.join(basedir,
                                       collection.replace('_','-'),
                                       'checkplots',
                                       'checkplot-%s-%s.pkl' % (objectid,
                                                                magcol)) for
                          magcol in res[collection]['lcmagcols'].split(',')]

                for cp in cplist:
                    if os.path.exists(cp):
                        cpd = _read_checkplot_picklefile(cp)
                        cpd['objectinfo']['simbad_status'] = (
                            'ok: updated from query'
                        )
                        cpd['objectinfo']['simbad_nmatches'] = 1
                        cpd['objectinfo']['simbad_best_mainid'] = (
                            parsed['simbad_best_mainid']
                        )
                        cpd['objectinfo']['simbad_best_objtype'] = (
                            parsed['simbad_best_objtype']
                        )
                        cpd['objectinfo']['simbad_best_allids'] = (
                            parsed['simbad_best_allids']
                        )
                        cpd['objectinfo']['simbad_best_distarcsec'] = (
                            5.0
                        )
                        _write_checkplot_picklefile(
                            cpd,
                            outfile=cp,
                            protocol=pickle.HIGHEST_PROTOCOL,
                        )
                        LOGINFO('updated checkplot pickle for object: %s' %
                                cp)

        else:
            updated = False

        if updated:

            return sqlite_column_search(
                basedir,
                getcolumns=['simbad_best_mainid',
                            'simbad_best_objtype',
                            'simbad_best_allids',
                            'simbad_best_distarcsec'],
                conditions="(objectid = '%s')" % objectid,
                lcclist=[collection],
                limit=1,
                raiseonfail=raiseonfail,
                incoming_userid=incoming_userid,
                incoming_role=incoming_role,
            )
        else:
            return res

    else:
        LOGWARNING('SIMBAD server requests did not succeed '
                   'for object ID: %s in collection: %s' % (objectid,
                                                            collection))
        return res



def sqlite_sesame_fulltext_search(
        basedir,
        ftsquerystr,
        getcolumns=None,
        conditions=None,
        lcclist=None,
        raiseonfail=False,
        incoming_userid=2,
        incoming_role='anonymous',
        fail_if_conditions_invalid=True,
        censor_searchargs=False,
        updatedb_from_sesame=True,
        force_update=False,
        update_checkplot_pickle=True,
):

    '''This runs a full-text search query followed by a SESAME lookup and
    cone-search if that fails.

    The use case is to try to complete FTS queries that don't return anything
    locally, so we do a SESAME look-up, then a cone-search at the coordinates
    returned.

    '''

    # force a literal name search because this is a special mode dedicated to
    # looking up object names
    fulltext_search = sqlite_fulltext_search(
        basedir,
        # fix quotes
        '"%s"' % ftsquerystr.replace('"','').replace('&quot;',''),
        getcolumns=getcolumns,
        conditions=conditions,
        lcclist=lcclist,
        raiseonfail=raiseonfail,
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
        fail_if_conditions_invalid=fail_if_conditions_invalid,
        censor_searchargs=censor_searchargs
    )

    nmatches = sum([fulltext_search[x]['nmatches']
                    for x in fulltext_search['databases']])


    if nmatches == 0 or force_update:

        LOGWARNING('no local name matches found or force_update = True, '
                   'looking up object name in SIMBAD')

        sesame_lookup = sesame_query(
            ftsquerystr.replace('"','').replace('&quot;',''),
            raiseonfail=raiseonfail
        )

        if sesame_lookup is not None:

            # special handling if the provided object type is not a star. we'll
            # expand the search radius to 1 deg so people can look at LCs we
            # have near the cluster center or object center. in this case, we
            # will update the extra_info_json for each object in the DB. this
            # way, we'll be able to return searches for this term much quicker
            # from the local DB.
            if (sesame_lookup['simbad_best_objtype'] is not None and
                ( ('OpC' in sesame_lookup['simbad_best_objtype']) or
                  ('Cl' in sesame_lookup['simbad_best_objtype']) or
                  ('Gl' in sesame_lookup['simbad_best_objtype']) or
                  ('As*' in sesame_lookup['simbad_best_objtype']) or
                  ('St*' in sesame_lookup['simbad_best_objtype']) or
                  ('MGr' in sesame_lookup['simbad_best_objtype']) or
                  ('Cld' in sesame_lookup['simbad_best_objtype']) or
                  ('PN' in sesame_lookup['simbad_best_objtype']) or
                  ('Ne' in sesame_lookup['simbad_best_objtype']) or
                  ('C?*' in sesame_lookup['simbad_best_objtype']) or
                  ('SR' in sesame_lookup['simbad_best_objtype']) or
                  ('SNR' in sesame_lookup['simbad_best_objtype']) )):
                search_radius = 60.0
                update_mode = 'extra'
                LOGINFO('running star cluster or non-stellar object '
                        'query using simbad_best_objtype: %s '
                        'for object_name: %s' %
                        (sesame_lookup['simbad_best_objtype'],
                         ftsquerystr))

            else:

                search_radius = 0.08
                update_mode = 'simbad'

            # do the cone search
            cone_search = sqlite_kdtree_conesearch(
                basedir,
                sesame_lookup['ra'],
                sesame_lookup['decl'],
                search_radius,
                getcolumns=getcolumns,
                conditions=conditions,
                lcclist=lcclist,
                raiseonfail=raiseonfail,
                incoming_userid=incoming_userid,
                incoming_role=incoming_role,
                fail_if_conditions_invalid=fail_if_conditions_invalid,
                censor_searchargs=censor_searchargs
            )

            # if we're supposed to update the database after the query
            # completes, do that here
            nmatches = sum([cone_search[x]['nmatches']
                            for x in fulltext_search['databases']])

            LOGINFO('matching objects found in cone '
                    'search after SIMBAD lookup: %s' %
                    nmatches)

            if nmatches > 0:

                conesearch_matched_collections = [
                    x for x in cone_search['databases'] if
                    (cone_search[x]['nmatches'] > 0)
                ]

                LOGINFO('matched objects found in collections: %s' %
                        conesearch_matched_collections)

                if updatedb_from_sesame:

                    if update_mode == 'simbad':

                        query = (
                            "update object_catalog set "
                            "simbad_best_mainid = ?, "
                            "simbad_best_allids = ?, "
                            "simbad_best_objtype = ? where "
                            "objectid = ?"
                        )
                        params = [sesame_lookup['simbad_best_mainid'],
                                  sesame_lookup['simbad_best_allids'],
                                  sesame_lookup['simbad_best_objtype']]


                    # otherwise, the update mode is 'extra', so we'll be
                    # patching the JSON in the extra_info_json column of the
                    # object_catalog table
                    else:

                        query = (
                            "update object_catalog set "
                            "extra_info_json = json_replace("
                            "extra_info_json, '$.parent', json(?)"
                            ") where objectid = ?"
                        )
                        params = [json.dumps({
                            'simbad_best_mainid':(
                                sesame_lookup['simbad_best_mainid']
                            ),
                            'simbad_best_allids':(
                                sesame_lookup['simbad_best_allids']
                            ),
                            'simbad_best_objtype':(
                                sesame_lookup['simbad_best_objtype']
                            ),
                        })]

                    # get these collections and update them
                    for coll in conesearch_matched_collections:

                        dbinfo = sqlite_get_collections(
                            basedir,
                            lcclist=[coll],
                            return_connection=True,
                            incoming_userid=incoming_userid,
                            incoming_role=incoming_role
                        )
                        db, cur = dbinfo['connection'], dbinfo['cursor']

                        cur.execute('begin')

                        # now, for each matched object, look up by db_oid and
                        # update its SIMBAD info
                        for row in cone_search[coll]['result']:

                            oid = row['db_oid']
                            matchdist = row['dist_arcsec']
                            qparams = tuple(params + [oid])

                            cur.execute(query, qparams)

                            LOGINFO(
                                "updated objectid = %s in "
                                "collection = %s with "
                                "match_dist = %.3f arcsec, "
                                "with SIMBAD attrs:\n"
                                "simbad_best_mainid = %s, "
                                "simbad_best_allids = %s, "
                                "simbad_best_objtype = %s"
                                % (oid,
                                   coll,
                                   matchdist,
                                   sesame_lookup['simbad_best_mainid'],
                                   sesame_lookup['simbad_best_allids'],
                                   sesame_lookup['simbad_best_objtype'])
                            )

                            if update_checkplot_pickle:

                                cplist = [
                                    os.path.join(
                                        basedir,
                                        coll.replace('_','-'),
                                        'checkplots',
                                        'checkplot-%s-%s.pkl' % (oid,
                                                                 magcol)
                                    ) for magcol in
                                    cone_search[coll]['lcmagcols'].split(',')
                                ]

                                for cp in cplist:

                                    if os.path.exists(cp):

                                        cpd = _read_checkplot_picklefile(cp)

                                        if update_mode == 'simbad':

                                            cpd['objectinfo'][
                                                'simbad_status'
                                            ] = (
                                                'ok: updated from query'
                                            )
                                            cpd['objectinfo'][
                                                'simbad_nmatches'
                                            ] = 1
                                            cpd['objectinfo'][
                                                'simbad_best_mainid'
                                            ] = (
                                                sesame_lookup[
                                                    'simbad_best_mainid'
                                                ]
                                            )
                                            cpd['objectinfo'][
                                                'simbad_best_objtype'
                                            ] = (
                                                sesame_lookup[
                                                    'simbad_best_objtype'
                                                ]
                                            )
                                            cpd['objectinfo'][
                                                'simbad_best_allids'
                                            ] = (
                                                sesame_lookup[
                                                    'simbad_best_allids'
                                                ]
                                            )
                                            cpd['objectinfo'][
                                                'simbad_best_distarcsec'
                                            ] = (
                                                matchdist
                                            )

                                        else:
                                            comment_update = (
                                                'SIMBAD parent object: '
                                                'main ID = %s, '
                                                'object type = %s, '
                                                'all IDs =  %s'
                                            ) % (
                                                sesame_lookup[
                                                    'simbad_best_mainid'
                                                ],
                                                sesame_lookup[
                                                    'simbad_best_objtype'
                                                ],
                                                sesame_lookup[
                                                    'simbad_best_allids'
                                                ]
                                            )

                                            if 'comments' in cpd:
                                                if cpd['comments'] is None:
                                                    cpd['comments'] = (
                                                        comment_update
                                                    )
                                                else:
                                                    cpd['comments'] = (
                                                        cpd['comments'] +
                                                        '; ' +
                                                        comment_update
                                                    )
                                            if 'objectcomments' in cpd:
                                                if (cpd['objectcomments'] is
                                                    None):
                                                    cpd['objectcomments'] = (
                                                        comment_update
                                                    )
                                                else:
                                                    cpd['objectcomments'] = (
                                                        cpd['objectcomments'] +
                                                        '; ' +
                                                        comment_update
                                                    )

                                        _write_checkplot_picklefile(
                                            cpd,
                                            outfile=cp,
                                            protocol=(
                                                pickle.HIGHEST_PROTOCOL
                                            ),
                                        )
                                        LOGINFO(
                                            'updated checkplot '
                                            'pickle for object: %s' %
                                            cp
                                        )


                        db.commit()
                        db.close()

                # at the end, return the now updated fulltext search result
                return sqlite_fulltext_search(
                    basedir,
                    '"%s"' % ftsquerystr,
                    getcolumns=getcolumns,
                    conditions=conditions,
                    lcclist=conesearch_matched_collections,
                    raiseonfail=raiseonfail,
                    incoming_userid=incoming_userid,
                    incoming_role=incoming_role,
                    fail_if_conditions_invalid=fail_if_conditions_invalid,
                    censor_searchargs=censor_searchargs
                )

            # if no matches were found in the cone search either, return the
            # original fulltext_search
            else:
                LOGERROR('no matches found in cone search '
                         'either after SIMBAD lookup')
                return fulltext_search

        else:
            return fulltext_search

    else:
        return fulltext_search



###################
## SQLITE SEARCH ##
###################

def add_collection_info(row, collection):
    '''This just adds the collection ID to a dict from sqlite.Row.
    '''
    row = dict(row)
    row['collection'] = collection
    return row



def sqlite_namewrap_fulltext_search(
        basedir,
        ftsquerystr,
        getcolumns=None,
        conditions=None,
        lcclist=None,
        raiseonfail=False,
        incoming_userid=2,
        incoming_role='anonymous',
        fail_if_conditions_invalid=True,
        censor_searchargs=False,
):
    '''This wraps the function below to search the usual way first and then by
    name if the usual FTS fails.

    This exists because users will never remember to put quotes around object
    names so we should try it for them.

    '''

    fulltext_search = sqlite_fulltext_search(
        basedir,
        ftsquerystr,
        getcolumns=getcolumns,
        conditions=conditions,
        lcclist=lcclist,
        raiseonfail=raiseonfail,
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
        fail_if_conditions_invalid=fail_if_conditions_invalid,
        censor_searchargs=censor_searchargs
    )

    nmatches = sum([fulltext_search[x]['nmatches']
                    for x in fulltext_search['databases']])

    if nmatches == 0:

        LOGWARNING('no matches found for an unquoted FTS, '
                   'trying a quoted FTS')

        # force a literal name search because this is a special mode dedicated
        # to looking up object names
        fulltext_search = sqlite_fulltext_search(
            basedir,
            # fix quotes
            '"%s"' % ftsquerystr.replace('"','').replace('&quot;',''),
            getcolumns=getcolumns,
            conditions=conditions,
            lcclist=lcclist,
            raiseonfail=raiseonfail,
            incoming_userid=incoming_userid,
            incoming_role=incoming_role,
            fail_if_conditions_invalid=fail_if_conditions_invalid,
            censor_searchargs=censor_searchargs
        )
        return fulltext_search

    else:
        return fulltext_search



def sqlite_fulltext_search(
        basedir,
        ftsquerystr,
        getcolumns=None,
        conditions=None,
        lcclist=None,
        raiseonfail=False,
        incoming_userid=2,
        incoming_role='anonymous',
        fail_if_conditions_invalid=True,
        censor_searchargs=False,
        override_action=None,
):

    '''This searches the specified collections for a full-text match.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. This is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    getcolumns is a list that specifies which columns to return after the query
    is complete.

    conditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, conditions will be disabled.

    lcclist is the list of light curve collection IDs to search in. If this is
    None, all light curve collections are searched.

    fail_if_conditions_invalid sets the behavior of the function if it finds
    that the conditions provided in conditions kwarg don't pass
    validate_sqlite_filters().

    censor_searchargs removes the ftsquerystr from the 'args' key of the output
    dict. This might be useful for running searches on otherwise private LC
    collections and generating public datasets out of them.

    FIXME: use the readonly authorizer here for sqlite3_to_memory calls.

    '''

    try:

        # get all the specified databases
        dbinfo = sqlite_get_collections(basedir,
                                        lcclist=lcclist,
                                        incoming_userid=incoming_userid,
                                        incoming_role=incoming_role,
                                        return_connection=False)


    except Exception as e:

        LOGEXCEPTION(
            "could not fetch available LC collections for "
            "userid: %s, role: %s. "
            "likely no collections matching this user's access level" %
            (incoming_userid, incoming_role)
        )
        return None

    # get the available databases and columns
    dbfiles = dbinfo['info']['object_catalog_path']
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc

    # we have some default columns that we'll always get

    # if we have some columns that the user provided, get them and append
    # default cols
    if getcolumns is not None:

        getcolumns = list(getcolumns)

        # make double sure that there are no columns requested that are NOT in
        # the intersection of all the requested collections
        column_check = set(getcolumns) - set(available_columns)
        if len(column_check) > 0:
            LOGWARNING('some requested columns cannot be found '
                       'in the intersection of all columns from '
                       'the requested collections')
            # remove these extraneous columns from the column request
            for c in column_check:
                LOGWARNING('removing extraneous column: %s' % c)
                getcolumns.remove(c)

        # get the requested columns together
        columnstr = ', '.join('a.%s' % (c,) for c in getcolumns)
        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, a.ra as db_ra, '
              'a.decl as db_decl, a.lcfname as db_lcfname, '
              'a.object_owner as owner, '
              'a.object_visibility as visibility, '
              'a.object_sharedwith as sharedwith, '
              'rank as relevance, a.extra_info_json as extra_info')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname',
                                       'owner',
                                       'visibility',
                                       'sharedwith',
                                       'relevance',
                                       'extra_info']


    # otherwise, if there are no columns, use the default ones
    else:

        columnstr = ('a.objectid as db_oid, a.ra as db_ra, '
                     'a.decl as db_decl, a.lcfname as db_lcfname, '
                     'a.object_owner as owner, '
                     'a.object_visibility as visibility, '
                     'a.object_sharedwith as sharedwith, '
                     'rank as relevance, a.extra_info_json as extra_info')

        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname',
                      'owner',
                      'visibility',
                      'sharedwith',
                      'relevance',
                      'extra_info']

    # this is the query that will be used for FTS
    q = ("select {columnstr} from {collection_id}.object_catalog a join "
         "{collection_id}.catalog_fts b on (a.rowid = b.rowid) where "
         "catalog_fts MATCH ? {conditions} "
         "order by bm25(catalog_fts)")

    # handle the extra conditions
    if conditions is not None and len(conditions) > 0:

        # validate this string
        conditions = validate_sqlite_filters(conditions,
                                             columnlist=available_columns)

        if not conditions and fail_if_conditions_invalid:

            LOGERROR('fail_if_conditions_invalid = True '
                     'and conditions did not pass '
                     'validate_sqlite_filters, returning early')
            return None


    # now we have to execute the FTS query for all of the attached databases.
    results = {}

    for lcc in uselcc:

        # get the database now
        dbindex = available_lcc.index(lcc)
        db, cur = sqlite3_to_memory(dbfiles[dbindex], lcc)

        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]
        lcc_lcmagcols = dbinfo['info']['lcmagcols'][dbindex]

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

        lcc_columnspec['relevance'] = {
            'title': 'relevance',
            'description':('SQLite FTS5 BM25 relevance '
                           '(smaller values -> more relevant)'),
            'dtype':'f8',
            'format':'%f',
            'index':False,
            'ftsindex':False,
        }
        lcc_columnspec['owner'] = {
            'title': 'object owner',
            'description':'userid of the owner of this object',
            'dtype':'i8',
            'format':'%i',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['visibility'] = {
            'title': 'object visibility',
            'description':(
                "visibility tag for this object. "
                "One of 'public', 'shared', 'unlisted', 'private'"
            ),
            'dtype':'U10',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['sharedwith'] = {
            'title': 'shared with',
            'description':("user/group IDs of LCC-Server users that "
                           "this object is shared with."),
            'dtype':'U20',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['collection'] = {
            'title': 'LC collection',
            'description':("the light curve collection this object belongs to"),
            'dtype':'U60',
            'format':'%s',
            'index':False,
            'ftsindex':False,
        }
        lcc_columnspec['extra_info'] = {
            'title': "additional information",
            'description':("additional information about the object "
                           "that doesn't necessarily fit into one "
                           "of the other columns"),
            'dtype':'U1024',
            'format':'%s',
            'index':False,
            'ftsindex':True,
        }

        # we should return all FTS indexed columns regardless of whether the
        # user selected them or not

        # NOTE: we can only return FTS indexed columns that are valid in the
        # current context, i.e. only the intersection of all FTS index columns
        # for all collections requrested for this search. so we DON'T have per
        # collection column keys in the result row, just the globally available
        # FTS column keys
        # add these to the default columns we return
        collection_ftscols = dbinfo['ftscols']
        for c in collection_ftscols:
            if c not in rescolumns:
                rescolumns.append(c)

        # add them to the SQL column statement too
        columnstr = (
            columnstr + ', ' +
            ', '.join(['a.%s' % x for x in collection_ftscols])
        )

        try:

            # if we have extra filters, apply them
            if conditions is not None and len(conditions) > 0:

                conditionstr = ' and (%s)' % conditions

                # replace the column names and add the table prefixes to them so
                # they remain unambiguous in case the 'where' columns are in
                # both the 'object_catalog a' and the 'catalog_fts b' tables

                # the space enforces the full column name must match. this is to
                # avoid inadvertently replacing stuff like: 'dered_jmag_kmag'
                # with 'dered_a.jmag_a.kmag'
                # FIXME: find a better way of doing this
                for c in rescolumns:
                    if '(%s ' % c in conditionstr:
                        conditionstr = (
                            conditionstr.replace('(%s ' % c,'(a.%s ' % c)
                        )

            else:

                conditionstr = ''


            # format the query
            thisq = q.format(columnstr=columnstr,
                             collection_id=lcc,
                             conditions=conditionstr)

            # we need to unescape the search string because it might contain
            # exact match strings that we might want to use with FTS
            unescapedstr = xhtml_unescape(ftsquerystr)
            if unescapedstr != ftsquerystr:
                ftsquerystr = unescapedstr
                LOGWARNING('unescaped FTS string because '
                           'it had quotes in it for exact matching: %r' %
                           unescapedstr)

            try:
                # execute the query
                LOGINFO('query = %s' % thisq.replace('?',"'%s'" % ftsquerystr))
                cur.execute(thisq, (ftsquerystr,))
                rows = cur.fetchall()
            except Exception as e:
                LOGEXCEPTION('query failed, probably a syntax error')
                rows = None

            if rows and len(rows) > 0:

                # check each object's permissions before adding it to the result
                # rows. this is fairly fast since we have the permissions model
                # entirely in memory from authdb
                rows = [
                    add_collection_info(x, lcc) for x in rows if
                    check_user_access(
                        userid=incoming_userid,
                        role=incoming_role,
                        action=(
                            'list' if not override_action
                            else override_action
                        ),
                        target_name='object',
                        target_owner=x['owner'],
                        target_visibility=x['visibility'],
                        target_sharedwith=x['sharedwith']
                    )
                ]

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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise

        # don't forget to close the open database at the end
        finally:
            db.close()


    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {
        'ftsquerystr':ftsquerystr if not censor_searchargs else 'redacted',
        'getcolumns':rescolumns + ['collection'],
        'lcclist':lcclist,
        'conditions':conditions if not censor_searchargs else 'redacted'
    }
    results['search'] = 'sqlite_fulltext_search'

    return results



def sqlite_column_search(
        basedir,
        getcolumns=None,
        conditions=None,
        sortby=None,
        limit=None,
        lcclist=None,
        raiseonfail=False,
        fail_if_conditions_invalid=True,
        incoming_userid=2,
        incoming_role='anonymous',
        censor_searchargs=False,
        override_action=None,
):
    '''This runs an arbitrary column search.

    basedir is the directory where lcc-index.sqlite is located.

    getcolumns is a list that specifies which columns to return after the query
    is complete.

    '''
    try:

        # get all the specified databases
        dbinfo = sqlite_get_collections(basedir,
                                        lcclist=lcclist,
                                        incoming_userid=incoming_userid,
                                        incoming_role=incoming_role,
                                        return_connection=False)


    except Exception as e:

        LOGEXCEPTION(
            "could not fetch available LC collections for "
            "userid: %s, role: %s. "
            "likely no collections matching this user's access level" %
            (incoming_userid, incoming_role)
        )
        return None

    # get the available databases and columns
    dbfiles = dbinfo['info']['object_catalog_path']
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # we have some default columns that we'll always get
    # if we have some columns to get, get them and append default cols
    if getcolumns is not None:

        getcolumns = list(getcolumns)

        # make double sure that there are no columns requested that are NOT in
        # the intersection of all the requested collections
        column_check = set(getcolumns) - set(available_columns)
        if len(column_check) > 0:
            LOGWARNING('some requested columns cannot be found '
                       'in the intersection of all columns from '
                       'the requested collections')
            # remove these extraneous columns from the column request
            for c in column_check:
                LOGWARNING('removing extraneous column: %s' % c)
                getcolumns.remove(c)

        # get the requested columns together
        columnstr = ', '.join('a.%s' % (c,) for c in getcolumns)
        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, a.ra as db_ra, '
              'a.decl as db_decl, a.lcfname as db_lcfname, '
              'a.object_owner as owner, '
              'a.object_visibility as visibility, '
              'a.object_sharedwith as sharedwith')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname',
                                       'owner',
                                       'visibility',
                                       'sharedwith']

    # otherwise, if there are no columns, use the default ones
    else:

        columnstr = ('a.objectid as db_oid, a.ra as db_ra, '
                     'a.decl as db_decl, a.lcfname as db_lcfname, '
                     'a.object_owner as owner, '
                     'a.object_visibility as visibility, '
                     'a.object_sharedwith as sharedwith')

        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname',
                      'owner',
                      'visibility',
                      'sharedwith']

    # this is the query that will be used
    q = ("select {columnstr} from {collection_id}.object_catalog a "
         "{wherecondition} {sortcondition} {limitcondition}")

    # validate the column conditions and add in any
    if conditions:

        wherecondition = validate_sqlite_filters(conditions,
                                                 columnlist=available_columns)

        # do not proceed if the sqlite filters don't validate
        if not wherecondition and fail_if_conditions_invalid:

            LOGERROR('fail_if_conditions_invalid = True and conditions did not '
                     'pass validate_sqlite_filters, returning early')
            return None

        else:

            wherecondition = 'where %s' % wherecondition

    else:

        # we will not proceed if the conditions are None or empty
        LOGERROR('no conditions specified to filter columns by, '
                 'will not fetch the entire database')
        return None


    # validate the sortby condition
    if sortby is not None:

        # validate the sort condition
        sortcondition = validate_sqlite_filters(
            sortby,
            columnlist=available_columns,
            allowedsqlwords=SQLITE_ALLOWED_ORDERBY_FOR_FILTERS
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
            allowedsqlwords=SQLITE_ALLOWED_LIMIT_FOR_FILTERS
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

        # get the database now
        dbindex = available_lcc.index(lcc)
        db, cur = sqlite3_to_memory(dbfiles[dbindex], lcc)

        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]
        lcc_lcmagcols = dbinfo['info']['lcmagcols'][dbindex]

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

        lcc_columnspec['owner'] = {
            'title': 'object owner',
            'description':'userid of the owner of this object',
            'dtype':'i8',
            'format':'%i',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['visibility'] = {
            'title': 'object visibility',
            'description':(
                "visibility tag for this object. "
                "One of 'public', 'shared', 'unlisted', 'private'"
            ),
            'dtype':'U10',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['sharedwith'] = {
            'title': 'shared with',
            'description':("user/group IDs of LCC-Server users that "
                           "this object is shared with."),
            'dtype':'U20',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['collection'] = {
            'title': 'LC collection',
            'description':("the light curve collection this object belongs to"),
            'dtype':'U60',
            'format':'%s',
            'index':False,
            'ftsindex':False,
        }

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

                # check permissions for each object before accepting it
                rows = [
                    add_collection_info(x, lcc) for x in rows if
                    check_user_access(
                        userid=incoming_userid,
                        role=incoming_role,
                        action=(
                            'list' if not override_action else override_action
                        ),
                        target_name='object',
                        target_owner=x['owner'],
                        target_visibility=x['visibility'],
                        target_sharedwith=x['sharedwith']
                    )
                ]

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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise

        # don't forget to close the DB at the end
        finally:
            db.close()

    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {
        'getcolumns':rescolumns + ['collection'],
        'conditions':conditions if not censor_searchargs else 'redacted',
        'sortby':sortby,
        'limit':limit,
        'lcclist':lcclist
    }
    results['search'] = 'sqlite_column_search'

    return results



def sqlite_sql_search(basedir,
                      sqlstatement,
                      lcclist=None,
                      fail_if_sql_invalid=True,
                      incoming_userid=2,
                      incoming_role='anonymous',
                      raiseonfail=False,
                      censor_searchargs=False):
    '''This runs an arbitrary SQL statement search.

    basedir is the directory where lcc-index.sqlite is located.

    ftsquerystr is string to query against the FTS indexed columns. this is in
    the usual FTS syntax:

    https://www.sqlite.org/fts5.html#full_text_query_syntax

    columns is a list that specifies which columns to return after the query is
    complete.

    conditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, conditions will be disabled.

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
                             conditions=None,
                             lcclist=None,
                             incoming_userid=2,
                             incoming_role='anonymous',
                             fail_if_conditions_invalid=True,
                             conesearchworkers=1,
                             raiseonfail=False,
                             censor_searchargs=False,
                             override_action=None):
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

    conditions is a string in SQL format that applies extra conditions to
    the where statement. This will be parsed and if it contains any non-allowed
    keywords, conditions will be disabled.

    '''

    try:

        # get all the specified databases
        dbinfo = sqlite_get_collections(basedir,
                                        lcclist=lcclist,
                                        incoming_userid=incoming_userid,
                                        incoming_role=incoming_role,
                                        return_connection=False)

    except Exception as e:

        LOGEXCEPTION(
            "could not fetch available LC collections for "
            "userid: %s, role: %s. "
            "likely no collections matching this user's access level" %
            (incoming_userid, incoming_role)
        )
        return None

    # get the available databases and columns
    dbfiles = dbinfo['info']['object_catalog_path']
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # get the requested columns together
    if getcolumns is not None:

        getcolumns = list(getcolumns)

        # make double sure that there are no columns requested that are NOT in
        # the intersection of all the requested collections
        column_check = set(getcolumns) - set(available_columns)
        if len(column_check) > 0:
            LOGWARNING('some requested columns cannot be found '
                       'in the intersection of all columns from '
                       'the requested collections')
            # remove these extraneous columns from the column request
            for c in column_check:
                LOGWARNING('removing extraneous column: %s' % c)
                getcolumns.remove(c)


        columnstr = ', '.join('a.%s' % c for c in getcolumns)

        # we add some columns that will always be present to use in sorting and
        # filtering
        columnstr = ', '.join(
            [columnstr,
             ('a.objectid as db_oid, b.objectid as kdtree_oid, '
              'a.ra as db_ra, a.decl as db_decl, '
              'b.ra as kdtree_ra, b.decl as kdtree_decl, '
              'a.lcfname as db_lcfname, '
              'a.object_owner as owner, '
              'a.object_visibility as visibility, '
              'a.object_sharedwith as sharedwith')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'kdtree_oid',
                                       'db_ra',
                                       'db_decl',
                                       'kdtree_ra',
                                       'kdtree_decl',
                                       'db_lcfname',
                                       'dist_arcsec',
                                       'owner',
                                       'visibility',
                                       'sharedwith']

    # otherwise, if there are no columns, get the default set of columns for a
    # 'check' cone-search query
    else:

        columnstr = ('a.objectid as db_oid, b.objectid as kdtree_oid, '
                     'a.ra as db_ra, a.decl as db_decl, '
                     'b.ra as kdtree_ra, b.decl as kdtree_decl, '
                     'a.lcfname as db_lcfname, '
                     'a.object_owner as owner, '
                     'a.object_visibility as visibility, '
                     'a.object_sharedwith as sharedwith')

        rescolumns = ['db_oid',
                      'kdtree_oid',
                      'db_ra',
                      'db_decl',
                      'kdtree_ra',
                      'kdtree_decl',
                      'db_lcfname',
                      'dist_arcsec',
                      'owner',
                      'visibility',
                      'sharedwith']


    # this is the query that will be used to query the database only
    q = ("select {columnstr} from {collection_id}.object_catalog a "
         "join _temp_objectid_list b on (a.objectid = b.objectid) "
         "{conditions} order by b.objectid asc")


    # handle the extra conditions
    if conditions is not None and len(conditions) > 0:

        # validate this string
        conditions = validate_sqlite_filters(conditions,
                                             columnlist=available_columns)

        if not conditions and fail_if_conditions_invalid:
            LOGERROR("fail_if_conditions_invalid = True and "
                     "conditions did not pass "
                     "validate_sqlite_filters, returning early...")
            return None

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

        # get the database now
        dbindex = available_lcc.index(lcc)
        db, cur = sqlite3_to_memory(dbfiles[dbindex], lcc)

        # get the kdtree path
        dbindex = available_lcc.index(lcc)

        kdtree_fpath = dbinfo['info']['kdtree_pkl_path'][dbindex]
        lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
        lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]
        lcc_lcmagcols = dbinfo['info']['lcmagcols'][dbindex]

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

        lcc_columnspec['owner'] = {
            'title': 'object owner',
            'description':'userid of the owner of this object',
            'dtype':'i8',
            'format':'%i',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['visibility'] = {
            'title': 'object visibility',
            'description':(
                "visibility tag for this object. "
                "One of 'public', 'shared', 'unlisted', 'private'"
            ),
            'dtype':'U10',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['sharedwith'] = {
            'title': 'shared with',
            'description':("user/group IDs of LCC-Server users that "
                           "this object is shared with."),
            'dtype':'U20',
            'format':'%s',
            'index':True,
            'ftsindex':False,
        }
        lcc_columnspec['collection'] = {
            'title': 'LC collection',
            'description':("the light curve collection this object belongs to"),
            'dtype':'U60',
            'format':'%s',
            'index':False,
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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
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
            if conditions is not None and len(conditions) > 0:

                conditionstr = 'where (%s)' % conditions

            else:

                conditionstr = ''

            # now, we'll get the corresponding info from the database
            thisq = q.format(columnstr=columnstr,
                             collection_id=lcc,
                             conditions=conditionstr)


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

            try:
                LOGINFO('query = %s' % thisq)
                cur.execute(thisq)

                # get the results and filter by permitted objects
                rows = [
                    add_collection_info(x, lcc) for x in cur.fetchall() if
                    check_user_access(
                        userid=incoming_userid,
                        role=incoming_role,
                        action=(
                            'list' if not override_action else override_action
                        ),
                        target_name='object',
                        target_owner=x['owner'],
                        target_visibility=x['visibility'],
                        target_sharedwith=x['sharedwith']
                    )
                ]

            except Exception as e:
                LOGEXCEPTION('query failed, probably an SQL error')
                rows = None

            # remove the temporary table
            cur.execute('drop table _temp_objectid_list')

            LOGINFO('table-kdtree match complete, generating result rows...')

            # for each row of the results, add in the distance from the center
            # of the cone search
            if rows:
                for row in rows:

                    ra = row['db_ra']
                    decl = row['db_decl']

                    # figure out the distances from the search center
                    searchcenter_distarcsec = great_circle_dist(
                        center_ra,
                        center_decl,
                        ra,
                        decl
                    )

                    # add in the distance column to the row
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
                results[lcc]['lcmagcols'] = lcc_lcmagcols
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid

            else:

                msg = ('failed to execute query for collection: %s, '
                       'likely no matches' % (lcc,))
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
                results[lcc]['lcmagcols'] = lcc_lcmagcols
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid

        except Exception as e:

            msg = ('failed to execute query for collection: %s, '
                   'exception: %s' %
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
            results[lcc]['lcmagcols'] = lcc_lcmagcols
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            if raiseonfail:
                raise


        # don't forget to close the database at the end
        finally:
            db.close()


    # at the end, add in some useful info
    results['databases'] = available_lcc
    results['columns'] = available_columns

    results['args'] = {
        'center_ra':center_ra if not censor_searchargs else None,
        'center_decl':center_decl if not censor_searchargs else None,
        'radius_arcmin':radius_arcmin,
        'getcolumns':rescolumns + ['collection'],
        'conditions':conditions if not censor_searchargs else 'redacted',
        'lcclist':lcclist
    }

    results['search'] = 'sqlite_kdtree_conesearch'

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
                         conditions=None,
                         fail_if_conditions_invalid=True,
                         lcclist=None,
                         incoming_userid=2,
                         incoming_role='anonymous',
                         max_matchradius_arcsec=30.0,
                         raiseonfail=False,
                         censor_searchargs=False,
                         override_action=None):
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

    e.g.:

    {'data': {'objectid': ['aaa', 'bbb', 'ccc', 'ddd', 'eee'],
              'ra': [289.99698, 293.358, 294.197, 291.36630375, 291.3625],
              'decl': [44.99839, -23.206, 23.181, 42.78435, -42.784]},
    'columns': ['objectid', 'ra', 'decl'],
    'types': ['str', 'float', 'float'],
    'colobjectid': 'objectid',
    'colra': 'ra',
    'coldec': 'decl'}

    if inputmatchcol is None
       and dbmatchcol is None
       and inputdata['xmatch_dist_arcsec'] is not None -> do coord xmatch

    if one of 'colra', 'coldec' is None, a coordinate xmatch is not possible.

    otherwise, inputmatchcol and dbmatchcol should both not be None and be names
    of columns in the input data dict and an available column in the light curve
    collections specified for use in the xmatch search.

    '''

    try:

        # get all the specified databases
        dbinfo = sqlite_get_collections(basedir,
                                        lcclist=lcclist,
                                        incoming_userid=incoming_userid,
                                        incoming_role=incoming_role,
                                        return_connection=False)


    except Exception as e:

        LOGEXCEPTION(
            "could not fetch available LC collections for "
            "userid: %s, role: %s. "
            "likely no collections matching this user's access level" %
            (incoming_userid, incoming_role)
        )
        return None

    # get the available databases and columns
    dbfiles = dbinfo['info']['object_catalog_path']
    available_lcc = dbinfo['databases']
    available_columns = dbinfo['columns']

    if lcclist is not None:

        inlcc = set([x.replace('-','_') for x in lcclist])
        uselcc = list(set(available_lcc).intersection(inlcc))

        if not uselcc:
            LOGERROR("none of the specified input LC collections are valid")
            return None

    else:

        LOGWARNING("no input LC collections specified, using all of them")
        uselcc = available_lcc


    # get the requested columns together
    if getcolumns is not None:

        getcolumns = list(getcolumns)

        # make double sure that there are no columns requested that are NOT in
        # the intersection of all the requested collections
        column_check = set(getcolumns) - set(available_columns)
        if len(column_check) > 0:
            LOGWARNING('some requested columns cannot be found '
                       'in the intersection of all columns from '
                       'the requested collections')
            # remove these extraneous columns from the column request
            for c in column_check:
                LOGWARNING('removing extraneous column: %s' % c)
                getcolumns.remove(c)


        columnstr = ', '.join('b.%s' % c for c in getcolumns)
        columnstr = ', '.join(
            [columnstr,
             ('b.objectid as db_oid, '
              'b.ra as db_ra, b.decl as db_decl, '
              'b.lcfname as db_lcfname, '
              'b.object_owner as owner, '
              'b.object_visibility as visibility, '
              'b.object_sharedwith as sharedwith')]
        )
        columnstr = columnstr.lstrip(',').strip()

        rescolumns = getcolumns[::] + ['db_oid',
                                       'db_ra',
                                       'db_decl',
                                       'db_lcfname',
                                       'owner',
                                       'visibility',
                                       'sharedwith']

    # otherwise, if there are no columns, get the default set of columns for a
    # 'check' cone-search query
    else:
        columnstr = ('b.objectid as db_oid, '
                     'b.ra as db_ra, b.decl as db_decl, '
                     'b.lcfname as db_lcfname, '
                     'b.object_owner as owner, '
                     'b.object_visibility as visibility, '
                     'b.object_sharedwith as sharedwith')
        rescolumns = ['db_oid',
                      'db_ra',
                      'db_decl',
                      'db_lcfname',
                      'owner',
                      'visibility',
                      'sharedwith']

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
        return None


    ###########################################
    ## figure out the xmatch type and run it ##
    ###########################################

    # handle the extra conditions
    if conditions is not None and len(conditions) > 0:

        # validate this string
        conditions = validate_sqlite_filters(conditions,
                                             columnlist=available_columns)

        if not conditions and fail_if_conditions_invalid:

            LOGERROR("fail_if_conditions_invalid = True "
                     "and conditions did not pass "
                     "validate_sqlite_filters, "
                     "returning early...")
            return None


    # handle xmatching by coordinates
    if xmatch_type == 'coord':

        results = {}

        q = (
            "select {columnstr} from {collection_id}.object_catalog b "
            "where b.objectid in ({placeholders}) {conditionstr} "
            "order by b.objectid"
        )

        # go through each LCC
        for lcc in uselcc:

            # get the database now
            dbindex = available_lcc.index(lcc)
            db, cur = sqlite3_to_memory(dbfiles[dbindex], lcc)

            # get the kdtree path
            kdtree_fpath = dbinfo['info']['kdtree_pkl_path'][dbindex]

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
                thiscol_name = (
                    col.replace(' ','_').replace('-','_').replace('.','_')
                )
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

            # this is the SQL to make an index on the match column in the xmatch
            # table
            index_temptable_q = (
                "create index xmatch_index "
                "on _temp_xmatch_table({xmatch_colname})"
            ).format(xmatch_colname=xmatch_col)

            # this is the SQL to insert all of the input data columns into the
            # xmatch table
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

            # this is the column string to be used in the query to join the LCC
            # and xmatch tables
            xmatch_columnstr = (
                '%s, %s' % (columnstr, ', '.join('a.%s' % x for x in col_names))
            )

            #
            # handle extra stuff that needs to go into the xmatch results
            #

            lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
            lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]
            lcc_lcmagcols = dbinfo['info']['lcmagcols'][dbindex]

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

            lcc_columnspec['owner'] = {
                'title': 'object owner',
                'description':'userid of the owner of this object',
                'dtype':'i8',
                'format':'%i',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['visibility'] = {
                'title': 'object visibility',
                'description':(
                    "visibility tag for this object. "
                    "One of 'public', 'shared', 'unlisted', 'private'"
                ),
                'dtype':'U10',
                'format':'%s',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['sharedwith'] = {
                'title': 'shared with',
                'description':("user/group IDs of LCC-Server users that "
                               "this object is shared with."),
                'dtype':'U20',
                'format':'%s',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['collection'] = {
                'title': 'LC collection',
                'description':(
                    "the light curve collection this object belongs to"
                ),
                'dtype':'U60',
                'format':'%s',
                'index':False,
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
                results[lcc]['lcmagcols'] = lcc_lcmagcols
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
            if conditions is not None and len(conditions) > 0:

                conditionstr = 'and (%s)' % conditions

            else:

                conditionstr = ''

            LOGINFO('query = %s' % q.format(
                columnstr=columnstr,
                collection_id=lcc,
                placeholders='<placeholders>',
                conditionstr=conditionstr)
            )

            # this handles the case where all queries over all input objects
            # return nothing so thisq in the for loop below is never populated
            thisq = q

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
                                 conditionstr=conditionstr)

                try:
                    cur.execute(thisq, tuple(matching_lcc_objectids))
                    rows = [
                        add_collection_info(x, lcc) for x in cur.fetchall() if
                        check_user_access(
                            userid=incoming_userid,
                            role=incoming_role,
                            action=(
                                'list' if not override_action
                                else override_action
                            ),
                            target_name='object',
                            target_owner=x['owner'],
                            target_visibility=x['visibility'],
                            target_sharedwith=x['sharedwith']
                        )
                    ]
                except Exception as e:
                    LOGEXCEPTION(
                        'xmatch object lookup for input object '
                        'with data: %r '
                        'returned an sqlite3 exception. '
                        'skipping this object' %
                        datatable[input_objind]
                    )
                    rows = None

                if rows and len(rows) > 0:

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

                    # we'll order the results of this objectid search by
                    # distance from the input object.
                    rows = sorted(rows, key=lambda row: row['dist_arcsec'])
                    this_lcc_results.extend(rows)

            #
            # done with this LCC, add in the results to the results dict
            #
            results[lcc] = {'result':this_lcc_results,
                            'query':thisq,
                            'success':True}
            results[lcc]['nmatches'] = len(this_lcc_results)
            msg = ("executed xmatch query successfully for collection: %s, "
                   "matching nrows: %s" % (lcc, results[lcc]['nmatches']))
            results[lcc]['message'] = msg
            LOGINFO(msg)

            results[lcc]['lcformatkey'] = lcc_lcformatkey
            results[lcc]['lcformatdesc'] = lcc_lcformatdesc
            results[lcc]['lcmagcols'] = lcc_lcmagcols
            results[lcc]['columnspec'] = lcc_columnspec
            results[lcc]['collid'] = lcc_collid

            # at the end, drop the temp xmatch table and close the DB
            cur.execute('drop table _temp_xmatch_table')
            db.close()


        #
        # done with all LCCs
        #

        # at the end, add in some useful info
        results['databases'] = available_lcc
        results['columns'] = available_columns

        results['args'] = {
            'inputdata':inputdata if not censor_searchargs else 'redacted',
            'xmatch_dist_arcsec':xmatch_dist_arcsec,
            'xmatch_closest_only':xmatch_closest_only,
            'inputmatchcol':inputmatchcol,
            'dbmatchcol':dbmatchcol,
            'getcolumns':rescolumns + ['collection'],
            'conditions':conditions if not censor_searchargs else 'redacted',
            'lcclist':lcclist
        }
        results['search'] = 'sqlite_xmatch_search'

        return results


    elif xmatch_type == 'column':

        # FIXME: change this to make sqlite do an FTS on each input object name
        # if the input match column is objectid.

        # this will be a straightforward table join using the inputmatchcol and
        # the dbmatchcol
        results = {}

        # if we have extra filters, apply them
        if conditions is not None and len(conditions) > 0:

            conditionstr = 'where (%s)' % conditions

            # replace the column names and add the table prefixes to them so
            # they remain unambiguous in case the 'where' columns are in
            # both the '_temp_xmatch_table a' and the 'object_catalog b' tables
            for c in rescolumns:
                if '(%s ' % c in conditionstr:
                    conditionstr = (
                        conditionstr.replace('(%s ' % c,'(b.%s ' % c)
                    )

        else:

            conditionstr = ''

        # we use a left outer join because we want to keep all the input columns
        # and notice when there are no database matches
        q = (
            "select {columnstr} from "
            "_temp_xmatch_table a "
            "left outer join "
            "{collection_id}.object_catalog b on "
            "(a.{input_xmatch_col} = b.{db_xmatch_col}) {conditionstr} "
            "order by a.{input_xmatch_col} asc"
        )

        # go through each LCC
        for lcc in uselcc:

            # get the database now
            dbindex = available_lcc.index(lcc)
            db, cur = sqlite3_to_memory(dbfiles[dbindex], lcc)

            # get the kdtree path
            kdtree_fpath = dbinfo['info']['kdtree_pkl_path'][dbindex]

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
                thiscol_name = (
                    col.replace(' ','_').replace('-','_').replace('.','_')
                )
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

            # this is the SQL to make an index on the match column in the xmatch
            # table
            index_temptable_q = (
                "create index xmatch_index "
                "on _temp_xmatch_table({xmatch_colname})"
            ).format(xmatch_colname=xmatch_col)

            # this is the SQL to insert all of the input data columns into the
            # xmatch table
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

            # this is the column string to be used in the query to join the LCC
            # and xmatch tables
            xmatch_columnstr = (
                '%s, %s' % (columnstr, ', '.join('a.%s' % x for x in col_names))
            )

            #
            # handle extra stuff that needs to go into the xmatch results
            #

            lcc_lcformatkey = dbinfo['info']['lcformatkey'][dbindex]
            lcc_lcformatdesc = dbinfo['info']['lcformatdesc'][dbindex]
            lcc_lcmagcols = dbinfo['info']['lcmagcols'][dbindex]

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

            # this is the extra spec for dist_arcsec
            lcc_columnspec['dist_arcsec'] = {
                'title': 'distance [arcsec]',
                'format': '%.3f',
                'description':'distance from search center in arcsec',
                'dtype':'<f8',
                'index':True,
                'ftsindex':False,
            }

            lcc_columnspec['owner'] = {
                'title': 'object owner',
                'description':'userid of the owner of this object',
                'dtype':'i8',
                'format':'%i',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['visibility'] = {
                'title': 'object visibility',
                'description':(
                    "visibility tag for this object. "
                    "One of 'public', 'shared', 'unlisted', 'private'"
                ),
                'dtype':'U10',
                'format':'%s',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['sharedwith'] = {
                'title': 'shared with',
                'description':("user/group IDs of LCC-Server users that "
                               "this object is shared with."),
                'dtype':'U20',
                'format':'%s',
                'index':True,
                'ftsindex':False,
            }
            lcc_columnspec['collection'] = {
                'title': 'LC collection',
                'description':(
                    "the light curve collection this object belongs to"
                ),
                'dtype':'U60',
                'format':'%s',
                'index':False,
                'ftsindex':False,
            }

            # execute the xmatch statement
            thisq = q.format(columnstr=xmatch_columnstr,
                             collection_id=lcc,
                             input_xmatch_col=xmatch_col,
                             db_xmatch_col=dbmatchcol,
                             conditionstr=conditionstr)

            try:

                cur.execute(thisq)

                rows = [
                    add_collection_info(x, lcc) for x in cur.fetchall() if
                    check_user_access(
                        userid=incoming_userid,
                        role=incoming_role,
                        action=(
                            'list' if not override_action
                            else override_action
                        ),
                        target_name='object',
                        target_owner=x['owner'],
                        target_visibility=x['visibility'],
                        target_sharedwith=x['sharedwith']
                    )
                ]

                # put the results into the right place
                results[lcc] = {
                    'result':rows,
                    'query':thisq,
                    'success':True if (rows and len(rows) > 0) else False
                }
                results[lcc]['nmatches'] = len(results[lcc]['result'])

                msg = ('executed query successfully for collection: %s'
                       ', matching nrows: %s' %
                       (lcc, results[lcc]['nmatches']))
                results[lcc]['message'] = msg
                LOGINFO(msg)

                results[lcc]['lcformatkey'] = lcc_lcformatkey
                results[lcc]['lcformatdesc'] = lcc_lcformatdesc
                results[lcc]['lcmagcols'] = lcc_lcmagcols
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
                results[lcc]['lcmagcols'] = lcc_lcmagcols
                results[lcc]['columnspec'] = lcc_columnspec
                results[lcc]['collid'] = lcc_collid

                if raiseonfail:
                    raise

            # close the DB at the end of LCC processing
            finally:
                # delete the temporary xmatch table
                cur.execute('drop table _temp_xmatch_table')
                db.close()

        #
        # done with all LCCs
        #

        # at the end, add in some useful info
        results['databases'] = available_lcc
        results['columns'] = available_columns

        results['args'] = {
            'inputdata':inputdata if not censor_searchargs else 'redacted',
            'xmatch_dist_arcsec':xmatch_dist_arcsec,
            'xmatch_closest_only':xmatch_closest_only,
            'inputmatchcol':inputmatchcol,
            'dbmatchcol':dbmatchcol,
            'getcolumns':rescolumns + ['collection'],
            'conditions':conditions if not censor_searchargs else 'redacted',
            'lcclist':lcclist
        }
        results['search'] = 'sqlite_xmatch_search'

        db.close()
        return results
