#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''abcat.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to interface with astrobase checkplots, lcproc object
catalogs and generate useful files and database tables for use with the LC
server.

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

import os.path
import pickle
import sqlite3
import json
import sys
import os
import importlib
import glob
from functools import reduce
from operator import getitem
from textwrap import indent
import gzip

import numpy as np
from scipy.spatial import cKDTree

from tqdm import tqdm

# from astrobase import lcdb


#########################################
## DEFAULT COLUMNS THAT ARE RECOGNIZED ##
#########################################

from .abcat_columns import COLUMN_INFO



#####################################################
## FUNCTIONS TO BREAK OUT INFO FROM LCPROC RESULTS ##
#####################################################

def kdtree_from_lclist(lclistpkl, outfile):
    '''
    This pulls out the kdtree and object IDs.

    '''

    with open(lclistpkl, 'rb') as infd:
        lclist = pickle.load(infd)

    if 'kdtree' in lclist and isinstance(lclist['kdtree'], cKDTree):

        kdtree = lclist['kdtree']
        objectids = lclist['objects']['objectid']
        ra, decl = lclist['objects']['ra'], lclist['objects']['decl']

        outdict = {'kdtree':kdtree,
                   'objectid':objectids,
                   'ra':ra,
                   'decl':decl,
                   'lclistpkl':os.path.abspath(lclistpkl)}

        with open(outfile, 'wb') as outfd:
            pickle.dump(outdict, outfd, protocol=pickle.HIGHEST_PROTOCOL)

        LOGINFO('wrote kdtree from %s to %s' % (lclistpkl, outfile))
        return outfile

    else:

        LOGERROR("no kdtree present in %s, can't continue" % lclistpkl)
        return None


def objectinfo_to_sqlite(augcatpkl,
                         outfile,
                         lcset_name=None,
                         lcset_desc=None,
                         lcset_project=None,
                         lcset_datarelease=None,
                         lcset_citation=None,
                         lcset_ispublic=True,
                         colinfo=None,
                         indexcols=None,
                         ftsindexcols=None):

    '''This writes the object information to an SQLite file.

    lcset_* sets some metadata for the project. this is used by the top-level
    lcc-collections.sqlite database for all LC collections.

    FIXME: add this stuff

    makes indexes for fast look up by objectid by default and any columns
    included in indexcols. also makes a full-text search index for any columns
    in ftsindexcols.

    If colinfo is not None, it should be either a dict or JSON with elements
    that are of the form:

    'column_name':{'title':'column title',
                   'dtype':numpy dtype of the column,
                   'format':string format specifier for this column,
                   'description':'a long description of the column',
                   'index': True if this should be indexed, False otherwise,
                   'ftsindex': True if this should be FTS indexed, or False},

    where column_name should be each column in the augcatpkl file. Any column
    that doesn't have a key in colinfo won't have any extra information
    associated with it.

    NOTE: This function requires FTS5 to be available in SQLite because we don't
    want to mess with ranking algorithms to be implemented for FTS4.

    '''

    with open(augcatpkl, 'rb') as infd:
        augcat = pickle.load(infd)

    # pull the info columns out of the augcat
    cols = list(augcat['objects'].keys())

    # get the magnitude columns
    magcols = augcat['magcols']

    # separate the info cols into columns that are independent of magcol and
    # those affiliated with each magcol
    mag_affil_cols = []

    for mc in magcols:
        for col in cols:
            if mc in col:
                mag_affil_cols.append(col)

    unaffiliated_cols = list(set(cols) - set(mag_affil_cols))


    # get the dtypes for each column to generate the create statement
    coldefs = []
    colnames = []

    LOGINFO('collecting column information from %s' % augcatpkl)

    defaultcolinfo = {}

    # go through the unaffiliated columns first

    for col in unaffiliated_cols:

        thiscol_name = col.replace('.','_')
        thiscol_dtype = augcat['objects'][col].dtype
        colnames.append(thiscol_name)

        # set up the default info element
        defaultcolinfo[thiscol_name] = {'title':None,
                                        'description':None,
                                        'dtype':None,
                                        'format':None,
                                        'index':False,
                                        'ftsindex':False}

        colinfo_key = col

        #
        # now go through the various formats
        #

        # strings
        if thiscol_dtype.type is np.str_:

            coldefs.append(('%s text' % thiscol_name, str))

            if colinfo_key in COLUMN_INFO:
                defaultcolinfo[thiscol_name] = COLUMN_INFO[colinfo_key]
            else:
                defaultcolinfo[thiscol_name]['format'] = '%s'

            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


        # floats
        elif thiscol_dtype.type is np.float64:

            coldefs.append(('%s double precision' % thiscol_name, float))

            if colinfo_key in COLUMN_INFO:
                defaultcolinfo[thiscol_name] = COLUMN_INFO[colinfo_key]
            else:
                defaultcolinfo[thiscol_name]['format'] = '%.7f'

            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


        # integers
        elif thiscol_dtype.type is np.int64:

            coldefs.append(('%s integer' % thiscol_name, int))

            if colinfo_key in COLUMN_INFO:
                defaultcolinfo[thiscol_name] = COLUMN_INFO[colinfo_key]
            else:
                defaultcolinfo[thiscol_name]['format'] = '%i'

            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


        # everything else is coerced into a string
        else:

            coldefs.append(('%s text' % thiscol_name, str))

            if colinfo_key in COLUMN_INFO:
                defaultcolinfo[thiscol_name] = COLUMN_INFO[colinfo_key]
            else:
                defaultcolinfo[thiscol_name]['format'] = '%s'

            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


    # now go though the mag affiliated columns, per magcol

    for mc in magcols:

        for col in mag_affil_cols:

            # see if this is a magcol affiliated column
            if mc in col:
                sub_mc = mc
            else:
                continue

            thiscol_name = col.replace('.','_')
            thiscol_dtype = augcat['objects'][col].dtype
            colnames.append(thiscol_name)

            # set up the default info element
            defaultcolinfo[thiscol_name] = {'title':None,
                                            'description':None,
                                            'dtype':None,
                                            'format':None,
                                            'index':False,
                                            'ftsindex':False}

            # this gets the correct substitution for the magcol
            if sub_mc is not None:
                colinfo_key = '{magcol}.%s' % col.split('.')[-1]
            else:
                colinfo_key = col

            #
            # now go through the various formats
            #

            # strings
            if thiscol_dtype.type is np.str_:

                coldefs.append(('%s text' % thiscol_name, str))

                if colinfo_key in COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = (
                        COLUMN_INFO[colinfo_key].copy()
                    )
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%s'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str

                if sub_mc is not None:
                    defaultcolinfo[thiscol_name]['title'] = (
                        defaultcolinfo[thiscol_name]['title'].format(
                            magcol=sub_mc
                        )
                    )
                    defaultcolinfo[thiscol_name]['description'] = (
                        defaultcolinfo[thiscol_name]['description'].format(
                            magcol=sub_mc
                        )
                    )

            # floats
            elif thiscol_dtype.type is np.float64:

                coldefs.append(('%s double precision' % thiscol_name, float))

                if colinfo_key in COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = (
                        COLUMN_INFO[colinfo_key].copy()
                    )
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%.7f'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str

                if sub_mc is not None:
                    defaultcolinfo[thiscol_name]['title'] = (
                        defaultcolinfo[thiscol_name]['title'].format(
                            magcol=sub_mc
                        )
                    )
                    defaultcolinfo[thiscol_name]['description'] = (
                        defaultcolinfo[thiscol_name]['description'].format(
                            magcol=sub_mc
                        )
                    )

            # integers
            elif thiscol_dtype.type is np.int64:

                coldefs.append(('%s integer' % thiscol_name, int))

                if colinfo_key in COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = (
                        COLUMN_INFO[colinfo_key].copy()
                    )
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%i'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str

                if sub_mc is not None:
                    defaultcolinfo[thiscol_name]['title'] = (
                        defaultcolinfo[thiscol_name]['title'].format(
                            magcol=sub_mc
                        )
                    )
                    defaultcolinfo[thiscol_name]['description'] = (
                        defaultcolinfo[thiscol_name]['description'].format(
                            magcol=sub_mc
                        )
                    )

            # everything is coerced into a string
            else:

                coldefs.append(('%s text' % thiscol_name, str))

                if colinfo_key in COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = (
                        COLUMN_INFO[colinfo_key].copy()
                    )
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%s'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str

                if sub_mc is not None:
                    defaultcolinfo[thiscol_name]['title'] = (
                        defaultcolinfo[thiscol_name]['title'].format(
                            magcol=sub_mc
                        )
                    )
                    defaultcolinfo[thiscol_name]['description'] = (
                        defaultcolinfo[thiscol_name]['description'].format(
                            magcol=sub_mc
                        )
                    )


    # now, we'll generate the create statement

    # now these are all cols
    cols = unaffiliated_cols + mag_affil_cols

    column_and_type_list = ', '.join([x[0] for x in coldefs])
    column_list = ', '.join(colnames)
    placeholders = ','.join(['?']*len(cols))

    sqlcreate = ("create table object_catalog ({column_type_list}, "
                 "primary key (objectid))")
    sqlcreate = sqlcreate.format(column_type_list=column_and_type_list)

    # this is the insert statement
    sqlinsert = ("insert into object_catalog ({column_list}) "
                 "values ({placeholders})")
    sqlinsert = sqlinsert.format(column_list=column_list,
                                 placeholders=placeholders)

    LOGINFO('objects in %s: %s, generating SQLite database...' %
            (augcatpkl, augcat['nfiles']))

    # connect to the database
    db = sqlite3.connect(outfile)
    cur = db.cursor()

    colformatters = [x[1] for x in coldefs]

    # start the transaction
    cur.execute('begin')

    # create the table
    cur.execute(sqlcreate)

    # now we'll insert things into the table
    for rowind in tqdm(range(augcat['objects'][cols[0]].size)):

        thisrow = [
            y(augcat['objects'][x][rowind]) for x,y in zip(cols, colformatters)
        ]

        for ind, rowelem in enumerate(thisrow):

            if isinstance(rowelem, (float, int)) and not np.isfinite(rowelem):
                thisrow[ind] = None
            elif isinstance(rowelem, str) and len(rowelem) == 0:
                thisrow[ind] = None
            elif isinstance(rowelem, str) and rowelem.strip() == 'nan':
                thisrow[ind] = None

        cur.execute(sqlinsert, tuple(thisrow))

    # get the column information if there is any
    if isinstance(colinfo, dict):

        overridecolinfo = colinfo

    elif isinstance(colinfo, str) and os.path.exists(colinfo):

        with open(colinfo,'r') as infd:
            overridecolinfo = json.load(infd)

    elif isinstance(colinfo, str):

        try:
            overridecolinfo = json.loads(colinfo)
        except:
            LOGERROR('could not understand colinfo argument, skipping...')
            overridecolinfo = None

    else:
        overridecolinfo = None

    if overridecolinfo:

        for col in defaultcolinfo:

            if col in overridecolinfo:

                if overridecolinfo[col]['title'] is not None:
                    defaultcolinfo[col]['title'] = overridecolinfo[col]['title']

                if overridecolinfo[col]['dtype'] is not None:
                    defaultcolinfo[col]['dtype'] = overridecolinfo[col]['dtype']

                if overridecolinfo[col]['format'] is not None:
                    defaultcolinfo[col]['format'] = (
                        overridecolinfo[col]['format']
                    )

                if overridecolinfo[col]['description'] is not None:
                    defaultcolinfo[col]['description'] = (
                        overridecolinfo[col]['description']
                    )


    # now create any indexes we want
    if indexcols:

        LOGINFO('creating indexes on columns %s' % repr(indexcols))

        for indexcol in indexcols:

            # the user gives the name of the col in the augcat pickle, which we
            # convert to the database column name
            indexcolname = indexcol.replace('.','_')
            sqlindex = ('create index %s_idx on object_catalog (%s)' %
                        (indexcolname, indexcolname))
            cur.execute(sqlindex)

    else:

        indexcols = []

        for icol in defaultcolinfo.keys():

            if defaultcolinfo[icol]['index']:
                sqlindex = ('create index %s_idx on object_catalog (%s)' %
                            (icol, icol))
                cur.execute(sqlindex)
                indexcols.append(icol)


    # create any full-text-search indices we want
    if ftsindexcols:

        LOGINFO('creating an FTS index on columns %s' % repr(ftsindexcols))

        # generate the FTS table structure
        ftscreate = ("create virtual table catalog_fts "
                     "using fts5({column_list}, content=object_catalog)")
        fts_column_list = ', '.join(
            [x.replace('.','_') for x in ftsindexcols]
        )
        ftscreate = ftscreate.format(column_list=fts_column_list)

        # create the FTS index
        cur.execute(ftscreate)

        # execute the rebuild command to activate the indices
        cur.execute("insert into catalog_fts(catalog_fts) values ('rebuild')")

        # FIXME: add the FTS trigger statements here for an update to the main
        # object_catalog table. see the astro-coffee implementation for hints

    else:

        ftsindexcols = []

        for icol in defaultcolinfo.keys():

            if defaultcolinfo[icol]['ftsindex']:
                ftsindexcols.append(icol)

        LOGINFO('creating an FTS index on columns %s' % repr(ftsindexcols))

        # generate the FTS table structure
        ftscreate = ("create virtual table catalog_fts "
                     "using fts5({column_list}, content=object_catalog)")
        fts_column_list = ', '.join(
            [x.replace('.','_') for x in ftsindexcols]
        )
        ftscreate = ftscreate.format(column_list=fts_column_list)

        # create the FTS index
        cur.execute(ftscreate)

        # execute the rebuild command to activate the indices
        cur.execute("insert into catalog_fts(catalog_fts) values ('rebuild')")


    # turn the column info into a JSON
    columninfo_json = json.dumps(defaultcolinfo)

    # add some metadata to allow reading the LCs correctly later

    m_indexcols = indexcols if indexcols is not None else []
    m_ftsindexcols = ftsindexcols if ftsindexcols is not None else []


    metadata = {
        'basedir':augcat['basedir'],
        'lcformat':augcat['lcformat'],
        'fileglob':augcat['fileglob'],
        'nobjects':augcat['nfiles'],
        'catalogcols':sorted(colnames),
        'indexcols':sorted([x.replace('.','_') for x in m_indexcols]),
        'ftsindexcols':sorted([x.replace('.','_') for x in m_ftsindexcols]),
        'lcset_name':lcset_name,
        'lcset_desc':lcset_desc,
        'lcset_project':lcset_project,
        'lcset_datarelease':lcset_datarelease,
        'lcset_citation':lcset_citation,
        'lcset_ispublic':lcset_ispublic
    }
    metadata_json = json.dumps(metadata)
    cur.execute(
        'create table catalog_metadata (metadata_json text, column_info text)'
    )
    cur.execute('insert into catalog_metadata values (?, ?)',
                (metadata_json, columninfo_json))

    # commit and close the database
    db.commit()
    db.close()

    return outfile



def objectinfo_to_postgres_table(lclistpkl,
                                 table,
                                 pghost=None,
                                 pguser=None,
                                 pgpass=None,
                                 pgport=None):
    '''
    This writes the object information to a Postgres table.

    '''



##############################################
## LIGHT CURVE FORMAT MODULES AND FUNCTIONS ##
##############################################

def check_extmodule(module, formatkey):
    '''This just imports the module specified.

    '''

    try:

        if os.path.exists(module):

            sys.path.append(os.path.dirname(module))
            importedok = importlib.import_module(
                os.path.basename(module.replace('.py',''))
            )
        else:
            importedok = importlib.import_module(module)

    except Exception as e:

        LOGEXCEPTION('could not import the module: %s for LC format: %s. '
                     'check the file path or fully qualified module name?'
                     % (module, formatkey))
        importedok = False

    return importedok



##############################################
## COLLECTING METADATA ABOUT LC COLLECTIONS ##
##############################################

SQLITE_LCC_CREATE = '''\
-- make the main table
create table lcc_index (
  collection_id text not null,
  lcformat_key text not null,
  lcformat_desc_path text not null,
  object_catalog_path text not null,
  kdtree_pkl_path text not null,
  lightcurves_dir_path text not null,
  periodfinding_dir_path text,
  checkplots_dir_path text,
  ra_min real not null,
  ra_max real not null,
  decl_min real not null,
  decl_max real not null,
  nobjects integer not null,
  catalog_columninfo_json text not null,
  columnlist text,
  indexedcols text,
  ftsindexedcols text,
  name text,
  description text,
  project text,
  citation text,
  ispublic integer,
  datarelease integer default 0,
  last_updated datetime,
  last_indexed datetime,
  primary key (collection_id, name, project, datarelease)
);

-- make some indexes

-- fts indexes below

-- activate the fts indexes
'''

SQLITE_LCC_INSERT = '''\
insert or replace into lcc_index (
  collection_id,
  lcformat_key, lcformat_desc_path,
  object_catalog_path, kdtree_pkl_path, lightcurves_dir_path,
  periodfinding_dir_path, checkplots_dir_path,
  ra_min, ra_max, decl_min, decl_max,
  nobjects,
  catalog_columninfo_json,
  columnlist, indexedcols, ftsindexedcols,
  name, description, project, citation, ispublic, datarelease,
  last_updated, last_indexed
) values (
  ?,
  ?,?,
  ?,?,?,
  ?,?,
  ?,?,?,?,
  ?,
  ?,
  ?,?,?,
  ?,?,?,?,?,?,
  ?,datetime('now')
)
'''



def sqlite_collect_lcc_info(
        lcc_basedir,
        collection_id,
        raiseonfail=False,
):
    '''This writes or updates the lcc-index.sqlite file in lcc_basedir.

    each LC collection is identified by its subdirectory name. The following
    files must be present in each LC collection subdirectory:

    - lclist-catalog.pkl
    - catalog-kdtree.pkl
    - catalog-objectinfo.sqlite
      - this must contain lcset_* metadata for the collection, so we can give it
        a name, description, project name, last time of update, datarelease
        number
    - lcformat-description.json
      - this contains the basic information for the LC format recognition

    Each LC collection must have the following subdirectories:

    input:
    - lightcurves/ -> the LCs in whatever format
    - periodfinding/ -> the periodfinding result pickles
    - checkplots/ -> the checkplot pickles

    At the top level of the basedir we have:

    - datasets/ -> the datasets generated from searches
    - products/ -> the lightcurves.zip and dataset.zip for each dataset

    - lcc-index.sqlite -> contains for each LC collection:

                          - collection-id (dirname), description, project name,
                            date of last update, number of objects, footprint in
                            RA/DEC, footprint in gl/gb, datareleae number, and
                            an ispublic flag

                          - basedir paths for each LC set to get to its catalog
                            sqlite, kdtree, and datasets

                          - columns, indexcols, ftscols for each dataset

                          - sets of columns, indexcols and ftscols for all LC
                            sets

    '''

    # find the root DB
    lccdb = os.path.join(lcc_basedir, 'lcc-index.sqlite')

    # if it exists already, open it
    if os.path.exists(lccdb):

        database = sqlite3.connect(
            lccdb,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = database.cursor()

    # if it doesn't exist, then make it
    else:

        database = sqlite3.connect(
            lccdb,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = database.cursor()

        cursor.executescript(SQLITE_LCC_CREATE)
        database.commit()

    #
    # now we're ready to operate on the given lcc-collection basedir
    #

    # 1. get the various paths
    object_catalog_path = os.path.abspath(os.path.join(lcc_basedir,
                                                       collection_id,
                                                       'lclist-catalog.pkl'))
    catalog_kdtree_path = os.path.abspath(os.path.join(lcc_basedir,
                                                       collection_id,
                                                       'catalog-kdtree.pkl'))
    catalog_objectinfo_path = os.path.abspath(
        os.path.join(lcc_basedir,
                     collection_id,
                     'catalog-objectinfo.sqlite')
    )

    lightcurves_dir_path = os.path.abspath(
        os.path.join(lcc_basedir,
                     collection_id,
                     'lightcurves')
    )

    periodfinding_dir_path = os.path.abspath(
        os.path.join(lcc_basedir,
                     collection_id,
                     'periodfinding')
    )

    checkplots_dir_path = os.path.abspath(
        os.path.join(lcc_basedir,
                     collection_id,
                     'checkplots')
    )

    lcformat_desc_path = os.path.abspath(
        os.path.join(lcc_basedir,
                     collection_id,
                     'lcformat-description.json')
    )

    # check that all of these exist
    if not os.path.exists(object_catalog_path):
        LOGERROR('could not find an object catalog pkl: %s '
                 'for collection: %s, cannot continue' %
                 (object_catalog_path, collection_id))
        return None

    if not os.path.exists(catalog_kdtree_path):
        LOGERROR('could not find a catalog kdtree pkl: %s '
                 'for collection: %s, cannot continue' %
                 (catalog_kdtree_path, collection_id))
        return None

    if not os.path.exists(catalog_objectinfo_path):
        LOGERROR('could not find a catalog objectinfo sqlite DB: %s '
                 'for collection: %s, cannot continue' %
                 (catalog_objectinfo_path, collection_id))
        return None

    if not os.path.exists(lightcurves_dir_path):
        LOGERROR('could not find the expected light curves directory: %s '
                 'for collection: %s, cannot continue' %
                 (lightcurves_dir_path, collection_id))
        return None

    if not os.path.exists(periodfinding_dir_path):
        LOGERROR('could not find the expected '
                 'period-finding results directory: %s '
                 'for collection: %s, cannot continue' %
                 (periodfinding_dir_path, collection_id))
        return None

    if not os.path.exists(checkplots_dir_path):
        LOGERROR('could not find the expected checkplot pickles directory: %s '
                 'for collection: %s, cannot continue' %
                 (checkplots_dir_path, collection_id))
        return None

    if not os.path.exists(lcformat_desc_path):
        LOGERROR('no lcformat-description.json file '
                 'found in collection directory: %s, cannot continue'
                 'for collection: %s, making a new one' %
                 (lcformat_desc_path,))

    # 2. check if we can successfully import the lcformat reader func
    try:


        # read the lcformat-description.json file to get the reader module,
        # reader function, normalization module, and normalization function

        with open(lcformat_desc_path,'rb') as infd:
            lcformat_dict = json.load(infd)

        lcformat_key = lcformat_dict['lc_formatkey']
        lcformat_fileglob = lcformat_dict['lc_fileglob']
        lcformat_reader_module = lcformat_dict['lc_readermodule']
        lcformat_reader_function = lcformat_dict['lc_readerfunc']
        lcformat_norm_module = lcformat_dict['lc_normalizemodule']
        lcformat_norm_function = lcformat_dict['lc_normalizefunc']

        # see if we can import the reader module
        readermodule = check_extmodule(lcformat_reader_module,
                                       lcformat_key)

        if lcformat_norm_module:
            normmodule = check_extmodule(lcformat_norm_module, lcformat_key)
        else:
            normmodule = None

        # then, get the function we need to read the lightcurve
        readerfunc = getattr(readermodule, lcformat_reader_function)

        if lcformat_norm_function:
            normfunc = getattr(normmodule, lcformat_norm_function)
        else:
            normfunc = None

        # use the lcformat_fileglob to find light curves in the LC dir
        lcformat_lcfiles = glob.glob(os.path.join(lightcurves_dir_path,
                                                  lcformat_fileglob))
        if len(lcformat_lcfiles) == 0:
            LOGERROR('no light curves for lcformat key: %s, '
                     'matching provided fileglob: %s '
                     'found in expected light curves directory: %s, '
                     'cannot continue' % (lcformat_key, lcformat_fileglob,
                                          lightcurves_dir_path))
            return None

        # finally, read in a light curve to see if it works as expected
        lcdict = readerfunc(lcformat_lcfiles[0])

        if isinstance(lcdict, (tuple, list)) and len(lcdict) == 2:
            lcdict = lcdict[0]

        LOGINFO('imported provided LC reader module and function, '
                'and test-read a %s light curve successfully from %s...' %
                (lcformat_key, lightcurves_dir_path))

        # now test the normalization function
        if normmodule and normfunc:
            normlcd = normfunc(lcdict)
            LOGINFO('normalization function tested and works OK')

    except Exception as e:

        LOGEXCEPTION('could not import provided LC reader module/function or '
                     'could not read in a light curve from the expected '
                     'LC directory, cannot continue')
        if raiseonfail:
            raise
        else:
            return None

    # 3. open the catalog sqlite and then:
    #    - get the minra, maxra, mindecl, maxdecl,
    #    - get the nobjects
    #    - get the column, index, and ftsindex information,
    #    - get the name, desc, project, citation, ispublic, datarelease,
    #      last_updated

    # now, calculate the required object info from this collection's
    # objectinfo-catalog.sqlite file

    try:

        objectinfo = sqlite3.connect(
            catalog_objectinfo_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        ocur = objectinfo.cursor()

        query = ("select count(*), min(ra), max(ra), min(decl), max(decl) from "
                 "object_catalog")
        ocur.execute(query)
        row = ocur.fetchone()

        # 1. this is the nobjects and footprint (or a poor approximation of one)
        # FIXME: maybe use a convex hull or alpha shape here
        nobjects, minra, maxra, mindecl, maxdecl = row

        # 2. get the column info out
        query = ("select metadata_json, column_info from catalog_metadata")
        ocur.execute(query)
        metadata, column_info = ocur.fetchone()

        # from the metadata, we collect the collection's name, description,
        # project, citation, ispublic, datarelease, columnlist, indexedcols,
        # ftsindexedcols
        metadata = json.loads(metadata)

        # this is the time when the catalog-objectinfo.sqlite was last created
        # (stat_result.st_ctime). we assume this is the same time at which the
        # collection was updated to add new items or update information
        last_updated = datetime.fromtimestamp(
            os.stat(catalog_objectinfo_path).st_ctime
        )

        # close the objectinfo-catalog.sqlite file
        ocur.close()
        objectinfo.close()

        # 3. put these things into the lcc-index database

        # prepare the query items
        items = (
            collection_id,
            lcformat_key,
            lcformat_desc_path,
            catalog_objectinfo_path,
            catalog_kdtree_path,
            lightcurves_dir_path,
            periodfinding_dir_path,
            checkplots_dir_path,
            minra, maxra, mindecl, maxdecl,
            nobjects,
            column_info,
            ','.join(metadata['catalogcols']),
            ','.join(metadata['indexcols']),
            ','.join(metadata['ftsindexcols']),
            metadata['lcset_name'],
            metadata['lcset_desc'],
            metadata['lcset_project'],
            metadata['lcset_citation'],
            metadata['lcset_ispublic'],
            metadata['lcset_datarelease'],
            last_updated
        )

        # 4. execute the queries to put all of this stuff into the lcc_index
        # table and commit
        cursor.execute(SQLITE_LCC_INSERT, items)
        database.commit()

        # all done!
        LOGINFO('added light curve collection: '
                '%s with %s objects, LCs at: %s to the light curve '
                'collection index database: %s' %
                (lcformat_key, nobjects, lightcurves_dir_path, lccdb))

        # return the path of the lcc-index.sqlite database
        return lccdb

    except Exception as e:

        LOGEXCEPTION('could not get collection data from the object '
                     'catalog SQLite database: %s, cannot continue' %
                     catalog_objectinfo_path)

        database.close()
        if raiseonfail:
            raise
        else:
            return None



##################################################
## FUNCTIONS THAT DEAL WITH LIGHT CURVE FORMATS ##
##################################################

def dict_get(datadict, keylist):
    '''
    This gets the requested key by walking the datadict.

    '''
    return reduce(getitem, keylist, datadict)



def get_lcformat_description(descpath):
    '''
    This reads the lcformat column description file and returns a dict.

    The description file is a JSON located under the collection's
    collection_id directory/lcformat-description.json.

    '''
    # read the JSON
    with open(descpath,'rb') as infd:

        formatdesc = json.load(infd)
        formatkey = formatdesc['lc_formatkey']

    # 1. generate the metadata info dict
    metadata_info = {}

    for key in formatdesc['metadata_keys']:

        desc, textform, caster = formatdesc['metadata_keys'][key]

        deref_key = key.split('.')

        thiskey_info = {'deref': deref_key,
                        'desc': desc,
                        'format': textform,
                        'caster': caster}
        metadata_info[key] = thiskey_info


    # 2. get the column info
    column_info = {}
    column_keys = []

    # 2a. first, get the unaffiliated columns
    for key in formatdesc['unaffiliated_cols']:

        desc, textform, dtype = formatdesc['column_keys'][key]

        column_info[key] = {'desc':desc,
                            'format':textform,
                            'dtype':dtype}
        column_keys.append(key)

    # 2b. next, get the per magnitude columns
    apertures = formatdesc['mag_apertures']

    for key in formatdesc['per_aperture_cols']:

        for ap in apertures:

            fullkey = '%s_%s' % (key, ap)
            desc, textform, dtype = formatdesc['column_keys'][key]
            desc = desc % ap

            column_info[fullkey] = {'desc':desc,
                                    'format':textform,
                                    'dtype':dtype}
            column_keys.append(fullkey)


    # 3. load the reader module and get the reader and normalize functions
    reader_module_name = formatdesc['lc_readermodule']
    reader_func_name = formatdesc['lc_readerfunc']
    norm_module_name = formatdesc['lc_normalizemodule']
    norm_func_name = formatdesc['lc_normalizefunc']

    # see if we can import the reader module
    readermodule = check_extmodule(reader_module_name, formatkey)

    if norm_module_name:
        normmodule = check_extmodule(norm_module_name, formatkey)
    else:
        normmodule = None

    # then, get the function we need to read the lightcurve
    readerfunc = getattr(readermodule, reader_func_name)

    if norm_module_name and norm_func_name:
        normfunc = getattr(normmodule, norm_func_name)
    else:
        normfunc = None


    # this is the final metadata dict

    returndict = {
        'formatkey':formatkey,
        'readerfunc':readerfunc,
        'normfunc':normfunc,
        'columns':column_info,
        'colkeys':column_keys,
        'metadata':metadata_info
    }

    return returndict



def convert_to_csvlc(lcfile,
                     objectid,
                     lcformat_dict,
                     csvlc_version=1,
                     comment_char='#',
                     column_separator=',',
                     skip_converted=False):
    '''This converts any readable LC to a common-format CSV LC.

    The first 3 lines of the file are always:

    LCC-CSVLC-<csvlc_version>
    <comment_char>
    <column_separator>

    The next lines are offset with comment_char and are JSON formatted
    descriptions of: (i) the object metadata, (ii) the column info. Finally, we
    have the columns separated with column_separator.

    so reader functions can recognize it automatically (like
    astrobase.hatsurveys.hatlc.py).

    This will normalize the light curve as specified in the
    lcformat-description.json file.

    '''

    # the filename
    outfile = '%s-csvlc.gz' % objectid

    # we'll put the CSV LC in the same place as the original LC
    outpath = os.path.join(os.path.dirname(lcfile), outfile)

    # if we're supposed to skip an existing file, do so here
    if skip_converted and os.path.exists(outpath):
        LOGWARNING('%s exists already and skip_converted = True, skipping...' %
                   outpath)
        return outpath

    # use the lcformat_dict to read (and normalize) the lcdict
    readerfunc = lcformat_dict['readerfunc']
    normfunc = lcformat_dict['normfunc']

    lcdict = readerfunc(lcfile)

    if isinstance(lcdict, (tuple, list)) and isinstance(lcdict[0], dict):
        lcdict = lcdict[0]

    if normfunc:
        lcdict = normfunc(lcdict)

    # extract the metadata keys
    meta = {}

    for key in lcformat_dict['metadata']:

        try:
            thismetainfo = lcformat_dict['metadata'][key]
            val = dict_get(lcdict, thismetainfo['deref'])
            meta[thismetainfo['deref'][-1]] = {
                'val':val,
                'desc':thismetainfo['desc'],
            }
        except:
            pass

    # extract the column info
    columns = {}

    # generate the format string for each line
    line_formstr = []

    available_keys = []
    ki = 0

    for key in lcformat_dict['colkeys']:

        if key in lcdict:

            thiscolinfo = lcformat_dict['columns'][key]

            line_formstr.append(thiscolinfo['format'])

            columns[key] = {
                'colnum': ki,
                'dtype':thiscolinfo['dtype'],
                'desc':thiscolinfo['desc']
            }
            available_keys.append(key)
            ki = ki + 1

    # generate the header bits
    metajson = indent(json.dumps(meta, indent=2), '%s ' % comment_char)
    coljson = indent(json.dumps(columns, indent=2), '%s ' % comment_char)

    # the final format string for each column line
    line_formstr = '%s\n' % ('%s' % column_separator).join(line_formstr)

    # now, put together everything
    with gzip.open(outpath, 'wb') as outfd:

        # first, write the format spec
        outfd.write(('LCC-CSVLC-V%s\n' % csvlc_version).encode())
        outfd.write(('%s\n' % comment_char).encode())
        outfd.write(('%s\n' % column_separator).encode())

        # second, write the metadata JSON
        outfd.write(('%s OBJECT METADATA\n' % comment_char).encode())
        outfd.write(('%s\n' % metajson).encode())
        outfd.write(('%s\n' % (comment_char,)).encode())

        # third, write the column JSON
        outfd.write(('%s COLUMN DEFINITIONS\n' % comment_char).encode())
        outfd.write(('%s\n' % coljson).encode())

        # finally, prepare to write the LC columns
        outfd.write(('%s\n' % (comment_char,)).encode())
        outfd.write(('%s LIGHTCURVE\n' % comment_char).encode())

        # last, write the columns themselves
        nlines = len(lcdict[lcformat_dict['colkeys'][0]])

        for lineind in range(nlines):

            thisline = [
                lcdict[x][lineind] for x in available_keys
            ]
            formline = line_formstr % tuple(thisline)
            outfd.write(formline.encode())

    return outpath



#############################################
## FUNCTIONS FOR COLLECTION INDEX DATABASE ##
#############################################

def calculate_collection_footprint(lcc_basedir,
                                   collection_id=None):
    '''This calculates the sky footprint of a single or all collections in an
    lcc-index.sqlite file.

    '''
