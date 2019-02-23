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
from textwrap import indent, dedent
import gzip
import operator
from functools import partial
from itertools import count as icounter
from datetime import datetime

import numpy as np
from scipy.spatial import cKDTree

from tqdm import tqdm

from tornado.escape import squeeze


#########################################
## DEFAULT COLUMNS THAT ARE RECOGNIZED ##
#########################################

from .abcat_columns import COLUMN_INFO, COMPOSITE_COLUMN_INFO



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


# this is used to map the operator spec in the COMPOSITE_COLUMN_INFO dict to an
# actual Python operator function
OPERATORS = {'+':operator.add,
             '-':operator.sub,
             '*':operator.mul,
             '/':operator.truediv}



def objectinfo_to_sqlite(augcatpkl,
                         outfile,
                         lcc_name,
                         lcc_desc=None,
                         lcc_project=None,
                         lcc_datarelease=None,
                         lcc_citation=None,
                         lcc_owner=1,
                         lcc_visibility='public',
                         lcc_sharedwith=None,
                         colinfo=None,
                         indexcols=None,
                         ftsindexcols=None,
                         overwrite_existing=False):

    '''This writes the object information for an LCC to an SQLite file.

    NOTE: This function requires FTS5 to be available in SQLite because we don't
    want to mess with text-search ranking algorithms to be implemented for FTS4.

    lcc_name must be provided. It will be used as name of the DB in several
    frontend LCC server controls. This is a string, e.g. 'HATNet DR0: Kepler
    Field'.

    The other lcc_* kwargs set some metadata for the project. this is used by
    the top-level lcc-index.sqlite database for all LC collections. The frontend
    will use these to render HTML descriptions, etc.

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

    If colinfo is not provided, this function will use the column definitions
    provided in abcat.COLUMN_INFO and abcat.COMPOSITE_COLUMN_INFO. These are
    fairly extensive and should cover all of the data that the upstream
    astrobase tools can generate for object information.

    This function makes indexes for fast look up by objectid by default and any
    columns included in indexcols. also makes a full-text search index for any
    columns in ftsindexcols. If either of these are not provided, will look for
    and make indices as specified in abcat_columns.COLUMN_INFO and
    COMPOSITE_COLUMN_INFO.

    If overwrite_existing is True, any existing catalog DB in the target
    directory will be overwritten.

    '''

    if os.path.exists(outfile):
        LOGWARNING('An existing objectinfo catalog DB exists at: %s' % outfile)

    if overwrite_existing and os.path.exists(outfile):
        LOGWARNING('overwrite_existing = True, removing old DB: %s' % outfile)
        os.remove(outfile)
    elif not overwrite_existing and os.path.exists(outfile):
        LOGWARNING('not overwriting existing catalog DB and returning it')
        return outfile


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
        if thiscol_dtype.type is np.str_ or thiscol_dtype.type is np.unicode_:

            coldefs.append(('%s text' % thiscol_name, str))

            if colinfo_key in COLUMN_INFO:
                defaultcolinfo[thiscol_name] = COLUMN_INFO[colinfo_key]
            else:
                defaultcolinfo[thiscol_name]['format'] = '%s'

            # this gets the string representation of the numpy dtype object
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


    # go through the composite unaffiliated columns next (these are generated
    # from two key:val pairs in the objectinfo dict
    composite_cols = COMPOSITE_COLUMN_INFO.keys()

    # make sure not to get columns that are already in the augcat
    composite_cols = list(set(composite_cols) - set(augcat['objects'].keys()))

    for col in composite_cols:

        #
        # actually generate the column
        #
        col_opstr, col_operand1, col_operand2 = (
            COMPOSITE_COLUMN_INFO[col]['from']
        )
        col_op = OPERATORS[col_opstr]

        try:

            # this should magically work because of numpy arrays (hopefully)
            augcat['objects'][col] = col_op(augcat['objects'][col_operand1],
                                            augcat['objects'][col_operand2])
            augcat['columns'].append(col)
            LOGINFO('generated composite column: %s '
                    'using operator: %r on cols: %s and %s' % (col,
                                                               col_op,
                                                               col_operand1,
                                                               col_operand2))

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

                if colinfo_key in COMPOSITE_COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = COMPOSITE_COLUMN_INFO[
                        colinfo_key
                    ]
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%s'

                # this gets the string representation of the numpy dtype
                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


            # floats
            elif thiscol_dtype.type is np.float64:

                coldefs.append(('%s double precision' % thiscol_name, float))

                if colinfo_key in COMPOSITE_COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = COMPOSITE_COLUMN_INFO[
                        colinfo_key
                    ]
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%.7f'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


            # integers
            elif thiscol_dtype.type is np.int64:

                coldefs.append(('%s integer' % thiscol_name, int))

                if colinfo_key in COMPOSITE_COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = COMPOSITE_COLUMN_INFO[
                        colinfo_key
                    ]
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%i'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str


            # everything else is coerced into a string
            else:

                coldefs.append(('%s text' % thiscol_name, str))

                if colinfo_key in COMPOSITE_COLUMN_INFO:
                    defaultcolinfo[thiscol_name] = COMPOSITE_COLUMN_INFO[
                        colinfo_key
                    ]
                else:
                    defaultcolinfo[thiscol_name]['format'] = '%s'

                defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str

        # the operand columns aren't present in the augcat, skip
        except Exception as e:
            pass

    # finally, go though the mag affiliated columns, per magcol

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

            # everything else is coerced into a string
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
    all_available_cols = (unaffiliated_cols +
                          list(composite_cols) +
                          mag_affil_cols)

    # test if these cols are all in the augcat
    cols = []
    for col in all_available_cols:
        if col in augcat['objects']:
            cols.append(col)

    # generate the final column list for insertion
    column_and_type_list = ', '.join([x[0] for x in coldefs])

    # this is the final SQL used to create the database.
    # extra columns we create:
    # extra_info_json
    # object_owner
    # object_visibility
    # object_sharedwith
    sqlcreate = dedent(
        '''create table object_catalog ({column_type_list},
        extra_info_json text default
        '{{"bibcode": null, "parent": null, "comments": null}}',
        object_owner integer default 1,
        object_visibility text default 'public',
        object_sharedwith text,
        primary key (objectid))'''
    )
    sqlcreate = sqlcreate.format(column_type_list=column_and_type_list)

    # add in the owner, visibility, and sharedwith columns. set these values for
    # each object to the same as the provided values for this collection in
    # lcc_owner, lcc_visibility, and lcc_sharedwith

    # this is the insert statement
    column_list = ', '.join(colnames +
                            ['object_owner',
                             'object_visibility',
                             'object_sharedwith'])
    placeholders = ','.join(['?']*(len(cols) + 3))

    sqlinsert = ("insert into object_catalog ({column_list}) "
                 "values ({placeholders})")
    sqlinsert = sqlinsert.format(column_list=column_list,
                                 placeholders=placeholders)

    LOGINFO('objects in %s: %s, generating SQLite database...' %
            (augcatpkl, augcat['nfiles']))

    LOGINFO('CREATE TABLE statement will be: "%s"' % sqlcreate)
    LOGINFO('INSERT statement will be: "%s"' % sqlinsert)

    # connect to the database
    db = sqlite3.connect(outfile)
    cur = db.cursor()

    colformatters = [x[1] for x in coldefs]

    #
    # start the transaction
    #
    cur.executescript('pragma journal_mode = wal; '
                      'pragma journal_size_limit = 52428800;')
    cur.execute('begin')

    #
    # create the version info table that we can use for migrations later
    #
    cur.executescript(
        "create table object_catalog_vinfo (dbver integer, "
        "lccserver_vtag text, "
        "vdate date); "
        "insert into object_catalog_vinfo values (2, 'v0.2', '2018-08-31');"
    )

    #
    # create the main object_catalog table
    #
    cur.execute(sqlcreate)

    LOGINFO('object_catalog table created successfully, '
            'now inserting objects...')

    # now we'll insert things into the table
    for rowind in tqdm(range(augcat['objects'][cols[0]].size)):

        instance_markers = icounter(start=1)
        insert_done = False
        this_marker = 1

        # keep trying to insert until we succeed. this is to overcome multiple
        # objects with the same objectid (a common occurence when we have
        # multiple planets for a single transiting system for example). in this
        # case, we'll insert the objects with markers indicating how many
        # repeats there are
        while not insert_done:

            try:

                thisrow = [
                    y(augcat['objects'][x][rowind]) for
                    x,y in zip(cols, colformatters)
                ]

                if this_marker > 1:
                    new_objectid = '%s-%s' % (
                        augcat['objects']['objectid'][rowind],
                        this_marker
                    )
                    thisrow[cols.index('objectid')] = new_objectid

                # add in the per-object permissions using the LCC permissions as
                # base
                thisrow.extend([lcc_owner,
                                lcc_visibility,
                                lcc_sharedwith])

                for ind, rowelem in enumerate(thisrow):

                    if (isinstance(rowelem, (float, int)) and
                        not np.isfinite(rowelem)):
                        thisrow[ind] = None
                    elif isinstance(rowelem, str) and len(rowelem) == 0:
                        thisrow[ind] = None
                    elif isinstance(rowelem, str) and rowelem.strip() == 'nan':
                        thisrow[ind] = None


                cur.execute(sqlinsert, tuple(thisrow))
                instance_markers = icounter(start=1)
                insert_done = True
                this_marker = 1

            except sqlite3.IntegrityError:

                this_marker = next(instance_markers)

                if this_marker > 1:
                    LOGERROR(
                        "objectid: %s already exists in the DB, "
                        "will tag this objectid instance with marker '%s'"
                        % (augcat['objects']['objectid'][rowind],
                           this_marker)
                    )


    # get the column information if there is any
    if isinstance(colinfo, dict):

        overridecolinfo = colinfo

    elif isinstance(colinfo, str) and os.path.exists(colinfo):

        with open(colinfo,'r') as infd:
            overridecolinfo = json.load(infd)

    elif isinstance(colinfo, str):

        try:
            overridecolinfo = json.loads(colinfo)
        except Exception as e:
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

        # make an index on the object_owner and object_status columns
        # this doesn't show up in the column or indexes lists
        cur.execute('create index object_owner_idx '
                    'on object_catalog (object_owner)')
        cur.execute('create index object_visibility_idx '
                    'on object_catalog (object_visibility)')
        cur.execute('create index object_sharedwith_idx '
                    'on object_catalog (object_sharedwith)')

    # create any full-text-search indices we want
    if ftsindexcols:

        LOGINFO('creating an FTS index on columns %s' % repr(ftsindexcols))

        #
        # generate the FTS table structure
        #

        # the extra_info_json will be a JSON column for storing stuff that
        # doesn't really belong in the other columns like comments, object
        # cluster membership, and object histories, etc.
        ftscreate = ("create virtual table catalog_fts "
                     "using fts5({column_list}, extra_info_json, "
                     "content=object_catalog)")
        fts_column_list = ', '.join(
            [x.replace('.','_') for x in ftsindexcols]
        )
        ftscreate = ftscreate.format(column_list=fts_column_list)

        # create the FTS index
        cur.execute(ftscreate)

        # create triggers to update the FTS indices automatically if the main
        # object_catalog table gets updated
        new_fts_column_list = ', '.join(
            ['new.%s' % x.replace('.','_') for x in ftsindexcols]
        )
        fts_triggers = (
            "create trigger fts_beforeupdate before update on object_catalog "
            "begin "
            "delete from catalog_fts where rowid=old.rowid; "
            "end; "
            "create trigger fts_afterupdate after update on object_catalog "
            "begin "
            "insert into catalog_fts(rowid, {column_list}, extra_info_json) "
            "values (new.rowid, {new_column_list}, new.extra_info_json); "
            "end;"
        ).format(column_list=fts_column_list,
                 new_column_list=new_fts_column_list)
        LOGINFO('FTS trigger create statement will be: %s' % fts_triggers)
        cur.executescript(fts_triggers)

        # execute the rebuild command to activate the indices
        cur.execute("insert into catalog_fts(catalog_fts) values ('rebuild')")

    else:

        ftsindexcols = []

        for icol in defaultcolinfo.keys():

            if defaultcolinfo[icol]['ftsindex']:
                ftsindexcols.append(icol)

        LOGINFO('creating an FTS index on columns %s' % repr(ftsindexcols))

        #
        # generate the FTS table structure
        #

        # the extra_info_json will be a JSON column for storing stuff that
        # doesn't really belong in the other columns like comments, object
        # cluster membership, and object histories, etc.
        ftscreate = ("create virtual table catalog_fts "
                     "using fts5({column_list}, extra_info_json, "
                     "content=object_catalog)")
        fts_column_list = ', '.join(
            [x.replace('.','_') for x in ftsindexcols]
        )
        ftscreate = ftscreate.format(column_list=fts_column_list)

        # create the FTS index
        cur.execute(ftscreate)

        # create triggers to update the FTS indices automatically if the main
        # object_catalog table gets updated
        new_fts_column_list = ', '.join(
            ['new.%s' % x.replace('.','_') for x in ftsindexcols]
        )
        fts_triggers = (
            "create trigger fts_beforeupdate before update on object_catalog "
            "begin "
            "delete from catalog_fts where rowid=old.rowid; "
            "end; "
            "create trigger fts_afterupdate after update on object_catalog "
            "begin "
            "insert into catalog_fts(rowid, {column_list}, extra_info_json) "
            "values (new.rowid, {new_column_list}, new.extra_info_json); "
            "end;"
        ).format(column_list=fts_column_list,
                 new_column_list=new_fts_column_list)
        LOGINFO('FTS trigger create statement will be: %s' % fts_triggers)
        cur.executescript(fts_triggers)

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
        'magcols':augcat['magcols'],
        'catalogcols':sorted(colnames),
        'indexcols':sorted([x.replace('.','_') for x in m_indexcols]),
        'ftsindexcols':sorted([x.replace('.','_') for x in m_ftsindexcols]),
        'lcc_name':lcc_name,
        'lcc_desc':lcc_desc,
        'lcc_project':lcc_project,
        'lcc_datarelease':lcc_datarelease,
        'lcc_citation':lcc_citation,
        'lcc_owner':lcc_owner,
        'lcc_visibility':lcc_visibility,
        'lcc_sharedwith':lcc_sharedwith
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


##################################################
## FUNCTIONS THAT DEAL WITH LIGHT CURVE FORMATS ##
##################################################

def dict_get(datadict, keylist):
    '''
    This gets the requested key by walking the datadict.

    '''
    return reduce(getitem, keylist, datadict)



def get_lcformat_description(descpath):
    '''This reads the lcformat column description file and returns a dict.

    The description file is a JSON located under the collection's
    collection_id directory/lcformat-description.json.

    For the values in the JSON keys lc_readermodule, lc_readerfunc_kwargs,
    lc_normalizemodule, lc_normalizefunc_kwargs, you can use automatic
    substitutions for your user home and the current LC collection directories
    by using:

    {{home_dir}} -> substitute this pattern for the user's home directory

    {{collection_dir}} -> substitute this pattern for the current LC
                          collection's directory

    See an example at:

    https://github.com/waqasbhatti/lcc-server/docs/lcformat-desc-example.json

    This example LC format description JSON is associated with the
    lcc-server-setup.ipynb notebook at:

    https://github.com/waqasbhatti/astrobase-notebooks

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
    aperturejoiner = formatdesc['aperture_joiner']

    for key in formatdesc['per_aperture_cols']:

        for ap in apertures:

            fullkey = '%s%s%s' % (key, aperturejoiner, ap)
            desc, textform, dtype = formatdesc['column_keys'][key]
            if '%s' in desc:
                desc = desc % ap

            column_info[fullkey] = {'desc':desc,
                                    'format':textform,
                                    'dtype':dtype}
            column_keys.append(fullkey)


    # 3. load the reader module and get the reader and normalize functions
    reader_module_name = formatdesc['lc_readermodule']
    reader_func_name = formatdesc['lc_readerfunc']
    reader_func_kwargs = formatdesc['lc_readerfunc_kwargs']

    norm_module_name = formatdesc['lc_normalizemodule']
    norm_func_name = formatdesc['lc_normalizefunc']
    norm_func_kwargs = formatdesc['lc_normalizefunc_kwargs']

    #
    # do some convenient directory name substitutions in the reader module
    # import paths and reader function kwargs
    #
    if '{{collection_dir}}' in reader_module_name:
        reader_module_name = reader_module_name.replace(
            '{{collection_dir}}',
            os.path.abspath(os.path.dirname(descpath))
        )
    elif '{{home_dir}}' in reader_module_name:
        reader_module_name = reader_module_name.replace(
            '{{home_dir}}',
            os.path.abspath(os.path.expanduser('~'))
        )

    if isinstance(reader_func_kwargs, dict):
        for kwarg in reader_func_kwargs:
            if (isinstance(reader_func_kwargs[kwarg], str) and
                '{{collection_dir}}' in reader_func_kwargs[kwarg]):
                reader_func_kwargs[kwarg] = reader_func_kwargs[kwarg].replace(
                    '{{collection_dir}}',
                    os.path.abspath(os.path.dirname(descpath))
                )
    elif isinstance(reader_func_kwargs, dict):
        for kwarg in reader_func_kwargs:
            if (isinstance(reader_func_kwargs[kwarg], str) and
                '{{home_dir}}' in reader_func_kwargs[kwarg]):
                reader_func_kwargs[kwarg] = reader_func_kwargs[kwarg].replace(
                    '{{home_dir}}',
                    os.path.abspath(os.path.expanduser('~'))
                )

    #
    # do some convenient directory name substitutions in the norm module
    # import paths and norm function kwargs
    #
    if (norm_module_name is not None and
        '{{collection_dir}}' in norm_module_name):
        norm_module_name = norm_module_name.replace(
            '{{collection_dir}}',
            os.path.abspath(os.path.dirname(descpath))
        )
    elif norm_module_name is not None and '{{home_dir}}' in norm_module_name:
        norm_module_name = norm_module_name.replace(
            '{{home_dir}}',
            os.path.abspath(os.path.expanduser('~'))
        )

    if isinstance(norm_func_kwargs, dict):
        for kwarg in norm_func_kwargs:
            if (isinstance(norm_func_kwargs[kwarg], str) and
                '{{collection_dir}}' in norm_func_kwargs[kwarg]):
                norm_func_kwargs[kwarg] = norm_func_kwargs[kwarg].replace(
                    '{{collection_dir}}',
                    os.path.abspath(os.path.dirname(descpath))
                )
    elif isinstance(norm_func_kwargs, dict):
        for kwarg in norm_func_kwargs:
            if (isinstance(norm_func_kwargs[kwarg], str) and
                '{{home_dir}}' in norm_func_kwargs[kwarg]):
                norm_func_kwargs[kwarg] = norm_func_kwargs[kwarg].replace(
                    '{{home_dir}}',
                    os.path.abspath(os.path.expanduser('~'))
                )

    # see if we can import the reader module
    readermodule = check_extmodule(reader_module_name, formatkey)

    if norm_module_name:
        normmodule = check_extmodule(norm_module_name, formatkey)
    else:
        normmodule = None

    # then, get the function we need to read the lightcurve
    readerfunc_in = getattr(readermodule, reader_func_name)

    if norm_module_name and norm_func_name:
        normfunc_in = getattr(normmodule, norm_func_name)
    else:
        normfunc_in = None

    # add in any optional kwargs that need to be there for readerfunc
    if isinstance(reader_func_kwargs, dict):
        readerfunc = partial(readerfunc_in, **reader_func_kwargs)
    else:
        readerfunc = readerfunc_in

    # add in any optional kwargs that need to be there for normfunc
    if normfunc_in is not None:
        if isinstance(norm_func_kwargs, dict):
            normfunc = partial(normfunc_in, **norm_func_kwargs)
        else:
            normfunc = normfunc_in
    else:
        normfunc = None

    # get whether the measurements are in mags or fluxes
    flux_or_mag = formatdesc['lc_measurements_flux_or_mag']
    if flux_or_mag == 'flux':
        magsarefluxes = True
    elif flux_or_mag == 'mag':
        magsarefluxes = False

    # this is the final metadata dict
    returndict = {
        'formatkey':formatkey,
        'fileglob':formatdesc['lc_fileglob'],
        'readermodule':readermodule,
        'normmodule':normmodule,
        'readerfunc':readerfunc,
        'normfunc':normfunc,
        'columns':column_info,
        'colkeys':column_keys,
        'metadata':metadata_info,
        'magsarefluxes':magsarefluxes,
        'parsed_formatinfo':{
            'formatkey':formatkey,
            'fileglob':formatdesc['lc_fileglob'],
            'readermodule':reader_module_name,
            'readerfunc':reader_func_name,
            'readerfunc_kwargs':reader_func_kwargs,
            'normmodule':norm_module_name,
            'normfunc':norm_func_name,
            'normfunc_kwargs':norm_func_kwargs,
            'magsarefluxes':magsarefluxes,
        }
    }

    return returndict



def convert_to_csvlc(lcfile,
                     objectid,
                     lcformat_dict,
                     normalize_lc=False,
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
    lcformat-description.json file if normalize_lc is True. If this is False,
    will leave the light curve alone.

    '''

    # use the lcformat_dict to get the reader and normalization functions
    readerfunc = lcformat_dict['readerfunc']
    if normalize_lc:
        normfunc = lcformat_dict['normfunc']
    else:
        normfunc = None

    # if the object is not None, we can return early without trying to read the
    # original format LC if the output file exists already and skip_converted =
    # True
    if objectid is not None:

        # the filename
        outfile = '%s-csvlc.gz' % squeeze(objectid).replace(' ','-')

        # we'll put the CSV LC in the same place as the original LC
        outpath = os.path.join(os.path.dirname(lcfile), outfile)

        # if we're supposed to skip an existing file, do so here
        if skip_converted and os.path.exists(outpath):
            LOGWARNING(
                '%s exists already and skip_converted = True, skipping...' %
                outpath
            )
            return outpath

    # now read in the light curve
    lcdict = readerfunc(lcfile)
    if isinstance(lcdict, (tuple, list)) and isinstance(lcdict[0], dict):
        lcdict = lcdict[0]

    # at this point, we can get the objectid from the lcdict directly if it's
    # None, generate the output filepath, check if it exists, and return early
    # if skip_converted = True
    if objectid is None:

        objectid = lcdict['objectid']

        # the filename
        outfile = '%s-csvlc.gz' % squeeze(objectid).replace(' ','-')

        # we'll put the CSV LC in the same place as the original LC
        outpath = os.path.join(os.path.dirname(lcfile), outfile)

        # if we're supposed to skip an existing file, do so here
        if skip_converted and os.path.exists(outpath):
            LOGWARNING(
                '%s exists already and skip_converted = True, skipping...' %
                outpath
            )
            return outpath


    # normalize the lcdict if we have to
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
        except Exception as e:
            pass

    # extract the column info
    columns = {}

    # generate the format string for each line
    line_formstr = []

    available_keys = []
    ki = 0

    for key in lcformat_dict['colkeys']:

        try:
            dict_get(lcdict, key.split('.'))

            thiscolinfo = lcformat_dict['columns'][key]

            line_formstr.append(thiscolinfo['format'])

            columns[key] = {
                'colnum': ki,
                'dtype':thiscolinfo['dtype'],
                'desc':thiscolinfo['desc']
            }
            available_keys.append(key)
            ki = ki + 1

        except Exception as e:
            pass

    # generate the header bits
    metajson = indent(json.dumps(meta, indent=2), '%s ' % comment_char)
    coljson = indent(json.dumps(columns, indent=2), '%s ' % comment_char)

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

            thisline = []
            for x in available_keys:

                # we need to check if any col vals conflict with the specified
                # formatter. in this case, we'll turn the formatter into %s so
                # we don't fail here. this usually comes up if nan is provided
                # as a value to %i
                try:
                    thisline.append(
                        lcformat_dict['columns'][x]['format'] %
                        dict_get(lcdict, x.split('.'))[lineind]
                    )
                except Exception as e:
                    thisline.append(
                        str(dict_get(lcdict, x.split('.'))[lineind])
                    )

            formline = '%s\n' % ('%s' % column_separator).join(thisline)
            outfd.write(formline.encode())

    return outpath



##############################################
## COLLECTING METADATA ABOUT LC COLLECTIONS ##
##############################################

SQLITE_LCC_CREATE = '''\
create table lcc_index_vinfo (dbver integer,
                              lccserver_vtag text,
                              vdate date);
insert into lcc_index_vinfo values (2, 'v0.2', '2018-08-31');

pragma journal_mode = wal;
pragma journal_size_limit = 52428800;

-- make the main table
create table lcc_index (
  collection_id text not null,
  lcformat_key text not null,
  lcformat_desc_path text not null,
  lcformat_magcols text not null,
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
  datarelease integer default 0,
  last_updated datetime,
  last_indexed datetime,
  collection_owner integer default 1,
  collection_visibility integer default 2,
  collection_sharedwith text,
  primary key (collection_id, name, project, datarelease)
);

-- fts indexes below
create virtual table lcc_index_fts using fts5(
  collection_id,
  columnlist,
  name,
  description,
  project,
  citation,
  content=lcc_index
);

-- triggers for updating FTS index when things get changed
create trigger fts_before_update before update on lcc_index begin
    delete from lcc_index_fts where rowid=old.rowid;
end;

create trigger fts_before_delete before delete on lcc_index begin
    delete from lcc_index_fts where rowid=old.rowid;
end;

create trigger fts_after_update after update on lcc_index begin
    insert into lcc_index_fts(rowid, collection_id, columnlist, name,
                              description, project, citation)
    values (new.rowid, new.collection_id, new.columnlist, new.name,
            new.description, new.project, new.citation);
end;

create trigger fts_after_insert after insert on lcc_index begin
    insert into lcc_index_fts(rowid, collection_id, columnlist, name,
                              description, project, citation)
    values (new.rowid, new.collection_id, new.columnlist, new.name,
            new.description, new.project, new.citation);
end;

-- activate the fts indexes
insert into lcc_index_fts(lcc_index_fts) values ('rebuild');
'''

SQLITE_LCC_INSERT = '''\
insert or replace into lcc_index (
  collection_id,
  lcformat_key, lcformat_desc_path, lcformat_magcols,
  object_catalog_path, kdtree_pkl_path, lightcurves_dir_path,
  periodfinding_dir_path, checkplots_dir_path,
  ra_min, ra_max, decl_min, decl_max,
  nobjects,
  catalog_columninfo_json,
  columnlist, indexedcols, ftsindexedcols,
  name, description, project, citation, datarelease,
  last_updated, last_indexed,
  collection_owner, collection_visibility, collection_sharedwith
) values (
  ?,
  ?,?,?,
  ?,?,?,
  ?,?,
  ?,?,?,?,
  ?,
  ?,
  ?,?,?,
  ?,?,?,?,?,
  ?,datetime('now'),
  ?,?,?
)
'''


def sqlite_make_lcc_index_db(lcc_basedir):
    '''
    This just makes the lcc-index.sqlite file in the DB.

    '''
    # find the root DB
    lccdb = os.path.join(lcc_basedir, 'lcc-index.sqlite')

    database = sqlite3.connect(
        lccdb,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cursor = database.cursor()

    cursor.executescript(SQLITE_LCC_CREATE)
    database.commit()

    return lccdb



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
      - this must contain lcc_* metadata for the collection, so we can give it
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

        # read the lcformat-description.json file to get the reader function
        lcformat_dict = get_lcformat_description(lcformat_desc_path)
        readerfunc = lcformat_dict['readerfunc']
        normmodule = lcformat_dict['normmodule']
        normfunc = lcformat_dict['normfunc']
        lcformat_fileglob = lcformat_dict['fileglob']
        lcformat_key = lcformat_dict['formatkey']

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
            normfunc(lcdict)
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
    #    - get the magcols from the metadata
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

        # check if the lcc_ispublic
        # if so, set collection_owner to user_id = 1 (the admin user)
        # and set the collection_visibility to 2 (public)
        # collections can only be owned by the admin user

        # prepare the query items
        items = (
            collection_id,
            lcformat_key,
            lcformat_desc_path,
            ','.join(metadata['magcols']),
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
            metadata['lcc_name'],
            metadata['lcc_desc'],
            metadata['lcc_project'],
            metadata['lcc_citation'],
            metadata['lcc_datarelease'],
            last_updated,
            metadata['lcc_owner'],
            metadata['lcc_visibility'],
            metadata['lcc_sharedwith']
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
