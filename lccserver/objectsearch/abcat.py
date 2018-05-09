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

import numpy as np
from scipy.spatial import cKDTree

from tqdm import tqdm

from astrobase import lcdb


#########################################
## DEFAULT COLUMNS THAT ARE RECOGNIZED ##
#########################################

COLUMN_INFO = {
    'abs_gaiamag':{
        'title':'M<sub>G</sub> [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'absolute GAIA magnitude'
    },
    '{magcol}.beyond1std':{
        'title':'f<sub>&gt; 1.0-&sigma;</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':('fraction of measurements beyond 1-stdev '
                       'for mag col: {magcol}')
    },
    '{magcol}.comments':{
        'title':'comments',
        'dtype':'U600',
        'format':'%s',
        'description':("comments on the object and its time-series "
                       "for mag col: {magcol}")
    },
    '{magcol}.eta_normal':{
        'title':'&eta;',
        'dtype':'f8',
        'format':'%.5f',
        'description':('eta variability index of the '
                       'time-series for mag col: {magcol}')
    },
    '{magcol}.kurtosis':{
        'title':'kurtosis',
        'dtype':'f8',
        'format':'%.5f',
        'description':'kurtosis of the time-series for mag col: {magcol}'
    },
    '{magcol}.linear_fit_slope':{
        'title':'m<sub>linfit</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('slope of a linear fit to the time-series '
                       'for mag col: {magcol}')
    },
    '{magcol}.mad':{
        'title':'MAD',
        'dtype':'f8',
        'format':'%.5f',
        'description':('median absolute deviation of the time-series '
                       'for mag col: {magcol}')
    },
    '{magcol}.mag_iqr':{
        'title':'IQR',
        'dtype':'f8',
        'format':'%.5f',
        'description':('interquartile range of the time-series '
                       'for mag col: {magcol}')
    },
    '{magcol}.magnitude_ratio':{
        'title':'&Delta;',
        'dtype':'f8',
        'format':'%.5f',
        'description':('(max mag - med mag)/(max mag - min mag) '
                       'for mag col: {magcol}')
    },
    '{magcol}.median':{
        'title':'median',
        'dtype':'f8',
        'format':'%.5f',
        'description':'median of the time-series for mag col: {magcol}'
    },
    '{magcol}.objectisvar':{
        'title':'variability flag',
        'dtype':'i8',
        'format':'%i',
        'description':("for mag col: {magcol}, 0 = unreviewed, 1 = variable, "
                       "2 = not variable, 3 = can't tell")
    },
    '{magcol}.skew':{
        'title':'skew',
        'dtype':'f8',
        'format':'%.5f',
        'description':'skew of the time-series for mag col: {magcol}'
    },
    '{magcol}.stdev':{
        'title':'&sigma;',
        'dtype':'f8',
        'format':'%.5f',
        'description':('standard deviation of the time-series '
                       'for mag col: {magcol}'),
    },
    '{magcol}.stetsonj':{
        'title':'J<sub>stetson</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson J variability index of the '
                       'time-series for mag col: {magcol}')
    },
    '{magcol}.stetsonk':{
        'title':'K<sub>stetson</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson K variability index of the '
                       'time-series for mag col: {magcol}')
    },
    '{magcol}.varepoch':{
        'title':'epoch [JD]',
        'dtype':'f8',
        'format':'%.6f',
        'description':('for mag col: {magcol}, JD epoch of minimum '
                       ' light if periodic variable')
    },
    '{magcol}.varisperiodic':{
        'title':'periodicity flag',
        'dtype':'i8',
        'format':'%i',
        'description':('for mag col: {magcol}, 0 = undetermined, 1 = periodic, '
                       '2 = not periodic, 3 = quasi-periodic')
    },
    '{magcol}.varperiod':{
        'title':'period [days]',
        'dtype':'f8',
        'format':'%.6f',
        'description':'for mag col: {magcol}, period of variability in days'
    },
    '{magcol}.vartags':{
        'title':'variable tags',
        'dtype':'U600',
        'format':'%s',
        'description':'for mag col: {magcol}, variability tags for this object'
    },
    'bmag':{
        'title':'<em>B</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog magnitude in B filter'
    },
    'color_classes':{
        'title':'color classes',
        'dtype':'U600',
        'format':'%s',
        'description':'stellar classification using SEGUE color cuts in ugriz'
    },
    'decl':{
        'title':'&delta;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'declination of the object [J2000 decimal degrees]'
    },
    'extinction_bmag':{
        'title':'A<sub>B</sub> [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in B band [mag]'
    },
    'extinction_hmag':{
        'title':'A<sub>H</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS H band [mag]'
    },
    'extinction_imag':{
        'title':'A<sub>I</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in I band [mag]'
    },
    'extinction_jmag':{
        'title':'A<sub>J</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS J band [mag]'
    },
    'extinction_kmag':{
        'title':'A<sub>Ks</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS Ks band [mag]'
    },
    'extinction_rmag':{
        'title':'A<sub>R</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in R band [mag]'
    },
    'extinction_sdssg':{
        'title':'A<sub>g</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS g band [mag]'
    },
    'extinction_sdssi':{
        'title':'A<sub>i</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS i band [mag]'
    },
    'extinction_sdssr':{
        'title':'A<sub>r</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS r band [mag]'
    },
    'extinction_sdssu':{
        'title':'A<sub>u</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS u band [mag]'
    },
    'extinction_sdssz':{
        'title':'A<sub>z</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS z band [mag]'
    },
    'extinction_vmag':{
        'title':'A<sub>V</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in V band [mag]'
    },
    'gaia_id':{
        'title':'GAIA DR2 ID',
        'dtype':'U40',
        'format':'%s',
        'description':'cross-matched GAIA DR2 source ID of the object'
    },
    'gaia_parallax':{
        'title':'&omega; [mas]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'GAIA DR2 parallax of the object in milliarcsec'
    },
    'gaia_parallax_err':{
        'title':'&sigma;<sub>&omega;</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('error in the GAIA DR2 parallax of '
                       'the object in milliarcsec')
    },
    'gaia_status':{
        'title':'GAIA status',
        'dtype':'U50',
        'format':'%s',
        'description':'GAIA cross-match status'
    },
    'gaiamag':{
        'title':'G [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'GAIA DR2 magnitude'
    },
    'gb':{
        'title':'M<sub>G</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'absolute GAIA magnitude'
    },
    'gl':{
        'title':'l',
        'dtype':'f8',
        'format':'%.5f',
        'description':'galactic longitude [decimal degrees]'
    },
    'hmag':{
        'title':'<em>H</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS H band magnitude'
    },
    'imag':{
        'title':'<em>I</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog I band magnitude'
    },
    'jmag':{
        'title':'<em>J</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS J band magnitude'
    },
    'kmag':{
        'title':'<em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS Ks band magnitude'
    },
    'lcfname':{
        'title':'LC filename',
        'dtype':'U600',
        'format':'%s',
        'description':'light curve filename'
    },
    'ndet':{
        'title':'M<sub>G</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'absolute GAIA magnitude'
    },
    '{magcol}.ndet':{
        'title':'ndet ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':('number of non-nan time-series observations '
                       'for mag col: {magcol}')
    },
    'objectid':{
        'title':'object ID',
        'dtype':'U600',
        'format':'%s',
        'description':'object ID'
    },
    'objecttags':{
        'title':'object tags',
        'dtype':'U600',
        'format':'%s',
        'description':'object type tags'
    },
    'pmdecl':{
        'title':'pmDEC [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in declination [mas/yr]'
    },
    'pmra':{
        'title':'pmRA [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in right ascension [mas/yr]'
    },
    'propermotion':{
        'title':'PM [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'total proper motion [mas/yr]'
    },
    'ra':{
        'title':'&alpha;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'right ascension of the object [J2000 decimal degrees]'
    },
    'rmag':{
        'title':'<em>R</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog R band magnitude'
    },
    'rpmj':{
        'title':'RPM<sub>J</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':'reduced proper motion of the object using 2MASS J mag'
    },
    'sdssg':{
        'title':'<em>g</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS g band magnitude'
    },
    'sdssi':{
        'title':'<em>i</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS i band magnitude'
    },
    'sdssr':{
        'title':'<em>r</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS r band magnitude'
    },
    'sdssu':{
        'title':'<em>u</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS u band magnitude'
    },
    'sdssz':{
        'title':'<em>z</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS z band magnitude'
    },
    'simbad_best_mainid':{
        'title':'external object ID',
        'dtype':'U50',
        'format':'%s',
        'description':'main SIMBAD ID for this object'
    },
    'simbad_best_objtype':{
        'title':'type flag',
        'dtype':'U20',
        'format':'%s',
        'description':('<a href="http://simbad.u-strasbg.fr/guide/chF.htx">'
                       'SIMBAD object type flag for this object</a>')
    },
    'simbad_best_allids':{
        'title':'other IDs',
        'dtype':'U600',
        'format':'%s',
        'description':'other object IDs from SIMBAD'
    },
    'simbad_best_distarcsec':{
        'title':'SIMBAD dist',
        'dtype':'f8',
        'format':'%.5f',
        'description':'distance in arcseconds from closest SIMBAD cross-match'
    },
    'twomassid':{
        'title':'2MASS ID',
        'dtype':'U60',
        'format':'%s',
        'description':'2MASS ID for this object'
    },
    'vmag':{
        'title':'<em>V</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog V band magnitude'
    },
}


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
                         colinfo=None,
                         indexcols=None,
                         ftsindexcols=None):

    '''This writes the object information to an SQLite file.

    makes indexes for fast look up by objectid by default and any columns
    included in indexcols. also makes a full-text search index for any columns
    in ftsindexcols.

    If colinfo is not None, it should be either a dict or JSON with elements
    that are of the form:

    'column_name':{'title':'column title',
                   'description':'a long description of the column',
                   'dtype':numpy dtype of the column,
                   'format':string format specifier for this column},

    where column_name should be each column in the augcatpkl file. Any column
    that doesn't have a key in colinfo, it won't have any extra information
    associated with it.

    NOTE: This requires FTS5 to be available in SQLite because we don't want to
    mess with ranking algorithms to be implemented for FTS4.

    '''

    with open(augcatpkl, 'rb') as infd:
        augcat = pickle.load(infd)

    # pull the columns out of the augcat
    cols = list(augcat['objects'].keys())

    # get the dtypes for each column to generate the create statement
    coldefs = []
    colnames = []

    LOGINFO('collecting column information from %s' % augcatpkl)


    defaultcolinfo = {}


    for col in cols:

        thiscol_name = col.replace('.','_')
        thiscol_dtype = augcat['objects'][col].dtype
        colnames.append(thiscol_name)

        defaultcolinfo[thiscol_name] = {'title':None,
                                        'description':None,
                                        'dtype':None,
                                        'format':None}

        # strings
        if thiscol_dtype.type is np.str_:

            coldefs.append(('%s text' % thiscol_name, str))
            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str
            defaultcolinfo[thiscol_name]['format'] = '%s'

        # floats
        elif thiscol_dtype.type is np.float64:

            coldefs.append(('%s double precision' % thiscol_name, float))
            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str
            defaultcolinfo[thiscol_name]['format'] = '%.7f'

        # integers
        elif thiscol_dtype.type is np.int64:

            coldefs.append(('%s integer' % thiscol_name, int))
            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str
            defaultcolinfo[thiscol_name]['format'] = '%i'

        # everything is coerced into a string
        else:

            coldefs.append(('%s text' % thiscol_name, str))
            defaultcolinfo[thiscol_name]['dtype'] = thiscol_dtype.str
            defaultcolinfo[thiscol_name]['format'] = '%s'

    # now, we'll generate the create statement

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

        # FIXME: add the FTS trigger statements here for an update to the main
        # object_catalog table. see the astro-coffee implementation for hints


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
            LOGERROR('could not understand colinfo argument')
            overridecolinfo = None

    else:
        overridecolinfo = None

    if overridecolinfo:

        for col in defaultcolinfo:

            if col in overridecolinfo:

                if overridecolinfo[col]['title'] is not None:
                    defaultcolinfo[col]['title'] = overridecolinfo[col]['name']

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

    # turn the column info into a JSON
    columninfo_json = json.dumps(defaultcolinfo)

    # add some metadata to allow reading the LCs correctly later
    metadata = {'basedir':augcat['basedir'],
                'lcformat':augcat['lcformat'],
                'fileglob':augcat['fileglob'],
                'nobjects':augcat['nfiles'],
                'catalogcols':colnames,
                'indexcols':[x.replace('.','_') for x in indexcols],
                'ftsindexcols':[x.replace('.','_') for x in ftsindexcols]}
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
