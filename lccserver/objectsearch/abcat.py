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
        'description':'absolute GAIA magnitude',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.beyond1std':{
        'title':'f<sub>&gt; 1.0-&sigma;</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.3f',
        'description':('fraction of measurements beyond 1-stdev '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.comments':{
        'title':'comments ({magcol})',
        'dtype':'U600',
        'format':'%s',
        'description':("comments on the object and its time-series "
                       "for mag col: {magcol}"),
        'index':False,
        'ftsindex':True,
    },
    '{magcol}.eta_normal':{
        'title':'&eta; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('eta variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.kurtosis':{
        'title':'kurtosis ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'kurtosis of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.linear_fit_slope':{
        'title':'m<sub>linfit</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('slope of a linear fit to the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.mad':{
        'title':'MAD ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('median absolute deviation of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.mag_iqr':{
        'title':'IQR ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('interquartile range of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.magnitude_ratio':{
        'title':'&Delta; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('(max mag - med mag)/(max mag - min mag) '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.median':{
        'title':'median ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'median of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.ndet':{
        'title':'nobs ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':('number of non-nan time-series observations '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.objectisvar':{
        'title':'variability flag ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':("for mag col: {magcol}, 0 = unreviewed, 1 = variable, "
                       "2 = not variable, 3 = can't tell"),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.skew':{
        'title':'skew ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'skew of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stdev':{
        'title':'&sigma; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('standard deviation of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stetsonj':{
        'title':'J<sub>stetson</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson J variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stetsonk':{
        'title':'K<sub>stetson</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson K variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.varepoch':{
        'title':'epoch ({magcol}) [JD]',
        'dtype':'f8',
        'format':'%.6f',
        'description':('for mag col: {magcol}, JD epoch of minimum '
                       'light if periodic variable'),
        'index':False,
        'ftsindex':False,
    },
    '{magcol}.varisperiodic':{
        'title':'periodicity flag ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':('for mag col: {magcol}, 0 = undetermined, 1 = periodic, '
                       '2 = not periodic, 3 = quasi-periodic'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.varperiod':{
        'title':'period ({magcol}) [days]',
        'dtype':'f8',
        'format':'%.6f',
        'description':'for mag col: {magcol}, period of variability in days',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.vartags':{
        'title':'variable tags ({magcol})',
        'dtype':'U600',
        'format':'%s',
        'description':'for mag col: {magcol}, variability tags for this object',
        'index':False,
        'ftsindex':True,
    },
    'bmag':{
        'title':'<em>B</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog magnitude in B filter',
        'index':True,
        'ftsindex':False,
    },
    'color_classes':{
        'title':'color classes',
        'dtype':'U600',
        'format':'%s',
        'description':'stellar classification using SDSS/SEGUE color cuts',
        'index':False,
        'ftsindex':True,
    },
    'decl':{
        'title':'&delta;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'declination of the object [J2000 decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_bmag':{
        'title':'A<sub>B</sub> [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in B band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_hmag':{
        'title':'A<sub>H</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS H band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_imag':{
        'title':'A<sub>I</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in I band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_jmag':{
        'title':'A<sub>J</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS J band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_kmag':{
        'title':'A<sub>Ks</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS Ks band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_rmag':{
        'title':'A<sub>R</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in R band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssg':{
        'title':'A<sub>g</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS g band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssi':{
        'title':'A<sub>i</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS i band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssr':{
        'title':'A<sub>r</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS r band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssu':{
        'title':'A<sub>u</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS u band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssz':{
        'title':'A<sub>z</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS z band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_vmag':{
        'title':'A<sub>V</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in V band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'gaia_id':{
        'title':'GAIA DR2 ID',
        'dtype':'U40',
        'format':'%s',
        'description':'cross-matched GAIA DR2 source ID of the object',
        'index':True,
        'ftsindex':False,
    },
    'gaia_parallax':{
        'title':'&omega; [mas]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'GAIA DR2 parallax of the object in milliarcsec',
        'index':True,
        'ftsindex':False,
    },
    'gaia_parallax_err':{
        'title':'&sigma;<sub>&omega;</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('error in the GAIA DR2 parallax of '
                       'the object in milliarcsec'),
        'index':True,
        'ftsindex':False,
    },
    'gaia_status':{
        'title':'GAIA status',
        'dtype':'U50',
        'format':'%s',
        'description':'GAIA cross-match status',
        'index':False,
        'ftsindex':False,
    },
    'gaiamag':{
        'title':'G [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'GAIA DR2 magnitude',
        'index':True,
        'ftsindex':False,
    },
    'gb':{
        'title':'b',
        'dtype':'f8',
        'format':'%.5f',
        'description':'galactic latitude [decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'gl':{
        'title':'l',
        'dtype':'f8',
        'format':'%.5f',
        'description':'galactic longitude [decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'hmag':{
        'title':'<em>H</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS H band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'imag':{
        'title':'<em>I</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog I band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'jmag':{
        'title':'<em>J</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS J band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'kmag':{
        'title':'<em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS Ks band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'lcfname':{
        'title':'LC filename',
        'dtype':'U600',
        'format':'%s',
        'description':'light curve filename',
        'index':False,
        'ftsindex':False,
    },
    'ndet':{
        'title':'M<sub>G</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'absolute GAIA magnitude',
        'index':True,
        'ftsindex':False,
    },
    'objectid':{
        'title':'object ID',
        'dtype':'U600',
        'format':'%s',
        'description':'object ID',
        'index':False,
        'ftsindex':True,
    },
    'objecttags':{
        'title':'object tags',
        'dtype':'U600',
        'format':'%s',
        'description':'object type tags',
        'index':False,
        'ftsindex':True,
    },
    'pmdecl':{
        'title':'pmDEC [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in declination [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'pmra':{
        'title':'pmRA [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in right ascension [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'propermotion':{
        'title':'PM [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'total proper motion [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'ra':{
        'title':'&alpha;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'right ascension of the object [J2000 decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'rmag':{
        'title':'<em>R</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog R band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'rpmj':{
        'title':'RPM<sub>J</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':'reduced proper motion of the object using 2MASS J mag',
        'index':True,
        'ftsindex':False,
    },
    'sdssg':{
        'title':'<em>g</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS g band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssi':{
        'title':'<em>i</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS i band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssr':{
        'title':'<em>r</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS r band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssu':{
        'title':'<em>u</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS u band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssz':{
        'title':'<em>z</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS z band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'simbad_best_mainid':{
        'title':'external object ID',
        'dtype':'U50',
        'format':'%s',
        'description':'main SIMBAD ID for this object',
        'index':True,
        'ftsindex':False,
    },
    'simbad_best_objtype':{
        'title':'type flag',
        'dtype':'U20',
        'format':'%s',
        'description':('<a href="http://simbad.u-strasbg.fr/guide/chF.htx">'
                       'SIMBAD object type flag for this object</a>'),
        'index':False,
        'ftsindex':True,
    },
    'simbad_best_allids':{
        'title':'other IDs',
        'dtype':'U600',
        'format':'%s',
        'description':'other object IDs from SIMBAD',
        'index':False,
        'ftsindex':True,
    },
    'simbad_best_distarcsec':{
        'title':'SIMBAD dist',
        'dtype':'f8',
        'format':'%.5f',
        'description':'distance in arcseconds from closest SIMBAD cross-match',
        'index':True,
        'ftsindex':False,
    },
    'twomassid':{
        'title':'2MASS ID',
        'dtype':'U60',
        'format':'%s',
        'description':'2MASS ID for this object',
        'index':True,
        'ftsindex':False,
    },
    'vmag':{
        'title':'<em>V</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog V band magnitude',
        'index':True,
        'ftsindex':False,
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


    # turn the column info into a JSON
    columninfo_json = json.dumps(defaultcolinfo)

    # add some metadata to allow reading the LCs correctly later

    m_indexcols = indexcols if indexcols is not None else []
    m_ftsindexcols = ftsindexcols if ftsindexcols is not None else []


    metadata = {'basedir':augcat['basedir'],
                'lcformat':augcat['lcformat'],
                'fileglob':augcat['fileglob'],
                'nobjects':augcat['nfiles'],
                'catalogcols':colnames,
                'indexcols':[x.replace('.','_') for x in m_indexcols],
                'ftsindexcols':[x.replace('.','_') for x in m_ftsindexcols]}
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
## COLLECTING METADATA ABOUT LC COLLECTIONS ##
##############################################

def collect_lcc_info(lcc_basedir, outfile):
    '''This writes or updates the lcc-collections.sqlite file in lcc_basedir.

    each LC collection is identified by its subdirectory name. The following
    files must be present in each LC collection subdirectory:

    - lclist-catalog.pkl
    - catalog-kdtree.pkl
    - catalog-objectinfo.sqlite
      - this must contain lcset_* metadata for the collection, so we can give it
        a name, description, project name, last time of update, datarelease
        number

    Each LC collection must have the following subdirectories:

    input:
    - lightcurves/ -> the LCs in whatever format
    - periodfinding/ -> the periodfinding result pickles
    - checkplots/ -> the checkplot pickles

    output:
    - datasets/ -> the datasets generated from searches
    - products/ -> the lightcurves.zip and dataset.zip for each dataset

    lcc-collections.sqlite -> contains for each LC collection:

                       - name, description, project name, date of last update,
                         number of objects, footprint in RA/DEC, footprint in
                         gl/gb, datareleae number, and an ispublic flag

                       - basedir paths for each LC set to get to its catalog
                         sqlite, kdtree, and datasets
                       - columns, indexcols, ftscols for each dataset
                       - sets of columns, indexcols and ftscols for all LC sets

    '''
