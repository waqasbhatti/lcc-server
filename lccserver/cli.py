#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''cli.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains the CLI implementation for the LCC server.

lcc-server [options] <command>

where command is one of:

init-basedir        -> initializes the basedir for the LCC server.
                       this is an interactive command.

add-collection      -> adds an LC collection to the LCC server.
                       this is an interactive command.

del-collection      -> removes an LC collection from the LCC server
                       this will ask you for confirmation.

run-server          -> runs an instance of the LCC server to provide
                       the main search interface and a backing instance
                       of the checkplotserver for serving objectinfo

and options are:

--basedir           -> the base directory to execute all commands from
                       this is the directory where all of the LCC server's
                       files and directories will be created and where it will
                       run from when executed.

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
import shutil
import json
import glob
import sqlite3
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor
import asyncio
from functools import partial
import pickle

import multiprocessing as mp
NCPUS = mp.cpu_count()

import numpy as np
from tornado.escape import squeeze

#######################
## PREPARING BASEDIR ##
#######################

def prepare_basedir(basedir,
                    site_project="Example Project",
                    site_project_link="https://example.org/astro/project",
                    site_department="Department of Astronomy",
                    site_department_link="https://example.org/astro",
                    site_institution="Example University",
                    site_institution_link="https://example.org",
                    site_institution_logo=None):
    '''
    This prepares the given base directory for use with LCC server.

    We need the following structure (e.g. for two LC collections):
    .
    ├── csvlcs
    │   ├── collection-id-1 -> ../collection-id-1/lightcurves
    │   └── collection-id-2 -> ../collection-id-2/lightcurves
    ├── datasets
    ├── docs
    │   └── static
    ├── lccjsons
    │   ├── collection-id-1/lcformat-description.json
    │   └── collection-id-2/lcformat-description.json
    ├── products
    ├── collection-id-1
    │   ├── checkplots -> canonical-directory-for-checkplots-collection-id-1
    │   ├── lightcurves -> canonical-directory-for-lightcurves-collection-id-1
    │   └── periodfinding -> canonical-directory-for-pfresults-collection-id-1
    └── collection-id-2
        ├── checkplots -> canonical-directory-for-checkplots-collection-id-2
        ├── lightcurves -> canonical-directory-for-lightcurves-collection-id-2
        └── periodfinding -> canonical-directory-for-pfresults-collection-id-2

    We need the following files in the basedir:

    site-info.json: contains key:val pairs from siteinfo_* kwargs.

    We need the following files in basedir/docs:

    doc-index.json
    citation.md
    lcformat.md

    '''

    # make the basedir if it doesn't exist
    if not os.path.exists(basedir):
        os.mkdir(basedir)

    #
    # make the subdirectories
    #

    # csvlcs directory - symlinks to individual collection LC dirs
    if not os.path.exists(os.path.join(basedir,'csvlcs')):
        os.mkdir(os.path.join(basedir,'csvlcs'))

    # datasets directory - data CSVs, JSONs, and pickles go here
    if not os.path.exists(os.path.join(basedir,'datasets')):
        os.mkdir(os.path.join(basedir,'datasets'))

    # docs directory - docs .md files plus a static subdir for images, etc.
    if not os.path.exists(os.path.join(basedir,'docs')):
        os.makedirs(os.path.join(basedir,'docs','static'))

    # lccjsons directory - symlinks to lcformat-description.json per collection
    if not os.path.exists(os.path.join(basedir,'lccjsons')):
        os.mkdir(os.path.join(basedir,'lccjsons'))

    # products directory - LC zips go here
    if not os.path.exists(os.path.join(basedir,'products')):
        os.mkdir(os.path.join(basedir,'products'))

    LOGINFO('created LCC server sub-directories under basedir: %s' % basedir)

    #
    # generate the site-info.json file in the basedir
    #
    if os.path.exists(os.path.join(basedir,'site-info.json')):

        LOGERROR('site-info.json already exists in basedir: %s, skipping...' %
                 basedir)

    else:

        siteinfo = {"project":site_project,
                    "project_link":site_project_link,
                    "department":site_department,
                    "department_link":site_department_link,
                    "institution":site_institution,
                    "institution_link":site_institution_link}

        # check if the site institution logo file is not None and exists
        # if it does, copy it over to the basedir/docs/static directory
        if site_institution_logo and os.path.exists(site_institution_logo):

            shutil.copy(site_institution_logo,
                        os.path.join(basedir,'docs','static'))
            siteinfo['institution_logo'] = (
                '/doc-static/%s' % os.path.basename(site_institution_logo)
            )

        # if it doesn't exist, we'll just use the name of the institution as the
        # link target
        else:

            siteinfo["institution_logo"] = None


        # write site-json to the basedir
        with open(os.path.join(basedir,'site-info.json'),'w') as outfd:
            json.dump(siteinfo, outfd, indent=2)

        LOGINFO('created site-info.json: %s' %
                os.path.join(basedir,'site-info.json'))

    #
    # generate doc-index.json and barebones citation.md and lcformat.md
    #
    if os.path.exists(os.path.join(basedir,'docs','doc-index.json')):

        LOGERROR('doc-index.json already exists in %s/%s, skipping...' %
                 (basedir, 'docs'))

    else:

        docindex = {"citation": "Citing the data available on this server",
                    "lcformat": "Light curve columns and metadata description"}

        with open(os.path.join(basedir,'docs','doc-index.json'),'w') as outfd:
            json.dump(docindex, outfd, indent=2)

        with open(os.path.join(basedir,'docs','citation.md'),'w') as outfd:
            outfd.write(
                "Citation instructions for your project's data go here.\n"
            )

        with open(os.path.join(basedir,'docs','lcformat.md'),'w') as outfd:
            outfd.write("Light curve column descriptions for "
                        "your project's data go here.\n")

        LOGINFO('created doc-index.json: %s and '
                'barebones %s/docs/citation.md, %s/docs/lcformat.md' %
                (os.path.join(basedir,'docs','doc-index.json'),
                 basedir,
                 basedir))



###################################
## PREPPING FOR AN LC COLLECTION ##
###################################

def new_collection_directories(basedir,
                               collection_id,
                               lightcurve_dir=None,
                               checkplot_dir=None,
                               pfresult_dir=None):
    '''This just adds a new collection's subdirs to the basedir.

    Links the lightcurves subdir to basedir/csvlcs/collection_id

    Also generates a stub lcformat-description.json in the collection subdir and
    links it to basedir/lccjsons/collection_id/lcformat-description.json.

    '''

    if os.path.exists(os.path.join(basedir, collection_id)):

        LOGERROR('directory: %s for '
                 'collection: %s already exists, not touching it' %
                 (os.path.join(basedir,collection_id), collection_id))
        return None

    else:

        os.mkdir(os.path.join(basedir,collection_id))

        #
        # 1. make the checkplots subdir
        #
        if checkplot_dir and os.path.exists(checkplot_dir):

            os.symlink(os.path.abspath(checkplot_dir),
                       os.path.join(basedir,collection_id, 'checkplots'))
            LOGINFO('linked provided checkplot '
                    'directory: %s for collection: %s to %s' %
                    (os.path.abspath(checkplot_dir),
                     collection_id,
                     os.path.join(basedir,collection_id, 'checkplots')))

        else:
            LOGWARNING('no existing checkplot '
                       'directory for collection: %s, making a new one at: %s' %
                       (collection_id,
                        os.path.join(basedir,collection_id, 'checkplots')))
            os.mkdir(os.path.join(basedir,collection_id, 'checkplots'))

        #
        # 2. make the pfresults subdir
        #
        if pfresult_dir and os.path.exists(pfresult_dir):

            os.symlink(os.path.abspath(pfresult_dir),
                       os.path.join(basedir,collection_id, 'periodfinding'))
            LOGINFO('linked provided pfresult '
                    'directory: %s for collection: %s to %s' %
                    (os.path.abspath(pfresult_dir),
                     collection_id,
                     os.path.join(basedir,collection_id, 'periodfinding')))

        else:
            LOGWARNING('no existing pfresult '
                       'directory for collection: %s, making a new one at: %s' %
                       (collection_id,
                        os.path.join(basedir,collection_id, 'periodfinding')))
            os.mkdir(os.path.join(basedir,collection_id, 'periodfinding'))


        #
        # 3. make the light curve subdir
        #
        if lightcurve_dir and os.path.exists(lightcurve_dir):

            os.symlink(os.path.abspath(lightcurve_dir),
                       os.path.join(basedir,collection_id, 'lightcurves'))
            LOGINFO('linked provided lightcurve '
                    'directory: %s for collection: %s to %s' %
                    (os.path.abspath(lightcurve_dir),
                     collection_id,
                     os.path.join(basedir,collection_id, 'lightcurves')))

        else:
            LOGWARNING('no existing lightcurve '
                       'directory for collection: %s, making a new one at: %s' %
                       (collection_id,
                        os.path.join(basedir,collection_id, 'lightcurves')))
            os.mkdir(os.path.join(basedir,collection_id, 'lightcurves'))

        # symlink this light curve dir to basedir/csvlcs/collection_id
        os.symlink(os.path.abspath(os.path.join(basedir,
                                                collection_id,
                                                'lightcurves')),
                   os.path.join(basedir, 'csvlcs', collection_id))

        # generate a stub lcformat-description.json file in the
        # basedir/collection_id directory
        lcformat_json_stub = os.path.join(os.path.dirname(__file__),
                                          'backend',
                                          'lcformat-jsons',
                                          'lcformat-description.json')
        shutil.copy(lcformat_json_stub, os.path.join(basedir, collection_id))

        # symlink this to lccjsons/collection_id/lcformat-description.json
        os.makedirs(os.path.join(basedir,'lccjsons',collection_id))
        os.symlink(os.path.abspath(os.path.join(basedir,
                                                collection_id,
                                                'lcformat-description.json')),
                   os.path.join(basedir,
                                'lccjsons',
                                collection_id,
                                'lcformat-description.json'))

        # tell the user they need to fill this file in
        LOGINFO(
            'generated a stub lcformat-description.json file at: %s' %
            os.path.join(basedir, collection_id, 'lcformat-description.json')
        )
        LOGINFO('please fill this out using the instructions within so '
                'LCC server can read your original format light curves '
                'and convert them to common LCC CSVLC format')

        #
        # return the finished LC collection directory at the end
        #
        return os.path.join(basedir, collection_id)



def convert_original_lightcurves(basedir,
                                 collection_id,
                                 original_lcdir=None,
                                 convert_workers=NCPUS,
                                 csvlc_version=1,
                                 comment_char='#',
                                 column_separator=',',
                                 skip_converted=True):
    '''This converts original format light curves to the common LCC CSV format.

    This is optional since the LCC server can do this conversion on-the-fly if
    necessary, but this slows down LC zip operations if people request large
    numbers of light curves.

    If original_lcdir is not None, this will be used as the source of the light
    curves. If it is None, we'll assume that the lightcurves to convert are
    present in the basedir/collection_id/lightcurves directory already.

    The light curves will be read using the module and function specified in the
    basedir/collection_id/lcformat-description.json file. Next, they will be
    normalized using the module and function specified in the
    basedir/collection_id/lcformat-description.json file. Finally, the
    conversion will take place and put the output light curves into the
    basedir/lc_collection/lightcurves directory.

    convert_workers controls the number of parallel workers used to convert
    the light curves.

    csvlc_version, comment_char, column_separator, skip_converted control the
    CSV LC output and are passed directly to abcat.convert_to_csvlc.

    '''

    from .backend import abcat
    from .backend import datasets
    datasets.set_logger_parent(__name__)
    abcat.set_logger_parent(__name__)

    # make sure we have a filled out lcformat-description.json for this
    # collection
    lcformjson = os.path.join(basedir,
                              collection_id,
                              'lcformat-description.json')

    if not os.path.exists(lcformjson):
        LOGERROR("no lcformat-description.json "
                 "file found for collection: %s in dir: %s, "
                 "can't continue" %
                 (collection_id, os.path.join(basedir, collection_id)))
        return None

    else:

        try:

            lcformatdict = abcat.get_lcformat_description(lcformjson)

        except Exception as e:

            LOGEXCEPTION("lcformat-description.json "
                         "could not be loaded, can't continue")
            return None

        # now that we have the lcformatdict, we can start processing the light
        # curves

        if original_lcdir and os.path.exists(original_lcdir):
            input_lcdir = original_lcdir
        else:
            input_lcdir = os.path.join(basedir, collection_id, 'lightcurves')

        # list the light curves using the fileglob for this LC format
        input_lclist = glob.glob(
            os.path.join(input_lcdir, lcformatdict['fileglob'])
        )

        if len(input_lclist) == 0:

            LOGERROR("no light curves found for LC format: %s, "
                     "collection: %s in input LC dir: %s "
                     "(using glob: '%s') , can't continue" %
                     (lcformatdict['formatkey'],
                      collection_id,
                      input_lcdir,
                      lcformatdict['fileglob']))
            return None

        converter_options = {'csvlc_version':csvlc_version,
                             'comment_char':comment_char,
                             'column_separator':column_separator,
                             'skip_converted':skip_converted}

        tasks = [(x, y, lcformatdict, converter_options) for x, y in
                 zip(input_lclist, (None for x in input_lclist))]

        # do the conversion
        LOGINFO('converting light curves...')
        pool = mp.Pool(convert_workers)
        results = pool.map(datasets.csvlc_convert_worker, tasks)
        pool.close()
        pool.join()
        LOGINFO('LC conversion complete.')

        # if the original_lcdir != basedir/collection_id/lightcurves, then
        # symlink the output CSVs to that directory
        if (original_lcdir and
            os.path.abspath(original_lcdir) != os.path.abspath(basedir,
                                                               collection_id,
                                                               'lightcurves')):

            LOGINFO(
                'symlinking output light curves to '
                'collection lightcurves dir: %s...' %
                os.path.join(basedir, collection_id, 'lightcurves')
            )

            for lc in results:
                os.symlink(os.path.abspath(lc),
                           os.path.join(basedir,
                                        collection_id,
                                        'lightcurves',
                                        os.path.basename(lc)))

            LOGINFO('symlinking complete.')

        return results



################################################
## GENERATING PER LC COLLECTION INFO CATALOGS ##
################################################

def generate_augmented_lclist_catalog(
        basedir,
        collection_id,
        lclist_pkl,
        magcol,
        checkplot_glob='checkplot-*.pkl*',
        nworkers=NCPUS,
        infokeys=[
            # key, dtype, first level, overwrite=T|append=F, None sub, nan sub
            ('comments',
             np.unicode_, False, True, '', ''),
            ('objectinfo.objecttags',
             np.unicode_, True, True, '', ''),
            ('objectinfo.twomassid',
             np.unicode_, True, True, '', ''),
            ('objectinfo.bmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.vmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.rmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.imag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.jmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.hmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.kmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.sdssu',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.sdssg',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.sdssr',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.sdssi',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.sdssz',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_bmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_vmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_rmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_imag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_jmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_hmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_kmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_sdssu',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_sdssg',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_sdssr',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_sdssi',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.dered_sdssz',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_bmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_vmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_rmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_imag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_jmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_hmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_kmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_sdssu',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_sdssg',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_sdssr',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_sdssi',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.extinction_sdssz',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.color_classes',
             np.unicode_, True, True, '', ''),
            ('objectinfo.pmra',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.pmdecl',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.propermotion',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.rpmj',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.gl',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.gb',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.gaia_status',
             np.unicode_, True, True, '', ''),
            ('objectinfo.gaia_ids.0',
             np.unicode_, True, True, '', ''),
            ('objectinfo.gaiamag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.gaia_parallax',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.gaia_parallax_err',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.abs_gaiamag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.simbad_best_mainid',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_objtype',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_allids',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_distarcsec',
             np.float_, True, True, np.nan, np.nan),
            ('varinfo.vartags',
             np.unicode_, False, True, '', ''),
            ('varinfo.varperiod',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.varepoch',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.varisperiodic',
             np.int_, False, True, 0, 0),
            ('varinfo.objectisvar',
             np.int_, False, True, 0, 0),
            ('varinfo.features.median',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.mad',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.stdev',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.mag_iqr',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.skew',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.kurtosis',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.stetsonj',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.stetsonk',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.eta_normal',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.linear_fit_slope',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.magnitude_ratio',
             np.float_, False, True, np.nan, np.nan),
            ('varinfo.features.beyond1std',
             np.float_, False, True, np.nan, np.nan)
        ]
):
    '''This generates a lclist-catalog.pkl file containing extra info from
    checkplots.

    basedir is the base directory for the LCC server

    collection_id is the directory name of the collection you want to process

    lclist_pkl is the path to the original list catalog pickle created for the
    light curves in the collection using astrobase.lcproc.make_lclist

    magcol is the LC magnitude column being used in the checkplots' feature
    keys. This will be added as a prefix to the infokeys.

    nworkers sets the number of parallel workers to use when gathering info from
    the checkplots.

    infokeys is a list of key specs to extract from each checkplot. the provided
    list is a good default and should contain the most useful keys from
    checkplots generated using astrobase.checkplot.checkplot_pickle or
    astrobase.lcproc.runcp.

    key specs are tuples of the form:

    (key, dtype, first level, overwrite=T|append=F, None sub, nan sub)

    where:

    key -> the name of dict key to extract from the checkplot dict. this can be
           a single key, e.g. 'comments' to extract checkplot_dict['comments'],
           or a multiple level key, e.g. 'varinfo.features.stetsonj' to extract
           checkplot_dict['varinfo']['features']['stetsonj']. For multi-level
           keys, the last item will be used as the canonical name of the key
           when writing it to output catalog_pickle_dict['objects'].

    dtype -> the numpy dtype for the value of the key

    first level -> True means the key is not associated with the provided
                   magcol. if this is False, the output key string in the
                   catalog_pickle_dict['objects'] will be 'magcol.key'. This
                   allows you to run this function multiple times with different
                   magcols and add in info that may differ between them,
                   e.g. adding in aep_000.varinfo.varperiod and
                   atf_000.varinfo.varperiod for period-finding run on the
                   magcols aep_000 and atf_000 separately.

    overwrite_append -> currently unused. used to set if the key should
                        overwrite an existing key of the same name in the output
                        catalog_pickle_dict['objects']. This is currently True
                        for all keys.

    None sub -> what to use to substitute a None value in the key. We use this
                so we can turn the output catalog_pickle_dict into a numpy array
                of a single dtype so we can index it more efficiently.

    nan sub -> what to use to substitute a nan value in the key. We use this
               so we can turn the output catalog_pickle_dict into a numpy array
               of a single dtype so we can index it more efficiently.

    '''

    from astrobase import lcproc
    lcproc.set_logger_parent(__name__)

    # get the basedir/collection_id/checkplots directory
    cpdir = os.path.join(basedir, collection_id, 'checkplots')

    # check if there are any checkplots in there
    cplist = glob.glob(os.path.join(cpdir, checkplot_glob))

    # use lcproc.add_cpinfo_to_lclist with LC format info to update the catalog
    # write to basedir/collection_id/lclist-catalog.pkl
    augcat = lcproc.add_cpinfo_to_lclist(
        cplist,
        lclist_pkl,
        magcol,
        os.path.join(basedir, collection_id, 'lclist-catalog.pkl'),
        infokeys=infokeys,
        nworkers=nworkers
    )

    return augcat



def generate_catalog_kdtree(basedir,
                            collection_id):
    '''This generates the kd-tree pickle for spatial searches.

    '''

    from .backend import abcat
    abcat.set_logger_parent(__name__)

    # get basedir/collection_id/lclist-catalog.pkl
    lclist_catalog_pickle = os.path.join(basedir,
                                         collection_id,
                                         'lclist-catalog.pkl')

    # pull out the kdtree and write to basedir/collection_id/catalog-kdtree.pkl
    return abcat.kdtree_from_lclist(lclist_catalog_pickle,
                                    os.path.join(basedir,
                                                 collection_id,
                                                 'catalog-kdtree.pkl'))


def generate_catalog_database(
        basedir,
        collection_id,
        collection_info={
            'name':'Example LC Collection',
            'desc':'This is an example light curve collection.',
            'project':'LCC-Server Example Project',
            'datarelease':1,
            'citation':'Your citation goes here (2018)',
            'ispublic':True
        },
        colinfo=None,
        indexcols=None,
        ftsindexcols=None
):
    '''This generates the objectinfo-catalog.sqlite database.

    basedir is the base directory of the LCC server

    collection_id is the directory name of the LC collection we're working on.

    collection_info is a dict that MUST have the keys listed in the
    example. replace these values with those appropriate for your LC collection.

    If colinfo is not None, it should be either a dict or JSON with elements
    that are of the form:

    'column_name':{
        'title':'column title',
        'dtype':numpy dtype of the column,
        'format':string format specifier for this column,
        'description':'a long description of the column',
        'index': True if this should be indexed, False otherwise,
        'ftsindex': True if this should be full-text-search indexed, or False
    }

    where column_name should be each column in the augcatpkl file. Any column
    that doesn't have a key in colinfo won't have any extra information
    associated with it.

    If colinfo is not provided (i.e. is None by default), this function will use
    the column definitions provided in lccserver.abcat.COLUMN_INFO and
    lccserver.abcat.COMPOSITE_COLUMN_INFO. These are fairly extensive and should
    cover all of the data that the upstream astrobase tools can generate for
    object information in checkplots.

    indexcols and ftsindexcols are custom lists of columns to index in the
    output SQLite database. Normally, these aren't needed, because you'll
    specify all of the info in the colinfo kwarg.

    '''
    from .backend import abcat
    abcat.set_logger_parent(__name__)

    # get basedir/collection_id/lclist-catalog.pkl
    lclist_catalog_pickle = os.path.join(basedir,
                                         collection_id,
                                         'lclist-catalog.pkl')

    # write the catalog-objectinfo.sqlite DB to basedir/collection_id
    return abcat.objectinfo_to_sqlite(
        lclist_catalog_pickle,
        os.path.join(basedir, collection_id, 'catalog-objectinfo.sqlite'),
        lcset_name=collection_info['name'],
        lcset_desc=collection_info['desc'],
        lcset_project=collection_info['project'],
        lcset_datarelease=collection_info['datarelease'],
        lcset_citation=collection_info['citation'],
        lcset_ispublic=collection_info['ispublic'],
        colinfo=colinfo,
        indexcols=indexcols,
        ftsindexcols=ftsindexcols
    )



##########################################
## GENERATING LCC SERVER ROOT DATABASES ##
##########################################

def new_lcc_index_db(basedir):
    '''
    This generates an lcc-index DB in the basedir.

    '''

    from .backend import abcat
    abcat.set_logger_parent(__name__)

    return abcat.sqlite_make_lcc_index_db(basedir)



def new_lcc_datasets_db(basedir):
    '''
    This generates an lcc-datasets DB in the basedir.

    '''
    from .backend import datasets
    datasets.set_logger_parent(__name__)

    from .backend import abcat
    abcat.set_logger_parent(__name__)

    return datasets.sqlite_datasets_db_create(basedir)



def add_collection_to_lcc_index(basedir,
                                collection_id,
                                raiseonfail=False):
    '''
    This adds an LC collection to the index DB.

    basedir is the base directory of the LCC server

    collection_id is the directory name of the LC collection we're working on.

    '''

    from .backend import abcat
    abcat.set_logger_parent(__name__)

    return abcat.sqlite_collect_lcc_info(basedir,
                                         collection_id,
                                         raiseonfail=raiseonfail)



def remove_collection_from_lcc_index(basedir,
                                     collection_id,
                                     remove_files=False):
    '''
    This removes an LC collection from the index DB.

    Optionally removes the files as well.

    '''

    # find the root DB
    lccdb = os.path.join(basedir, 'lcc-index.sqlite')

    database = sqlite3.connect(
        lccdb,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cursor = database.cursor()

    query = 'delete from lcc_index where collection_id = ?'
    params = (collection_id,)

    cursor.execute(query, params)
    database.commit()

    query = 'select from lcc_index where collection_id = ?'
    cursor.execute(query, params)
    rows = cursor.fetchall()

    if len(rows) == 0:
        LOGINFO('deleted collection: %s from index DB: %s' % (collection_id,
                                                              lccdb))
    database.close()

    if remove_files:

        LOGWARNING('removing files for '
                   'collection: %s in basedir: %s' % (collection_id,
                                                      basedir))
        shutil.rmtree(os.path.abspath(os.path.join(basedir, collection_id)))
        LOGWARNING('removed directory tree: %s' %
                   (os.path.abspath(os.path.join(basedir, collection_id))))


########################################
## STARTING AN INSTANCE OF THE SERVER ##
########################################

async def start_lccserver(executor):
    '''This starts the LCC server and a backing instance of checkplotserver.

    This is NOT meant for production, but is useful to make sure everything
    works correctly or for local browsing of your light curve data.

    All of the log lines from the LCC server instance and the backing
    checkplotserver instance will go to STDOUT. STDERR will also be directed to
    STDOUT.

    Hit Ctrl+C to stop the server instances.

    This is a bit hacked together. As usual, D. Beazley's PyMOTW was
    super-helpful:

    https://pymotw.com/3/asyncio/executors.html

    FIXME: fix spam of errors when we hit Ctrl+C

    '''

    # launch the indexserver
    loop = asyncio.get_event_loop()
    tasks = []

    subprocess_call_indexserver = partial(
        subprocess.call,
        'indexserver',
        shell=True
    )
    subprocess_call_cpserver = partial(
        subprocess.call,
        ('checkplotserver '
         '--standalone=1 '
         '--sharedsecret=.lccserver.secret-cpserver'),
        shell=True
    )

    tasks.append(loop.run_in_executor(executor, subprocess_call_indexserver))
    tasks.append(loop.run_in_executor(executor, subprocess_call_cpserver))

    completed, pending = await asyncio.wait(tasks)
    results = [t.result() for t in completed]



##############
## CLI MAIN ##
##############

def main():
    '''
    This drives the CLI.

    '''

    import argparse
    import sys
    import readline

    aparser = argparse.ArgumentParser(
        description='LCC server CLI'
    )

    aparser.add_argument('command',
                         choices=['init-basedir',
                                  'new-collection',
                                  'del-collection',
                                  'run-server'],
                         action='store',
                         type=str,
                         help=('command to run'))

    aparser.add_argument('--basedir',
                         action='store',
                         type=str,
                         help=("the base directory to run commands in"),
                         default=os.getcwd())

    args = aparser.parse_args()

    if args.command == 'run-server':

        currdir = os.getcwd()

        if currdir != args.basedir:
            os.chdir(args.basedir)

        executor = ProcessPoolExecutor()
        event_loop = asyncio.get_event_loop()

        try:
            event_loop.run_until_complete(
                start_lccserver(executor)
            )
        finally:
            event_loop.close()

        # go back to the original directory
        os.chdir(currdir)

    elif args.command == 'init-basedir':

        site_project = input('Project name [default: Example Project]: ')
        if not site_project or len(site_project.strip()) == 0:
            site_project = "Example Project"

        site_project_link = input(
            'Project URL [default: https://example.org/astro/project]: '
        )
        if not site_project_link or len(site_project_link.strip()) == 0:
            site_project_link = "https://example.org/astro/project"

        site_department = input(
            'Department [default: Department of Astronomy]: '
        )
        if not site_department or len(site_department.strip()) == 0:
            site_department = "Department of Astronomy"

        site_department_link = input(
            'URL for the department [default: https://example.org/astro]: '
        )
        if not site_department_link or len(site_department_link.strip()) == 0:
            site_department_link = "https://example.org/astro"

        site_institution = input(
            'Institution [default: Example University]: '
        )
        if not site_institution or len(site_institution.strip()) == 0:
            site_institution = "Example University"

        site_institution_link = input(
            'URL for the institution [default: https://example.org]: '
        )
        if not site_institution_link or len(site_institution_link.strip()) == 0:
            site_institution_link = "https://example.org"

        site_institution_logo = input(
            'Path to an institution logo image file [default: None]: '
        )
        if not site_institution_logo or len(site_institution_logo.strip()) == 0:
            site_institution_logo = None

        # make the basedir
        donebasedir = prepare_basedir(
            args.basedir,
            site_project=site_project,
            site_project_link=site_project_link,
            site_department=site_department,
            site_department_link=site_department_link,
            site_institution=site_institution,
            site_institution_link=site_institution_link,
            site_institution_logo=site_institution_logo
        )

        # make the lcc-index.sqlite database
        lcc_index = new_lcc_index_db(args.basedir)

        # make the lcc-datasets.sqlite database
        lcc_datasets = new_lcc_datasets_db(args.basedir)


    elif args.command == 'new-collection':

        collection_id = input(
            'Collection identifier '
            '[default: my-new-collection]'
        )
        if not collection_id or len(collection_id.strip()) == 0:
            collection_id = 'my-new-collection'
        else:
            collection_id = squeeze(
                collection_id
            ).strip(
            ).lower(
            ).replace(
                '/','-'
            ).replace(
                ' ','-'
            ).replace(
                '\t','-'
            )

        lightcurve_dir = input(
            'Original light curves directory [default: None]'
        )
        if not lightcurve_dir or len(lightcurve_dir.strip()) == 0:
            lightcurve_dir = None

        checkplot_dir = input('Original checkplot pickles '
                              'directory [default: None]')
        if not checkplot_dir or len(checkplot_dir.strip()) == 0:
            checkplot_dir = None

        # this is unused for now
        pfresult_dir = None

        from .backend import abcat
        abcat.set_logger_parent(__name__)

        # check if the collection_dir exists already and there's an
        # lcformat-description.json file in there
        if os.path.exists(os.path.join(args.basedir,
                                       collection_id,
                                       'lcformat-description.json')):

            print('Existing lcformat-description.json found at: %s' %
                  os.path.join(args.basedir,
                               collection_id,
                               'lcformat-description.json'))

            # read in the format description JSON and check if it makes sense
            try:

                lcform = abcat.get_lcformat_description(
                    os.path.join(args.basedir,
                                 collection_id,
                                 'lcformat-description.json')
                )
                print('Everything checks out!')

            except:
                print("Could not validate your lcformat-description.json file")
                raise

            collection_dir = os.path.join(args.basedir,
                                          collection_id)

        # if this doesn't exist, then we'll set up a new directory structure
        else:

            collection_dir = new_collection_directories(
                args.basedir,
                collection_id,
                lightcurve_dir=lightcurve_dir,
                checkplot_dir=checkplot_dir,
                pfresult_dir=pfresult_dir
            )

            # now, we'll ask the user to edit their lcformat-description.json
            # file if they agree, we'll launch a subprocess and wait for it to
            # exit: "$(EDITOR) /path/to/lcformat-description.json"
            editnow = input(
                "Do you want to edit this LC collection's "
                "lcformat-description.json file now? [Y/n] "
            )

            # if we agree to edit now, launch their editor
            if editnow.lower() == 'y' or not editnow or len(editnow) == 0:

                # get the user's editor
                if 'EDITOR' in os.environ:
                    editor = os.environ['EDITOR']
                elif 'VISUAL' in os.environ:
                    editor = os.environ['VISUAL']
                else:
                    editor = None

                use_editor = None

                while (not use_editor or len(use_editor.strip()) == 0):

                    ask_editor = input('Editor to use [default: %s]: ' % editor)
                    if ask_editor:
                        use_editor = ask_editor
                    elif ((not ask_editor or len(ask_editor.strip()) == 0) and
                          editor):
                        use_editor = editor
                    else:
                        use_editor = None

                editor_cmdline = '%s %s' % (
                    use_editor,
                    os.path.abspath(
                        os.path.join(collection_dir,
                                     'lcformat-description.json')
                    )
                )

                subprocess.call(editor_cmdline, shell=True)

                print("Validating your lcformat-description.json file...")
                try:

                    lcform = abcat.get_lcformat_description(
                        os.path.join(args.basedir,
                                     collection_id,
                                     'lcformat-description.json')
                    )
                    print('Everything checks out!')

                except:
                    print("Could not validate your "
                          "lcformat-description.json file")
                    raise

            # if we don't want to edit right now, drop out
            else:

                print(
                    "Returning without a validated lcformat-description.json "
                    "file.\n\nPlease edit:\n\n%s\n\nto describe "
                    "your original light curve format and run this "
                    "command again:\n\n"
                    "%s --basedir %s new-collection\n\nUse: %s for "
                    "the 'Collection identifier' prompt. "
                    "We'll then proceed to the next step."
                    % (os.path.abspath(
                        os.path.join(
                            collection_dir,
                            'lcformat-description.json'
                        )),
                       aparser.prog,
                       args.basedir,
                       collection_id)
                )
                sys.exit(0)

        #
        # the next step is to ask if we can reform the original LCs to the
        # common LCC CSV format
        #
        convertlcs = input(
            "The LCC server can convert your original format LCs "
            "to the common LCC CSV format as they are "
            "requested in search results, "
            "but it might slow down your queries. "
            "Do you want to convert your original format "
            "light curves to LCC CSV format now? [y/N] "
        )
        if convertlcs.strip().lower() == 'y':

            ret = input(
                "Please copy or symlink your original format "
                "light curves into this LC collection's "
                "lightcurves directory:\n\n"
                "%s \n\n"
                "We'll wait here until you're done. "
                "[hit Enter to continue]" %
                os.path.abspath(os.path.join(collection_dir, 'lightcurves'))
            )

            print('Launching conversion tasks. This might take a while...')

            converted = convert_original_lightcurves(
                args.basedir,
                collection_id
            )

            print('Done with LC conversion.')

        else:

            print("Skipping conversion. ")

        #
        # next, we'll set up the lclist.pkl file
        #

        from astrobase import lcproc
        lcproc.set_logger_parent(__name__)

        # pick the first LC in the LC dir and read it in using the provided LC
        # reader function
        lcdict = lcform['readerfunc'](
            glob.glob(
                os.path.abspath(
                    os.path.join(collection_dir,
                                 'lightcurves',
                                 lcform['fileglob'])
                )
            )[0]
        )
        if isinstance(lcdict, (tuple, list)) and isinstance(lcdict[0], dict):
            lcdict = lcdict[0]

        if 'columns' in lcdict:
            lc_keys = lcdict['columns']
        else:
            lc_keys = sorted(lcdict.keys())

        print('Your original format light curves '
              'contain the following keys '
              'when read into an lcdict:\n')
        for k in lc_keys:
            print(k)

        timecol, magcol, errcol = None, None, None

        # ask which timecol, magcol, errcol the user wants to use
        while not timecol or len(timecol.strip()) == 0:
            timecol = input(
                "\nWhich key in the lcdict do you want to use "
                "for the time column? "
            )

        # ask which magcol, magcol, errcol the user wants to use
        while not magcol or len(magcol.strip()) == 0:
            magcol = input(
                "Which key in the lcdict do you want to use "
                "for the mag column? "
            )

        # ask which errcol, magcol, errcol the user wants to use
        while not errcol or len(errcol.strip()) == 0:
            errcol = input(
                "Which key in the lcdict do you want to use "
                "for the err column? "
            )

        print('Generating a light curve list pickle...')

        lcproc.register_custom_lcformat(
            lcform['formatkey'],
            lcform['fileglob'],
            lcform['readerfunc'],
            [timecol],
            [magcol],
            [errcol]
        )

        lclpkl = lcproc.make_lclist(
            os.path.join(collection_dir,
                         'lightcurves'),
            os.path.join(collection_dir,'lclist.pkl'),
            lcformat=lcform['formatkey']
        )

        #
        # now we'll reform this pickle by adding in checkplot info
        #
        print('Done.')
        input("We will now generate an object catalog pickle "
              "using the information from checkplot pickles "
              "you have prepared from these light curves. "
              "Please copy or symlink your checkplot pickles "
              "into the directory:\n\n"
              "%s \n\n"
              "Note that this is optional, but highly recommended. "
              "If you don't have any checkplot pickles prepared, "
              "you can continue anyway, but the only "
              "columns available for searching will be:\n\n"
              "objectid, ra, decl, ndet, lcfname\n\n"
              "We'll wait here until you're done. [hit Enter to continue]" %
              os.path.abspath(os.path.join(collection_dir,'checkplots')))

        cpdir = os.path.join(collection_dir,'checkplots')
        if len(glob.glob(os.path.join(cpdir,'checkplot-*.pkl*'))) == 0:

            print('No checkplot pickles found. '
                  'Generating barebones object catalog pickle and kd-tree...')

            # add the magcols key to the unaugmented catalog
            with open(lclpkl,'rb') as infd:
                lcl = pickle.load(infd)
            lcl['magcols'] = [magcol]
            with open(lclpkl,'wb') as outfd:
                pickle.dump(lcl, outfd, pickle.HIGHEST_PROTOCOL)

            shutil.copy(lclpkl, os.path.join(collection_dir,
                                             'lclist-catalog.pkl'))

        else:

            print('Found %s checkplot pickles. '
                  'Generating object catalog pickle and kd-tree...')

            augcat_pkl = generate_augmented_lclist_catalog(
                args.basedir,
                collection_id,
                lclpkl,
                magcol,
            )

        kdt = generate_catalog_kdtree(args.basedir,
                                      collection_id)


        #
        # next, we'll ask the user about their collection
        #
        lcc_name = input(
            'Name of this LC collection [default: Example LC collection]: '
        )
        if not lcc_name or len(lcc_name.strip()) == 0:
            lcc_name = 'Example LC collection'

        lcc_project = input(
            'Project associated with this LC collection '
            '[default: Example Variable Star Survey]: '
        )
        if not lcc_project or len(lcc_project.strip()) == 0:
            lcc_project = 'Example Variable Star Survey'

        lcc_datarelease = input(
            'Data release number [default: 1]: '
        )
        if not lcc_datarelease or len(lcc_datarelease.strip()) == 0:
            lcc_datarelease = 1
        else:
            lcc_datarelease = int(lcc_datarelease)

        lcc_citation = input(
            'Citation for this collection '
            '[default: Me and my friends et al. (2018)]: '
        )
        if not lcc_citation or len(lcc_citation.strip()) == 0:
            lcc_citation = 'Me and my friends et al. (2018)'

        lcc_ispublic = input(
            'Is this LC collection public? [Y/n]: '
        )
        if ((not lcc_ispublic) or
            (len(lcc_ispublic.strip()) == 0) or
            (lcc_ispublic.strip().lower() == 'y')):
            lcc_ispublic = True
        else:
            False

        # launch the user's editor to edit this LCC's description
        print("We'll launch your editor to edit the description "
              "for this collection. You can use Markdown here.")

        # get the user's editor
        if 'EDITOR' in os.environ:
            editor = os.environ['EDITOR']
        elif 'VISUAL' in os.environ:
            editor = os.environ['VISUAL']
        else:
            editor = None

        use_editor = None

        while (not use_editor or len(use_editor.strip()) == 0):

            ask_editor = input('Editor to use [default: %s]: ' % editor)
            if ask_editor:
                use_editor = ask_editor
            elif ((not ask_editor or len(ask_editor.strip()) == 0) and
                  editor):
                use_editor = editor
            else:
                use_editor = None

        desc_fd, desc_tempfile = tempfile.mkstemp()

        editor_cmdline = '%s %s' % (
            use_editor,
            desc_tempfile
        )
        subprocess.call(editor_cmdline, shell=True)

        with open(desc_tempfile,'r') as infd:
            lcc_desc = infd.read()

        print('Saved LC collection description successfully!')
        print('Building object database...')
        catsqlite = generate_catalog_database(
            args.basedir,
            collection_id,
            collection_info={'name':lcc_name,
                             'desc':lcc_desc,
                             'project':lcc_project,
                             'datarelease':lcc_datarelease,
                             'citation':lcc_citation,
                             'ispublic':lcc_ispublic})

        print("Adding this collection to the LCC server's index...")
        catindex = add_collection_to_lcc_index(args.basedir,
                                               collection_id)

        print('\nAll done!\n')
        print('You can start an LCC server instance '
              'using the command below:\n')
        print('%s --basedir %s run-server' % (aparser.prog,
                                              args.basedir))
        sys.exit(0)

    else:

        print('Could not understand your command: %s' % args.command)
        sys.exit(0)



##############################
## DIRECT EXECUTION SUPPORT ##
##############################

if __name__ == '__main__':
    main()
