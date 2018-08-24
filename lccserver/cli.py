#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''cli.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains the CLI implementation for the LCC server.

lcc-server [options] <command>

where command is one of:

init                -> initializes the basedir for the LCC server
add-collection      -> adds an LC collection to the LCC server
remove-collection   -> removes an LC collection from the LCC server

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

import multiprocessing as mp
NCPUS = mp.cpu_count()

from .backend import abcat
from .backend import datasets

datasets.set_logger_parent(__name__)
abcat.set_logger_parent(__name__)


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

            siteinfo["institution_link"] = None


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
                       os.path.join(basedir,collection_id, 'pfresults'))
            LOGINFO('linked provided pfresult '
                    'directory: %s for collection: %s to %s' %
                    (os.path.abspath(pfresult_dir),
                     collection_id,
                     os.path.join(basedir,collection_id, 'pfresults')))

        else:
            LOGWARNING('no existing pfresult '
                       'directory for collection: %s, making a new one at: %s' %
                       (collection_id,
                        os.path.join(basedir,collection_id, 'pfresults')))
            os.mkdir(os.path.join(basedir,collection_id, 'pfresults'))


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
                                 skip_converted=False):
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

def generate_augmented_lclist_catalog(basedir,
                                      collection_id,
                                      lclist_pkl):
    '''This generates a lclist-catalog.pkl file containing extra info from
    checkplots.

    '''



def generate_catalog_kdtree(basedir,
                            collection_id,
                            lclist_pkl):
    '''This generates the kd-tree pickle for spatial searches.

    '''



def generate_catalog_database(basedir,
                              collection_id,
                              augcat_pkl):
    '''This generates the objectinfo-catalog.sqlite database.

    '''



##########################################
## GENERATING LCC SERVER ROOT DATABASES ##
##########################################

def new_lcc_index_db(basedir):
    '''
    This generates an lcc-index DB in the basedir.

    '''



def new_lcc_datasets_db(basedir):
    '''
    This generates an lcc-datasets DB in the basedir.

    '''



def add_collection_to_lcc_index(basedir,
                                collection_id,
                                collection_metadata=None):
    '''
    This adds an LC collection to the index DB.

    '''



def remove_collection_from_lcc_index(basedir,
                                     collection_id,
                                     remove_files=False):
    '''
    This removes an LC collection from the index DB.

    Optionally removes the files as well.

    '''



##############
## CLI MAIN ##
##############

def main():
    '''
    This drives the CLI.

    '''



##############################
## DIRECT EXECUTION SUPPORT ##
##############################

if __name__ == '__main__':
    main()
