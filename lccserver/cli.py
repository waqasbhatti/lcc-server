#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''cli.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains the CLI implementation for the LCC-Server.

lcc-server [options] COMMAND

where COMMAND is one of:

init-basedir        -> Initializes the basedir for the LCC-Server.
                       This is an interactive command.

add-collection      -> Adds a new LC collection to the LCC-Server.
                       This is an interactive command.

edit-collection     -> Edits an existing LC collection.
                       This is an interactive command.

del-collection      -> Removes an LC collection from the LCC-Server
                       This will ask you for confirmation.

run-server          -> Runs an instance of the LCC-Server to provide
                       the main search interface and a backing instance
                       of the checkplotserver for serving objectinfo

shell               -> Starts an IPython shell in the context of the specified
                       LCC-Server base directory. This is useful for debugging
                       and running queries directly using the
                       lccserver.backend.datasets.py and
                       lccserver.backend.dbsearch.py modules

and options are:

--basedir           -> The base directory to execute all commands from
                       this is the directory where all of the LCC-Server's
                       files and directories will be created and where it will
                       run from when executed.

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
import shutil
import json
import glob
import sqlite3
import tempfile
import subprocess
from concurrent.futures import ProcessPoolExecutor
import asyncio
from functools import partial
import getpass
from datetime import datetime

import multiprocessing as mp
NCPUS = mp.cpu_count()

import numpy as np
from tornado.escape import squeeze
from cryptography.fernet import Fernet


######################
## AUTH DB CREATION ##
######################

from .authnzerver import authdb


def create_authentication_database(authdb_path):
    '''This will make a new authentication database.

    Does NOT return engine or metadata, because those are opened in the
    background workers.

    '''

    authdb.create_sqlite_auth_db(authdb_path,
                                 echo=False,
                                 returnconn=False)


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
                    site_institution_logo=None,
                    interactive=False):
    '''
    This prepares the given base directory for use with LCC-Server.

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

    LOGINFO('Created LCC-Server sub-directories under basedir: %s' % basedir)

    #
    # generate the site-info.json file in the basedir
    #
    if os.path.exists(os.path.join(basedir,'site-info.json')):

        LOGERROR('site-info.json already exists in basedir: %s, skipping...' %
                 basedir)

    else:

        import secrets
        cachedir = '/tmp/lccs-%s' % secrets.token_urlsafe(8)

        siteinfo = {
            "project":site_project,
            "project_link":site_project_link,
            "department":site_department,
            "department_link":site_department_link,
            "institution":site_institution,
            "institution_link":site_institution_link,
            "signups_allowed": True,
            "logins_allowed": True,
            "rate_limit_active": True,
            "cache_location": cachedir,
            "query_timeout_sec": 30.0,
            "lczip_timeout_sec": 30.0,
            "lczip_max_nrows": 500,
            "dataset_rows_per_page": 500
        }

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


        # set up the email settings file
        emailsettings = {
            "email_sender":"LCC-Server <smtp_user_name@emailserver.org>",
            "email_server":"smtp.emailserver.org",
            "email_port":587,
            "email_user":"smtp_user_name",
            "email_pass":"smtp_user_password",
        }

        # write the email secrets file to the basedir
        with open(os.path.join(basedir,'.lccserver.secret-email'),
                  'w') as outfd:
            json.dump(emailsettings, outfd, indent=4)
        # chmod this file to 600
        os.chmod(os.path.join(basedir,'.lccserver.secret-email'), 0o100400)

        # update the site-info.json with the location of the email secrets file
        siteinfo["email_settings_file"] = (
            os.path.abspath(os.path.join(basedir,'.lccserver.secret-email'))
        )

        # write site-info.json to the basedir
        with open(os.path.join(basedir,'site-info.json'),'w') as outfd:
            json.dump(siteinfo, outfd, indent=4)

        # chmod this file to 600
        os.chmod(os.path.join(basedir,'site-info.json'), 0o100600)

        LOGINFO('Created site-info.json: %s' %
                os.path.join(basedir,'site-info.json'))
        LOGINFO('Please review this file and fill in details as appropriate.')
        LOGINFO("Edit the file pointed to by the 'email_settings_file' key "
                "in site-info.json to set up an email server "
                "(for notifications, etc.). ")

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
            json.dump(docindex, outfd, indent=4)

        with open(os.path.join(basedir,'docs','citation.md'),'w') as outfd:
            outfd.write(
                "Citation instructions for your project's data go here.\n"
            )

        with open(os.path.join(basedir,'docs','lcformat.md'),'w') as outfd:
            outfd.write("Light curve column descriptions for "
                        "your project's data go here.\n")

        LOGINFO('Created doc-index.json: %s and '
                'barebones %s/docs/citation.md, %s/docs/lcformat.md' %
                (os.path.join(basedir,'docs','doc-index.json'),
                 basedir,
                 basedir))


    # create our authentication database if it doesn't exist
    authdb_path = os.path.join(basedir, '.authdb.sqlite')

    if not os.path.exists(authdb_path):

        LOGINFO('No existing authentication DB found, making a new one...')

        # generate the initial DB
        create_authentication_database(authdb_path)

        if interactive:

            # ask the user for their email address and password the default
            # email address will be used for the superuser if the email address
            # is None, we'll use the user's UNIX ID@localhost if the password is
            # None, a random one will be generated

            try:
                userid = '%s@localhost' % getpass.getuser()
            except Exception as e:
                userid = 'lccadmin@localhost'

            inp_userid = input(
                '\nAdmin email address [default: %s]: ' %
                userid
            )
            if inp_userid and len(inp_userid.strip()) > 0:
                userid = inp_userid

            inp_pass = getpass.getpass(
                'Admin password [default: randomly generated]: '
            )
            if inp_pass and len(inp_pass.strip()) > 0:
                password = inp_pass
            else:
                password = None

        else:

            userid = None
            password = None

        # generate the admin users and initial DB info
        u, p = authdb.initial_authdb_inserts('sqlite:///%s' % authdb_path,
                                             superuser_email=userid,
                                             superuser_pass=password)

        if interactive:

            print('')
            if p:
                LOGWARNING('Generated random admin password: %s' % p)
            print('')

        else:

            creds = os.path.join(basedir,
                                 '.lccserver.admin-credentials')
            with open(creds,'w') as outfd:
                outfd.write('%s %s\n' % (u,p))
                os.chmod(creds, 0o100400)

    else:
        LOGINFO('Using existing authentication database.')


    # finally, we'll generate the server secrets now so we don't have to deal
    # with them later
    LOGINFO('Generating LCC-Server secret tokens...')
    session_secret = Fernet.generate_key()
    cpserver_secret = Fernet.generate_key()
    fernet_secret = Fernet.generate_key()
    session_secret_file = os.path.join(basedir,'.lccserver.secret')
    cpserver_secret_file = os.path.join(basedir,'.lccserver.secret-cpserver')
    fernet_secret_file = os.path.join(basedir,'.lccserver.secret-fernet')

    with open(session_secret_file,'wb') as outfd:
        outfd.write(session_secret)
    os.chmod(session_secret_file, 0o100400)

    with open(cpserver_secret_file,'wb') as outfd:
        outfd.write(cpserver_secret)
    os.chmod(cpserver_secret_file, 0o100400)

    with open(fernet_secret_file,'wb') as outfd:
        outfd.write(fernet_secret)
    os.chmod(fernet_secret_file, 0o100400)

    #
    # now we're all done.
    #
    LOGINFO('Done with LCC-Server basedir set up for: %s.' %
            os.path.abspath(basedir))



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
                'LCC-Server can read your original format light curves '
                'and convert them to common LCC CSVLC format')

        #
        # return the finished LC collection directory at the end
        #
        return os.path.join(basedir, collection_id)



def convert_original_lightcurves(basedir,
                                 collection_id,
                                 original_lcdir=None,
                                 normalize_lcs=False,
                                 convert_workers=NCPUS,
                                 csvlc_version=1,
                                 comment_char='#',
                                 column_separator=',',
                                 skip_converted=True,
                                 max_lcs=None):
    '''This converts original format light curves to the common LCC CSV format.

    This is optional since the LCC-Server can do this conversion on-the-fly if
    necessary, but this slows down LC zip operations if people request large
    numbers of light curves.

    If original_lcdir is not None, this will be used as the source of the light
    curves. If it is None, we'll assume that the lightcurves to convert are
    present in the basedir/collection_id/lightcurves directory already.

    The light curves will be read using the module and function specified in the
    basedir/collection_id/lcformat-description.json file.

    Next, if normalize_lcs is True, they will be normalized using the module and
    function specified in the basedir/collection_id/lcformat-description.json
    file.

    Finally, the conversion will take place and put the output light curves into
    the basedir/lc_collection/lightcurves directory.

    convert_workers controls the number of parallel workers used to convert
    the light curves.

    csvlc_version, comment_char, column_separator, skip_converted control the
    CSV LC output and are passed directly to abcat.convert_to_csvlc.

    '''

    from .backend import abcat
    from .backend import datasets

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
        input_lclist = sorted(
            glob.glob(
                os.path.join(input_lcdir, lcformatdict['fileglob'])
            )
        )

        if max_lcs is not None:
            input_lclist = input_lclist[:max_lcs]

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
                             'normalize_lc':normalize_lcs,
                             'comment_char':comment_char,
                             'column_separator':column_separator,
                             'skip_converted':skip_converted}

        tasks = [(x, y, lcformjson, converter_options) for x, y in
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

        # we'll also generate the basic lclist.pkl




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
        infokeys=(
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
            ('objectinfo.gaia_absmag',
             np.float_, True, True, np.nan, np.nan),
            ('objectinfo.simbad_best_mainid',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_objtype',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_allids',
             np.unicode_, True, True, '', ''),
            ('objectinfo.simbad_best_distarcsec',
             np.float_, True, True, np.nan, np.nan),
            #
            # TIC info
            #
            ('objectinfo.ticid',
             np.unicode_, True, True, '', ''),
            ('objectinfo.tic_version',
             np.unicode_, True, True, '', ''),
            ('objectinfo.tessmag',
             np.float_, True, True, np.nan, np.nan),
            #
            # variability info
            #
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
             np.float_, False, True, np.nan, np.nan),
        )
):
    '''This generates a lclist-catalog.pkl file containing extra info from
    checkplots.

    basedir is the base directory for the LCC-Server

    collection_id is the directory name of the collection you want to process

    lclist_pkl is the path to the original list catalog pickle created for the
    light curves in the collection using astrobase.lcproc.catalogs.make_lclist.

    magcol is the LC magnitude column being used in the checkplots' feature
    keys. This will be added as a prefix to the infokeys.

    nworkers sets the number of parallel workers to use when gathering info from
    the checkplots.

    infokeys is a list of key specs to extract from each checkplot. the provided
    list is a good default and should contain the most useful keys from
    checkplots generated using astrobase.checkplot.checkplot_pickle or
    astrobase.lcproc.checkplotgen.runcp.

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

    from astrobase.lcproc.catalogs import add_cpinfo_to_lclist

    # get the basedir/collection_id/checkplots directory
    cpdir = os.path.join(basedir, collection_id, 'checkplots')

    # check if there are any checkplots in there
    cplist = glob.glob(os.path.join(cpdir, checkplot_glob))

    # use lcproc.add_cpinfo_to_lclist with LC format info to update the catalog
    # write to basedir/collection_id/lclist-catalog.pkl
    augcat = add_cpinfo_to_lclist(
        cplist,
        lclist_pkl,
        magcol,
        os.path.join(basedir, collection_id, 'lclist-catalog.pkl'),
        infokeys=list(infokeys),
        nworkers=nworkers
    )

    return augcat



def generate_catalog_kdtree(basedir,
                            collection_id):
    '''This generates the kd-tree pickle for spatial searches.

    '''

    from .backend import abcat

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
        collection_owner=1,
        collection_visibility='public',
        collection_sharedwith=None,
        collection_info=None,
        colinfo=None,
        indexcols=None,
        ftsindexcols=None,
        overwrite_existing=False,
):
    '''This generates the objectinfo-catalog.sqlite database.

    basedir is the base directory of the LCC-Server

    collection_id is the directory name of the LC collection we're working on.

    collection_owner is the user ID of the owner of this collection. By
    conventions, the LCC-Server uses the following three users by default:

    1 -> LCC-Server admin
    2 -> the anonymous user
    3 -> the locked user (who can't access anything)

    collection_visibility is one of:

    'public'  -> the LC collection will be visible to anyone who uses the server
    'shared'  -> the LC collection will only be visible to userids provided in
                 collection_sharedwith
    'private' -> the LC collection will only be visible to the collection_owner
                 user ID

    colection_sharedwith is a comma-separated string of user IDs that this
    collection is shared with.

    collection_info is a dict that MUST have the keys listed in the
    example:

        collection_info = {
            'name':'Example LC Collection',
            'desc':'This is an example light curve collection.',
            'project':'LCC-Server Example Project',
            'datarelease':1,
            'citation':'Your citation goes here (2018)',
        }

    Replace these values with those appropriate for your LC collection.

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

    If overwrite_existing is True, any existing catalog DB in the target
    directory will be overwritten.

    '''
    from .backend import abcat

    if collection_info is None:
        collection_info = {
            'name':'Example LC Collection',
            'desc':'This is an example light curve collection.',
            'project':'LCC-Server Example Project',
            'datarelease':1,
            'citation':'Your citation goes here (2018)',
        }

    # get basedir/collection_id/lclist-catalog.pkl
    lclist_catalog_pickle = os.path.join(basedir,
                                         collection_id,
                                         'lclist-catalog.pkl')

    # write the catalog-objectinfo.sqlite DB to basedir/collection_id
    return abcat.objectinfo_to_sqlite(
        lclist_catalog_pickle,
        os.path.join(basedir, collection_id, 'catalog-objectinfo.sqlite'),
        collection_info['name'],  # lcc_name arg for abcat.objectinfo_to_sqlite
        lcc_desc=collection_info['desc'],
        lcc_project=collection_info['project'],
        lcc_datarelease=collection_info['datarelease'],
        lcc_citation=collection_info['citation'],
        lcc_owner=collection_owner,
        lcc_visibility=collection_visibility,
        lcc_sharedwith=collection_sharedwith,
        colinfo=colinfo,
        indexcols=indexcols,
        ftsindexcols=ftsindexcols,
        overwrite_existing=overwrite_existing
    )



##########################################
## GENERATING LCC SERVER ROOT DATABASES ##
##########################################

def new_lcc_index_db(basedir):
    '''
    This generates an lcc-index DB in the basedir.

    '''

    from .backend import abcat

    return abcat.sqlite_make_lcc_index_db(basedir)



def new_lcc_datasets_db(basedir):
    '''
    This generates an lcc-datasets DB in the basedir.

    '''
    from .backend import datasets

    return datasets.sqlite_make_lcc_datasets_db(basedir)



def add_collection_to_lcc_index(basedir,
                                collection_id,
                                raiseonfail=False):
    '''
    This adds an LC collection to the index DB.

    basedir is the base directory of the LCC-Server

    collection_id is the directory name of the LC collection we're working on.

    '''

    from .backend import abcat

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

    query = 'select collection_id from lcc_index where collection_id = ?'
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


#########################
## GENERATING LCC MAPS ##
#########################

def update_lcc_footprint_svg(basedir,
                             overwrite=False):
    '''
    This function updates all LCC maps to generate a coverage SVG.

    '''

    from lccserver.backend import footprints
    from lccserver.backend import dbsearch

    # get all collections
    coll_info = dbsearch.sqlite_list_collections(basedir,
                                                 incoming_role='superuser',
                                                 incoming_userid=1)
    collections = coll_info['databases']
    collection_dirs = [x.replace('_','-') for x in collections]

    # generate the footprint pickles for all collections
    for coll in collections:
        coll_footprint_pkl = os.path.join(basedir,
                                          coll.replace('_','-'),
                                          'catalog-footprint.pkl')

        if (not os.path.exists(coll_footprint_pkl)) or overwrite:
            footprints.generate_collection_footprint(basedir,
                                                     coll,
                                                     conditions='ndet > 99')
        else:
            LOGWARNING('footprint info for collection: %s '
                       'exists and overwrite = False, not updating...' % coll)

    # now generate the updated SVG
    footprints.collection_overview_svg(basedir,
                                       collection_dirs,
                                       use_alpha=0.8,
                                       use_hull='concave',
                                       use_projection='mollweide',
                                       use_colormap='inferno')




########################################
## STARTING AN INSTANCE OF THE SERVER ##
########################################

async def start_lccserver(basedir, executor):
    '''This starts the authnzerver, the LCC-Server and a backing instance of
    checkplotserver.

    This is NOT meant for production, but is useful to make sure everything
    works correctly or for local browsing of your light curve data.

    All of the log lines from the LCC-Server instance and the backing
    checkplotserver instance will go to STDOUT. STDERR will also be directed to
    STDOUT.

    Hit Ctrl+C to stop the server instances.

    This is a bit hacked together. As usual, D. Hellmann's PyMOTW was
    super-helpful:

    https://pymotw.com/3/asyncio/executors.html

    FIXME: fix spam of errors when we hit Ctrl+C

    '''

    # launch the lcc-server HTTP servers

    loop = asyncio.get_event_loop()
    tasks = []
    subprocess_call_authnzerver = partial(
        subprocess.call,
        "authnzerver --basedir='%s'" % os.path.abspath(basedir),
        shell=True
    )
    subprocess_call_cpserver = partial(
        subprocess.call,
        ("checkplotserver "
         "--standalone=1 "
         "--sharedsecret='%s'" % os.path.join(os.path.abspath(basedir),
                                              '.lccserver.secret-cpserver')),
        shell=True
    )

    # we'll run the indexserver using 0.0.0.0:12500 so it can be accessed from
    # outside localhost if no firewalls are in the way.
    subprocess_call_indexserver = partial(
        subprocess.call,
        ("indexserver --serve='0.0.0.0' --basedir='%s'" %
         os.path.abspath(basedir)),
        shell=True
    )

    tasks.append(loop.run_in_executor(executor, subprocess_call_authnzerver))
    tasks.append(loop.run_in_executor(executor, subprocess_call_cpserver))
    tasks.append(loop.run_in_executor(executor, subprocess_call_indexserver))

    completed, pending = await asyncio.wait(tasks)



##############
## CLI MAIN ##
##############

CMD_HELP = '''COMMAND is one of:

init-basedir        -> Initializes the basedir for the LCC-Server.
                       This is an interactive command.

add-collection      -> Adds an LC collection to the LCC-Server.
                       This is an interactive command.

edit-collection     -> Edits an existing LC collection.
                       This is an interactive command.

del-collection      -> Removes an LC collection from the LCC-Server
                       This will ask you for confirmation.

run-server          -> Runs an instance of the LCC-Server to provide
                       the main search interface and a backing instance
                       of the checkplotserver for serving objectinfo.

shell               -> Starts an IPython shell in the context of the specified
                       LCC-Server base directory. This is useful for debugging
                       and running queries directly using the
                       lccserver.backend.datasets.py and
                       lccserver.backend.dbsearch.py modules
'''

def main():
    '''
    This drives the CLI.

    '''

    import argparse
    import sys
    import readline
    import glob
    import pickle

    def completer_func_directories(text, state):
        """
        This tries to complete directory and file names.

        adapted from:

        https://gist.github.com/iamatypeofwalrus/5637895#gistcomment-2581493
        """

        if '~' in text:
            text = os.path.expanduser('~')

        # autocomplete directories with having a trailing slash
        if os.path.isdir(text):
            text += '/'

        return [x for x in glob.glob(text + '*')][state]

    readline.set_completer(completer_func_directories)
    readline.set_completer_delims('\t')
    readline.parse_and_bind('tab: complete')

    aparser = argparse.ArgumentParser(
        description='The LCC-Server CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=CMD_HELP
    )

    aparser.add_argument('command',
                         choices=['init-basedir',
                                  'add-collection',
                                  'edit-collection',
                                  'del-collection',
                                  'run-server',
                                  'shell'],
                         action='store',
                         type=str,
                         help='COMMAND to run')

    aparser.add_argument('--basedir',
                         action='store',
                         type=str,
                         help=("The base directory to run commands in. "
                               "This is the current directory by default: "
                               "%(default)s"),
                         default=os.getcwd())

    args = aparser.parse_args()

    ########################
    ## RUNNING THE SERVER ##
    ########################

    if args.command == 'run-server':

        currdir = os.getcwd()

        if currdir != args.basedir:
            os.chdir(args.basedir)

        executor = ProcessPoolExecutor(max_workers=3)
        event_loop = asyncio.get_event_loop()

        try:
            event_loop.run_until_complete(
                start_lccserver(args.basedir, executor)
            )
        finally:
            event_loop.close()

        # go back to the original directory
        os.chdir(currdir)


    ############################
    ## BASEDIR INITIALIZATION ##
    ############################

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
        prepare_basedir(
            args.basedir,
            site_project=site_project,
            site_project_link=site_project_link,
            site_department=site_department,
            site_department_link=site_department_link,
            site_institution=site_institution,
            site_institution_link=site_institution_link,
            site_institution_logo=site_institution_logo,
            interactive=True
        )

        # make the lcc-index.sqlite database
        new_lcc_index_db(args.basedir)

        # make the lcc-datasets.sqlite database
        new_lcc_datasets_db(args.basedir)


    #########################################
    ## ADDING A NEW LIGHT CURVE COLLECTION ##
    #########################################

    elif args.command == 'add-collection':

        collection_id = input(
            'Collection identifier '
            '[default: my-new-collection] '
        )
        if not collection_id or len(collection_id.strip()) == 0:
            collection_id = 'my-new-collection'
        else:
            # normalize the LC collection ID to our preferred format
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
            ).replace(
                '_','-'
            )

        lightcurve_dir = input(
            'Original light curves directory [default: None] '
        )
        if not lightcurve_dir or len(lightcurve_dir.strip()) == 0:
            lightcurve_dir = None

        checkplot_dir = input('Original checkplot pickles '
                              'directory [default: None] ')
        if not checkplot_dir or len(checkplot_dir.strip()) == 0:
            checkplot_dir = None

        # this is unused for now
        pfresult_dir = None

        from .backend import abcat

        # check if the collection_dir exists already and there's an
        # lcformat-description.json file in there
        if os.path.exists(os.path.join(args.basedir,
                                       collection_id,
                                       'lcformat-description.json')):

            LOGINFO('Existing lcformat-description.json found at: %s' %
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
                LOGINFO('Everything checks out!')

            except Exception as e:
                LOGERROR(
                    "Could not validate your lcformat-description.json file!"
                )
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

                except Exception as e:
                    print("Could not validate your "
                          "lcformat-description.json file!")
                    raise

            # if we don't want to edit right now, drop out
            else:

                print(
                    "Returning without a validated lcformat-description.json "
                    "file.\n\nPlease edit:\n\n%s\n\nto describe "
                    "your original light curve format and run this "
                    "command again:\n\n"
                    "%s --basedir %s add-collection\n\nUse: %s for "
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
        input(
            "Please copy or symlink your original format "
            "light curves into this LC collection's "
            "lightcurves directory:\n\n"
            "%s \n\n"
            "We'll wait here until you're ready. "
            "[hit Enter to continue]" %
            os.path.abspath(os.path.join(collection_dir, 'lightcurves'))
        )

        convertlcs = input(
            "The LCC-Server can convert your original format LCs "
            "to the common LCC CSV format as they are "
            "requested in search results, "
            "but it might slow down your queries. "
            "Do you want to convert your original format "
            "light curves to LCC CSV format now? [y/N] "
        )

        if convertlcs and convertlcs.strip().lower() == 'y':

            # ask if the output converted LCs should be normalized if normfunc
            # is not None
            if lcform['normfunc'] is not None:

                print(
                    'An LC normalization function has been specified:'
                    '\n\n%s\n\n'
                    'Generally, the output converted LCs should not be '
                    'normalized to preserve their original measurement values.'
                    % repr(lcform['normfunc'])
                )
                norm_converted_lcs = (
                    input('Do you want to normalize the '
                          'output converted light curves? [y/N] ')
                )
                if (norm_converted_lcs and
                    norm_converted_lcs.strip().lower() == 'y'):
                    do_normalization_for_converted_lcs = True
                else:
                    do_normalization_for_converted_lcs = False

            else:
                do_normalization_for_converted_lcs = False

            print('Launching conversion tasks. This might take a while...')
            convert_original_lightcurves(
                args.basedir,
                collection_id,
                normalize_lcs=do_normalization_for_converted_lcs
            )

            print('Done with LC conversion.')

        else:

            print("Skipping conversion. ")

        #
        # next, we'll set up the lclist.pkl file
        #
        from astrobase import lcproc
        from astrobase.lcproc.catalogs import make_lclist

        try:

            # pick the first LC in the LC dir and read it in using the provided
            # LC reader function
            lcdict = lcform['readerfunc'](
                glob.glob(
                    os.path.abspath(
                        os.path.join(collection_dir,
                                     'lightcurves',
                                     lcform['fileglob'])
                    )
                )[0]
            )
            if (isinstance(lcdict, (tuple, list)) and
                isinstance(lcdict[0], dict)):
                lcdict = lcdict[0]

            if 'columns' in lcdict:
                lc_keys = lcdict['columns']
            else:
                lc_keys = sorted(lcdict.keys())

            # attempt to complete lcdict column keys
            def completer_func_cols(text, state):
                """
                This tries to complete lcdict column keys.

                """
                return [x for x in lc_keys if x.startswith(text)][state]

            readline.set_completer(completer_func_cols)
            readline.set_completer_delims('\t')
            readline.parse_and_bind('tab: complete')

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
                if ((not timecol) or
                    (len(timecol.strip()) == 0) or
                    (timecol not in lc_keys)):
                    print("The provided timecol: %s "
                          "isn't present in lcdict keys." % timecol)
                    timecol = None

            # ask which magcol, magcol, errcol the user wants to use
            while not magcol or len(magcol.strip()) == 0:
                magcol = input(
                    "Which key in the lcdict do you want to use "
                    "for the mag column? "
                )
                if ((not magcol) or
                    (len(magcol.strip()) == 0) or
                    (magcol not in lc_keys)):
                    print("The provided magcol: %s "
                          "isn't present in lcdict keys." % magcol)
                    magcol = None

            # ask which errcol, magcol, errcol the user wants to use
            while not errcol or len(errcol.strip()) == 0:
                errcol = input(
                    "Which key in the lcdict do you want to use "
                    "for the err column? "
                )
                if ((not errcol) or
                    (len(errcol.strip()) == 0) or
                    (errcol not in lc_keys)):
                    print("The provided errcol: %s "
                          "isn't present in lcdict keys." % errcol)
                    errcol = None

            # turn off the completer
            readline.set_completer()

        # we can't proceed without doing an LC read test
        except Exception as e:

            print("Could not run a light curve read test using LCs "
                  "in %s.\n" % os.path.join(collection_dir,
                                            'lightcurves'))
            print("This may indicate an issue with your LC reader "
                  "function or there are no LCs in the "
                  "lightcurves directory.\n")
            LOGEXCEPTION('Exception raised was:\n')
            print("\nOnce this is fixed, call lcc-server again:\n")
            print("lcc-server --basedir %s add-collection" % args.basedir)
            sys.exit(1)

        #
        # generate the LC list pickle
        #
        print('Generating a light curve list using magcol: %s...' %
              magcol)
        lcproc.register_lcformat(
            lcform['parsed_formatinfo']['formatkey'],
            lcform['parsed_formatinfo']['fileglob'],
            [timecol],
            [magcol],
            [errcol],
            lcform['parsed_formatinfo']['readermodule'],
            lcform['parsed_formatinfo']['readerfunc'],
            readerfunc_kwargs=lcform['parsed_formatinfo']['readerfunc_kwargs'],
            normfunc_module=lcform['parsed_formatinfo']['normmodule'],
            normfunc=lcform['parsed_formatinfo']['normfunc'],
            normfunc_kwargs=lcform['parsed_formatinfo']['normfunc_kwargs'],
            magsarefluxes=lcform['magsarefluxes'],
            overwrite_existing=True
        )

        lclpkl = make_lclist(
            os.path.join(collection_dir,
                         'lightcurves'),
            os.path.join(collection_dir,'lclist.pkl'),
            lcformat=lcform['formatkey']
        )
        with open(lclpkl,'rb') as infd:
            collection_lcinfo = pickle.load(infd)

        collection_lclist = collection_lcinfo['objects']['lcfname']
        print('Done. %s total light curves found.\n' % len(collection_lclist))

        #
        # Get the checkplot pickles from the user
        #
        input("We will now generate a base object catalog "
              "using the information from any checkplot pickles "
              "you have prepared from these light curves. "
              "Please copy or symlink your checkplot pickles "
              "into the directory:\n\n"
              "%s \n\n"
              "NOTE: this is optional, but highly recommended. "
              "If you don't have any checkplot pickles prepared, "
              "we can prepare checkplot pickles "
              "automatically from your light curves.\n\n"
              "We'll wait here until you're ready. [hit Enter to continue]" %
              os.path.abspath(os.path.join(collection_dir,'checkplots')))

        cpdir = os.path.join(collection_dir,'checkplots')

        #
        # we'll ask the user if they want to run period-finding on their light
        # curves next
        #
        print("\nDo you want to run period-finding on your light curves?")
        print("This is optional and will probably take some time to run,\n"
              "but will add in phased light curve plots to the detailed\n"
              "object information displayed by the LCC-Server.\n"
              "NOTE: If you have already generated checkplot pickles with\n"
              "period-finding information in them, hit Enter below to skip\n"
              "this step.\n")
        run_periodfinding = input(
            "Run period-finding now? [y/N] "
        )

        if run_periodfinding and run_periodfinding.strip().lower() == 'y':

            print(
                "\nRunning period-finders: GLS, BLS, PDM with automatic\n"
                "period interval determination for GLS & PDM, and\n"
                "using a period interval of [1.0, 100.0] days for BLS.\n"
                "This will take a while..."
            )

            from astrobase.lcproc.periodsearch import parallel_pf

            periodfinding_pickles = parallel_pf(
                collection_lclist,
                os.path.join(collection_dir,
                             'periodfinding'),
                lcformat=lcform['formatkey'],
                pfmethods=('gls','bls','pdm'),
                pfkwargs=({},{'startp':1.0,'endp':100.0},{}),
                getblssnr=False
            )
            print('Done.\n')

        else:
            periodfinding_pickles = [None for x in collection_lclist]

        #
        # generate checkplot pickles now if None were found
        #
        if len(glob.glob(os.path.join(cpdir,'checkplot-*.pkl*'))) == 0:

            # Here, we can optionally generate checkplot pickles in 'fast'
            # mode
            print("No existing checkplot pickles were found.\n\n"
                  "Do you want to generate checkplot pickles for "
                  "your light curves? These will contain the "
                  "following information:\n")
            print("finder chart, variability features, colors "
                  "(if mags available in LC objectinfo), "
                  "color features (if mags available in LC objectinfo), "
                  "GAIA and SIMBAD cross-match info\n")
            print("Calls to external services such as "
                  "GAIA, 2MASS DUST, SIMBAD, and "
                  "NASA SkyView will run using a hard 10 second timeout.\n")
            cpmake = input('Generate checkplot pickles [y/N]: ')

            if (cpmake and
                len(cpmake.strip()) > 0 and
                cpmake.lower().strip() == 'y'):

                print('Generating checkplot pickles. This may take a while...')

                # here, we have to initialize networking in the main thread
                # before forking for MacOS. see:
                # https://bugs.python.org/issue30385#msg293958
                # if this doesn't work, Python will segfault.
                # the workaround noted in the report is to launch
                # lcc-server like so:
                # env no_proxy='*' lcc-server add-collection
                if sys.platform == 'darwin':
                    import requests
                    requests.get('http://captive.apple.com/hotspot-detect.html')

                from astrobase.lcproc.checkplotgen import parallel_cp

                parallel_cp(
                    periodfinding_pickles,
                    cpdir,
                    os.path.join(collection_dir, 'lightcurves'),
                    fast_mode=10.0,
                    lcfnamelist=collection_lclist,
                    lclistpkl=lclpkl,
                    minobservations=49,
                    lcformat=lcform['formatkey']
                )

                print('Done.\n'
                      'Generating augmented object '
                      'catalog and kd-tree...')
                generate_augmented_lclist_catalog(
                    args.basedir,
                    collection_id,
                    lclpkl,
                    magcol,
                )

            else:

                print('No checkplot pickles found. '
                      'Generating bare-bones base object '
                      'catalog and kd-tree...')

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
                  'Generating augmented object catalog and kd-tree...' %
                  len(glob.glob(os.path.join(cpdir,'checkplot-*.pkl*'))))

            generate_augmented_lclist_catalog(
                args.basedir,
                collection_id,
                lclpkl,
                magcol,
            )

        # generate the kd-tree pickle for fast spatial searches
        generate_catalog_kdtree(args.basedir, collection_id)

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

        lcc_visibility = input(
            'Is this LC collection publicly visible or unlisted? [P/u] '
        )
        if ((not lcc_visibility) or
            (len(lcc_visibility.strip()) == 0) or
            (lcc_visibility.strip().lower() == 'p')):
            lcc_visibility = 'public'
        else:
            lcc_visibility = 'unlisted'

        # launch the user's editor to edit this LCC's description
        print(
            "We'll now launch your editor to edit the description "
            "for this collection. You can use Markdown here."
            "\nDOIs and NASA ADS bibcodes will be linkified automatically."
        )

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

            # this helps tell the frontend that we're using markdown for the
            # description
            lcc_desc = '#!MKD %s' % lcc_desc

        print('Saved LC collection description successfully!')
        os.remove(desc_tempfile)

        print('Building object database...')
        generate_catalog_database(
            args.basedir,
            collection_id,
            collection_visibility=lcc_visibility,
            collection_info={'name':lcc_name,
                             'desc':lcc_desc,
                             'project':lcc_project,
                             'datarelease':lcc_datarelease,
                             'citation':lcc_citation}
        )

        print("Adding this collection to the LCC-Server's index...")
        add_collection_to_lcc_index(args.basedir, collection_id)

        print("Adding this collection the LCC-Server's footprint SVG...")
        update_lcc_footprint_svg(args.basedir,overwrite=False)

        # we need to generate a tiny dataset so the LCC-server doesn't complain
        # that it can't find any existing datasets
        print('\nDone.\n')
        print('Running a quick search to test your '
              'LCC-Server installation...\n')

        from lccserver.backend import dbsearch, datasets
        res = dbsearch.sqlite_kdtree_conesearch(args.basedir,
                                                lcdict['objectinfo']['ra'],
                                                lcdict['objectinfo']['decl'],
                                                5.0)
        setid, dt = datasets.sqlite_prepare_dataset(
            args.basedir,
            dataset_owner=2,
            dataset_visibility='public'
        )
        setid, csvlcs_to_generate, all_original_lcs, actual_nrows, npages = (
            datasets.sqlite_new_dataset(
                args.basedir,
                setid,
                dt,
                res,
                dataset_visibility='public',
            )
        )
        datasets.sqlite_make_dataset_lczip(args.basedir,
                                           setid,
                                           csvlcs_to_generate,
                                           all_original_lcs)
        datasets.sqlite_get_dataset(args.basedir,
                                    setid,
                                    'json-preview')

        print('\nAll done!\n')
        print('You can start an LCC-Server instance '
              'using the command below:\n')
        print('%s --basedir %s run-server' % (aparser.prog,
                                              args.basedir))

        print('\nYou can start an IPython shell to explore this LC collection '
              'using the command below:\n')
        print('%s --basedir %s shell' % (aparser.prog,
                                         args.basedir))

        sys.exit(0)

    elif args.command == 'edit-collection':

        # find the root DB
        lccdb = os.path.join(args.basedir, 'lcc-index.sqlite')

        database = sqlite3.connect(
            lccdb,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = database.cursor()

        cursor.execute('select collection_id, ispublic, name, nobjects, '
                       'last_updated from lcc_index order by '
                       'last_updated desc')
        rows = cursor.fetchall()

        collection_list = [x[0] for x in rows]

        # add an autocompleter for collection lookup
        def collection_autocompleter(text, state):
            return [x for x in collection_list if x.startswith(text)][state]

        readline.set_completer(collection_autocompleter)
        readline.set_completer_delims('\t')
        readline.parse_and_bind('tab: complete')

        print('\nCurrent LC collections:\n')
        print('%-18.18s %-10.10s %-23.23s %-10.10s %-19.19s' %
              ('Collection ID','Is Public','Name','Objects','Last Updated'))
        for row in rows:
            print('%-18.18s %-10.10s %-23.23s %-10.10s %-19.19s' % row)

        edit_coll = None

        while not edit_coll:

            collinput = input('\nCollection ID to edit: ')

            if (collinput is not None and
                len(collinput.strip()) > 0 and
                collinput in collection_list):

                edit_coll = collinput.strip()


            else:
                print('Unknown collection: %s' % collinput)
                edit_coll = None

        # now that we have a collection to edit, get its info
        # the ONLY fields that are editable are:
        # - name, description, project, citation, ispublic, datarelease
        query = ("select name, project, datarelease, "
                 "citation, ispublic, description "
                 "from lcc_index where collection_id = ?")
        params = (edit_coll,)
        cursor.execute(query, params)
        row = cursor.fetchone()

        #
        # go into the interactive loop starting the edit for this collection
        #
        lcc_name = input(
            'Name of this LC collection [current: %s]: ' % row[0]
        )
        if not lcc_name or len(lcc_name.strip()) == 0:
            lcc_name = row[0]

        lcc_project = input(
            'Project associated with this LC collection '
            '[current: %s]: ' % row[1]
        )
        if not lcc_project or len(lcc_project.strip()) == 0:
            lcc_project = row[1]

        lcc_datarelease = input(
            'Data release number [current: %s]: ' % row[2]
        )
        if not lcc_datarelease or len(lcc_datarelease.strip()) == 0:
            lcc_datarelease = int(row[2])
        else:
            lcc_datarelease = int(lcc_datarelease)

        lcc_citation = input(
            'Citation for this collection '
            '[current: %s]: ' % row[3]
        )
        if not lcc_citation or len(lcc_citation.strip()) == 0:
            lcc_citation = row[3]

        ispublic_str = 'y' if row[4] else 'n'
        lcc_ispublic = input(
            'Is this LC collection public? [current: %s]: ' % ispublic_str
        )
        if ((not lcc_ispublic) or
            (len(lcc_ispublic.strip()) == 0) or
            (lcc_ispublic.strip().lower() == 'y')):
            lcc_ispublic = True
        else:
            False

        # launch the user's editor to edit this LCC's description
        print("We'll now launch your editor to edit the description "
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

        # fill in the existing description
        with open(desc_fd, 'w') as outfd:
            # don't write the #!MKD label to the editor
            outfd.write(row[5][6:])

        editor_cmdline = '%s %s' % (
            use_editor,
            desc_tempfile
        )
        subprocess.call(editor_cmdline, shell=True)

        with open(desc_tempfile,'r') as infd:
            lcc_desc = infd.read()

            # this helps tell the frontend that we're using markdown for the
            # description
            lcc_desc = '#!MKD %s' % lcc_desc

        # write the new stuff to the database
        query = ("update lcc_index set "
                 "name = ?, project = ?, datarelease = ?, "
                 "citation = ?, ispublic = ?, description = ?, "
                 "last_updated = ? "
                 "where collection_id = ?")
        params = (lcc_name,
                  lcc_project,
                  lcc_datarelease,
                  lcc_citation,
                  1 if lcc_ispublic else 0,
                  lcc_desc,
                  datetime.utcnow().isoformat(),
                  edit_coll)
        cursor.execute(query, params)
        database.commit()

        print('Saved new LC collection metadata successfully!')
        os.remove(desc_tempfile)
        database.close()
        sys.exit(0)


    elif args.command == 'del-collection':

        # find the root DB
        lccdb = os.path.join(args.basedir, 'lcc-index.sqlite')

        database = sqlite3.connect(
            lccdb,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cursor = database.cursor()

        cursor.execute('select collection_id, ispublic, name, nobjects, '
                       'last_updated from lcc_index order by '
                       'last_updated desc')
        rows = cursor.fetchall()
        database.close()

        collection_list = [x[0] for x in rows]

        # add an autocompleter for collection lookup
        def collection_autocompleter(text, state):
            return [x for x in collection_list if x.startswith(text)][state]

        readline.set_completer(collection_autocompleter)
        readline.set_completer_delims('\t')
        readline.parse_and_bind('tab: complete')

        print('\nCurrent LC collections:\n')
        print('%-18.18s %-10.10s %-23.23s %-10.10s %-19.19s' %
              ('Collection ID','Is Public','Name','Objects','Last Updated'))
        for row in rows:
            print('%-18.18s %-10.10s %-23.23s %-10.10s %-19.19s' % row)

        removecoll = input('\nCollection ID to remove: ')
        if removecoll and len(removecoll.strip()) > 0:

            confirm = input('Removing collection: %s. Proceed? [y/N]' %
                            removecoll)

            if (confirm and
                len(confirm.strip()) > 0 and
                confirm.strip().lower() == 'y'):

                remove_collection_from_lcc_index(
                    args.basedir,
                    removecoll
                )

                print(
                    'Removed collection: %s from index DB. '
                    'Files in its LCC-Server directory:\n\n%s\n\nhave '
                    'NOT been touched. Remove them to permanently delete '
                    'this collection.'
                    % (removecoll,
                       os.path.abspath(os.path.join(args.basedir, removecoll)))
                )
                sys.exit(0)

            else:
                print('No collection removed.')
                sys.exit(0)

        else:
            print('No collection removed.')
            sys.exit(0)

    elif args.command == 'shell':

        print('Launching an IPython shell '
              'using LCC-Server base directory context:\n\n%s'
              % os.path.abspath(args.basedir))
        from IPython import embed

        # import some useful modules
        from .backend import abcat
        from .backend import datasets
        from .backend import dbsearch
        import pprint
        import json
        import numpy as np
        import gzip
        import glob
        import pickle
        from importlib import reload

        BD = os.path.abspath(args.basedir)

        print('\nLCC-Server modules: abcat, datasets, '
              'dbsearch have been imported automatically.')
        print('The variable BD is set to the current base directory.')
        print("You can call functions with this set, e.g.\n\n"
              ">>> pprint.pprint(dbsearch."
              "sqlite_get_collections(BD)['info'])\n")

        print("Other auto-imported modules "
              "include: numpy as np, pickle, gzip, json.")
        print("The reload() function is available as well.")
        print("Have fun!")

        embed(display_banner=None)

    else:

        print('Could not understand your command: %s' % args.command)
        sys.exit(0)



##############################
## DIRECT EXECUTION SUPPORT ##
##############################

if __name__ == '__main__':
    main()
