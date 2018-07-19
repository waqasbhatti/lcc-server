#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''datasets.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - May 2018
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
import os.path
import sqlite3
import pickle
import secrets
import gzip
from zipfile import ZipFile
import json
import subprocess
from multiprocessing import Pool


from . import dbsearch
dbsearch.set_logger_parent(__name__)

from . import abcat
abcat.set_logger_parent(__name__)

#########################################
## INITIALIZING A DATASET INDEX SQLITE ##
#########################################

SQLITE_DATASET_CREATE = '''\
-- make the main table

create table lcc_datasets (
  setid text not null,
  created_on datetime not null,
  last_updated datetime not null,
  nobjects integer not null,
  status text not null,
  is_public integer,
  lczip_shasum text,
  cpzip_shasum text,
  pfzip_shasum text,
  dataset_shasum text,
  name text,
  description text,
  citation text,
  primary key (setid)
);

-- set the WAL mode on
pragma journal_mode = wal;
pragma journal_size_limit = 52428800;
'''

def sqlite_datasets_db_create(basedir):
    '''
    This makes a new datasets DB in basedir.

    Most of the information is stored in the dataset pickle itself.

    '''

    # make the datasets database
    datasets_dbf = os.path.abspath(
        os.path.join(basedir, 'lcc-datasets.sqlite')
    )
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    cur.executescript(SQLITE_DATASET_CREATE)
    db.commit()

    db.close()

    return datasets_dbf



########################################
## FUNCTIONS THAT OPERATE ON DATASETS ##
########################################

def sqlite_prepare_dataset(basedir,
                           ispublic=True):
    '''
    This generates a setid to use for the next step below.

    datasets can have the following statuses:

    'initialized'
    'complete'
    'failed'

    '''

    # open the datasets database
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    # generate an 8-byte random token for use as the setid
    setid = secrets.token_urlsafe(8).replace('-','Z').replace('_','a')
    creationdt = datetime.utcnow().isoformat()

    # update the database to prepare for this new dataset
    query = ("insert into lcc_datasets "
             "(setid, created_on, last_updated, nobjects, status) "
             "values (?, ?, ?, ?, ?)")
    params = (setid,
              creationdt,
              creationdt,
              0,
              'initialized')

    cur.execute(query, params)
    db.commit()
    db.close()

    return setid, creationdt



def sqlite_new_dataset(basedir,
                       setid,
                       creationdt,
                       searchresult,
                       ispublic=True):
    '''
    create new dataset function.

    ispublic controls if the dataset is public

    this produces a pickle that goes into /datasets/random-set-id.pkl

    and also produces an entry that goes into the lcc-datasets.sqlite DB

    the pickle has the following structure:

    {'setid': the randomly generated set id,
     'name': the name of the dataset or None,
     'desc': a description of the dataset or None,
     'ispublic': boolean indicating if the dataset is public, default True,
     'collections': the names of the collections making up this dataset,
     'columns': a list of the columns in the search result,
     'result': a list containing the search result lists of dicts / collection,
     'searchtype': what kind of search produced this dataset,
     'searchargs': the args dict from the search result dict,
     'success': the boolean from the search result dict
     'message': the message from the search result dict,
     'lczipfpath': the path to the light curve ZIP in basedir/products/,
     later: 'cpzipfpath': path to the checkplot ZIP in basedir/products,
     later: 'pfzipfpath': path to the periodfinding ZIP in basedir/products,}


    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    productdir = os.path.abspath(os.path.join(basedir, 'products'))

    # get some stuff out of the search result
    collections = searchresult['databases']
    columns = searchresult['args']['getcolumns']

    # the result should be a dict with key:val = collection:result
    result = {x:searchresult[x]['result'] for x in collections}

    searchtype = searchresult['search']
    searchargs = searchresult['args']

    # success and message is a dict for each collection searched
    success = {x:searchresult[x]['success'] for x in collections}
    message = {x:searchresult[x]['message'] for x in collections}

    # get the LC formats for each collection searched so we can reform LCs into
    # a common format later on
    lcformatkey = {x:searchresult[x]['lcformatkey'] for x in collections}
    lcformatdesc = {x:searchresult[x]['lcformatdesc'] for x in collections}

    # total number of objects found
    nmatches = {x:searchresult[x]['nmatches'] for x in collections}

    total_nmatches = sum(searchresult[x]['nmatches'] for x in collections)

    # create the dict for the dataset pickle
    dataset = {
        'setid':setid,
        'name':'New dataset',
        'desc':'Created at %s' % creationdt,
        'ispublic':ispublic,
        'collections':collections,
        'columns':columns,
        'result':result,
        'searchtype':searchtype,
        'searchargs':searchargs,
        'success':success,
        'lcformatkey':lcformatkey,
        'lcformatdesc':lcformatdesc,
        'message':message,
        'nmatches':nmatches
    }

    # generate the dataset pickle filepath
    dataset_fname = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_fname)

    # generate the name of the lczipfile
    lczip_fname = 'lightcurves-%s.zip' % setid
    lczip_fpath = os.path.join(productdir, lczip_fname)

    # generate the name of the cpzipfile
    cpzip_fname = 'checkplots-%s.zip' % setid
    cpzip_fpath = os.path.join(productdir, cpzip_fname)

    # generate the name of the pfzipfile
    pfzip_fname = 'pfresults-%s.zip' % setid
    pfzip_fpath = os.path.join(productdir, pfzip_fname)

    # put these into the dataset dict
    dataset['lczipfpath'] = lczip_fpath
    dataset['cpzipfpath'] = cpzip_fpath
    dataset['pfzipfpath'] = pfzip_fpath

    # get the list of all light curve files for this dataset
    dataset_lclist = []

    for collection in collections:

        thiscoll_lclist = [x['db_lcfname'] for x in result[collection]]
        dataset_lclist.extend(thiscoll_lclist)

    dataset['lclist'] = dataset_lclist


    # write the pickle to the datasets directory
    with gzip.open(dataset_fpath,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset pickle for search results to %s, setid: %s' %
            (dataset_fpath, setid))

    # generate the SHA256 sum for the written file
    try:
        p = subprocess.run('sha256sum %s' % dataset_fpath,
                           shell=True, timeout=60.0, capture_output=True)
        shasum = p.stdout.decode().split()[0]

    except Exception as e:

        LOGWARNING('could not calculate SHA256 sum for %s' % dataset_fpath)
        shasum = 'warning-no-sha256sum-available'

    # open the datasets database
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    # generate the entry in the lcc-datasets.sqlite table and commit it
    query = ("update lcc_datasets set "
             "last_updated = ?, nobjects = ?, "
             "status = ?, dataset_shasum = ?")

    params = (datetime.utcnow().isoformat(),
              total_nmatches,
              'complete',
              shasum)
    cur.execute(query, params)
    db.commit()
    db.close()

    LOGINFO('updated entry for setid: %s, total nmatches: %s' %
            (setid, total_nmatches))

    # return the setid
    return setid


############################################
## FUNCTIONS THAT DEAL WITH LC COLLECTION ##
############################################

def csvlc_convert_worker(task):
    '''
    This is a worker for the function below.

    '''

    lcfile, formatdict, convertopts = task

    try:
        csvlc = abcat.convert_to_csvlc(lcfile,
                                       formatdict,
                                       **convertopts)
        LOGINFO('converted %s -> %s ok' % (lcfile, csvlc))
        return csvlc
    except Exception as e:
        LOGEXCEPTION('failed to convert %s' % lcfile)
        return '%s conversion to CSVLC failed' % lcfile



def sqlite_make_dataset_lczip(basedir,
                              setid,
                              convert_to_csvlc=True,
                              converter_processes=4,
                              converter_csvlc_version=1,
                              converter_comment_char='#',
                              converter_column_separator=',',
                              converter_skip_converted=True,
                              override_lcdir=None):
    '''
    This makes a zip file for the light curves in the dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))

    # look in the datasetdir for the dataset pickle
    dataset_fpath = os.path.join(datasetdir, 'dataset-%s.pkl.gz' % setid)

    if os.path.exists(dataset_fpath):

        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)

        # if we're supposed to regen the LCs into a common format, do so here
        if convert_to_csvlc:

            dataset_lclist = []

            # we'll do this by collection
            for collection in dataset['collections']:

                # load the format description
                lcformatdesc = dataset['lcformatdesc'][collection]
                lcformatdict = abcat.get_lcformat_description(
                    lcformatdesc
                )
                convertopts = {'csvlc_version':converter_csvlc_version,
                               'comment_char':converter_comment_char,
                               'column_separator':converter_column_separator,
                               'skip_converted':converter_skip_converted}

                collection_lclist = [
                    x['db_lcfname'] for x in dataset['result'][collection]
                ]

                # handle the lcdir override if present
                if override_lcdir and os.path.exists(override_lcdir):
                    collection_lclist = [
                        os.path.join(override_lcdir,
                                     os.path.basename(x))
                        for x in collection_lclist
                    ]

                # now, we'll convert these light curves in parallel
                pool = Pool(converter_processes)
                tasks = [(x, lcformatdict, convertopts) for x in
                         collection_lclist]
                results = pool.map(csvlc_convert_worker, tasks)
                pool.close()
                pool.join()

                dataset_lclist.extend(results)

        else:

            # get the list of light curve files
            dataset_lclist = dataset['lclist']

            # if we're collecting from some special directory
            if override_lcdir and os.path.exists(override_lcdir):

                dataset_lclist = [os.path.join(override_lcdir,
                                               os.path.basename(x)) for x in
                                  dataset_lclist]


        zipfile_lclist = {os.path.basename(x):'ok' for x in dataset_lclist}

        # get the expected name of the output zipfile
        lczip_fpath = dataset['lczipfpath']

        LOGINFO('writing %s LC files to zip file: %s for setid: %s...' %
                (len(dataset_lclist), lczip_fpath, setid))

        # set up the zipfile
        with ZipFile(lczip_fpath, 'w', allowZip64=True) as outzip:
            for lcf in dataset_lclist:
                if os.path.exists(lcf):
                    outzip.write(lcf, os.path.basename(lcf))
                else:
                    zipfile_lclist[os.path.basename(lcf)] = 'missing'

            # add the manifest to the zipfile
            outzip.writestr('lczip-manifest.json',
                            json.dumps(zipfile_lclist,
                                       ensure_ascii=True,
                                       indent=2))

        LOGINFO('done, zip written successfully.')

        try:

            p = subprocess.run('sha256sum %s' % lczip_fpath,
                               shell=True, timeout=60.0, capture_output=True)
            shasum = p.stdout.decode().split()[0]

        except Exception as e:

            LOGWARNING('could not calculate SHA256 sum for %s' % dataset_fpath)
            shasum = 'warning-no-sha256sum-available'

        # open the datasets database
        datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = db.cursor()

        # generate the entry in the lcc-datasets.sqlite table and commit it
        query = ("update lcc_datasets set "
                 "last_updated = ?, lczip_shasum = ?")

        params = (datetime.utcnow().isoformat(), shasum)
        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated entry for setid: %s with LC zip SHASUM' % setid)
        return lczip_fpath

    else:

        LOGERROR('setid: %s, dataset pickle expected at %s does not exist!' %
                 (setid, dataset_fpath))
        return None



# LATER
def sqlite_remove_dataset(basedir, setid):
    '''
    This removes the specified dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



# LATER
def sqlite_update_dataset(basedir, setid, updatedict):
    '''
    This updates a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



######################################
## LISTING AND GETTING DATASET INFO ##
######################################

def sqlite_list_datasets(basedir, require_ispublic=True):
    '''
    This just lists all the datasets available.

    '''

    datasets_dbf = os.path.abspath(
        os.path.join(basedir, 'lcc-datasets.sqlite')
    )
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()




def sqlite_get_dataset(basedir, setid, returnjson=False):
    '''
    This gets the dataset as a dictionary and optionally as JSON.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



################################################################
## FUNCTIONS THAT WRAP DBSEARCH FUNCTIONS AND RETURN DATASETS ##
################################################################

# ALL LATER

def sqlite_dataset_fulltext_search(basedir,
                                   ftsquerystr,
                                   getcolumns=None,
                                   extraconditions=None,
                                   lcclist=None,
                                   require_ispublic=True):
    '''
    This does a full-text search and returns a dataset.

    '''


def sqlite_dataset_column_search(basedir,
                                 getcolumns=None,
                                 conditions=None,
                                 sortby=None,
                                 limit=None,
                                 lcclist=None,
                                 require_ispublic=True):
    '''
    This does a column search and returns a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_dataset_sql_search(basedir,
                              sqlstatement,
                              lcclist=None,
                              require_ispublic=True):
    '''
    This does an arbitrary SQL search and returns a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_dataset_kdtree_conesearch(basedir,
                                     center_ra,
                                     center_decl,
                                     radius_arcmin,
                                     getcolumns=None,
                                     extraconditions=None,
                                     lcclist=None,
                                     require_ispublic=True,
                                     conesearchworkers=1):
    '''
    This does a cone-search and returns a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_xmatch_search(basedir,
                         inputdata,
                         xmatch_dist_arcsec=3.0,
                         xmatch_closest_only=False,
                         inputmatchcol=None,
                         dbmatchcol=None,
                         getcolumns=None,
                         extraconditions=None,
                         lcclist=None,
                         require_ispublic=None,
                         max_matchradius_arcsec=30.0):
    '''This does an xmatch between the input and LCC databases and returns a
    dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



#####################################
## SEARCHING FOR STUFF IN DATASETS ##
#####################################
