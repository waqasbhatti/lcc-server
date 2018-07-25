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
from textwrap import indent
from numpy import nan


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
  queried_collections text,
  query_type text,
  query_params text,
  name text,
  description text,
  citation text,
  primary key (setid)
);

-- reversed time lookup fast index
create index updated_time_idx on lcc_datasets (last_updated desc);

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
             "(setid, created_on, last_updated, nobjects, status, is_public) "
             "values (?, ?, ?, ?, ?, ?)")
    params = (setid,
              creationdt,
              creationdt,
              0,
              'initialized',
              1 if ispublic else 0)

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

    # get the columnspecs and actual collectionids for each collection searched
    # so we can return the column names and descriptions as well
    columnspec = {x:searchresult[x]['columnspec'] for x in collections}
    collid = {x:searchresult[x]['collid'] for x in collections}

    # these are the common columns across all collections. I know there's a
    # better way of flattening lists, but it's 4 AM
    columns = ','.join([','.join(list(searchresult[x]['result'][0].keys()))
                        for x in collections]).split(',')

    # total number of objects found
    nmatches = {x:searchresult[x]['nmatches'] for x in collections}

    total_nmatches = sum(searchresult[x]['nmatches'] for x in collections)

    setname = 'New dataset using collections: %s' % ', '.join(collections)
    setdesc = 'Created at %s UTC, using query: %s' % (creationdt, searchtype)

    # create the dict for the dataset pickle
    dataset = {
        'setid':setid,
        'name':setname,
        'desc':setdesc,
        'ispublic':ispublic,
        'collections':collections,
        'columns':columns,
        'result':result,
        'searchtype':searchtype,
        'searchargs':searchargs,
        'success':success,
        'lcformatkey':lcformatkey,
        'lcformatdesc':lcformatdesc,
        'columnspec':columnspec,
        'collid':collid,
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
                           shell=True, timeout=60.0,
                           stdout=subprocess.PIPE,stderr=subprocess.PIPE)
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
             "name = ?, description = ?, "
             "last_updated = ?, nobjects = ?, "
             "status = ?, dataset_shasum = ?, is_public = ?, "
             "queried_collections = ?, query_type = ?, query_params = ? "
             "where setid = ?")

    params = (
        setname,
        setdesc,
        datetime.utcnow().isoformat(),
        total_nmatches,
        'in progress',
        shasum,
        1 if ispublic else 0,
        ', '.join(collections),
        searchtype,
        json.dumps(searchargs),
        setid
    )
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

    lcfile, objectid, formatdict, convertin_opts = task
    convertopts = convertin_opts.copy()

    try:
        csvlc = abcat.convert_to_csvlc(lcfile,
                                       objectid,
                                       formatdict,
                                       **convertopts)
        LOGINFO('converted %s -> %s ok' % (lcfile, csvlc))
        return csvlc

    except Exception as e:

        return '%s conversion to CSVLC failed' % lcfile




def sqlite_make_dataset_lczip(basedir,
                              setid,
                              convert_to_csvlc=True,
                              converter_processes=4,
                              converter_csvlc_version=1,
                              converter_comment_char='#',
                              converter_column_separator=',',
                              converter_skip_converted=True,
                              override_lcdir=None,
                              link_csvlc_files=True):
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

                # we'll use this to form CSV filenames
                collection_objectidlist = [
                    x['db_oid'] for x in dataset['result'][collection]
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
                tasks = [(x, y, lcformatdict, convertopts) for x,y in
                         zip(collection_lclist, collection_objectidlist)]
                results = pool.map(csvlc_convert_worker, tasks)
                pool.close()
                pool.join()

                # if we're supposed to make links, do so here
                if link_csvlc_files:

                    # get this collection's LC directory under the LCC basedir
                    thiscoll_lcdir = os.path.join(
                        os.path.dirname(lcformatdesc),
                        'lightcurves'
                    )
                    if os.path.exists(thiscoll_lcdir):
                        for rind, rlc in enumerate(results):
                            outpath = os.path.abspath(
                                os.path.join(thiscoll_lcdir,
                                             os.path.basename(rlc))
                            )
                            if os.path.exists(outpath):
                                LOGWARNING(
                                    'not linking CSVLC: %s to %s because '
                                    ' it exists already' % (rlc, outpath)
                                )
                            elif os.path.exists(rlc):

                                LOGINFO(
                                    'linking CSVLC: %s -> %s OK' %
                                    (rlc, outpath)
                                )
                                os.symlink(rlc, outpath)

                            else:

                                LOGWARNING(
                                    'CSVLC: %s probably does not '
                                    'exist, skipping linking...' % rlc
                                )
                                # the LC won't exist, but that's fine, we'll
                                # catch it later down below

                            # put the output path into the actual results list
                            results[rind] = outpath


                # update this collection's light curve list
                for olc, nlc, dsrow in zip(collection_lclist,
                                           results,
                                           dataset['result'][collection]):

                    # make sure we don't include broken or missing LCs
                    if os.path.exists(nlc):
                        dsrow['db_lcfname'] = nlc
                        if 'lcfname' in dsrow:
                            dsrow['db_lcfname'] = nlc
                    else:
                        dsrow['db_lcfname'] = None
                        if 'lcfname' in dsrow:
                            dsrow['db_lcfname'] = None


                # update the global LC list
                dataset_lclist.extend(results)

            # we'll need to update the dataset pickle to reflect the new LC
            # locations
            dataset['lclist'] = dataset_lclist

            # write the changes to the pickle and update the SHASUM
            with gzip.open(dataset_fpath,'wb') as outfd:
                pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

            try:

                p = subprocess.run('sha256sum %s' % dataset_fpath,
                                   shell=True,
                                   timeout=60.0,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
                shasum = p.stdout.decode().split()[0]

            except Exception as e:

                LOGWARNING('could not calculate SHA256 sum for %s' %
                           dataset_fpath)
                shasum = 'warning-no-sha256sum-available'

            # update the database with the new SHASUM
            datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
            db = sqlite3.connect(
                datasets_dbf,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
            cur = db.cursor()

            # generate the entry in the lcc-datasets.sqlite table and commit it
            query = ("update lcc_datasets set "
                     "last_updated = ?, dataset_shasum = ? where setid = ?")

            params = (datetime.utcnow().isoformat(), shasum, setid)
            cur.execute(query, params)
            db.commit()
            db.close()


        # if we're not converting LCs, just update the LC locations if override
        # is provided
        else:

            # get the list of light curve files
            dataset_lclist = dataset['lclist']

            # if we're collecting from some special directory
            if override_lcdir and os.path.exists(override_lcdir):

                dataset_lclist = [os.path.join(override_lcdir,
                                               os.path.basename(x)) for x in
                                  dataset_lclist]

                # we need to override all the light curves
                for dsrow in dataset['result'][collection]:

                    dsrow['db_lcfname'] = os.path.join(
                        override_lcdir,
                        os.path.basename(dsrow['db_lcfname'])
                    )
                dataset['lclist'] = dataset_lclist

                # write the changes to the pickle and update the SHASUM
                with gzip.open(dataset_fpath,'wb') as outfd:
                    pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

                try:

                    p = subprocess.run('sha256sum %s' % dataset_fpath,
                                       shell=True,
                                       timeout=60.0,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
                    shasum = p.stdout.decode().split()[0]

                except Exception as e:

                    LOGWARNING('could not calculate SHA256 sum for %s' %
                               dataset_fpath)
                    shasum = 'warning-no-sha256sum-available'

                # update the database with the new SHASUM
                datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
                db = sqlite3.connect(
                    datasets_dbf,
                    detect_types=(
                        sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
                    )
                )
                cur = db.cursor()

                # generate the entry in the lcc-datasets.sqlite table and commit
                # it
                query = ("update lcc_datasets set "
                         "last_updated = ?, dataset_shasum = ? where setid = ?")

                params = (datetime.utcnow().isoformat(),
                          shasum, setid)
                cur.execute(query, params)
                db.commit()
                db.close()

        #
        # FINALLY, CARRY OUT THE ZIP OPERATION
        #
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
                               shell=True, timeout=60.0,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
            shasum = p.stdout.decode().split()[0]

        except Exception as e:

            LOGWARNING('could not calculate SHA256 sum for %s' % lczip_fpath)
            shasum = 'warning-no-sha256sum-available'

        # open the datasets database
        datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = db.cursor()

        # generate the entry in the lcc-datasets.sqlite table and commit it
        # once we get to this point, the dataset is finally considered complete
        query = ("update lcc_datasets set status = ?, "
                 "last_updated = ?, lczip_shasum = ? where setid = ?")

        params = ('complete', datetime.utcnow().isoformat(), shasum, setid)
        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated entry for setid: %s with LC zip SHASUM' % setid)
        return lczip_fpath

    else:

        LOGERROR('setid: %s, dataset pickle expected at %s does not exist!' %
                 (setid, dataset_fpath))
        db.close()
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

def sqlite_list_datasets(basedir,
                         nrecent=25,
                         require_status='complete',
                         require_ispublic=True):
    '''
    This just lists all the datasets available.

    setid
    created_on
    last_updated
    nobjects
    status
    is_public
    dataset_fpath
    dataset_shasum
    lczip_fpath
    lczip_shasum
    name
    description
    citation

    possible statuses:

    initialized
    in progress
    complete
    broken

    '''

    datasets_dbf = os.path.abspath(
        os.path.join(basedir, 'lcc-datasets.sqlite')
    )
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )

    # return nice dict-ish things
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    query = ("select setid, created_on, last_updated, nobjects, is_public, "
             "dataset_shasum, lczip_shasum, cpzip_shasum, pfzip_shasum, "
             "name, description, citation, "
             "queried_collections, query_type, query_params from "
             "lcc_datasets where status = ? {public_cond} "
             "order by last_updated desc limit {nrecent}")

    # make sure we never get more than 1000 recent datasets
    if nrecent > 1000:
        nrecent = 1000


    if require_ispublic:
        query = query.format(public_cond='and (is_public = 1)',
                             nrecent=nrecent)
    else:
        query = query.format(public_cond='',
                             nrecent=nrecent)

    cur.execute(query, (require_status,))
    rows = cur.fetchall()

    if rows and len(rows) > 0:

        rows = [dict(x) for x in rows]

        # we'll generate fpaths for the various products
        for row in rows:

            dataset_pickle = os.path.join(
                basedir,'datasets','dataset-%s.pkl.gz' % row['setid']
            )
            dataset_lczip = os.path.join(
                basedir,'products','lightcurves-%s.zip' % row['setid']
            )
            dataset_cpzip = os.path.join(
                basedir,'products','checkplots-%s.zip' % row['setid']
            )
            dataset_pfzip = os.path.join(
                basedir,'products','pfresults-%s.zip' % row['setid']
            )

            if os.path.exists(dataset_pickle):
                row['dataset_fpath'] = dataset_pickle
            else:
                row['dataset_fpath'] = None

            if os.path.exists(dataset_lczip):
                row['lczip_fpath'] = dataset_lczip
            else:
                row['lczip_fpath'] = None

            if os.path.exists(dataset_cpzip):
                row['cpzip_fpath'] = dataset_cpzip
            else:
                row['cpzip_fpath'] = None

            if os.path.exists(dataset_pfzip):
                row['pfzip_fpath'] = dataset_pfzip
            else:
                row['pfzip_fpath'] = None

        # this is the returndict
        returndict = {
            'status':'ok',
            'result':rows,
            'message':'found %s datasets in total' % len(rows)
        }

    else:

        returndict = {
            'status':'failed',
            'result': None,
            'message':'found no datasets matching query parameters'
        }

    db.close()
    return returndict



def sqlite_get_dataset(basedir,
                       setid,
                       returnjson=False,
                       generatecsv=True,
                       forcecomplete=False):
    '''This gets the dataset as a dictionary and optionally as JSON.

    If generatecsv is True, we'll generate a CSV for the dataset table and write
    it to the products directory (or retrieve it from cache if it exists
    already).

    if forcecomplete, we'll force-set the dataset status to complete. this can
    be useful when there's too many LCs to zip and we don't want to wait around
    for this.

    Returns a dict generated from the dataset pickle.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    dataset_pickle = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_pickle)

    # get the lczip, cpzip, pfzip, dataset pkl shasums from the DB
    # also get the created_on, last_updated, nobjects, status
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    if not os.path.exists(dataset_fpath):

        LOGWARNING('expected dataset pickle does not exist for setid: %s' %
                   setid)

        # if the dataset pickle doesn't exist, check the DB for its status
        query = ("select created_on, last_updated, is_public, status from "
                 "lcc_datasets where setid = ?")
        cur.execute(query, (setid,))
        row = cur.fetchone()

        if row and len(row) > 0:

            # check the dataset's status

            # this should only be initialized if the dataset pickle doesn't
            # exist
            if row[-1] == 'initialized':

                dataset_status = 'in progress'

            # if the status is anything else, the dataset is in an unknown
            # state, and is probably broken
            else:

                dataset_status = 'broken'

            returndict = {
                'setid': setid,
                'created_on':row[0],
                'last_updated':row[1],
                'nobjects':0,
                'status': dataset_status,
                'name':None,
                'desc':None,
                'ispublic': row[-2],
                'columns':None,
                'searchtype':None,
                'searchargs':None,
                'lczip':None,
                'cpzip':None,
                'pfzip':None,
                'lczip_shasum':None,
                'pfzip_shasum':None,
                'cpzip_shasum':None,
                'dataset_shasum':None,
                'dataset_csv':None,
                'csv_shasum':None,
                'collections':None,
                'result':None,
            }

            LOGWARNING("dataset: %s is in state: %s" % (setid, dataset_status))

            db.close()
            return returndict

        # if no dataset entry in the DB, then this DS doesn't exist at all
        else:

            LOGERROR('requested dataset: %s does not exist' % setid)
            return None

    #
    # otherwise, proceed as normal
    #

    # read in the pickle
    with gzip.open(dataset_fpath,'rb') as infd:
        dataset = pickle.load(infd)

    returndict = {
        'setid':dataset['setid'],
        'name':dataset['name'],
        'desc':dataset['desc'],
        'ispublic':dataset['ispublic'],
        'columns':dataset['columns'],
        'searchtype':dataset['searchtype'],
        'searchargs':dataset['searchargs'],
        'lczip':dataset['lczipfpath'],
        'cpzip':dataset['cpzipfpath'],
        'pfzip':dataset['pfzipfpath'],
    }

    query = ("select created_on, last_updated, nobjects, status, "
             "lczip_shasum, cpzip_shasum, pfzip_shasum, dataset_shasum "
             "from lcc_datasets where setid = ?")
    params = (dataset['setid'],)
    cur.execute(query, params)
    row = cur.fetchone()
    db.close()

    # update these in the returndict
    returndict['created_on'] = row[0]
    returndict['last_updated'] = row[1]
    returndict['nobjects'] = row[2]
    returndict['status'] = row[3]
    returndict['lczip_shasum'] = row[4]
    returndict['cpzip_shasum'] = row[5]
    returndict['pfzip_shasum'] = row[6]
    returndict['dataset_shasum'] = row[7]

    # the results are per-collection
    returndict['collections'] = dataset['collections']
    returndict['result'] = {}

    for coll in dataset['collections']:

        returndict['result'][coll] = {'data':dataset['result'][coll],
                                      'success':dataset['success'][coll],
                                      'message':dataset['message'][coll],
                                      'nmatches':dataset['nmatches'][coll],
                                      'columnspec':dataset['columnspec'][coll],
                                      'collid':dataset['collid'][coll]}

    # make the CSV if told to do so
    if generatecsv:

        csv = generate_dataset_csv(
            basedir,
            returndict,
        )
        returndict['dataset_csv'] = csv

        # get the SHASUM of the CSV
        try:
            p = subprocess.run('sha256sum %s' % csv,
                               shell=True, timeout=60.0,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
            shasum = p.stdout.decode().split()[0]
            returndict['csv_shasum'] = shasum

        except Exception as e:

            LOGWARNING('could not calculate SHA256 sum for %s' % csv)
            shasum = 'warning-no-sha256sum-available'
            returndict['csv_shasum'] = shasum

    else:
        returndict['dataset_csv'] = None
        returndict['csv_shasum'] = None

    # if we're told to force complete the dataset, do so here
    if forcecomplete and returndict['status'] != 'complete':

        datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = db.cursor()
        query = ("update lcc_datasets set last_updated = ?, "
                 "nobjects = ?, status = ? where setid = ?")
        params = (datetime.utcnow().isoformat(),
                  sum(dataset['nmatches'][x] for x in dataset['collections']),
                  'complete',
                  dataset['setid'])
        cur.execute(query, params)
        db.commit()
        db.close()

        if not os.path.exists(returndict['lczip']):
            returndict['lczip'] = None
        if not os.path.exists(returndict['cpzip']):
            returndict['cpzip'] = None
        if not os.path.exists(returndict['pfzip']):
            returndict['pfzip'] = None

        # blank out the original LCs
        for collection in returndict['collections']:
            for row in returndict['result'][collection]['data']:
                if 'db_lcfname' in row:
                    row['db_lcfname'] = None
                if 'lcfname' in row:
                    row['lcfname'] = None

        LOGWARNING("forced 'complete' status for '%s' dataset: %s" %
                   (returndict['status'], returndict['setid']))

    elif forcecomplete and returndict['status'] == 'complete':
        LOGERROR('not going to force completion '
                 'for an already complete dataset: %s'
                 % returndict['setid'])

    if returnjson:

        retjson = json.dumps(returndict)
        retjson = retjson.replace('nan','null')

        return retjson

    else:

        return returndict



def generate_dataset_csv(
        basedir,
        in_dataset,
        force=False,
        separator='|',
        comment='#',
):
    '''
    This generates a CSV for the dataset's data table.

    Requires the output from sqlite_get_dataset or postgres_get_dataset.

    '''

    dataset = in_dataset.copy()
    productdir = os.path.abspath(os.path.join(basedir,
                                              'datasets'))
    setid = dataset['setid']
    dataset_csv = os.path.join(productdir,'dataset-%s.csv' % setid)

    if os.path.exists(dataset_csv) and not force:

        return dataset_csv

    else:

        setcols = dataset['columns']

        # FIXME: we need to get columnspec per collection
        # FIXME: this should be the same for each collection
        # FIXME: but this might break later
        firstcoll = dataset['collections'][0]
        colspec = dataset['result'][firstcoll]['columnspec']

        # generate the header JSON now
        header = {
            'setid':setid,
            'created':'%sZ' % dataset['created_on'],
            'updated':'%sZ' % dataset['last_updated'],
            'public':dataset['ispublic'],
            'searchtype':dataset['searchtype'],
            'searchargs':dataset['searchargs'],
            'collections':dataset['collections'],
            'columns':setcols[::],
            'nobjects':dataset['nobjects'],
            'coldesc':{}
        }

        # generate the format string from here
        formspec = []

        # go through each column and get its info from colspec
        # also build up the format string for the CSV
        for col in setcols:

            header['coldesc'][col] = {
                'desc': colspec[col]['description'],
                'dtype': colspec[col]['dtype']
            }
            formspec.append(colspec[col]['format'])

        # there's an extra collection column needed for the CSV
        formspec.append('%s')
        header['columns'].append('collection')
        header['coldesc']['collection'] = {
            'desc':'LC collection of this object',
            'dtype':'U60'
        }

        # generate the JSON header for the CSV
        csvheader = json.dumps(header, indent=2)
        csvheader = indent(csvheader, '%s ' % comment)

        # finalize the formspec
        formstr = separator.join(formspec)

        # write to the output file now
        with open(dataset_csv,'wb') as outfd:

            # write the header first
            outfd.write(('%s\n' % csvheader).encode())

            # we'll go by collection_id first, then by entry
            for collid in dataset['collections']:

                for entry in dataset['result'][collid]['data']:

                    # censor the light curve filenames
                    # also make sure the actual files exist, otherwise,
                    # return nothing for those entries
                    if 'db_lcfname' in entry:

                        if (entry['db_lcfname'] is not None and
                            os.path.exists(entry['db_lcfname'])):

                            entry['db_lcfname'] = entry['db_lcfname'].replace(
                                os.path.abspath(basedir),
                                '/l'
                            )
                        else:
                            entry['db_lcfname'] = 'missing'

                    if 'lcfname' in entry:

                        if (entry['lcfname'] is not None and
                            os.path.exists(entry['lcfname'])):

                            entry['lcfname'] = entry['lcfname'].replace(
                                os.path.abspath(basedir),
                                '/l'
                            )
                        else:
                            entry['lcfname'] = 'missing'

                    # do the formatting more carefully
                    row = []
                    for ic, col in enumerate(setcols):

                        if 'f' in formspec[ic] and entry[col] is None:
                            row.append(nan)

                        # at some point, numpy started complaining about nans
                        # not being convertible to integers
                        elif 'i' in formspec[ic] and entry[col] is None:
                            row.append(-9999)

                        else:
                            row.append(entry[col])

                    row.append(collid)
                    rowstr = formstr % tuple(row)
                    outfd.write(('%s\n' % rowstr).encode())

        LOGINFO('wrote CSV: %s for dataset: %s' % (dataset_csv, setid))
        return dataset_csv



def generate_dataset_tablerows(
        basedir,
        in_dataset,
        headeronly=False,
        strformat=False,
):
    '''
    This generates row elements useful direct insert into an HTML template.

    Requires the output from sqlite_get_dataset or postgres_get_dataset.

    '''

    dataset = in_dataset.copy()
    setid = dataset['setid']
    setcols = dataset['columns']

    # FIXME: we need to get columnspec per collection
    # FIXME: this should be the same for each collection
    # FIXME: but this might break later
    firstcoll = dataset['collections'][0]
    colspec = dataset['result'][firstcoll]['columnspec']

    # generate the header JSON now
    header = {
        'setid':setid,
        'status':dataset['status'],
        'created':'%sZ' % dataset['created_on'],
        'updated':'%sZ' % dataset['last_updated'],
        'public':dataset['ispublic'],
        'searchtype':dataset['searchtype'],
        'searchargs':dataset['searchargs'],
        'collections':dataset['collections'],
        'columns':setcols[::],
        'nobjects':dataset['nobjects'],
        'coldesc':{}
    }

    # go through each column and get its info from colspec
    # also build up the format string for the CSV
    for col in setcols:

        header['coldesc'][col] = {
            'title': colspec[col]['title'],
            'desc': colspec[col]['description'],
            'dtype': colspec[col]['dtype'],
            'format': colspec[col]['format'],
        }

    # there's an extra collection column needed for the CSV
    header['columns'].append('collection')
    header['coldesc']['collection'] = {
        'title':'LC collection',
        'desc':'LC collection of this object',
        'dtype':'U60',
        'format':'%s',
    }

    if headeronly:
        return header

    table_rows = []

    # we'll go by collection_id first, then by entry
    for collid in dataset['collections']:
        for entry in dataset['result'][collid]['data']:

            # censor the light curve filenames
            # also make sure the actual files exist, otherwise,
            # return nothing for those entries
            if 'db_lcfname' in entry:

                if (entry['db_lcfname'] is not None and
                    os.path.exists(entry['db_lcfname'])):

                    entry['db_lcfname'] = entry['db_lcfname'].replace(
                        os.path.abspath(basedir),
                        '/l'
                    )
                else:
                    entry['db_lcfname'] = None

            if 'lcfname' in entry:

                if (entry['lcfname'] is not None and
                    os.path.exists(entry['lcfname'])):

                    entry['lcfname'] = entry['lcfname'].replace(
                        os.path.abspath(basedir),
                        '/l'
                    )
                else:
                    entry['lcfname'] = None

            if strformat:

                row = []

                for col in setcols:

                    if col in ('lcfname', 'db_lcfname'):
                        if entry[col] is not None:
                            row.append('<a href="%s">download light curve</a>' %
                                       entry[col])
                        else:
                            row.append('<span class="text-danger">'
                                       'unavailable or missing</span>')

                    # take care with nans, Nones, and missing integers
                    else:

                        colform = header['coldesc'][col]['format']
                        if 'f' in colform and entry[col] is None:
                            row.append('nan')
                        elif 'i' in colform and entry[col] is None:
                            row.append('-9999')
                        else:
                            row.append(colform % entry[col])

            else:
                row = [entry[col] for col in setcols]

            row.append(collid)
            table_rows.append(row)

    return header, table_rows



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
