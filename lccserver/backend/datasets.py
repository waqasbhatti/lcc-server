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
from multiprocessing import Pool
from textwrap import indent
from functools import reduce
import hashlib

from . import dbsearch
dbsearch.set_logger_parent(__name__)

from . import abcat
abcat.set_logger_parent(__name__)

#########################################
## INITIALIZING A DATASET INDEX SQLITE ##
#########################################

SQLITE_DATASET_CREATE = '''\
-- think about adding some more useful columns
-- user_sessionid -> salted hash of the user's session token
-- user_metadata_json -> info from user (e.g. who the dataset is shared with)
-- objectid_list  -> list of all object IDs in this dataset (for dataset FTS)
-- convex_hull_polygon -> for queries like "all datasets in this sky region"

-- this is the main table
create table lcc_datasets (
  setid text not null,
  created_on datetime not null,
  last_updated datetime not null,
  nobjects integer not null,
  status text not null,
  is_public integer,
  lczip_cachekey text,
  queried_collections text,
  query_type text,
  query_params text,
  name text,
  description text,
  citation text,
  primary key (setid)
);

-- reversed and forward time lookup fast indexes
create index fwd_time_idx on lcc_datasets (last_updated asc);
create index rev_time_idx on lcc_datasets (last_updated desc);

-- index on the dataset cache key
create index lczip_cachekey_idx on lcc_datasets (lczip_cachekey);

-- index on the ispublic item
create index ispublic_idx on lcc_datasets (is_public);


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
     'lczipfpath': the path to the light curve ZIP in basedir/products/}


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

    xcolumns = []
    for coll in collections:
        try:
            coll_columns = list(searchresult[coll]['result'][0].keys())
            xcolumns.append(set(coll_columns))
        except Exception as e:
            pass

    # xcolumns is now a list of sets of column keys from all collections.
    # we can only display columns that are common to all collections, so we need
    # to do a reduce operation on set intersections of all of these sets.
    columns = reduce(lambda x,y: x.intersection(y), xcolumns)
    columns = list(columns)

    # we want to return the columns in the order they were requested, so we need
    # to reorder them here
    reqcols = searchargs['getcolumns']
    for c in columns:
        if c not in reqcols:
            reqcols.append(c)

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
        'columns':reqcols,  # these are the columns guaranteed to be in all
                            # collections
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

    # put these into the dataset dict
    dataset['lczipfpath'] = lczip_fpath


    # write the pickle to the datasets directory
    with gzip.open(dataset_fpath,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset pickle for search results to %s, setid: %s' %
            (dataset_fpath, setid))

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
             "status = ?, is_public = ?, "
             "queried_collections = ?, query_type = ?, query_params = ? "
             "where setid = ?")

    params = (
        setname,
        setdesc,
        datetime.utcnow().isoformat(),
        total_nmatches,
        'in progress',
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

    lcfile, objectid, formatjson, convertin_opts = task
    convertopts = convertin_opts.copy()
    formatdict = abcat.get_lcformat_description(formatjson)

    try:
        csvlc = abcat.convert_to_csvlc(lcfile,
                                       objectid,
                                       formatdict,
                                       **convertopts)
        LOGINFO('converted %s -> %s ok' % (lcfile, csvlc))
        return csvlc

    except Exception as e:

        return '%s conversion to CSVLC failed' % os.path.basename(lcfile)




def generate_lczip_cachekey(lczip_lclist):
    '''
    This generates the cache key for an LCZIP based on its LC list.

    '''

    sorted_lclist_json = json.dumps(lczip_lclist)
    cachekey = hashlib.sha256(sorted_lclist_json.encode()).hexdigest()

    return cachekey



def sqlite_make_dataset_lczip(basedir,
                              setid,
                              converter_processes=4,
                              converter_csvlc_version=1,
                              converter_comment_char='#',
                              converter_column_separator=',',
                              converter_skip_converted=True,
                              override_lcdir=None,
                              max_dataset_lcs=20000):
    '''
    This makes a zip file for the light curves in the dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')

    # look in the datasetdir for the dataset pickle
    dataset_fpath = os.path.join(datasetdir, 'dataset-%s.pkl.gz' % setid)

    if os.path.exists(dataset_fpath):

        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)


        # get the list of original format light curves for this dataset
        # we'll use these as the basis of the cache key
        dataset_original_lclist = []

        for coll in dataset['collections']:

            thiscoll_lclist = [
                x['db_lcfname'] for x in dataset['result'][coll]
            ]
            dataset_original_lclist.extend(thiscoll_lclist)

        dataset_original_lclist = sorted(dataset_original_lclist)
        dataset_lczip_cachekey = generate_lczip_cachekey(
            dataset_original_lclist
        )

        # check the cachekey against the database to see if a dataset with
        # identical LCs has already been collected
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = db.cursor()

        # this will fetch the original set LC zip if there are multiple links to
        # it for many datasets
        cur.execute("select setid, lczip_cachekey from lcc_datasets where "
                    "lczip_cachekey = ? order by last_updated asc limit 1",
                    (dataset_lczip_cachekey,))

        row = cur.fetchone()
        db.close()

        # if LCs corresponding to the ones included in this dataset are found in
        # another dataset, then we'll symlink that dataset's LC ZIP to
        # products/lightcurves-[this setid].zip, and call it a day
        skip_lc_collection = False

        if row and len(row) > 0:

            other_setid, other_dataset_cachekey = row
            other_lczip = os.path.join(basedir,
                                       'products',
                                       'lightcurves-%s.zip' % other_setid)

            if os.path.exists(other_lczip):

                # we'll remove the current dataset LC zip in favor of the other
                # one every time
                if os.path.exists(dataset['lczipfpath']):
                    LOGWARNING('overwriting existing symlink %s, '
                               'which points to actual file: %s' %
                               (dataset['lczipfpath'],
                                os.path.abspath(other_lczip)))
                    os.remove(dataset['lczipfpath'])

                os.symlink(os.path.abspath(other_lczip),
                           dataset['lczipfpath'])
                skip_lc_collection = True

            else:

                # FIXME: LC conversion from original format -> CSV LCs is
                # currently triggered if the ZIP doesn't exist. This means that
                # the process for converting original format LCs to CSV LCs in
                # the basedir/csvlcs/[collection] directory will kick off even
                # if there are more than 20,000 light curves requested for this
                # dataset. Normally, this should be a fast operation because
                # we'll have converted all of the original format LCs beforehand
                # and linked them into the csvlcs directory, but if this is not
                # the case, then this operation will continue for a long time
                # and block the 'complete' status of the accompanying
                # dataset.

                # What should we do about this? We could set skip_lc_collection
                # to True below, but that risks leaving some light curves in an
                # unconverted state if this wasn't done before hand. If we do
                # set skip_lc_collection = True below, then we should also send
                # some sort of warning back to the calling function that
                # indicates that we gave up instead of collecting LCs.

                # I think the only decent option here is to keep
                # skip_lc_collection False in this pathological case. When the
                # user comes back to the dataset page after a long time,
                # everything should be OK because there were too many objects to
                # collect LCs for, but the relevant CSVs and data table rows
                # will have the correct links to individual LCs and should be
                # OK.

                # Actually, the problem becomes hugely apparent if the query
                # itself goes to the background instead of just the LC
                # zipping. We'll fix this in searchserver_handlers, so it uses
                # the same logic as the timeout for the > 20k LCs problem.

                skip_lc_collection = False

        # only collect LCs if we have to, we'll use the already generated link
        # if we have the same LCs collected already somewhere else
        if not skip_lc_collection:

            LOGINFO('no cached LC zip found for dataset: %s, regenerating...' %
                    setid)

            dataset_lclist = []

            # we'll do this by collection
            for collection in dataset['collections']:

                # load the format description
                lcformatdesc = dataset['lcformatdesc'][collection]
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
                tasks = [(x, y, lcformatdesc, convertopts) for x,y in
                         zip(collection_lclist, collection_objectidlist)]
                results = pool.map(csvlc_convert_worker, tasks)
                pool.close()
                pool.join()

                #
                # link the generated CSV LCs to the output directory
                #

                # get this collection's output LC directory under the LCC
                # basedir basedir/csvlcs/<collection>/lightcurves/<lcfname>
                thiscoll_lcdir = os.path.join(
                    basedir,
                    'csvlcs',
                    os.path.dirname(lcformatdesc).split('/')[-1],
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
                            os.symlink(os.path.abspath(rlc), outpath)

                        else:

                            LOGWARNING(
                                '%s probably does not '
                                'exist, skipping linking...' % rlc
                            )
                            # the LC won't exist, but that's fine, we'll
                            # catch it later down below

                        # put the output path into the actual results list
                        results[rind] = outpath


                # update this collection's light curve list
                for nlc, dsrow in zip(results,
                                      dataset['result'][collection]):

                    # make sure we don't include broken or missing LCs
                    if os.path.exists(nlc):
                        dsrow['db_lcfname'] = nlc
                        if 'lcfname' in dsrow:
                            dsrow['lcfname'] = nlc
                    else:
                        dsrow['db_lcfname'] = None
                        if 'lcfname' in dsrow:
                            dsrow['lcfname'] = None


                # update the global LC list
                dataset_lclist.extend(results)

            #
            # update the dataset pickle with the new light curve locations
            #
            with gzip.open(dataset_fpath,'wb') as outfd:
                pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)
            LOGINFO('updated dataset pickle after LC collection completed')

            #
            # FINALLY, CARRY OUT THE ZIP OPERATION (IF NEEDED)
            #
            zipfile_lclist = {os.path.basename(x):'ok' for x in dataset_lclist}

            # if there are too many LCs to collect, bail out
            if len(dataset_lclist) > max_dataset_lcs:

                LOGERROR('LCS in dataset: %s > max_dataset_lcs: %s, '
                         'not making a zip file' % (len(dataset_lclist),
                                                    max_dataset_lcs))

            # otherwise, run the collection
            else:

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


        # if we don't need to collect LCs, then we can just re-use the other
        # dataset's LC ZIP
        else:

            LOGINFO("re-using identical LC collected "
                    "ZIP from dataset: %s at %s, "
                    "symlinked to this dataset's LC ZIP: %s" %
                    (other_setid, other_lczip, dataset['lczipfpath']))

            # update the final LC locations in this case as well
            for collection in dataset['collections']:

                lcformatdesc = dataset['lcformatdesc'][collection]
                # get this collection's output LC directory under the LCC
                # basedir basedir/csvlcs/<collection>/lightcurves/<lcfname>
                thiscoll_lcdir = os.path.join(
                    basedir,
                    'csvlcs',
                    os.path.dirname(lcformatdesc).split('/')[-1],
                )

                # update the output filename
                for dsrow in dataset['result'][collection]:

                    if dsrow['db_lcfname']:
                        dsrow['db_lcfname'] = os.path.join(thiscoll_lcdir,
                                                           '%s-csvlc.gz' %
                                                           dsrow['db_oid'])
                        if 'lcfname' in dsrow:
                            dsrow['lcfname'] = os.path.join(thiscoll_lcdir,
                                                            '%s-csvlc.gz' %
                                                            dsrow['db_oid'])
                    else:
                        dsrow['db_lcfname'] = None
                        if 'lcfname' in dsrow:
                            dsrow['lcfname'] = None

            #
            # update the dataset pickle with the new light curve locations
            #
            with gzip.open(dataset_fpath,'wb') as outfd:
                pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)
            LOGINFO('updated dataset pickle after LC collection completed')

        #
        # done with collecting light curves
        #

        # generate the entry in the lcc-datasets.sqlite table and commit it
        # once we get to this point, the dataset is finally considered complete
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        cur = db.cursor()

        query = ("update lcc_datasets set status = ?, "
                 "last_updated = ?, lczip_cachekey = ? where setid = ?")

        params = ('complete',
                  datetime.utcnow().isoformat(),
                  dataset_lczip_cachekey,
                  setid)
        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated entry for setid: %s with LC zip cachekey' % setid)
        return dataset['lczipfpath']

    else:

        LOGERROR('setid: %s, dataset pickle expected at %s does not exist!' %
                 (setid, dataset_fpath))
        return None



# LATER
def sqlite_remove_dataset(basedir, setid):
    '''
    This removes the specified dataset.

    '''


# LATER
def sqlite_update_dataset(basedir, setid, updatedict):
    '''
    This updates a dataset.

    '''



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
    lczip_fpath
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
            dataset_csv = os.path.join(
                basedir,'datasets','dataset-%s.csv' % row['setid']
            )
            dataset_lczip = os.path.join(
                basedir,'products','lightcurves-%s.zip' % row['setid']
            )

            if os.path.exists(dataset_pickle):
                row['dataset_fpath'] = dataset_pickle
            else:
                row['dataset_fpath'] = None

            if os.path.exists(dataset_csv):
                row['dataset_csv'] = dataset_csv
            else:
                row['dataset_csv'] = None

            if os.path.exists(dataset_lczip):
                row['lczip_fpath'] = dataset_lczip
            else:
                row['lczip_fpath'] = None

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
                       forcecsv=False,
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
                'dataset_csv':None,
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
    }

    query = ("select created_on, last_updated, nobjects, status "
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

    # the results are per-collection
    returndict['collections'] = dataset['collections']
    returndict['result'] = {}

    for coll in dataset['collections']:

        returndict['result'][coll] = {'data':dataset['result'][coll][::],
                                      'success':dataset['success'][coll],
                                      'message':dataset['message'][coll],
                                      'nmatches':dataset['nmatches'][coll],
                                      'columnspec':dataset['columnspec'][coll],
                                      'collid':dataset['collid'][coll]}

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

        # link the CSV LCs to the output csvlcs directory if available
        # this should let these through to generate_dataset_tablerows even if
        # the dataset is forced to completion
        for collection in returndict['collections']:
            for row in returndict['result'][collection]['data']:

                # NOTE: here we rely on the fact that the collection names
                # are normalized by the CLI (assuming that's the only way
                # people generate these collections). The generated collection
                # IDs never include a '_', so we can safely change from the DB
                # collection ID which can't have a '-' to a collection ID ==
                # directory name on disk, which can't have a '_'
                # this will probably come back to haunt me

                csvlc_original = os.path.join(os.path.abspath(basedir),
                                              collection.replace('_','-'),
                                              'lightcurves',
                                              '%s-csvlc.gz' %
                                              row['db_oid'])
                csvlc_link = os.path.join(os.path.abspath(basedir),
                                          'csvlcs',
                                          collection.replace('_','-'),
                                          '%s-csvlc.gz' % row['db_oid'])

                if (os.path.exists(csvlc_original) and
                    not os.path.exists(csvlc_link)):

                    os.symlink(os.path.abspath(csvlc_original),
                               csvlc_link)
                    csvlc = csvlc_link

                elif (os.path.exists(csvlc_original) and
                      os.path.exists(csvlc_link)):

                    csvlc = csvlc_link

                else:

                    csvlc = None

                if 'db_lcfname' in row:
                    row['db_lcfname'] = csvlc

                if 'lcfname' in row:
                    row['lcfname'] = csvlc

                # changing the row keys here also automatically changes the
                # corresponding row key in the original dataset dict
                # this shouldn't be happening though, because we (attempted to)
                # make a copy of the dataset row list above (apparently not!)

        LOGWARNING("forced 'complete' status for '%s' dataset: %s" %
                   (returndict['status'], returndict['setid']))

        # write the original dataset dict back to the pickle after the
        # lcfname items have been censored
        with gzip.open(dataset_fpath,'wb') as outfd:
            pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset pickle with CSVLC filenames')

    elif forcecomplete and returndict['status'] == 'complete':
        LOGERROR('not going to force completion '
                 'for an already complete dataset: %s'
                 % returndict['setid'])

    # make the CSV at the end if told to do so
    if generatecsv:
        csv = generate_dataset_csv(
            basedir,
            returndict,
            force=forcecsv,
        )
        returndict['dataset_csv'] = csv
    else:
        returndict['dataset_csv'] = None

    # if we're returning JSON, do that
    if returnjson:

        retjson = json.dumps(returndict)
        retjson = retjson.replace('nan','null')

        return retjson

    # otherwise, return the usual dict
    else:
        return returndict



def generate_dataset_tablerows(
        basedir,
        in_dataset,
        giveupafter=3000,
        headeronly=False,
        strformat=False,
        datarows_bypass_cache=False,
):
    '''This generates row elements useful for direct insert into a HTML table.

    Requires the output from sqlite_get_dataset or postgres_get_dataset.

    '''

    setid = in_dataset['setid']


    # check the cache first
    cached_dataset_header = os.path.join(basedir,
                                         'datasets',
                                         'dataset-%s-header.json' % setid)
    cached_dataset_tablerows_strformat = os.path.join(
        basedir,
        'datasets',
        'dataset-%s-rows-strformat-limit-%s.json' % (setid, giveupafter)
    )
    cached_dataset_tablerows = os.path.join(
        basedir,
        'datasets',
        'dataset-%s-rows-limit-%s.json' % (setid, giveupafter)
    )

    # the cached header is always used if available
    if os.path.exists(cached_dataset_header):

        with open(cached_dataset_header,'rb') as infd:
            header = json.load(infd)

            if headeronly:
                LOGINFO('returning cached header for dataset: %s' % setid)
                return header

    # we'll also use the cached strformat if requested
    if (strformat and (not datarows_bypass_cache) and
        (os.path.exists(cached_dataset_tablerows_strformat))):

        with open(cached_dataset_tablerows_strformat,'rb') as infd:
            table_rows = json.load(infd)

        LOGINFO('returning cached header and '
                'strformat table rows for dataset: %s' % setid)
        return header, table_rows

    # if strformat is not requested and we're still allowed to return items from
    # the cache if they exist, then do so here
    elif ((not strformat) and (not datarows_bypass_cache) and
          (os.path.exists(cached_dataset_tablerows))):

        with open(cached_dataset_tablerows,'rb') as infd:
            table_rows = json.load(infd)

        LOGINFO('returning cached header and '
                'raw table rows for dataset: %s' % setid)
        return header, table_rows

    #
    # otherwise, we're not allowed to bypass the cache, so proceed to the actual
    # processing
    #

    # we'll get the common columns across all collections
    xcolumns = []

    # this is the merged colspec dictionary across all collections
    colspec = {}

    for coll in in_dataset['collections']:
        try:
            # get this collection's column keys from the first row
            coll_columns = list(in_dataset['result'][coll]['data'][0].keys())

            # append them as a set to the global list of all collections'
            # columns
            xcolumns.append(set(coll_columns))

            # finally, for each column in this collection, get its
            # specifications
            for cc in coll_columns:
                colspec[cc] = in_dataset['result'][coll]['columnspec'][cc]
        except Exception as e:
            pass

    # xcolumns is now a list of sets of column keys from all collections.
    # we can only display columns that are common to all collections, so we need
    # to do a reduce operation on set intersections of all of these sets.
    columns = reduce(lambda x,y: x.intersection(y), xcolumns)
    columns = list(columns)

    # we need to reorder the common columns in the order they were requested in
    requested_cols = in_dataset['columns']

    # but the requested columns might contain columns that are not common across
    # all collections, so we need to be careful
    final_cols = []
    for col in requested_cols:
        if col in columns:
            final_cols.append(col)

    # generate the header JSON now
    header = {
        'setid':setid,
        'status':in_dataset['status'],
        'created':'%sZ' % in_dataset['created_on'],
        'updated':'%sZ' % in_dataset['last_updated'],
        'public':in_dataset['ispublic'],
        'searchtype':in_dataset['searchtype'],
        'searchargs':in_dataset['searchargs'],
        'collections':in_dataset['collections'],
        'columns':final_cols,
        'nobjects':in_dataset['nobjects'],
        'coldesc':{}
    }

    # go through each column and get its info from colspec
    # also build up the format string for the CSV
    for col in final_cols:

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

    # write this new header to a JSON that can be cached
    with open(cached_dataset_header,'w') as outfd:
        json.dump(header, outfd)

    if headeronly:
        return header

    table_rows = []
    nitems = 0

    if giveupafter is None or giveupafter is False:
        maxrows = sum(len(in_dataset['result'][collid]['data']) for
                      collid in in_dataset['collections'])
    else:
        maxrows = giveupafter

    # we'll go by collection_id first, then by entry
    for collid in in_dataset['collections']:
        for ientry in in_dataset['result'][collid]['data']:

            # this is to avoid weird breakage when we call
            # generate_dataset_tablerows again
            # (EVEN IF A COPY OF THE DATASET DICT IS PASSED TO IT - WTF?!)
            entry = ientry.copy()

            if nitems > maxrows:
                LOGWARNING('reached %s rows, returning early' % giveupafter)
                break

            # censor the light curve filenames
            # also make sure the actual files exist, otherwise,
            # return nothing for those entries
            if 'db_lcfname' in entry:

                if (entry['db_lcfname'] is not None and
                    os.path.exists(entry['db_lcfname'])):

                    entry['db_lcfname'] = entry['db_lcfname'].replace(
                        os.path.join(os.path.abspath(basedir), 'csvlcs'),
                        '/l'
                    )
                else:
                    entry['db_lcfname'] = None

            elif 'lcfname' in entry:

                if (entry['lcfname'] is not None and
                    os.path.exists(entry['lcfname'])):

                    entry['lcfname'] = entry['lcfname'].replace(
                        os.path.join(os.path.abspath(basedir), 'csvlcs'),
                        '/l'
                    )
                else:
                    entry['lcfname'] = None

            # if we're returning in string format, we need to format each
            # row according to the per-column format strings
            if strformat:

                row = []

                for col in final_cols[:-1]:

                    # reformat the downloadable LC link
                    if col in ('lcfname', 'db_lcfname'):
                        if entry[col] is not None:
                            row.append(
                                '<a href="%s">download light curve</a>' %
                                (entry[col],)
                            )
                        else:
                            row.append('unavailable or missing')

                    # take care with nans, Nones, and missing integers
                    else:

                        colform = header['coldesc'][col]['format']
                        if 'f' in colform and entry[col] is None:
                            row.append('nan')
                        elif 'i' in colform and entry[col] is None:
                            row.append('-9999')
                        else:
                            thisr = colform % entry[col]
                            # replace any pipe characters with commas
                            # to save us from broken CSVs
                            thisr = thisr.replace('|',', ')
                            row.append(thisr)

            # otherwise, we can just return the row for this entry
            else:
                row = [entry[col] for col in final_cols[:-1]]

            # at end of the row processing, append the collection ID
            row.append(collid)

            # append this row to the table_rows
            table_rows.append(row)
            nitems = nitems + 1


    # now that we're done with the table rows, dump them to JSON as appropriate
    if strformat:
        with open(cached_dataset_tablerows_strformat,'w') as outfd:
            json.dump(table_rows, outfd)

    else:

        with open(cached_dataset_tablerows,'w') as outfd:
            json.dump(table_rows, outfd)

    #
    # at the end, return our requested items
    #
    return header, table_rows



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

    productdir = os.path.abspath(os.path.join(basedir,
                                              'datasets'))
    setid = in_dataset['setid']
    in_dataset_csv = os.path.join(productdir,'dataset-%s.csv' % setid)

    if not force and os.path.exists(in_dataset_csv):

        LOGINFO('returning cached version of %s' % in_dataset_csv)
        return in_dataset_csv

    else:

        # generate the strformatted table rows using generate_dataset_tablerows
        header, datarows = generate_dataset_tablerows(
            basedir,
            in_dataset,
            giveupafter=None,
            headeronly=False,
            strformat=True,
            datarows_bypass_cache=force,
        )

        LOGINFO('generating new CSV for in_dataset: %s' % setid)

        # generate the JSON header for the CSV
        csvheader = json.dumps(header, indent=2)
        csvheader = indent(csvheader, '%s ' % comment)

        # write to the output file now
        with open(in_dataset_csv,'wb') as outfd:

            # write the header first
            outfd.write(('%s\n' % csvheader).encode())

            for row in datarows:
                outfd.write(('%s\n' % separator.join(row)).encode())


        LOGINFO('wrote CSV: %s for in_dataset: %s' % (in_dataset_csv, setid))
        return in_dataset_csv



#####################################
## SEARCHING FOR STUFF IN DATASETS ##
#####################################

# TODO: these functions search in the lcc-datasets.sqlite databases:
# - full text match on dataset ID, description, name, searchargs
# - cone search and overlapping query on dataset footprint
