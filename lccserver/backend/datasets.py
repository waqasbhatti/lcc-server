#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''datasets.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - May 2018
License: MIT - see the LICENSE file for the full text.

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
from functools import reduce, partial
import hashlib
from datetime import datetime
from random import sample
import re
import unicodedata

from tornado.escape import xhtml_unescape
import bleach

from . import abcat
from ..authnzerver.authdb import check_user_access, check_role_limits

#########################################
## INITIALIZING A DATASET INDEX SQLITE ##
#########################################

# -- FIXME: think about adding some more useful columns
# -- objectid_list  -> list of all object IDs in this dataset (for dataset FTS)
# -- convex_hull_polygon -> for queries like "all datasets in this sky region"
SQLITE_DATASET_CREATE = '''\
create table lcc_datasets_vinfo (dbver integer,
                                 lccserver_vtag text,
                                 vdate date);
insert into lcc_datasets_vinfo values (2, 'v0.2', '2018-08-31');

-- set the WAL mode on
pragma journal_mode = wal;
pragma journal_size_limit = 52428800;

-- this is the main table
create table lcc_datasets (
  setid text not null,
  created_on datetime not null,
  last_updated datetime not null,
  nobjects integer not null,
  status text not null,
  lczip_cachekey text,
  queried_collections text,
  query_type text,
  query_params text,
  name text,
  description text,
  citation text,
  dataset_owner integer default 1,
  dataset_visibility text default 'public',
  dataset_sharedwith text,
  dataset_sessiontoken text,
  primary key (setid)
);

-- reversed and forward time lookup fast indexes
create index fwd_time_idx on lcc_datasets (last_updated asc);
create index rev_time_idx on lcc_datasets (last_updated desc);

-- index on the dataset cache key
create index lczip_cachekey_idx on lcc_datasets (lczip_cachekey);

-- index on the ispublic, owner, visibility columns
create index owner_idx on lcc_datasets (dataset_owner);
create index visibility_idx on lcc_datasets (dataset_visibility);

-- fts indexes below
create virtual table lcc_datasets_fts using fts5(
  setid,
  queried_collections,
  query_type,
  query_params,
  name,
  description,
  citation,
  content=lcc_datasets
);

-- triggers for updating FTS index when things get changed
create trigger fts_before_update before update on lcc_datasets begin
    delete from lcc_datasets_fts where rowid=old.rowid;
end;

create trigger fts_before_delete before delete on lcc_datasets begin
    delete from lcc_datasets_fts where rowid=old.rowid;
end;

create trigger fts_after_update after update on lcc_datasets begin
    insert into lcc_datasets_fts(
        rowid, setid, queried_collections, query_type,
        query_params, name, description, citation
    )
    values (new.rowid, new.setid, new.queried_collections,
            new.query_type, new.query_params, new.name,
            new.description, new.citation);
end;

create trigger fts_after_insert after insert on lcc_datasets begin
    insert into lcc_datasets_fts(rowid, setid, queried_collections, query_type,
                                 query_params, name, description, citation)
    values (new.rowid, new.setid, new.queried_collections,
            new.query_type, new.query_params, new.name,
            new.description, new.citation);
end;

-- activate the fts indexes
insert into lcc_datasets_fts(lcc_datasets_fts) values ('rebuild');
'''



def sqlite_make_lcc_datasets_db(basedir):
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


#####################
## RESULT PIPELINE ##
#####################

def _sort_key(row, k=None, rev=False, dtype='f8'):
    '''
    This returns the sort key from the row element.

    Handles case where row[k] may be None (or an SQL NULL).

    '''

    # if we're sorting in reverse order, sort None items to the end of the list
    if row[k] is None and rev is True:

        if 'f' in dtype or 'i' in dtype:
            return -9999
        else:
            return 'zzzz'

    # if we're sorting in forward order, sort None items to the end of the list
    elif row[k] is None and rev is False:

        if 'f' in dtype or 'i' in dtype:
            return 9999
        else:
            return 'aaaa'

    # if item is not None, return it directly
    else:
        return row[k]



def results_sort_by_keys(rows, coldesc, sorts=()):
    '''
    This sorts the results by the given sorts list.

    rows is the list of sqlite3.Row objects returned from a query.

    sorts is a list of tuples like so:

    ('sqlite.Row key to sort by', 'asc|desc')

    The sorts are applied in order of their appearance in the list.

    Returns the sorted list of sqlite3.Row items.

    '''

    if len(rows) > 0:

        # we iterate backwards from the last sort spec to the first to handle
        # stuff like order by object asc, ndet desc, sdssr asc as expected
        for s in sorts[::-1]:

            key, order = s
            dtype = coldesc[key]['dtype']

            if order == 'asc':

                keyfunc = partial(_sort_key, k=key, rev=False, dtype=dtype)
                rows = sorted(rows, key=keyfunc, reverse=False)

            elif order == 'desc':

                keyfunc = partial(_sort_key, k=key, rev=True, dtype=dtype)
                rows = sorted(rows, key=keyfunc, reverse=True)

    return rows



def results_limit_rows(rows,
                       rowlimit=None,
                       incoming_userid=2,
                       incoming_role='anonymous'):
    '''
    This justs limits the rows based on the permissions and rowlimit.

    '''

    # check how many results the user is allowed to have
    role_limits = check_role_limits(incoming_role)

    if rowlimit is not None and rowlimit > role_limits['max_rows']:
        return rows[:role_limits['max_rows']]

    elif rowlimit is not None and rowlimit < role_limits['max_rows']:
        return rows[:rowlimit]

    # if no specific row limit is provided, make sure to never go above the
    # allowed max_rows for the role
    else:
        return rows[:role_limits['max_rows']]



def results_random_sample(rows, sample_count=None):
    '''This returns sample_count uniformly sampled without replacement rows.

    '''

    if sample_count is not None and 0 < sample_count < len(rows):
        return sample(rows, sample_count)
    else:
        return rows



########################################
## FUNCTIONS THAT OPERATE ON DATASETS ##
########################################

def sqlite_prepare_dataset(basedir,
                           dataset_owner=2,
                           dataset_visibility='unlisted',
                           dataset_sharedwith=None):
    '''This generates a setid to use for the next step below.

    datasets can have the following statuses:

    'initialized'
    'in progress'
    'complete'
    'failed'

    owner is the user ID of the owner of the dataset. The default is 2, which
    corresponds to the anonymous user. In general, this should be set to the
    integer user ID of an existing user in the users table of the
    lcc-basedir/.authdb.sqlite DB.

    visibility is one of:

    'public'   -> dataset is visible to anyone and shows up in the public list
    'unlisted' -> dataset is unlisted but accessible at its URL
    'shared'   -> dataset is visible to owner user ID
                  and the user IDs in the shared_with column
                  FIXME: we'll add groups and group ID columns later
    'private'  -> dataset is only visible to the owner

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
             "(setid, "
             "created_on, "
             "last_updated, "
             "nobjects, "
             "status, "
             "dataset_owner, "
             "dataset_visibility, "
             "dataset_sharedwith) "
             "values (?, ?, ?, ?, ?, ?, ?, ?)")
    params = (setid,
              creationdt,
              creationdt,
              0,
              'initialized',
              dataset_owner,
              dataset_visibility,
              dataset_sharedwith)

    cur.execute(query, params)
    db.commit()
    db.close()

    return setid, creationdt



def process_dataset_pgrow(
        entry,
        basedir,
        lcformatdescs,
        columnlist,
        columndesc,
        outrows=None,
        out_strformat_rows=None,
        all_original_lcs=None,
        csvlcs_to_generate=None,
        csvfd=None,
):
    '''
    This processes a single row entry for a dataset page.

    '''

    if ( (all_original_lcs is not None) and
         (csvlcs_to_generate is not None) ):

        all_original_lcs.append(entry['db_lcfname'])

        csvlc = '%s-csvlc.gz' % entry['db_oid']
        csvlc_path = os.path.join(os.path.abspath(basedir),
                                  'csvlcs',
                                  entry['collection'].replace('_','-'),
                                  csvlc)

        if 'db_lcfname' in entry:

            csvlcs_to_generate.append(
                (entry['db_lcfname'],
                 entry['db_oid'],
                 lcformatdescs[entry['collection']],
                 entry['collection'],
                 csvlc_path)
            )

            entry['db_lcfname'] = '/l/%s/%s' % (
                entry['collection'].replace('_','-'),
                csvlc
            )

        elif 'lcfname' in entry:

            csvlcs_to_generate.append(
                (entry['lcfname'],
                 entry['db_oid'],
                 lcformatdescs[entry['collection']],
                 entry['collection'],
                 csvlc_path)
            )

            entry['lcfname'] = '/l/%s/%s' % (
                entry['collection'].replace('_','-'),
                csvlc
            )

    # generate the the normal data table row and append it to the output rows
    outrow = [entry[c] for c in columnlist]
    if outrows is not None:
        outrows.append(outrow)

    # the string formatted data table row
    # this will be written to the CSV and to the page JSON
    out_strformat_row = []

    # generate the string formatted row.
    for c in columnlist:

        cform = columndesc[c]['format']
        if 'f' in cform and entry[c] is None:
            out_strformat_row.append('nan')
        elif 'i' in cform and entry[c] is None:
            out_strformat_row.append('-9999')
        else:
            thisr = cform % entry[c]
            # replace any pipe characters with semi-colons
            # to save us from broken CSVs
            thisr = thisr.replace('|','; ')
            out_strformat_row.append(thisr)

    # append the strformat row to the output strformat rows
    if out_strformat_rows is not None:
        out_strformat_rows.append(out_strformat_row)

    # write this row to the CSV
    if csvfd is not None:
        outline = '%s\n' % '|'.join(out_strformat_row)
        csvfd.write(outline.encode())

    if outrows is None and out_strformat_rows is None:
        return outrow, out_strformat_row



def process_dataset_page(
        basedir,
        datasetdir,
        dataset,
        page,
        pgslice,
):
    '''
    This processes a single dataset page.

    '''
    #
    # these are all the dataset result row entries for this page
    #
    LOGINFO('working on rows[%s:%s]' % (pgslice[0],pgslice[1]))
    pgrows = dataset['result'][pgslice[0]:pgslice[1]]

    # update the page number
    page_number = page + 1
    setid = dataset['setid']

    # get the lcformatdescs from the dataset
    lcformatdescs = dataset['lcformatdescs']

    # open the pageX.pkl file
    page_rows_pkl = os.path.join(
        datasetdir,
        'dataset-%s-rows-page%s.pkl' %
        (setid, page_number)
    )
    page_rows_strpkl = os.path.join(
        datasetdir,
        'dataset-%s-rows-page%s-strformat.pkl' %
        (setid, page_number)
    )
    page_rows_pklfd = open(page_rows_pkl,'wb')
    page_rows_strpklfd = open(page_rows_strpkl,'wb')

    outrows = []
    out_strformat_rows = []

    columnlist = dataset['columns']
    columndesc = dataset['coldesc']

    # now we'll go through all the rows
    for entry in pgrows:

        process_dataset_pgrow(
            entry,
            basedir,
            lcformatdescs,
            columnlist,
            columndesc,
            outrows=outrows,
            out_strformat_rows=out_strformat_rows,
            all_original_lcs=None,
            csvlcs_to_generate=None,
            csvfd=None
        )

    #
    # at the end of the page, write the JSON files for this page
    #
    pickle.dump(outrows, page_rows_pklfd,
                pickle.HIGHEST_PROTOCOL)
    pickle.dump(out_strformat_rows, page_rows_strpklfd,
                pickle.HIGHEST_PROTOCOL)

    page_rows_pklfd.close()
    page_rows_strpklfd.close()

    LOGINFO('wrote page %s pickles: %s and %s, for setid: %s' %
            (page_number, page_rows_pkl, page_rows_strpkl, setid))

    return page_rows_pkl, page_rows_strpkl



def sqlite_new_dataset(basedir,
                       setid,
                       creationdt,
                       searchresult,
                       results_sortspec=None,
                       results_limitspec=None,
                       results_samplespec=None,
                       incoming_userid=2,
                       incoming_role='anonymous',
                       incoming_session_token=None,
                       dataset_visibility='unlisted',
                       dataset_sharedwith=None,
                       rows_per_page=1000,
                       render_first_page=True):
    '''This is the new-style dataset pickle maker.

    Converts the results from the backend into a data table with rows from all
    collections in a single array instead of broken up by collection. This
    allows us to resort on their properties over the entire dataset instead of
    just per collection.

    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    productdir = os.path.abspath(os.path.join(basedir, 'products'))

    # get some stuff out of the search result
    collections = searchresult['databases']
    lcmagcols = {c:searchresult[c]['lcmagcols'] for c in collections}

    searchtype = searchresult['search']
    searchargs = searchresult['args']

    # add in the sortspec, samplespec, limitspec, visibility, and sharedwith to
    # searchargs so these can be stored in the DB and the pickle for later
    # retrieval
    searchargs['sortspec'] = results_sortspec
    searchargs['limitspec'] = results_limitspec
    searchargs['samplespec'] = results_samplespec
    searchargs['visibility'] = dataset_visibility
    searchargs['sharedwith'] = dataset_sharedwith

    # get the columnspecs and actual collectionids for each collection searched
    # so we can return the column names and descriptions as well
    columnspec = {x:searchresult[x]['columnspec'] for x in collections}
    collid = {x:searchresult[x]['collid'] for x in collections}

    # get the lcformatdescs from the searchresults
    lcformatdescs = {x:searchresult[x]['lcformatdesc'] for x in collections}

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

    # go through each column and get its info from colspec
    # also build up the format string for the CSV
    coldesc = {}

    for col in reqcols:

        coldesc[col] = {
            'title': columnspec[collections[0]][col]['title'],
            'desc': columnspec[collections[0]][col]['description'],
            'dtype': columnspec[collections[0]][col]['dtype'],
            'format': columnspec[collections[0]][col]['format'],
        }

    # each collection result from the search backend is a list of dicts. we'll
    # collect all of them into a single data table. Each row returned by the
    # search backend has its collection noted in the row['collection'] key.
    rows = []

    for coll in collections:
        rows.extend(searchresult[coll]['result'])

    # apply the result pipeline to sort, limit, sample correctly
    # the search backend takes care of per object permissions
    # order of operations: sample -> sort -> rowlimit
    if results_samplespec is not None:
        rows = results_random_sample(rows, sample_count=results_samplespec)


    if isinstance(results_sortspec, (tuple,list)):

        # reform special case of single sortspec
        if (len(results_sortspec) == 2 and
            isinstance(results_sortspec[0],str) and
            isinstance(results_sortspec[1],str) and
            results_sortspec[1].strip().lower() in ('asc','desc')):
            LOGWARNING(
                'reforming single sortspec item %s to expected list of lists'
                % results_sortspec
            )
            results_sortspec = [[results_sortspec[0], results_sortspec[1]]]

        # apply the sort spec
        rows = results_sort_by_keys(rows,
                                    coldesc,
                                    sorts=results_sortspec)

    # this is always applied so we can restrict the number of rows by role
    rows = results_limit_rows(rows,
                              rowlimit=results_limitspec,
                              incoming_userid=incoming_userid,
                              incoming_role=incoming_role)

    # updated date
    last_updated = datetime.utcnow().isoformat()

    # the paths to the complete dataset pickle and CSV
    # generate the dataset pickle filepath
    dataset_fname = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_fname)
    dataset_csv = dataset_fpath.replace('.pkl.gz','.csv')

    # generate the name of the lczipfile
    lczip_fname = 'lightcurves-%s.zip' % setid
    lczip_fpath = os.path.join(productdir, lczip_fname)

    # figure out the number of pages
    if len(rows) % rows_per_page:
        npages = int(len(rows)/rows_per_page) + 1
    else:
        npages = int(len(rows)/rows_per_page)

    page_slices = [[x*rows_per_page, x*rows_per_page+rows_per_page]
                   for x in range(npages)]

    # the header for the dataset
    dataset = {
        'setid': setid,
        'name': setname,
        'desc': setdesc,
        'citation': None,
        'created': creationdt,
        'updated': last_updated,
        'owner': incoming_userid,
        'visibility': dataset_visibility,
        'sharedwith': dataset_sharedwith,
        'searchtype': searchtype,
        'searchargs': searchargs,
        'collections': collections,
        'lcmagcols':lcmagcols,
        'lcformatdescs':lcformatdescs,
        'coll_dirs': collid,
        'npages':npages,
        'rows_per_page':rows_per_page,
        'page_slices':page_slices,
        'nmatches_by_collection':nmatches,
        'total_nmatches': total_nmatches,
        'actual_nrows':len(rows),
        'columns': reqcols,
        'coldesc': coldesc,
        'dataset_csv':dataset_csv,
        'dataset_pickle':dataset_fpath,
        'lczipfpath':lczip_fpath,
    }

    # generate the JSON header for the CSV
    csvheader = json.dumps(dataset, indent=2)
    csvheader = indent(csvheader, '# ')

    #
    # add in the rows to turn the header into the complete dataset pickle
    #
    dataset['result'] = rows

    # open the datasets database
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    # generate the entry in the lcc-datasets.sqlite table and commit it
    query = (
        "update lcc_datasets set "
        "name = ?, "
        "description = ?, "
        "last_updated = ?, "
        "nobjects = ?, "
        "dataset_owner = ?, "
        "dataset_visibility = ?, "
        "dataset_sharedwith = ?, "
        "dataset_sessiontoken = ?, "
        "status = ?, "
        "queried_collections = ?, "
        "query_type = ?, "
        "query_params = ? "
        "where setid = ?"
    )

    params = (
        setname,
        setdesc,
        last_updated,
        dataset['actual_nrows'],
        dataset['owner'],
        dataset['visibility'],
        dataset['sharedwith'],
        incoming_session_token,
        'in progress',
        ', '.join(collections),
        searchtype,
        json.dumps(searchargs),
        setid
    )
    cur.execute(query, params)
    db.commit()
    db.close()

    LOGINFO('updated DB entry for setid: %s, total nmatches: %s' %
            (setid, total_nmatches))

    csvfd = open(dataset_csv,'wb')

    # write the header to the CSV file
    csvfd.write(('%s\n' % csvheader).encode())

    all_original_lcs = []
    csvlcs_to_generate = []

    LOGINFO('writing dataset rows to CSV and main pickle...')

    # run through the rows and generate the CSV
    for entry in dataset['result']:

        process_dataset_pgrow(
            entry,
            basedir,
            lcformatdescs,
            dataset['columns'],
            dataset['coldesc'],
            csvfd=csvfd,
            all_original_lcs=all_original_lcs,
            csvlcs_to_generate=csvlcs_to_generate,
        )

    #
    # finish up the CSV when we're done with all of the rows
    #
    csvfd.close()
    LOGINFO('wrote CSV: %s for setid: %s' % (dataset_csv, setid))

    #
    # next, write the pickle to the datasets directory
    #

    # write the full pickle gz
    with gzip.open(dataset_fpath,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset pickle for search results to %s, setid: %s' %
            (dataset_fpath, setid))

    # write the header pickle
    dataset_header_pkl = os.path.join(datasetdir,
                                      'dataset-%s-header.pkl' % setid)

    with open(dataset_header_pkl,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset header pickle: %s for dataset setid: %s' %
            (dataset_header_pkl, setid))

    actual_nrows = dataset['actual_nrows']

    if render_first_page:

        process_dataset_page(
            basedir,
            datasetdir,
            dataset,
            0,
            [0,rows_per_page]
        )

    del dataset

    # return the setid
    return (
        setid,
        csvlcs_to_generate,
        sorted(all_original_lcs),
        actual_nrows,
        npages
    )



def sqlite_render_dataset_page(basedir,
                               setid,
                               page_number):
    '''
    This renders a single page of the already completed dataset.

    The page_number is ** 1-indexed ** page number.

    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))

    # get the full pickle
    setpickle = os.path.join(datasetdir, 'dataset-%s.pkl.gz' % setid)

    # open it and get the page slices we needed
    with gzip.open(setpickle,'rb') as infd:
        ds = pickle.load(infd)

    page = page_number - 1
    if page < 0:
        page = 0

    try:

        pgslice = ds['page_slices'][page]

        # process this page
        page_pkl, strpage_pkl = process_dataset_page(
            basedir,
            datasetdir,
            ds,
            page,
            pgslice,
        )

        del ds
        return page_pkl, strpage_pkl

    except IndexError:

        LOGERROR('requested page_number = %s is '
                 'out of bounds for dataset: %s' %
                 (page_number, setid))

        return None, None



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
                              dataset_csvlcs_to_generate,
                              dataset_all_original_lcs,
                              converter_processes=4,
                              converter_csvlc_version=1,
                              converter_comment_char='#',
                              converter_column_separator=',',
                              converter_skip_converted=True,
                              override_lcdir=None,
                              force_collection=False,
                              max_dataset_lcs=2500):
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

        # 1. use the provided list of original LCs to generate a cache key.
        dataset_lczip_cachekey = generate_lczip_cachekey(
            dataset_all_original_lcs
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

                skip_lc_collection = False

        # only collect LCs if we have to, we'll use the already generated link
        # if we have the same LCs collected already somewhere else
        if not skip_lc_collection or force_collection is True:

            LOGINFO('no cached LC zip found for dataset: %s, regenerating...' %
                    setid)


            convertopts = {'csvlc_version':converter_csvlc_version,
                           'comment_char':converter_comment_char,
                           'column_separator':converter_column_separator,
                           'skip_converted':converter_skip_converted}

            # these are the light curves to regenerate
            tasks = [(x[0], x[1], x[2], convertopts)
                     for x in dataset_csvlcs_to_generate]

            # now, we'll convert these light curves in parallel
            pool = Pool(converter_processes)
            results = pool.map(csvlc_convert_worker, tasks)
            pool.close()
            pool.join()

            #
            # link the generated CSV LCs to the output directory
            #

            for item, res in zip(dataset_csvlcs_to_generate, results):

                orig, oid, lcformatdesc, coll, outcsvlc = item
                if not os.path.exists(outcsvlc):
                    try:
                        os.symlink(os.path.abspath(res), outcsvlc)
                    except Exception as e:
                        LOGEXCEPTION(
                            'could not symlink %s -> %s' % (
                                os.path.abspath(res),
                                outcsvlc
                            )
                        )

            #
            # FINALLY, CARRY OUT THE ZIP OPERATION (IF NEEDED)
            #
            zipfile_lclist = (
                [x[-1] for x in dataset_csvlcs_to_generate]
            )

            # do not generate LCs if their number > max number allowed
            if len(zipfile_lclist) > max_dataset_lcs:

                LOGERROR('LCs in dataset: %s > max_dataset_lcs: %s, '
                         ' will not generate a ZIP file.' %
                         (len(zipfile_lclist), max_dataset_lcs))

                lczip_generated = False

            else:

                # get the expected name of the output zipfile
                lczip_fpath = dataset['lczipfpath']

                LOGINFO('writing %s LC files to zip file: %s for setid: %s...' %
                        (len(zipfile_lclist), lczip_fpath, setid))

                # set up the zipfile
                with ZipFile(lczip_fpath, 'w', allowZip64=True) as outzip:

                    for ind_lcf, lcf in enumerate(zipfile_lclist):

                        if os.path.exists(lcf):

                            lcf_collname = os.path.split(
                                os.path.dirname(lcf)
                            )[-1]
                            lcf_archivename = os.path.basename(lcf)

                            outzip.write(
                                lcf,
                                arcname='%s-%s' % (lcf_collname,
                                                   lcf_archivename)
                            )
                        else:
                            zipfile_lclist[ind_lcf] = (
                                '%s missing' % (os.path.basename(lcf))
                            )

                    # add the manifest to the zipfile
                    outzip.writestr(
                        'lczip-manifest.json',
                        json.dumps(
                            [os.path.basename(x) for x in zipfile_lclist],
                            ensure_ascii=True,
                            indent=2
                        )
                    )

                LOGINFO('done, zip written successfully.')
                lczip_generated = True


        # if we don't need to collect LCs, then we can just re-use the other
        # dataset's LC ZIP
        else:

            LOGINFO("re-using identical LC collected "
                    "ZIP from dataset: %s at %s, "
                    "symlinked to this dataset's LC ZIP: %s" %
                    (other_setid, other_lczip, dataset['lczipfpath']))
            lczip_generated = True

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

        # update the dataset pickle and dataset header pickle with the LC zip
        # information and the fact that all LCs have been collected.

        # update the header pickle
        dataset_header_pkl = os.path.join(datasetdir,
                                          'dataset-%s-header.pkl' % setid)

        with open(dataset_header_pkl,'rb') as infd:
            setheader = pickle.load(infd)

        setheader['updated'] = datetime.utcnow().isoformat()

        with open(dataset_header_pkl,'wb') as outfd:
            pickle.dump(setheader, outfd, pickle.HIGHEST_PROTOCOL)

        # update the full dataset pickle
        dataset['updated'] = datetime.utcnow().isoformat()

        with gzip.open(dataset_fpath,'wb') as outfd:
            pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated entry for setid: %s with LC zip cachekey' % setid)

        return dataset['lczipfpath'], lczip_generated

    else:

        LOGERROR('setid: %s, dataset pickle expected at %s does not exist!' %
                 (setid, dataset_fpath))
        return None, False



######################################
## LISTING AND GETTING DATASET INFO ##
######################################

def sqlite_check_dataset_access(
        setid,
        action,
        incoming_userid=2,
        incoming_role='anonymous',
        database=None
):
    '''This is a function to check single dataset accessibility.

    action = 'list' permissions are not handled here, but are handled directly
    in functions that return rows of results by filtering on them using the
    authdb.check_user_access function directly.

    setid is the dataset ID

    action is one of {'view','edit','delete',
                      'make_public','make_private',
                      'make_shared','make_unlisted',
                      'change_owner'}

    incoming_userid and incoming_role are the user ID and role of the user to
    check access for.

    database is either the path to the lcc-datasets.sqlite DB or the connection
    handler to such a connection to re-use an already open connection.

    '''

    # return immediately if the action doesn't make sense
    if action not in ('view',
                      'edit',
                      'delete',
                      'make_public',
                      'make_private',
                      'make_shared',
                      'make_unlisted',
                      'change_owner'):
        return False

    if isinstance(database,str) and os.path.exists(database):

        datasets_dbf = os.path.join(database)
        db = sqlite3.connect(
            datasets_dbf,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        close_at_end = True

    elif database is not None:

        cur = database.cursor()
        close_at_end = False

    else:
        LOGERROR('no database provided to check access in')
        return False

    # this is the query to run
    query = (
        "select dataset_owner, dataset_visibility, dataset_sharedwith from "
        "lcc_datasets where setid = ?"
    )
    params = (setid,)
    cur.execute(query, params)
    row = cur.fetchone()

    # check the dataset access
    if row and len(row) > 0:

        accessok = check_user_access(
            userid=incoming_userid,
            role=incoming_role,
            action=action,
            target_name='dataset',
            target_owner=row['dataset_owner'],
            target_visibility=row['dataset_visibility'],
            target_sharedwith=row['dataset_sharedwith'],
        )

        cur.close()
        if close_at_end:
            db.close()

        return accessok

    # the dataset doesn't exist, this is a failure
    else:
        cur.close()
        if close_at_end:
            db.close()

        return False



def sqlite_list_datasets(basedir,
                         setfilter=None,
                         useronly=False,
                         nrecent=25,
                         require_status='complete',
                         incoming_userid=2,
                         incoming_role='anonymous'):
    '''This just lists all the datasets available.

    incoming_userid is used to check permissions on the list operation. This is
    2 -> anonymous user by default.

    setid
    created_on
    last_updated
    nobjects
    status

    dataset_owner
    dataset_visibility
    dataset_sharedwith
    dataset_sessiontoken

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

    setfilter is an FTS5 query string with valid column names:

    setid,
    queried_collections,
    query_type,
    query_params,
    name,
    description,
    citation,

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

    # if no filters are specified, just return the most recent datasets
    if not setfilter:

        if not useronly:

            query = ("select setid, created_on, last_updated, nobjects, "
                     "name, description, citation, "
                     "queried_collections, query_type, query_params, "
                     "dataset_owner, dataset_visibility, "
                     "dataset_sharedwith, dataset_sessiontoken "
                     "from "
                     "lcc_datasets where status = ?"
                     "order by last_updated desc limit ?")
            params = (require_status, nrecent)

        else:

            query = ("select setid, created_on, last_updated, nobjects, "
                     "name, description, citation, "
                     "queried_collections, query_type, query_params, "
                     "dataset_owner, dataset_visibility, "
                     "dataset_sharedwith, dataset_sessiontoken "
                     "from "
                     "lcc_datasets where status = ? and dataset_owner = ? "
                     "order by last_updated desc limit ?")
            params = (require_status, incoming_userid, nrecent)


    else:

        if not useronly:

            query = (
                "select a.setid, a.created_on, a.last_updated, a.nobjects, "
                "a.name, a.description, a.citation, "
                "a.queried_collections, a.query_type, a.query_params, "
                "a.dataset_owner, a.dataset_visibility, "
                "a.dataset_sharedwith, a.dataset_sessiontoken, b.rank "
                "from "
                "lcc_datasets a join lcc_datasets_fts b on "
                "(a.rowid = b.rowid) "
                "where a.status = ? and "
                "lcc_datasets_fts MATCH ? "
                "order by bm25(lcc_datasets_fts), a.last_updated desc limit ?"
            )

            # we need to unescape the search string because it might contain
            # exact match strings that we might want to use with FTS
            unescapedstr = xhtml_unescape(setfilter)
            if unescapedstr != setfilter:
                setfilter = unescapedstr
                LOGWARNING('unescaped FTS setfilter because '
                           'it had quotes in it for exact matching: %r' %
                           unescapedstr)
            setfilter = setfilter.replace('\n','')
            params = (require_status, setfilter, nrecent)

        else:

            query = (
                "select a.setid, a.created_on, a.last_updated, a.nobjects, "
                "a.name, a.description, a.citation, "
                "a.queried_collections, a.query_type, a.query_params, "
                "a.dataset_owner, a.dataset_visibility, "
                "a.dataset_sharedwith, a.dataset_sessiontoken, b.rank "
                "from "
                "lcc_datasets a join lcc_datasets_fts b on "
                "(a.rowid = b.rowid) "
                "where a.status = ? and a.dataset_owner = ? and "
                "lcc_datasets_fts MATCH ? "
                "order by bm25(lcc_datasets_fts), a.last_updated desc limit ?"
            )

            # we need to unescape the search string because it might contain
            # exact match strings that we might want to use with FTS
            unescapedstr = xhtml_unescape(setfilter)
            if unescapedstr != setfilter:
                setfilter = unescapedstr
                LOGWARNING('unescaped FTS setfilter because '
                           'it had quotes in it for exact matching: %r' %
                           unescapedstr)
            setfilter = setfilter.replace('\n','')
            params = (require_status, incoming_userid, setfilter, nrecent)


    # make sure we never get more than 1000 recent datasets
    if nrecent > 1000:
        nrecent = 1000

    cur.execute(query, params)
    xrows = cur.fetchall()

    if xrows and len(xrows) > 0:

        # filter the rows depending on check_user_access
        rows = [
            dict(x) for x in xrows if check_user_access(
                userid=incoming_userid,
                role=incoming_role,
                action='list',
                target_name='dataset',
                target_owner=x['dataset_owner'],
                target_visibility=x['dataset_visibility'],
                target_sharedwith=x['dataset_sharedwith']
            )
        ]

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
            'message':'No datasets matching query parameters were found.'
        }

    db.close()
    return returndict



def sqlite_get_dataset(
        basedir,
        setid,
        returnspec,
        incoming_userid=2,
        incoming_role='anonymous'
):
    '''This gets the dataset as a dict.

    returnspec is what to return:

    -> 'json-header'      -> only the dataset header
    -> 'json-preview'     -> header + first page of data table
    -> 'strjson-preview'  -> header + first page of strformatted data table
    -> 'json-page-XX'     -> requested page XX of the data table
    -> 'strjson-page-XX'  -> requested page XX of the strformatted data table
    -> 'pickle'           -> full pickle

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
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # get the dataset's info from the DB
    query = ("select created_on, last_updated, status, "
             "dataset_owner, dataset_visibility, dataset_sharedwith, "
             "dataset_sessiontoken "
             "from lcc_datasets where setid = ?")
    cur.execute(query, (setid,))
    row = cur.fetchone()

    if row and len(row) > 0 and os.path.exists(dataset_fpath):

        dataset_status = row['status']
        #
        # otherwise, proceed as normal
        #
        dataset_accessible = sqlite_check_dataset_access(
            setid,
            'view',
            incoming_userid=incoming_userid,
            incoming_role=incoming_role,
            database=db
        )

        # check if we can access this dataset
        if dataset_accessible:

            # check what we want to return
            if returnspec == 'pickle':

                getpath = dataset_fpath

                with gzip.open(getpath,'rb') as infd:
                    outdict = pickle.load(infd)

                db.close()
                outdict['status'] = dataset_status
                outdict['session_token'] = row['dataset_sessiontoken']
                if 'citation' not in outdict:
                    outdict['citation'] = None
                return outdict

            elif returnspec == 'json-header':

                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                db.close()
                header['status'] = dataset_status
                header['session_token'] = row['dataset_sessiontoken']
                header['currpage'] = 1
                if 'citation' not in header:
                    header['citation'] = None
                return header

            elif returnspec == 'json-preview':

                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)

                getpath2 = os.path.join(datasetdir,
                                        'dataset-%s-rows-page1.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                if os.path.exists(getpath2):
                    with open(getpath2, 'rb') as infd:
                        table_preview = pickle.load(infd)
                else:
                    LOGERROR('requested page: %s for dataset: %s does not exist'
                             % (1, setid))
                    table_preview = []

                header['rows'] = table_preview
                header['status'] = dataset_status
                header['currpage'] = 1
                header['session_token'] = row['dataset_sessiontoken']
                if 'citation' not in header:
                    header['citation'] = None
                db.close()
                return header

            elif returnspec == 'strjson-preview':

                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)

                getpath2 = os.path.join(datasetdir,
                                        'dataset-%s-rows-page1-strformat.pkl' %
                                        setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)
                if os.path.exists(getpath2):
                    with open(getpath2, 'rb') as infd:
                        table_preview = pickle.load(infd)
                else:
                    LOGERROR('requested page: %s for dataset: %s does not exist'
                             % (1, setid))
                    table_preview = []

                header['rows'] = table_preview
                header['status'] = dataset_status
                header['currpage'] = 1
                header['session_token'] = row['dataset_sessiontoken']
                if 'citation' not in header:
                    header['citation'] = None
                db.close()
                return header

            elif returnspec.startswith('json-page-'):

                page_to_get = int(returnspec.split('-')[-1])

                # get the header first
                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                # get the requested page
                if 0 < page_to_get <= header['npages']:

                    getpath2 = os.path.join(
                        datasetdir,
                        'dataset-%s-rows-page%s.pkl' % (setid, page_to_get)
                    )
                    if os.path.exists(getpath2):
                        with open(getpath2, 'rb') as infd:
                            setrows = pickle.load(infd)
                    else:

                        LOGWARNING(
                            'requested page: %s for dataset: %s '
                            'does not exist, attempting to generate...'
                            % (page_to_get, setid)
                        )

                        pkl, strpkl = sqlite_render_dataset_page(
                            basedir,
                            setid,
                            page_to_get
                        )

                        if pkl and os.path.exists(pkl):

                            with open(pkl, 'rb') as infd:
                                setrows = pickle.load(infd)

                        else:

                            LOGERROR(
                                'could not generate page %s of '
                                'dataset %s on-demand'
                                % (page_to_get, setid)
                            )
                            setrows = []

                        setrows = []

                else:
                    LOGERROR(
                        'page requested: %s is out of bounds '
                        'for dataset: %s, which has npages = %s'
                        % (page_to_get, setid, header['npages'])
                    )
                    setrows = []

                header['rows'] = setrows
                header['status'] = dataset_status
                header['currpage'] = page_to_get
                header['session_token'] = row['dataset_sessiontoken']
                if 'citation' not in header:
                    header['citation'] = None
                db.close()

                return header

            elif returnspec.startswith('strjson-page-'):

                page_to_get = int(returnspec.split('-')[-1])
                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                # get the requested page
                if 0 < page_to_get <= header['npages']:

                    getpath2 = os.path.join(
                        datasetdir,
                        'dataset-%s-rows-page%s-strformat.pkl' % (setid,
                                                                  page_to_get)
                    )
                    if os.path.exists(getpath2):
                        with open(getpath2, 'rb') as infd:
                            setrows = pickle.load(infd)
                    else:

                        LOGWARNING(
                            'requested page: %s for dataset: %s '
                            'does not exist, attempting to generate...'
                            % (page_to_get, setid)
                        )

                        pkl, strpkl = sqlite_render_dataset_page(
                            basedir,
                            setid,
                            page_to_get
                        )

                        if strpkl and os.path.exists(strpkl):

                            with open(pkl, 'rb') as infd:
                                setrows = pickle.load(infd)

                        else:

                            LOGERROR(
                                'could not generate page %s of '
                                'dataset %s on-demand'
                                % (page_to_get, setid)
                            )
                            setrows = []

                        setrows = []

                else:
                    LOGERROR(
                        'page requested: %s is out of bounds '
                        'for dataset: %s, which has npages = %s'
                        % (page_to_get, setid, header['npages'])
                    )
                    setrows = []

                header['rows'] = setrows
                header['status'] = dataset_status
                header['currpage'] = int(page_to_get)
                header['session_token'] = row['dataset_sessiontoken']
                if 'citation' not in header:
                    header['citation'] = None
                db.close()

                return header

            else:

                LOGERROR('unknown return spec. not returning anything')
                db.close()
                return None

        # otherwise, the dataset is not accessible
        else:
            LOGERROR('dataset: %s is not accessible by userid: %s, role: %s' %
                     (setid, incoming_userid, incoming_role))
            db.close()
            return None

    else:

        LOGERROR(
            'no database matching setid: %s found' % setid
        )
        db.close()
        return None


#######################
## CHANGING DATASETS ##
#######################

def sqlite_change_dataset_visibility(
        basedir,
        setid,
        new_visibility='unlisted',
        incoming_userid=2,
        incoming_role='anonymous',
        incoming_session_token=None
):
    '''This changes the visibility of a dataset.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the actions to test are

    'make_public', 'make_private', 'make_unlisted', 'make_shared'

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    dataset_pickle = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_pickle)

    # return immediately if the dataset doesn't exist
    if not os.path.exists(dataset_fpath):
        LOGERROR('could not find dataset with setid: %s' % setid)
        return None

    dataset_pickleheader = 'dataset-%s-header.pkl' % setid
    dataset_pickleheader_fpath = os.path.join(datasetdir,
                                              dataset_pickleheader)

    # get the lczip, cpzip, pfzip, dataset pkl shasums from the DB
    # also get the created_on, last_updated, nobjects, status
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # get the dataset's info from the DB
    query = ("select created_on, last_updated, status, "
             "dataset_owner, dataset_visibility, dataset_sharedwith, "
             "dataset_sessiontoken "
             "from lcc_datasets where setid = ?")
    cur.execute(query, (setid,))
    row = cur.fetchone()

    if row and len(row) > 0 and os.path.exists(dataset_fpath):

        dataset_status = row['status']

        # only complete datasets can be edited
        if dataset_status != 'complete':
            LOGERROR('dataset %s status = %s. only complete '
                     'datasets can be edited' % (setid, dataset_status))
            db.close()
            return None

        #
        # otherwise, proceed as normal
        #
        visibility_action = 'make_%s' % new_visibility

        dataset_visibility_changeable = sqlite_check_dataset_access(
            setid,
            visibility_action,
            incoming_userid=incoming_userid,
            incoming_role=incoming_role,
            database=db
        )

        # check if we can edit this dataset
        if not dataset_visibility_changeable:

            LOGERROR('dataset %s is not editable by '
                     'user_id = %s, role = %s, session_token = %s'
                     % (setid,
                        incoming_userid,
                        incoming_role,
                        incoming_session_token))
            db.close()
            return None

        # additional check for anonymous users
        if (incoming_role == 'anonymous' and
            incoming_session_token != row['dataset_sessiontoken']):

            LOGERROR('dataset %s is not editable by '
                     'users with role = %s, session_token = %s, '
                     'required session_token = %s'
                     % (setid,
                        incoming_role,
                        incoming_session_token,
                        row['dataset_sessiontoken']))
            db.close()
            return None


        # finally, do the actual update

        #
        # update the main dataset pickle
        #
        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)

        old_visibility = dataset['visibility'][::]
        dataset['visibility'] = new_visibility
        dataset['updated'] = datetime.utcnow().isoformat()

        with gzip.open(dataset_fpath,'wb') as outfd:
            pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset main pickle: %s '
                'setid: %s, old visibility = %s -> new visibility = %s' %
                (dataset_fpath, setid, old_visibility, new_visibility))

        #
        # update the dataset pickle header
        #
        with open(dataset_pickleheader_fpath,'rb') as infd:
            header = pickle.load(infd)

        header['visibility'] = new_visibility
        header['updated'] = datetime.utcnow().isoformat()
        with open(dataset_pickleheader_fpath,'wb') as outfd:
            pickle.dump(header, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset header pickle: %s '
                'setid: %s, old visibility = %s -> new visibility = %s' %
                (dataset_pickleheader_fpath,
                 setid,
                 old_visibility,
                 new_visibility))

        #
        # update the database entry
        #
        query = (
            "update lcc_datasets set "
            "dataset_visibility = ?, last_updated = ? "
            "where setid = ?"
        )
        # the new session token associated with the dataset is that of the user
        # making the change of ownership (i.e. this superuser)
        params = (new_visibility, datetime.utcnow().isoformat(), setid)

        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated database entry: %s '
                'setid: %s, old visibility = %s -> new visibility = %s' %
                (dataset_pickleheader_fpath,
                 setid,
                 old_visibility,
                 new_visibility))
        return new_visibility

    # if the dataset was not found or is not accessible
    else:

        LOGERROR('could not find dataset with setid: %s' % setid)
        db.close()
        return None



def sqlite_change_dataset_owner(
        basedir,
        setid,
        new_owner_userid,
        incoming_userid=2,
        incoming_role='anonymous',
        incoming_session_token=None
):
    '''This changes the owner of a dataset.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the action to test is 'change_owner'

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    dataset_pickle = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_pickle)

    # return immediately if the dataset doesn't exist
    if not os.path.exists(dataset_fpath):
        LOGERROR('could not find dataset with setid: %s' % setid)
        return None

    dataset_pickleheader = 'dataset-%s-header.pkl' % setid
    dataset_pickleheader_fpath = os.path.join(datasetdir,
                                              dataset_pickleheader)

    # get the lczip, cpzip, pfzip, dataset pkl shasums from the DB
    # also get the created_on, last_updated, nobjects, status
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # get the dataset's info from the DB
    query = ("select created_on, last_updated, status, "
             "dataset_owner, dataset_visibility, dataset_sharedwith, "
             "dataset_sessiontoken "
             "from lcc_datasets where setid = ?")
    cur.execute(query, (setid,))
    row = cur.fetchone()

    if row and len(row) > 0 and os.path.exists(dataset_fpath):

        dataset_status = row['status']

        # only complete datasets can be edited
        if dataset_status != 'complete':
            LOGERROR('dataset %s status = %s. only complete '
                     'datasets can be edited' % (setid, dataset_status))
            db.close()
            return None

        #
        # otherwise, proceed as normal
        #
        dataset_owner_changeable = sqlite_check_dataset_access(
            setid,
            'change_owner',
            incoming_userid=incoming_userid,
            incoming_role=incoming_role,
            database=db
        )

        # check if we can edit this dataset
        if not dataset_owner_changeable:

            LOGERROR('dataset %s is not editable by user_id = %s, role = %s'
                     % (setid, incoming_userid, incoming_role))
            db.close()
            return None

        # additional check
        if (incoming_role not in ('superuser','staff')):

            LOGERROR('dataset %s is not editable by '
                     'users with role = %s, session_token = %s'
                     % (incoming_role,
                        setid,
                        incoming_session_token))
            db.close()
            return None

        # finally, do the actual update

        #
        # update the main dataset pickle
        #
        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)

        old_owner_userid = dataset['owner']
        dataset['owner'] = new_owner_userid
        dataset['updated'] = datetime.utcnow().isoformat()

        with gzip.open(dataset_fpath,'wb') as outfd:
            pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset main pickle: %s '
                'setid: %s, old owner = %s -> new owner = %s' %
                (dataset_fpath, setid, old_owner_userid, new_owner_userid))

        #
        # update the dataset pickle header
        #
        with open(dataset_pickleheader_fpath,'rb') as infd:
            header = pickle.load(infd)

        header['owner'] = new_owner_userid
        header['updated'] = datetime.utcnow().isoformat()
        with open(dataset_pickleheader_fpath,'wb') as outfd:
            pickle.dump(header, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset header pickle: %s '
                'setid: %s, old owner = %s -> new owner = %s' %
                (dataset_pickleheader_fpath,
                 setid,
                 old_owner_userid,
                 new_owner_userid))

        #
        # update the database entry
        #
        query = (
            "update lcc_datasets set "
            "dataset_owner = ?, dataset_sessiontoken = ?, last_updated = ? "
            "where setid = ?"
        )
        # the new session token associated with the dataset is that of the user
        # making the change of ownership (i.e. this superuser)
        params = (new_owner_userid,
                  incoming_session_token,
                  datetime.utcnow().isoformat(),
                  setid)

        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated database entry: %s '
                'setid: %s, old owner = %s -> new owner = %s' %
                (dataset_pickleheader_fpath,
                 setid,
                 old_owner_userid,
                 new_owner_userid))
        return new_owner_userid

    # if the dataset was not found or is not accessible
    else:

        LOGERROR('could not find dataset with setid: %s' % setid)
        db.close()
        return None


def _slugify_dataset_name(setname):
    '''
    This is based on the Django slugify function.

    https://github.com/django/django/blob/
    495abe00951ceb9787d7f36590f71aa14c973d3d/django/utils/text.py#L399

    '''

    normalized = unicodedata.normalize(
        'NFKD', setname
    ).encode(
        'ascii', 'ignore'
    ).decode(
        'ascii'
    )

    slugified = re.sub(r'[^\w\s-]', '', normalized).strip().lower()
    return re.sub(r'[-\s]+', '-', slugified)



def sqlite_edit_dataset(basedir,
                        setid,
                        updatedict=None,
                        incoming_userid=2,
                        incoming_role='anonymous',
                        incoming_session_token=None):
    '''This edits a dataset.

    updatedict is a dict containing the keys to update in the dataset pickle.

    updatedb is a list of tuples containing the columns and new values for the
    columns to update the lcc_datasets table listing for this dataset.

    The action to test is 'edit'

    The default incoming_userid is set to 2 -> anonymous user for
    safety. Anonymous users cannot edit datasets by default.

    The only allowed edits are to:

    name
    description
    citation

    Otherwise, datasets are supposed to be immutable (FIXME: for now).

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    dataset_pickle = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_pickle)

    # return immediately if the dataset doesn't exist
    if not os.path.exists(dataset_fpath):
        LOGERROR('could not find dataset with setid: %s' % setid)
        return None

    dataset_pickleheader = 'dataset-%s-header.pkl' % setid
    dataset_pickleheader_fpath = os.path.join(datasetdir,
                                              dataset_pickleheader)

    # get the lczip, cpzip, pfzip, dataset pkl shasums from the DB
    # also get the created_on, last_updated, nobjects, status
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # get the dataset's info from the DB
    query = ("select created_on, last_updated, status, "
             "dataset_owner, dataset_visibility, dataset_sharedwith, "
             "dataset_sessiontoken "
             "from lcc_datasets where setid = ?")
    cur.execute(query, (setid,))
    row = cur.fetchone()

    if row and len(row) > 0 and os.path.exists(dataset_fpath):

        dataset_status = row['status']

        # only complete datasets can be edited
        if dataset_status != 'complete':
            LOGERROR('dataset %s status = %s. only complete '
                     'datasets can be edited' % (setid, dataset_status))
            db.close()
            return None

        #
        # otherwise, proceed as normal
        #
        dataset_editable = sqlite_check_dataset_access(
            setid,
            'edit',
            incoming_userid=incoming_userid,
            incoming_role=incoming_role,
            database=db
        )

        # check if we can edit this dataset
        if not dataset_editable:

            LOGERROR('dataset %s is not editable by user_id = %s, role = %s'
                     % (setid, incoming_userid, incoming_role))
            db.close()
            return None

        # additional check for anonymous users
        if (incoming_role == 'anonymous'):

            LOGERROR('dataset %s is not editable by anonymous '
                     'users with session_token = %s'
                     % (setid, incoming_session_token))
            db.close()
            return None

        ds_update = {}
        db_update = {}

        # do the edits
        # 1. restrict to required length
        # 2. bleach the input
        for key in updatedict:

            if key == 'name':

                cleaned_name = bleach.clean(
                    updatedict['name'][:280],
                    strip=True
                )
                ds_update['name'] = cleaned_name
                # add in the slug
                ds_update['slug'] = _slugify_dataset_name(cleaned_name)

                db_update['name'] = cleaned_name

            if key == 'description':

                cleaned_description = bleach.clean(
                    updatedict['description'][:1024],
                    strip=True
                )
                ds_update['desc'] = cleaned_description
                db_update['description'] = cleaned_description

            if key == 'citation':

                cleaned_citation = bleach.clean(
                    updatedict['citation'][:280],
                    strip=True
                )
                ds_update['citation'] = cleaned_citation
                db_update['citation'] = cleaned_citation

        #
        # update the main dataset pickle
        #
        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)

        dataset.update(ds_update)
        dataset['updated'] = datetime.utcnow().isoformat()
        with gzip.open(dataset_fpath,'wb') as outfd:
            pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset main pickle: %s '
                'with updatedict = %r for setid = %s' %
                (dataset_fpath, ds_update, setid))

        #
        # update the dataset pickle header
        #
        with open(dataset_pickleheader_fpath,'rb') as infd:
            header = pickle.load(infd)

        header.update(ds_update)
        header['updated'] = datetime.utcnow().isoformat()
        with open(dataset_pickleheader_fpath,'wb') as outfd:
            pickle.dump(header, outfd, pickle.HIGHEST_PROTOCOL)

        LOGINFO('updated dataset header pickle: %s '
                'with updatedict = %r for setid = %s' %
                (dataset_pickleheader_fpath, ds_update, setid))
        #
        # update the database entry
        #
        query = (
            "update lcc_datasets set {update_elems} where setid = ?"
        )
        update_elems = []
        params = []
        for key in db_update:
            update_elems.append('%s = ?' % key)
            params.append(db_update[key])

        update_elems.append('last_updated = ?')
        params.append(datetime.utcnow().isoformat())

        query = query.format(update_elems=', '.join(update_elems))
        params = tuple(params + [setid])

        cur.execute(query, params)
        db.commit()
        db.close()

        LOGINFO('updated dataset database entry '
                'with updatedict = %r for setid = %s' %
                (db_update, setid))

        return ds_update

    # if the dataset was not found or is not accessible
    else:

        LOGERROR('could not find dataset with setid: %s' % setid)
        db.close()
        return None



def sqlite_delete_dataset(basedir,
                          setid,
                          incoming_userid=2,
                          incoming_role='anonymous',
                          incoming_session_token=None):
    '''This 'deletes' the specified dataset.

    'delete' in this case means:

    - set the visibility to private
    - set the owner to superuser

    This effectively makes the dataset vanish for normal users but we don't
    actually delete anything. FIXME: later, we may want to do a periodic actual
    delete of datasets that have been 'deleted'.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the action to test is 'delete'

    only superusers and staff can delete datasets.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    dataset_pickle = 'dataset-%s.pkl.gz' % setid
    dataset_fpath = os.path.join(datasetdir, dataset_pickle)

    # return immediately if the dataset doesn't exist
    if not os.path.exists(dataset_fpath):
        LOGERROR('could not find dataset with setid: %s' % setid)
        return None

    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    db.row_factory = sqlite3.Row

    # check 'delete' permissions
    dataset_deleteable = sqlite_check_dataset_access(
        setid,
        'delete',
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
        database=db
    )

    if not dataset_deleteable:
        LOGERROR('dataset %s is not editable by '
                 'user_id = %s, role = %s, session_token = %s'
                 % (setid,
                    incoming_userid,
                    incoming_role,
                    incoming_session_token))
        db.close()
        return None

    # change the owner
    changed_owner = sqlite_change_dataset_owner(
        basedir,
        setid,
        1,
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
        incoming_session_token=incoming_session_token
    )

    # change the visibility
    changed_visibility = sqlite_change_dataset_visibility(
        basedir,
        setid,
        new_visibility='private',
        incoming_userid=incoming_userid,
        incoming_role=incoming_role,
        incoming_session_token=incoming_session_token
    )

    # return True if deleted
    if changed_owner == 1 and changed_visibility == 'private':

        LOGWARNING('dataset %s has been marked as deleted by '
                   'user_id %s, role = %s, session_token = %s'
                   % (setid,
                      incoming_userid,
                      incoming_role,
                      incoming_session_token))
        return True

    else:

        LOGERROR('attempt to delete dataset %s failed. '
                 'user_id %s, role = %s, session_token = %s'
                 % (setid,
                    incoming_userid,
                    incoming_role,
                    incoming_session_token))

        return False
