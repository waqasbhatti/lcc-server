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
from functools import reduce
import hashlib
from datetime import datetime
from random import sample

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
insert into lcc_datasets_vinfo values (1, 'v0.2', '2018-08-31');

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


#####################
## RESULT PIPELINE ##
#####################

def results_sort_by_keys(rows, sorts=()):
    '''
    This sorts the results by the given sorts list.

    rows is the list of sqlite3.Row objects returned from a query.

    sorts is a list of tuples like so:

    ('sqlite.Row key to sort by', 'asc|desc')

    The sorts are applied in order of their appearance in the list.

    Returns the sorted list of sqlite3.Row items.

    '''

    # we iterate backwards from the last sort spec to the first
    # to handle stuff like order by object asc, ndet desc, sdssr asc as expected
    for s in sorts[::-1]:
        key, order = s
        if order == 'asc':
            rows = sorted(rows, key=lambda row: row[key], reversed=False)
        elif order == 'desc':
            rows = sorted(rows, key=lambda row: row[key], reversed=True)

    return rows



def results_apply_permissions(rows,
                              action='view',
                              target='object',
                              owner_key='owner',
                              visibility_key='visibility',
                              sharedwith_key='sharedwith',
                              incoming_userid=2,
                              incoming_role='anonymous'):
    '''This applies permissions to each row of the result for action and target.

    rows is the list of sqlite3.Row objects returned from a query.

    action is the action to check against permissions, e.g. 'view', 'list', etc.

    target is the item to apply the permission for, e.g. 'object', 'dataset',
    etc.

    incoming_userid and incoming_role are the user ID and role to check
    permission for against the rows, action, and target.

    '''

    return [
        x for x in rows if
        check_user_access(
            userid=incoming_userid,
            role=incoming_role,
            action=action,
            target_name=target,
            target_owner=x[owner_key],
            target_visibility=x[visibility_key],
            target_sharedwith=x[sharedwith_key]
        )
    ]



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
                           dataset_visibility='public',
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

    'public'  -> dataset is visible to anyone
    'shared'  -> dataset is visible to owner user ID
                 and the user IDs in the shared_with column
                 FIXME: we'll add groups and group ID columns later
    'private' -> dataset is only visible to the owner

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
                       dataset_visibility='public',
                       dataset_sharedwith=None,
                       make_dataset_csv=True,
                       rows_per_page=500):
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

    searchtype = searchresult['search']
    searchargs = searchresult['args']


    # add in the sortspec, samplespec, limitspec, visibility, and sharedwith to
    # searchargs so these can be stored in the DB and the pickle for later
    # retrieval
    searchargs['sortspec'] = results_sortspec
    searchargs['limitspec'] = results_sortspec
    searchargs['samplespec'] = results_sortspec
    searchargs['visibility'] = dataset_visibility
    searchargs['sharedwith'] = dataset_sharedwith

    # get the columnspecs and actual collectionids for each collection searched
    # so we can return the column names and descriptions as well
    columnspec = {x:searchresult[x]['columnspec'] for x in collections}
    collid = {x:searchresult[x]['collid'] for x in collections}

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

    if results_sortspec is not None:
        rows = results_sort_by_keys(rows, sorts=results_sortspec)

    if results_limitspec is not None:
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
        'created': creationdt,
        'updated': last_updated,
        'owner': incoming_userid,
        'visibility': dataset_visibility,
        'sharedwith': dataset_sharedwith,
        'searchtype': searchtype,
        'searchargs': searchargs,
        'collections': collections,
        'coll_dirs': collid,
        'npages':npages,
        'rows_per_page':rows_per_page,
        'page_slices':page_slices,
        'nmatches_by_collection':nmatches,
        'total_nmatches': total_nmatches,
        'columns': reqcols,
        'coldesc': {},
        'dataset_csv':dataset_csv,
        'dataset_pickle':dataset_fpath,
        'lczipfpath':lczip_fpath,
    }

    # go through each column and get its info from colspec
    # also build up the format string for the CSV
    for col in reqcols:

        dataset['coldesc'][col] = {
            'title': columnspec[collections[0]][col]['title'],
            'desc': columnspec[collections[0]][col]['description'],
            'dtype': columnspec[collections[0]][col]['dtype'],
            'format': columnspec[collections[0]][col]['format'],
        }

    # generate the JSON header for the CSV
    csvheader = json.dumps(dataset, indent=2)
    csvheader = indent(csvheader, '# ')

    # 1. write the header pickle to the datasets directory
    dataset_header_pkl = os.path.join(datasetdir,
                                      'dataset-%s-header.pkl' % setid)

    with open(dataset_header_pkl,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset header pickle: %s for dataset setid: %s' %
            (dataset_header_pkl, setid))

    # add in the rows to turn the header into the complete dataset pickle
    dataset['result'] = rows

    # open the datasets database
    datasets_dbf = os.path.join(basedir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    # 3. generate the entry in the lcc-datasets.sqlite table and commit it
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
        total_nmatches,
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


    # 4. we'll now create the auxiliary files in datasets/ for this dataset
    # FIXME: some of these will have to be regenerated if people edit datasets
    #
    # - dataset-<setid>-rows-page1.pkl -> pageX.pkl as required
    # - dataset-<setid>-rows-page1-strformat.pkl -> pageX-strformat.pkl
    #                                                as required
    # - dataset-<setid>.csv

    # we'll open ALL of these files at once and go through the object row loop
    # ONLY ONCE

    csvfd = open(dataset_csv,'wb')

    # write the header to the CSV file
    csvfd.write(('%s\n' % csvheader).encode())

    all_original_lcs = []
    csvlcs_to_generate = []
    csvlcs_ready = []

    # this lets the frontend track which LCs are missing and update these later
    # this contains (objectid, collection) tuples
    objectids_later_csvlcs = []

    # we'll iterate by pages
    for page, pgslice in enumerate(page_slices):

        pgrows = rows[pgslice[0]:pgslice[1]]
        page_number = page + 1

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

        # now we'll go through all the rows
        for entry in pgrows:

            all_original_lcs.append(entry['db_lcfname'])
            csvlc = '%s-csvlc.gz' % entry['db_oid']
            csvlc_path = os.path.join(os.path.abspath(basedir),
                                      'csvlcs',
                                      entry['collection'].replace('_','-'),
                                      csvlc)

            if 'db_lcfname' in entry and os.path.exists(csvlc_path):

                entry['db_lcfname'] = '/l/%s/%s' % (
                    entry['collection'].replace('_','-'),
                    csvlc
                )
                csvlcs_ready.append(csvlc_path)

            elif 'db_lcfname' in entry and not os.path.exists(csvlc_path):

                csvlcs_to_generate.append(
                    (entry['db_lcfname'],
                     entry['db_oid'],
                     searchresult[entry['collection']]['lcformatdesc'],
                     entry['collection'],
                     csvlc_path)
                )
                objectids_later_csvlcs.append(
                    (entry['db_oid'],
                     entry['collection'])
                )

                entry['db_lcfname'] = '/l/%s/%s' % (
                    entry['collection'].replace('_','-'),
                    csvlc
                )

            elif 'lcfname' in entry and os.path.exists(csvlc_path):

                entry['lcfname'] = '/l/%s/%s' % (
                    entry['collection'].replace('_','-'),
                    csvlc
                )
                csvlcs_ready.append(csvlc_path)

            elif 'lcfname' in entry and not os.path.exists(csvlc_path):

                csvlcs_to_generate.append(
                    (entry['lcfname'],
                     entry['db_oid'],
                     searchresult[entry['collection']]['lcformatdesc'],
                     entry['collection'],
                     csvlc_path)
                )
                objectids_later_csvlcs.append(
                    (entry['db_oid'],
                     entry['collection'])
                )

                entry['lcfname'] = '/l/%s/%s' % (
                    entry['collection'].replace('_','-'),
                    csvlc
                )

            # the normal data table row
            outrow = [entry[c] for c in reqcols]
            outrows.append(outrow)

            # the string formatted data table row
            # this will be written to the CSV and to the page JSON
            out_strformat_row = []

            for c in reqcols:

                cform = dataset['coldesc'][c]['format']
                if 'f' in cform and entry[c] is None:
                    out_strformat_row.append('nan')
                elif 'i' in cform and entry[c] is None:
                    out_strformat_row.append('-9999')
                else:
                    thisr = cform % entry[c]
                    # replace any pipe characters with commas
                    # to save us from broken CSVs
                    thisr = thisr.replace('|',', ')
                    out_strformat_row.append(thisr)

            # append the strformat row to the page JSON
            out_strformat_rows.append(out_strformat_row)

            # write this row to the CSV
            outline = '%s\n' % '|'.join(out_strformat_row)
            csvfd.write(outline.encode())

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

    #
    # write the pickle to the datasets directory
    #

    # add in the pointers to unready CSVLCs
    dataset['csvlcs_in_progress'] = objectids_later_csvlcs

    with gzip.open(dataset_fpath,'wb') as outfd:
        pickle.dump(dataset, outfd, pickle.HIGHEST_PROTOCOL)

    LOGINFO('wrote dataset pickle for search results to %s, setid: %s' %
            (dataset_fpath, setid))

    # finish up the CSV when we're done with all of the pages
    #
    csvfd.close()
    LOGINFO('wrote CSV: %s for setid: %s' % (dataset_csv, setid))

    # return the setid
    return setid, csvlcs_to_generate, csvlcs_ready, sorted(all_original_lcs)



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
                              dataset_csvlcs_ready,
                              dataset_all_original_lcs,
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
        if not skip_lc_collection:

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
                    os.path.symlink(os.path.abspath(res), outcsvlc)

            #
            # FINALLY, CARRY OUT THE ZIP OPERATION (IF NEEDED)
            #
            zipfile_lclist = (
                dataset_csvlcs_ready +
                [x[-1] for x in dataset_csvlcs_to_generate]
            )

            # do not generate LCs if their number > max number allowed
            if len(zipfile_lclist) > max_dataset_lcs:

                LOGERROR('LCs in dataset: %s > max_dataset_lcs: %s, '
                         ' will not generate a ZIP file.' %
                         (len(zipfile_lclist), max_dataset_lcs))

            else:

                # get the expected name of the output zipfile
                lczip_fpath = dataset['lczipfpath']

                LOGINFO('writing %s LC files to zip file: %s for setid: %s...' %
                        (len(zipfile_lclist), lczip_fpath, setid))

                # set up the zipfile
                with ZipFile(lczip_fpath, 'w', allowZip64=True) as outzip:
                    for lcf in zipfile_lclist:
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
                      'make_public','make_private','make_shared',
                      'change_owner'}

    incoming_userid and incoming_role are the user ID and role of the user to
    check access for.

    database is either the path to the lcc-datasets.sqlite DB or the connection
    handler to such a connection to re-use an already open connection.

    '''

    # return immediately if the action doesn't make sense
    if action not in ('view','edit','delete',
                      'make_public','make_private','make_shared',
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

    query = ("select setid, created_on, last_updated, nobjects, "
             "name, description, citation, "
             "queried_collections, query_type, query_params, "
             "dataset_owner, dataset_visibility, "
             "dataset_sharedwith, dataset_sessiontoken "
             "from "
             "lcc_datasets where status = ?"
             "order by last_updated desc limit ?")

    # make sure we never get more than 1000 recent datasets
    if nrecent > 1000:
        nrecent = 1000

    cur.execute(query, (require_status, nrecent))
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
            'message':'found no datasets matching query parameters'
        }

    db.close()
    return returndict



def sqlite_get_dataset(basedir,
                       setid,
                       returnspec,
                       incoming_userid=2,
                       incoming_role='anonymous'):
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
             "dataset_owner, dataset_visibility, dataset_sharedwith "
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

                with open(getpath,'rb') as infd:
                    outdict = pickle.load(infd)

                db.close()
                outdict['status'] = dataset_status
                return outdict

            elif returnspec == 'json-header':

                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                db.close()
                header['status'] = dataset_status
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
                db.close()
                return header

            elif returnspec.startswith('json-page-'):

                page_to_get = returnspec.split('-')[-1]
                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                getpath2 = os.path.join(
                    datasetdir,
                    'dataset-%s-rows-page%s.pkl' % (setid, page_to_get)
                )
                if os.path.exists(getpath2):
                    with open(getpath2, 'rb') as infd:
                        setrows = pickle.load(infd)
                else:
                    LOGERROR('requested page: %s for dataset: %s does not exist'
                             % (page_to_get, setid))
                    setrows = []

                header['rows'] = setrows
                header['status'] = dataset_status
                header['currpage'] = int(page_to_get)
                db.close()
                return header

            elif returnspec.startswith('strjson-page-'):

                page_to_get = returnspec.split('-')[-1]
                getpath1 = os.path.join(datasetdir,
                                        'dataset-%s-header.pkl' % setid)
                with open(getpath1,'rb') as infd:
                    header = pickle.load(infd)

                getpath2 = os.path.join(
                    datasetdir,
                    'dataset-%s-rows-page%s-strformat.pkl' % (setid,
                                                              page_to_get)
                )
                if os.path.exists(getpath2):
                    with open(getpath2, 'rb') as infd:
                        setrows = pickle.load(infd)
                else:
                    LOGERROR('requested page: %s for dataset: %s does not exist'
                             % (page_to_get, setid))
                    setrows = []

                header['rows'] = setrows
                header['status'] = dataset_status
                header['currpage'] = int(page_to_get)
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



# LATER
def sqlite_remove_dataset(basedir,
                          setid,
                          incoming_userid=2,
                          incoming_role='anonymous'):
    '''
    This removes the specified dataset.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the action to test is 'delete'

    '''


# LATER
def sqlite_update_dataset(basedir,
                          setid,
                          updatedict=None,
                          updatedb=None,
                          incoming_userid=2,
                          incoming_role='anonymous'):
    '''This updates a dataset.

    updatedict is a dict containing the keys to update in the dataset pickle.

    updatedb is a list of tuples containing the columns and new values for the
    columns to update the lcc_datasets table listing for this dataset.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the action to test is 'edit'

    '''


#####################################
## SEARCHING FOR STUFF IN DATASETS ##
#####################################

# TODO: these functions search in the lcc-datasets.sqlite databases:
# - full text match on dataset ID, description, name, searchargs
# - cone search and overlapping query on dataset footprint
