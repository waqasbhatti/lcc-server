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
    role_maxrows = check_role_limits(incoming_role)

    if rowlimit is not None and rowlimit > role_maxrows:
        return rows[:role_maxrows]
    elif rowlimit is not None and rowlimit < role_maxrows:
        return rows[:rowlimit]
    else:
        return rows


def results_random_sample(rows, sample_count=None):
    '''This returns sample_count uniformly sampled without replacement rows.

    '''

    if sample_count is not None and 0 < sample_count < len(rows):
        return sample(rows, sample_count)
    else:
        return rows


pipeline_funcs = {
    'permissions':results_apply_permissions,
    'randomsample':results_random_sample,
    'sort':results_sort_by_keys,
    'limit':results_limit_rows
}


def results_pipeline(rows, operation_list):
    '''Runs operations in order against rows to produce the final result set.

    '''

    for op in operation_list:

        func, args, kwargs = op

        rows = func(rows, *args, **args)

        if len(rows) == 0:
            break

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
                       dataset_visibility='public',
                       dataset_sharedwith=None,
                       make_dataset_csv=True,
                       rows_per_page=500):
    '''This is the new-style dataset pickle maker.

    Converts the results from the backend into a data table with rows from all
    collections in a single array instead of broken up by collection. This
    allows us to resort on their properties over the entire dataset instead of
    just per collection.

    FIXME: implement strformat, make_dataset_csv, and rows_per_page to generate
    all the JSON headers and stuff in one shot so we can return the full dataset
    quickly.

    FIXME: convert original format LC names to LCC CSV filenames as well. Then
    the lczip function below can just check if the expected file exists at the
    specified location and regen it if it doesn't.

    FIXME: We'll check in this function if the output light curve CSVs exist for
    each row and mark them for regen if not present. This way, we can return the
    dataset view immediately while letting the LC ZIP finish in the
    background. When that finishes, the frontend polling JS will receive a LC
    ZIP done message along with its download path and a list of any regenerated
    light curves that should be added to the existing data table.

    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))
    productdir = os.path.abspath(os.path.join(basedir, 'products'))

    # get some stuff out of the search result
    collections = searchresult['databases']

    searchtype = searchresult['search']
    searchargs = searchresult['args']

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

    # 2. write the pickle to the datasets directory
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

    lcs_to_generate = []
    lcs_ready = []
    all_original_lcs = []

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
                lcs_ready.append(csvlc_path)

            elif 'db_lcfname' in entry and not os.path.exists(csvlc_path):

                lcs_to_generate.append(
                    (entry['db_lcfname'],
                     entry['db_oid'],
                     entry['collection'],
                     csvlc_path)
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
                lcs_ready.append(csvlc_path)

            elif 'lcfname' in entry and not os.path.exists(csvlc_path):

                lcs_to_generate.append(
                    (entry['lcfname'],
                     entry['db_oid'],
                     entry['collection'],
                     csvlc_path)
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
    # finish up the CSV when we're done with all of the pages
    #
    csvfd.close()
    LOGINFO('wrote CSV: %s for setid: %s' % (dataset_csv, setid))

    # return the setid
    return setid, lcs_to_generate, lcs_ready, sorted(all_original_lcs)



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
                              dataset_lcs_to_generate,
                              dataset_lcs_ready,
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
                          updatedict,
                          incoming_userid=2,
                          incoming_role='anonymous'):
    '''
    This updates a dataset.

    The default incoming_userid is set to 2 -> anonymous user for safety.

    the action to test is 'edit'

    '''



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
             "dataset_owner, dataset_visibility, dataset_sharedwith "
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
                       incoming_userid=2,
                       incoming_role='anonymous',
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
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # if the dataset pickle for this dataset doesn't exist yet, then it's either
    # just been initialized or it's broken
    if not os.path.exists(dataset_fpath):

        LOGWARNING('expected dataset pickle does not exist for setid: %s' %
                   setid)

        # if the dataset pickle doesn't exist, check the DB for its status
        query = ("select created_on, last_updated, status, "
                 "dataset_owner, dataset_visibility, dataset_sharedwith "
                 "from lcc_datasets where setid = ?")
        cur.execute(query, (setid,))
        row = cur.fetchone()

        if row and len(row) > 0:

            dataset_accessible = check_user_access(
                userid=incoming_userid,
                role=incoming_role,
                action='view',
                target_name='dataset',
                target_owner=row['dataset_owner'],
                target_visibility=row['dataset_visibility'],
                target_sharedwith=row['dataset_sharedwith']
            )

            if dataset_accessible:

                # check the dataset's status
                # this should only be initialized if the dataset pickle doesn't
                # exist
                if row['status'] == 'initialized':

                    dataset_status = 'in progress'

                # if the status is anything else, the dataset is in an unknown
                # state, and is probably broken
                else:

                    dataset_status = 'broken'

                returndict = {
                    'setid': setid,
                    'created_on':row['created_on'],
                    'last_updated':row['last_updated'],
                    'nobjects':0,
                    'status': dataset_status,
                    'owner': row['dataset_owner'],
                    'visibility': row['dataset_visibility'],
                    'sharedwith': row['dataset_sharedwith'],
                    'name':None,
                    'desc':None,
                    'columns':None,
                    'searchtype':None,
                    'searchargs':None,
                    'lczip':None,
                    'dataset_csv':None,
                    'collections':None,
                    'result':None,
                }

                LOGWARNING("dataset: %s is in state: %s" % (setid,
                                                            dataset_status))
                db.close()
                return returndict

            else:

                LOGWARNING(
                    'user: %s, role: %s does not have access to '
                    'dataset: %s owned by: %s, visibility: %s, '
                    'shared with: %s' %
                    (incoming_userid, incoming_role, setid,
                     row['dataset_owner'],
                     row['dataset_visibility'],
                     row['dataset_sharedwith'])
                )
                db.close()
                return None

        # if no dataset entry in the DB, then this DS doesn't exist at all
        else:

            LOGERROR('requested dataset: %s does not exist' % setid)
            db.close()
            return None

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

        # read in the pickle
        with gzip.open(dataset_fpath,'rb') as infd:
            dataset = pickle.load(infd)

        returndict = {
            'setid':dataset['setid'],
            'name':dataset['name'],
            'desc':dataset['desc'],
            'owner':dataset['owner'],
            'visibility':dataset['visibility'],
            'sharedwith':dataset['sharedwith'],
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

            returndict['result'][coll] = {
                'data':dataset['result'][coll][::],
                'success':dataset['success'][coll],
                'message':dataset['message'][coll],
                'nmatches':dataset['nmatches'][coll],
                'columnspec':dataset['columnspec'][coll],
                'collid':dataset['collid'][coll]
            }

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
            params = (
                datetime.utcnow().isoformat(),
                sum(dataset['nmatches'][x] for x in dataset['collections']),
                'complete',
                dataset['setid']
            )
            cur.execute(query, params)
            db.commit()
            db.close()

            if not os.path.exists(returndict['lczip']):
                returndict['lczip'] = None

            # link the CSV LCs to the output csvlcs directory if available this
            # should let these through to generate_dataset_tablerows even if the
            # dataset is forced to completion
            for collection in returndict['collections']:
                for row in returndict['result'][collection]['data']:

                    # NOTE: here we rely on the fact that the collection names
                    # are normalized by the CLI (assuming that's the only way
                    # people generate these collections). The generated
                    # collection IDs never include a '_', so we can safely
                    # change from the DB collection ID which can't have a '-' to
                    # a collection ID == directory name on disk, which can't
                    # have a '_'.
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
                    # corresponding row key in the original dataset dict this
                    # shouldn't be happening though, because we (attempted to)
                    # make a copy of the dataset row list above (apparently
                    # not!)

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


    # otherwise, the dataset is not accessible
    else:

        db.close()
        return None



#####################################
## SEARCHING FOR STUFF IN DATASETS ##
#####################################

# TODO: these functions search in the lcc-datasets.sqlite databases:
# - full text match on dataset ID, description, name, searchargs
# - cone search and overlapping query on dataset footprint
