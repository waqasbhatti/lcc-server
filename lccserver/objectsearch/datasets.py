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

import numpy as np

from . import dbsearch
dbsearch.set_logger_parent(__name__)


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
  is_public integer,
  name text,
  description text,
  citation text,
  primary key (setid)
);
'''

def sqlite_datasets_db_create(basedir):
    '''
    This makes a new datasets DB in basedir.

    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))

    # open the datasets database
    datasets_dbf = os.path.join(datasetdir, 'lcc-datasets.sqlite')
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

def sqlite_new_dataset(basedir,
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
     'cpzipfpath': the path to the checkplot ZIP in basedir/products,
     'pfzipfpath': the path to the periodfinding ZIP in basedir/products,}


    '''

    # get the dataset dir
    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))

    # open the datasets database
    datasets_dbf = os.path.join(datasetdir, 'lcc-datasets.sqlite')
    db = sqlite3.connect(
        datasets_dbf,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    cur = db.cursor()

    #



def sqlite_remove_dataset(basedir, setid):
    '''
    This removes the specified dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_update_dataset(basedir, setid, updatedict):
    '''
    This updates a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_list_datasets(basedir, require_ispublic=True):
    '''
    This just lists all the datasets available.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_get_dataset(basedir, setid):
    '''
    This gets the dataset as a dictionary.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



def sqlite_generate_dataset_lczip(basedir, setid):
    '''
    This generates a light curve zip for the specified setid.

    make sure to generate a SHA256 sum as well to ensure download integrity.

    this goes into a basedir/products/dataset-<setid>-lightcurves.zip file.

    '''



def sqlite_generate_dataset_cpzip(basedir, setid):
    '''
    This generates a checkplot zip for the specified setid.

    make sure to generate a SHA256 sum as well to ensure download integrity.

    this goes into a basedir/products/dataset-<setid>-checkplots.zip file.

    '''



def sqlite_generate_dataset_pfzip(basedir, setid):
    '''
    This generates a checkplot zip for the specified setid.

    make sure to generate a SHA256 sum as well to ensure download integrity.

    this goes into a basedir/products/dataset-<setid>-pfresults.zip file.

    '''


################################################################
## FUNCTIONS THAT WRAP DBSEARCH FUNCTIONS AND RETURN DATASETS ##
################################################################


def sqlite_dataset_fulltext_search(basedir,
                                   ftsquerystr,
                                   getcolumns=None,
                                   extraconditions=None,
                                   lcclist=None,
                                   require_ispublic=True):
    '''
    This does a full-text search and returns a dataset.

    '''

    datasetdir = os.path.abspath(os.path.join(basedir, 'datasets'))



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
