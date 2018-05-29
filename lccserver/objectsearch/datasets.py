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



########################################
## FUNCTIONS THAT OPERATE ON DATASETS ##
########################################

def sqlite_new_dataset(basedir,
                       searchresult,
                       ispublic=True):
    '''
    new dataset function.

    setispublic controls if the dataset is public

    '''



def sqlite_remove_dataset(basedir, setname):
    '''
    This removes the specified dataset.

    '''



def sqlite_update_dataset(basedir, setname, updatedict):
    '''
    This updates a dataset.

    '''


def sqlite_list_datasets(basedir, require_ispublic=True):
    '''
    This just lists all the datasets available.

    '''


def sqlite_get_dataset(basedir, setname):
    '''
    This gets the dataset as a dictionary.

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



def sqlite_dataset_sql_search(basedir,
                              sqlstatement,
                              lcclist=None,
                              require_ispublic=True):
    '''
    This does an arbitrary SQL search and returns a dataset.

    '''



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
