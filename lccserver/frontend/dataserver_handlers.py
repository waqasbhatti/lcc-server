#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''dataserver_handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) -
                             Apr 2018

These are Tornado handlers for the dataserver.

'''

####################
## SYSTEM IMPORTS ##
####################

import os
import os.path
import logging
import numpy as np


######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
import json

class FrontendEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return obj.decode()
        elif isinstance(obj, complex):
            return (obj.real, obj.imag)
        elif (isinstance(obj, (float, np.float64, np.float_)) and
              not np.isfinite(obj)):
            return None
        elif isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        else:
            return json.JSONEncoder.default(self, obj)

# this replaces the default encoder and makes it so Tornado will do the right
# thing when it converts dicts to JSON when a
# tornado.web.RequestHandler.write(dict) is called.
json._default_encoder = FrontendEncoder()

#############
## LOGGING ##
#############

# get a logger
LOGGER = logging.getLogger(__name__)

#####################
## TORNADO IMPORTS ##
#####################

import tornado.ioloop
import tornado.httpserver
import tornado.web

from tornado.escape import xhtml_escape, xhtml_unescape, url_unescape
from tornado import gen


###################
## LOCAL IMPORTS ##
###################

from ..objectsearch import dbsearch
dbsearch.set_logger_parent(__name__)
from ..objectsearch import datasets
datasets.set_logger_parent(__name__)


#############################
## DATASET DISPLAY HANDLER ##
#############################

class DatasetHandler(tornado.web.RequestHandler):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
        '''This runs the query.

        '''

        collections = yield self.executor.submit(
            dbsearch.sqlite_list_collections,
            self.basedir
        )

        collection_info = collections['info']
        all_columns = collections['columns']
        all_indexed_columns = collections['indexedcols']
        all_fts_columns = collections['ftscols']

        # censor some bits
        del collection_info['kdtree_pkl_path']
        del collection_info['object_catalog_path']

        # we'll reform the lcformatdesc path so it can be downloaded directly
        # from the LCC server
        lcformatdesc = collection_info['lcformatdesc']
        lcformatdesc = [
            '/c%s' % (x.replace(self.basedir,'')) for x in lcformatdesc
        ]
        collection_info['lcformatdesc'] = lcformatdesc

        returndict = {
            'status':'ok',
            'result':{'available_columns':all_columns,
                      'available_indexed_columns':all_indexed_columns,
                      'available_fts_columns':all_fts_columns,
                      'collections':collection_info},
            'message':(
                'found %s collections in total' %
                len(collection_info['collection_id'])
            )
        }

        # return to sender
        self.write(returndict)
        self.finish()



#############################
## DATASET LISTING HANDLER ##
#############################

class AllDatasetsHandler(tornado.web.RequestHandler):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
        '''This runs the query.

        '''

        collections = yield self.executor.submit(
            dbsearch.sqlite_list_collections,
            self.basedir
        )

        collection_info = collections['info']
        all_columns = collections['columns']
        all_indexed_columns = collections['indexedcols']
        all_fts_columns = collections['ftscols']

        # censor some bits
        del collection_info['kdtree_pkl_path']
        del collection_info['object_catalog_path']

        # we'll reform the lcformatdesc path so it can be downloaded directly
        # from the LCC server
        lcformatdesc = collection_info['lcformatdesc']
        lcformatdesc = [
            '/c%s' % (x.replace(self.basedir,'')) for x in lcformatdesc
        ]
        collection_info['lcformatdesc'] = lcformatdesc

        returndict = {
            'status':'ok',
            'result':{'available_columns':all_columns,
                      'available_indexed_columns':all_indexed_columns,
                      'available_fts_columns':all_fts_columns,
                      'collections':collection_info},
            'message':(
                'found %s collections in total' %
                len(collection_info['collection_id'])
            )
        }

        # return to sender
        self.write(returndict)
        self.finish()
