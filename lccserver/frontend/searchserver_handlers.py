#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''searchserver_handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) -
                             Apr 2018

These are Tornado handlers for the searchserver.

'''

####################
## SYSTEM IMPORTS ##
####################

import os
import os.path
import logging
import numpy as np
from datetime import datetime


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


###########################
## COLUMN SEARCH HANDLER ##
###########################

class ColumnSearchHandler(tornado.web.RequestHandler):
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



#########################
## CONE SEARCH HANDLER ##
#########################

class ConeSearchHandler(tornado.web.RequestHandler):
    '''
    This handles the cone search API.

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

        # FIXME: this is temporary
        # add error and escaping handling here
        # we'll need parsing for different kinds of RA/DEC
        # - sexagesimal
        # - decimal
        # - ':' vs ' ' as separators in
        # use the regex from previously


        center_ra = float(self.get_argument('center_ra'))
        center_decl = float(self.get_argument('center_decl'))
        radius_arcmin = float(self.get_argument('radius_arcmin'))
        result_ispublic = (
            True if int(self.get_argument('result_ispublic')) else False
        )

        # optional stream argument
        stream = self.get_argument('stream', default=0)


        #
        # we'll use line-delimited JSON to respond
        #
        # FIXME: this should be selectable using an arg: stream=1 or 0

        # FIXME: we might want to end early for API requests and just return the
        # setid, from which one can get the URL for the dataset if it finishes
        # looks like we can use RequestHandler.on_finish() to do this, but we
        # need to figure out how to pass stuff to that function from here (maybe
        # a self.postprocess_items boolean will work?)

        # Q1. prepare the dataset
        setinfo = yield self.executor.submit(
            datasets.sqlite_prepare_dataset,
            self.basedir,
            ispublic=result_ispublic
        )

        setid, creationdt = setinfo

        # A1. we have a setid, send this back to the client
        retdict = {
            "message":"received a setid: %s" % setid,
            "status":"streaming",
            "result":{"setid":setid},  # add submit datetime, args, etc.
            "time":'%sZ' % datetime.utcnow().isoformat()
        }
        retdict = '%s\n' % json.dumps(retdict)
        self.set_header('Content-Type','application/json')
        self.write(retdict)
        yield self.flush()

        # Q2. execute the query
        query_result = yield self.executor.submit(
            dbsearch.sqlite_kdtree_conesearch,
            self.basedir,
            center_ra,
            center_decl,
            radius_arcmin,
            # extra kwargs here later
        )

        # A2. we have the query result, send back a query completed message
        if query_result is not None:

            nrows = 42

            retdict = {
                "message":"query complete, objects matched: %s" % nrows,
                "status":"streaming",
                "result":{
                    "setid":setid,
                    "nrows":nrows
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # Q3. make the dataset pickle and close out dataset row in the DB
            dspkl_setid = yield self.executor.submit(
                datasets.sqlite_new_dataset,
                self.basedir,
                setid,
                creationdt,
                query_result,
                ispublic=result_ispublic
            )

            # A3. we have the dataset pickle generated, send back an update
            dataset_pickle = "/d/dataset-%s.pkl.gz" % dspkl_setid
            retdict = {
                "message":("dataset pickle generation complete: %s" %
                           dataset_pickle),
                "status":"streaming",
                "result":{
                    "setid":dspkl_setid,
                    "dataset_pickle":dataset_pickle
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # Q4. collect light curve ZIP file
            lczip = yield self.executor.submit(
                datasets.sqlite_make_dataset_lczip,
                self.basedir,
                dspkl_setid,
                # FIXME: think about this (probably not required on actual LC
                # server)
                override_lcdir=os.path.join(self.basedir,
                                            'hatnet-keplerfield',
                                            'lightcurves')
            )

            # A4. we're done with collecting light curves
            lczip_url = '/p/%s' % os.path.basename(lczip)
            retdict = {
                "message":("dataset LC ZIP complete: %s" % lczip_url),
                "status":"streaming",
                "result":{
                    "setid":dspkl_setid,
                    "dataset_lczip":lczip_url
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # A5. finish request by sending back the dataset URL
            dataset_url = "/set/%s" % dspkl_setid
            retdict = {
                "message":("dataset now ready: %s" % dataset_url),
                "status":"ok",
                "result":{
                    "setid":dspkl_setid,
                    "seturl":dataset_url
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            self.finish()

        else:

            retdict = {
                "status":"failed",
                "result":{
                    "setid":setid,
                    "nrows":0
                },
                "message":"query failed, no matching objects found"
            }
            self.write(retdict)
            self.finish()




#############################
## FULLTEXT SEARCH HANDLER ##
#############################

class FTSearchHandler(tornado.web.RequestHandler):
    '''
    This handles the FTS API.

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



###########################
## XMATCH SEARCH HANDLER ##
###########################

class XMatchHandler(tornado.web.RequestHandler):
    '''
    This handles the xmatch search API.

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
