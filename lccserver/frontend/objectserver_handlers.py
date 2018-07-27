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
from datetime import datetime, timedelta
import re
import glob

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

#####################
## TORNADO IMPORTS ##
#####################

import tornado.ioloop
import tornado.httpserver
import tornado.web

from tornado.escape import xhtml_escape
from base64 import b64encode
from tornado import gen

from tornado.httpclient import AsyncHTTPClient
from urllib.parse import urlencode



###################################################
## Handler to get the checkplot JSON all at once ##
###################################################

def check_for_checkplots(objectid, basedir, collection):
    '''
    This does all the hard work of finding the right checkplot to get.

    '''

    cpfpath = os.path.join(basedir,
                           collection,
                           'checkplots',
                           'checkplot-%s*.pkl*' % objectid)
    possible_checkplots = glob.glob(cpfpath)

    LOGGER.info('checkplot candidates found at: %r' % possible_checkplots)

    if len(possible_checkplots) == 0:

        return None, cpfpath

    elif len(possible_checkplots) == 1:

        return possible_checkplots[0], cpfpath

    # if there are multiple checkplots, they might .gz or cpserver-temp ones,
    # pick the canonical form of *.pkl if it exists, *.pkl.gz if it doesn't
    # exist. if neither of these exist, return None
    else:

        if cpfpath.replace('*','') in possible_checkplots:
            return cpfpath.replace('*',''), cpfpath

        elif cpfpath.replace('*','.gz') in possible_checkplots:
            return cpfpath.replace('*','.gz'), cpfpath

        else:
            return None, cpfpath



class ObjectCheckplotHandler(tornado.web.RequestHandler):
    '''
    This handles talking to the checkplotserver for checkplot info
    on a given object.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir,
                   signer,
                   fernet,
                   cpsharedkey,
                   cpaddress):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.apiversion = apiversion
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir
        self.signer = signer
        self.fernet = fernet
        self.cpkey = cpsharedkey
        self.cpaddr = cpaddress



    @gen.coroutine
    def get(self):
        '''This runs the query.

        /api/checkplot?<objectid>&<collection>

        where <objectid> is the db_oid (objectid in the database) and
        <collection> is the name of the collection the object can be found in.

        NOTE: <collection> is the actual collection name on disk NOT the
        db_collection_id (i.e. 'hatnet-keplerfield' instead of
        'hatnet_keplerfield')

        we'll look up the object:

        basedir/collection/checkplots/checkplot-<objectid>.pkl[.gz]

        if it exists, we'll ask the background checkplotserver for the checkplot
        info as JSON using the AsyncHTTPClient.

        FIXME: should we be able to search for objects in multiple collections?
        probably yes. for now, we'll restrict this to the collection provided as
        the argument.

        '''

        objectid = self.get_argument('objectid',default=None)
        collection = self.get_argument('collection',default=None)

        if not objectid:

            self.set_status(400)
            retdict = {'status':'failed',
                       'message':('No object ID was provided to '
                                  'fetch a checkplot for.'),
                       'result':None}
            raise tornado.web.Finish()

        if not collection:

            self.set_status(400)
            retdict = {'status':'failed',
                       'message':('No collection ID was provided to '
                                  'find the object in.'),
                       'result':None}
            self.write(retdict)
            raise tornado.web.Finish()

        objectid = xhtml_escape(objectid)
        collection = xhtml_escape(collection)

        # 1. get the canonical checkplot path
        checkplot_fpath, checkplot_fglob = yield self.executor.submit(
            check_for_checkplots,
            objectid,
            self.basedir,
            collection
        )

        # 2. ask the checkplotserver for this checkplot using our key
        if checkplot_fpath is not None:

            url = self.cpaddr + '/standalone'

            # generate our request URL
            req_url = url + '?' + urlencode(
                {'cp':b64encode(checkplot_fpath.encode()),
                 'key':self.cpkey})

            client = AsyncHTTPClient()
            resp = yield client.fetch(req_url, raise_error=False)

            if resp.code != 200:

                LOGGER.error('could not fetch checkplot: %s '
                             'for object: %s in collection: %s '
                             'from the backend checkplotserver at %s. '
                             'the error code was: %s from: %s'
                             % (checkplot_fpath,
                                objectid,
                                collection,
                                self.cpaddr, resp.code, resp.effective_url))

            # the checkplot server returns a JSON in case of success or error,
            # so that's what we'll forward to the client.
            self.set_header('Content-Type',
                            'application/json; charset=UTF-8')
            self.write(resp.body)
            self.finish()

        else:

            LOGGER.error(
                'could not find the requested checkplot using glob: %s' %
                checkplot_fglob
            )
            self.set_status(404)
            retdict = {
                'status':'failed',
                'result':None,
                'message':'could not find the requested checkplot on disk'
            }
            self.write(retdict)
            self.finish()
