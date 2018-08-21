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

import tornado.ioloop
import tornado.httpserver
import tornado.web

from tornado.escape import xhtml_escape, url_unescape
from base64 import b64encode
from tornado import gen

from tornado.httpclient import AsyncHTTPClient
from urllib.parse import urlencode


###################
## LOCAL IMPORTS ##
###################

from .. import __version__


###################################################
## Handler to get the checkplot JSON all at once ##
###################################################

def check_for_checkplots(objectid, basedir, collection):
    '''
    This does all the hard work of finding the right checkplot to get.

    '''

    cpfpath1 = os.path.join(basedir,
                            collection,
                            'checkplots',
                            'checkplot-%s*.pkl*' % objectid)

    # this is the annoying bit we need to fix, need to check with collection
    # underscore -> hyphen
    cpfpath2 = os.path.join(basedir,
                            collection.replace('_','-'),
                            'checkplots',
                            'checkplot-%s*.pkl*' % objectid)

    possible_checkplots = glob.glob(cpfpath1) + glob.glob(cpfpath2)

    LOGGER.info('checkplot candidates found at: %r' % possible_checkplots)

    if len(possible_checkplots) == 0:

        return None, [cpfpath1, cpfpath2]

    elif len(possible_checkplots) == 1:

        return possible_checkplots[0], [cpfpath1, cpfpath2]

    # if there are multiple checkplots, they might .gz or cpserver-temp ones,
    # pick the canonical form of *.pkl if it exists, *.pkl.gz if it doesn't
    # exist. if neither of these exist, return None
    else:

        LOGGER.warning('multiple checkplots found for %s in  %s' %
                       (objectid, collection))

        for cp in possible_checkplots:

            if cp.endswith('.pkl'):
                return cp, [cpfpath1, cpfpath2]
            elif cp.endswith('.pkl.gz'):
                return cp, [cpfpath1, cpfpath2]

        return None, [cpfpath1, cpfpath2]



class ObjectInfoPageHandler(tornado.web.RequestHandler):
    '''
    This just calls the handler below to load object info to a blank template.

    This listens on /obj/<collection>/<objectid>.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   signer,
                   fernet,
                   cpsharedkey,
                   cpaddress,
                   siteinfo):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.apiversion = apiversion
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.signer = signer
        self.fernet = fernet
        self.cpkey = cpsharedkey
        self.cpaddr = cpaddress
        self.siteinfo = siteinfo


    @gen.coroutine
    def get(self, collection, objectid):
        '''This runs the query.

        Just renders templates/objectinfo-async.html.

        That page has an async call to the objectinfo JSON API below.

        FIXME: FIXME: should probably check if the the object is actually public
        before trying to fetch it (this can just be a
        dbsearch.sqlite_column_search on this object's collection and objectid
        to see if it's public. if it's not, then return 401)
        '''

        try:

            collection = url_unescape(xhtml_escape(collection))
            objectid = url_unescape(xhtml_escape(objectid))

            self.render(
                'objectinfo-async.html',
                page_title='LCC server object info',
                collection=collection,
                objectid=objectid,
                lccserver_version=__version__,
                siteinfo=self.siteinfo
            )

        except Exception as e:

            self.set_status(400)
            self.render('errorpage.html',
                        page_title='400 - object request not valid',
                        error_message='Could not parse your object request.',
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)



class ObjectInfoHandler(tornado.web.RequestHandler):
    '''
    This handles talking to the checkplotserver for checkplot info
    on a given object.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
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

        FIXME: FIXME: should probably check if the the object is actually public
        before trying to fetch it (this can just be a
        dbsearch.sqlite_column_search on this object's collection and objectid
        to see if it's public. if it's not, then return 401)

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

        LOGGER.info('fpath = %s, glob = %s' % (checkplot_fpath,
                                               checkplot_fglob))

        # 2. ask the checkplotserver for this checkplot using our key
        if checkplot_fpath is not None:

            url = self.cpaddr + '/standalone'

            # generate our request URL
            req_url = url + '?' + urlencode(
                {'cp':b64encode(checkplot_fpath.encode()),
                 'key':self.cpkey}
            )

            client = AsyncHTTPClient(force_instance=True)
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
                self.set_status(resp.code)

            # the checkplot server returns a JSON in case of success or error,
            # so that's what we'll forward to the client.
            self.set_header('Content-Type',
                            'application/json; charset=UTF-8')

            # make sure once again to remove NaNs. do this carefully because
            # stray nulls can kill base64 encoded images

            # first, we'll deserialize to JSON
            # make sure to replace NaNs only in the right places
            rettext = resp.body.decode()
            rettext = (
                rettext.replace(
                    ': NaN',': null'
                ).replace(
                    ', NaN',', null'
                ).replace(
                    '[NaN','[null'
                )
            )
            client.close()

            self.write(rettext)
            self.finish()

        else:

            LOGGER.error(
                'could not find the requested checkplot using globs: %r' %
                checkplot_fglob
            )
            self.set_status(404)
            retdict = {
                'status':'failed',
                'result':None,
                'message':('could not find information for '
                           'object %s in collection %s' %
                           (objectid, collection))
            }
            self.write(retdict)
            self.finish()
