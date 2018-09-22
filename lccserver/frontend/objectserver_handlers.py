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
from .basehandler import BaseHandler
from ..backend import dbsearch


###################################################
## Handler to get the checkplot JSON all at once ##
###################################################

def check_for_checkplots(objectid,
                         basedir,
                         collection,
                         lcmagcols=None):
    '''This does all the hard work of finding the right checkplot to get.

    If the magcol used to generate the checkplots is provided in the magcol
    kwarg, this can short-circuit the glob needed to look up all checkplots for
    a certain object, making the checkplot retrieval much faster.

    '''

    cpdir1 = os.path.join(basedir,
                          collection,
                          'checkplots')
    cpdir2 = os.path.join(basedir,
                          collection.replace('_','-'),
                          'checkplots')

    LOGGER.info('searching for checkplots in: %s, %s' % (cpdir1, cpdir2))

    if os.path.exists(cpdir2):
        cpdir = cpdir2
    else:
        cpdir = cpdir1


    if lcmagcols is None:

        cpfpath = os.path.join(cpdir, 'checkplot-%s*.pkl*' % objectid)
        possible_checkplots = glob.glob(cpfpath)

    # if the magcol is provided, we just need to check if the target checkplot
    # pickles exist and don't need to glob through the directory
    else:

        possible_checkplots = []
        for magcol in lcmagcols:

            cpfpath = os.path.join(cpdir,
                                   'checkplot-%s-%s.pkl' %
                                   (objectid, magcol))
            LOGGER.info('checking %s' % cpfpath)

            if os.path.exists(cpfpath):

                possible_checkplots.append(cpfpath)

            else:

                cpfpath = os.path.join(cpdir,
                                       'checkplot-%s-%s.pkl.gz' %
                                       (objectid, magcol))
                LOGGER.info('checking %s' % cpfpath)

                if os.path.exists(cpfpath):
                    possible_checkplots.append(cpfpath)


    LOGGER.info('checkplot candidates found at: %r' %
                possible_checkplots)


    if len(possible_checkplots) == 0:

        return None

    elif len(possible_checkplots) == 1:

        return possible_checkplots[0]

    # if there are multiple checkplots, they might .gz or cpserver-temp ones,
    # pick the canonical form of *.pkl if it exists, *.pkl.gz if it doesn't
    # exist. if neither of these exist, return None
    else:

        LOGGER.warning('multiple checkplots found for %s in %s' %
                       (objectid, collection))

        for cp in possible_checkplots:

            if cp.endswith('.pkl'):
                return cp
            elif cp.endswith('.pkl.gz'):
                return cp
            else:
                return None



class ObjectInfoHandler(BaseHandler):
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
                   cpaddress,
                   authnzerver,
                   session_expiry,
                   fernetkey):
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
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.httpclient = AsyncHTTPClient(force_instance=True)



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

        '''

        objectid = self.get_argument('objectid', default=None)
        collection = self.get_argument('collection', default=None)
        lcmagcols = self.get_argument('lcmagcols', default=None)

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

        if lcmagcols is not None:
            lcmagcols = xhtml_escape(lcmagcols).split(',')
        if 'undefined' in lcmagcols or 'null' in lcmagcols:
            lcmagcols = None


        # check if we actually have access to this object
        access_check = yield self.executor.submit(
            dbsearch.sqlite_column_search,
            self.basedir,
            getcolumns=['objectid'],
            conditions="objectid = '%s'" % objectid.strip(),
            lcclist=[collection],
            incoming_userid=self.current_user['user_id'],
            incoming_role=self.current_user['user_role']
        )

        if access_check and len(access_check[collection]['result']) > 0:

            LOGGER.info('user_id = %s, role = %s, access OK for %s in %s' %
                        (self.current_user['user_id'],
                         self.current_user['user_role'],
                         objectid,
                         collection))

            # get lcmagcols from the access_check if available
            if 'lcmagcols' in access_check[collection]:
                lcmagcols = access_check[collection]['lcmagcols'].split(',')
            else:
                lcmagcols = None

            LOGGER.info('lcmagcols = %s' % lcmagcols)

            # 1. get the canonical checkplot path
            checkplot_fpath = yield self.executor.submit(
                check_for_checkplots,
                objectid,
                self.basedir,
                collection,
                lcmagcols=lcmagcols
            )
            LOGGER.info('found checkplot fpath = %s' % checkplot_fpath)

            # 2. ask the checkplotserver for this checkplot using our key
            if checkplot_fpath is not None:

                url = self.cpaddr + '/standalone'

                # generate our request URL
                req_url = url + '?' + urlencode(
                    {'cp':b64encode(checkplot_fpath.encode()),
                     'key':self.cpkey}
                )

                resp = yield self.httpclient.fetch(req_url,
                                                   raise_error=False)

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

                # the checkplot server returns a JSON in case of success or
                # error, so that's what we'll forward to the client.
                self.set_header('Content-Type',
                                'application/json; charset=UTF-8')

                try:

                    # make sure once again to remove NaNs. do this carefully
                    # because stray nulls can kill base64 encoded images

                    # first, we'll deserialize to JSON
                    # make sure to replace NaNs only in the right places
                    rettext = resp.body.decode()
                    rettext = (
                        rettext.replace(
                            ': NaN', ': null'
                        ).replace(
                            ', NaN', ', null'
                        ).replace(
                            'NaN,', 'null,'
                        ).replace(
                            '[NaN', '[null'
                        ).replace(
                            'NaN]', 'null]'
                        )
                    )

                except AttributeError as e:

                    rettext = json.dumps({
                        'status':'failed',
                        'message':('could not fetch checkplot for '
                                   'this object, checkplotserver '
                                   'error code: %s' % resp.code),
                        'result':None
                    })

                finally:

                    self.write(rettext)
                    self.finish()

            else:

                LOGGER.error(
                    'could not find the requested checkplot for %s in dir: %s' %
                    (objectid, collection)
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

        # if we don't have access to this object.
        else:

            LOGGER.error(
                'incoming user_id = %s, role = %s has no '
                'access to objectid %s in collection %s' %
                (self.current_user['user_id'],
                 self.current_user['user_role'],
                 objectid, collection)
            )
            self.set_status(401)

            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access to "
                           "object %s in collection %s" %
                           (objectid, collection))
            }
            self.write(retdict)
            self.finish()



class ObjectInfoPageHandler(BaseHandler):
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
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey):
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
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.httpclient = AsyncHTTPClient(force_instance=True)



    @gen.coroutine
    def get(self, collection, objectid):
        '''This runs the query.

        Just renders templates/objectinfo-async.html.

        That page has an async call to the objectinfo JSON API below.

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
                siteinfo=self.siteinfo,
                flash_messages=self.render_flash_messages(),
                user_account_box=self.render_user_account_box(),
            )

        except Exception as e:

            LOGGER.exception('could not handle incoming object request')

            self.set_status(400)
            self.render('errorpage.html',
                        page_title='400 - object request not valid',
                        error_message='Could not parse your object request.',
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),)
