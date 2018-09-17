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

from tornado.escape import xhtml_escape
from tornado.httpclient import AsyncHTTPClient
from tornado import gen


###################
## LOCAL IMPORTS ##
###################

from .. import __version__
from ..backend import dbsearch
from ..backend import datasets

from .basehandler import BaseHandler


#############################
## DATASET DISPLAY HANDLER ##
#############################

class DatasetHandler(BaseHandler):
    '''
    This handles loading a dataset.

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
        self.fernet = fernet,
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.httpclient = AsyncHTTPClient(force_instance=True)



    @gen.coroutine
    def get(self, setid):
        '''This runs the query.

        no args (used for populating the dataset page) -> sqlite_get_dataset(
                     self.basedir, setid, 'json-header',
                     incoming_userid=self.current_user['user_id'],
                     incoming_role=self.current_user['user_role']
        )

        ?json=1 -> sqlite_get_dataset(
                     self.basedir, setid, 'json-preview',
                     incoming_userid=self.current_user['user_id'],
                     incoming_role=self.current_user['user_role']
        )

        ?json=1&strformat=1 -> sqlite_get_dataset(
                     self.basedir, setid, 'strjson-preview',
                     incoming_userid=self.current_user['user_id'],
                     incoming_role=self.current_user['user_role']
        )

        ?json=1&page=XX -> sqlite_get_dataset(
                     self.basedir, setid, 'json-page-XX',
                     incoming_userid=self.current_user['user_id'],
                     incoming_role=self.current_user['user_role']
        )

        ?json=1&strformat=1&page=XX -> sqlite_get_dataset(
                     self.basedir, setid, 'strjson-page-XX',
                     incoming_userid=self.current_user['user_id'],
                     incoming_role=self.current_user['user_role']
        )

        'json-header'      -> only the dataset header
        'json-preview'     -> header + first page of data table
        'strjson-preview'  -> header + first page of strformatted data table
        'json-page-XX'     -> requested page XX of the data table
        'strjson-page-XX'  -> requested page XX of the strformatted data table

       '''

        # get the returnjson argument
        try:
            returnjson = xhtml_escape(self.get_argument('json',default='0'))
            returnjson = True if returnjson == '1' else False
        except Exception as e:
            returnjson = False

        if returnjson:

            # get the strformat argument
            try:
                strformat = xhtml_escape(self.get_argument('strformat',
                                                           default='0'))
                strformat = True if strformat == '1' else False
            except Exception as e:
                strformat = False

            # get any page argument
            try:

                setpage = abs(
                    int(
                        xhtml_escape(self.get_argument('page', default=1))
                    )
                )
                if setpage == 0:
                    setpage = 1

            except Exception as e:

                setpage = None

        else:

            strformat = False
            setpage = None


        if setid is None or len(setid) == 0:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render(
                    'errorpage.html',
                    error_message=message,
                    page_title='404 - No dataset with that setid',
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo,
                    flash_messages=self.render_flash_messages(),
                    user_account_box=self.render_user_account_box()
                )


        #
        # get the dataset ID from the provided URL
        #

        # get the setid
        setid = xhtml_escape(setid)

        # figure out the spec for the backend function
        if returnjson is False:

            func_spec = 'json-header'

        else:

            func_spec = []

            if strformat is True:
                func_spec.append('strjson')
            else:
                func_spec.append('json')

            if setpage is not None:
                func_spec.append('page-%s' % setpage)
            else:
                func_spec.append('preview')

            func_spec = '-'.join(func_spec)

        # retrieve this dataset based on the func_spec
        ds = yield self.executor.submit(
            datasets.sqlite_get_dataset,
            self.basedir, setid, func_spec,
            incoming_userid=self.current_user['user_id'],
            incoming_role=self.current_user['user_role']
        )

        # if there's no dataset at all, then return an error
        if ds is None:

            message = (
                "The requested dataset with setid: %s doesn't exist, "
                "or you don't have access to it." % setid
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title='404 - Dataset %s not found' % setid,
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo,
                            flash_messages=self.render_flash_messages(),
                            user_account_box=self.render_user_account_box())


        # next, if the dataset is returned but status is 'broken'
        elif ds is not None and ds['status'] == 'broken':

            message = (
                "Provided dataset ID: %s doesn't exist." % setid
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title=('404 - Dataset %s missing or broken' %
                                        setid),
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo,
                            flash_messages=self.render_flash_messages(),
                            user_account_box=self.render_user_account_box())


        # next, if the dataset is returned but is in progress
        elif ds is not None and ds['status'] == 'in progress':


            # first, we'll censor some bits
            dataset_pickle = '/d/dataset-%s.pkl.gz' % setid
            ds['dataset_pickle'] = dataset_pickle

            if os.path.exists(os.path.join(self.basedir,
                                           'datasets',
                                           'dataset-%s.csv' % setid)):
                dataset_csv = '/d/dataset-%s.csv' % setid
                ds['dataset_csv'] = dataset_csv

            else:
                dataset_csv = None
                ds['dataset_csv'] = None

            if os.path.exists(ds['lczipfpath']):

                dataset_lczip = ds['lczipfpath'].replace(
                    os.path.join(self.basedir, 'products'),
                    '/p'
                )
                ds['lczipfpath'] = dataset_lczip

            else:

                ds['lczipfpath'] = None

            # check if the current user is the dataset's owner
            if (self.current_user['user_id'] not in (2,3) and
                self.current_user['user_id'] == ds['owner']):

                ds['owned'] = True

            # if the current user is anonymous but their session token matches
            # that used to create the dataset, they're the owner
            elif (self.current_user['session_token'] ==
                  ds['session_token']):

                ds['owned'] = True

            # otherwise, this is a dataset not owned by the current user
            else:

                ds['owned'] = False

            # censor the session_token
            ds['session_token'] = 'redacted'

            # FIXME: look up the user ID of the dataset owner and get their
            # account name (maybe?)

            # if we're returning JSON
            if returnjson:

                dsjson = json.dumps(ds)
                dsjson = dsjson.replace('nan','null').replace('NaN','null')
                self.set_header('Content-Type',
                                'application/json; charset=UTF-8')
                self.write(dsjson)
                raise tornado.web.Finish()

            # otherwise, we'll return the dataset rendered page
            else:

                self.render(
                    'dataset-async.html',
                    page_title='LCC Dataset %s' % setid,
                    setid=setid,
                    header=ds,
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo,
                    flash_messages=self.render_flash_messages(),
                    user_account_box=self.render_user_account_box(),
                )

                raise tornado.web.Finish()

        #
        # if the dataset is complete
        #
        elif ds is not None and ds['status'] == 'complete':

            # first, we'll censor some bits
            dataset_pickle = '/d/dataset-%s.pkl.gz' % setid
            ds['dataset_pickle'] = dataset_pickle

            if os.path.exists(os.path.join(self.basedir,
                                           'datasets',
                                           'dataset-%s.csv' % setid)):
                dataset_csv = '/d/dataset-%s.csv' % setid
                ds['dataset_csv'] = dataset_csv

            else:
                dataset_csv = None
                ds['dataset_csv'] = None


            if os.path.exists(ds['lczipfpath']):

                dataset_lczip = ds['lczipfpath'].replace(
                    os.path.join(self.basedir, 'products'),
                    '/p'
                )
                ds['lczipfpath'] = dataset_lczip

            else:

                ds['lczipfpath'] = None

            # check if the current user is the dataset's owner
            if (self.current_user['user_id'] not in (2,3) and
                self.current_user['user_id'] == ds['owner']):

                ds['owned'] = True

            # if the current user is anonymous but their session token matches
            # that used to create the dataset, they're the owner
            elif (self.current_user['session_token'] ==
                  ds['session_token']):

                ds['owned'] = True

            # otherwise, this is a dataset not owned by the current user
            else:

                ds['owned'] = False

            # censor the session_token
            ds['session_token'] = 'redacted'

            # FIXME: look up the user ID of the dataset owner and get their
            # account name (maybe?)

            # if we're returning JSON
            if returnjson:

                dsjson = json.dumps(ds)
                dsjson = dsjson.replace('nan','null').replace('NaN','null')
                self.set_header('Content-Type',
                                'application/json; charset=UTF-8')
                self.write(dsjson)
                raise tornado.web.Finish()

            # otherwise, we'll return the dataset rendered page
            else:

                self.render(
                    'dataset-async.html',
                    page_title='LCC Dataset %s' % setid,
                    setid=setid,
                    header=ds,
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo,
                    flash_messages=self.render_flash_messages(),
                    user_account_box=self.render_user_account_box(),
                )


        # if we somehow get here, everything is broken
        else:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                self.finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title='404 - no dataset by that name exists',
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo,
                            flash_messages=self.render_flash_messages(),
                            user_account_box=self.render_user_account_box())


    @gen.coroutine
    def post(self, setid):
        '''This handles POSTs to the /set/<setid> endpoint.

        Used to handle changes to the dataset metadata and actual object lists.

        This will check the user actually owns the dataset and then check if the
        changes requested can be made.

        Things that can be edited at the moment:

        - visibility
        - (>= authenticated only) name -> restricted to 280 characters
        - (>= authenticated only) desc -> restricted to 1024 characters

        TODO: the changes will need to be propagated to the database entry, the
        full dataset pickle, the dataset header and page pickles. the dataset
        CSV will not be regenerated.

        '''


#############################
## DATASET LISTING HANDLER ##
#############################

class AllDatasetsHandler(BaseHandler):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.httpclient = AsyncHTTPClient(force_instance=True)


    @gen.coroutine
    def get(self):
        '''This just lists all datasets.

        '''

        self.render('dataset-list.html',
                    page_title='All datasets',
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo,
                    flash_messages=self.render_flash_messages(),
                    user_account_box=self.render_user_account_box())
