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
from datetime import datetime

import numpy as np

from cryptography.fernet import Fernet


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
                   siteinfo,
                   authnzerver,
                   session_expiry,
                   fernetkey,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.apiversion = apiversion
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.ratelimit = ratelimit
        self.cachedir = cachedir



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

                self.set_status(404)
                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.set_status(404)
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

                self.set_status(404)
                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.set_status(404)
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

            #
            # decide if this dataset can be returned
            #

            # check if the current user is anonymous or not
            if ( (self.current_user['user_role'] in
                  ('authenticated','staff','superuser')) and
                 (self.current_user['user_id'] == ds['owner']) ):

                ds['owned'] = True
                access_ok = True

                if self.current_user['user_role'] in ('staff','superuser'):
                    ds['editable'] = True

            # otherwise, if the current user's session_token matches the
            # session_token used to create the dataset, they're the
            # owner.
            elif ( (self.current_user['user_role'] == 'anonymous') and
                   (self.current_user['session_token'] ==
                    ds['session_token']) ):

                ds['owned'] = True
                access_ok = True

            # if the current user is anonymous and the session tokens don't
            # match, check if the dataset is public or unlisted
            elif ( (self.current_user['user_role'] == 'anonymous') and
                   (self.current_user['session_token'] !=
                    ds['session_token']) ):

                ds['owned'] = False
                if ds['visibility'] in ('public', 'unlisted'):
                    access_ok = True
                else:
                    access_ok = False

            # superusers and staff can see all datasets
            elif (self.current_user['user_role'] in ('superuser','staff')):

                access_ok = True
                ds['editable'] = True

                if self.current_user['user_id'] == ds['owner']:
                    ds['owned'] = True
                else:
                    ds['owned'] = False

            # otherwise, this is a dataset not owned by the current user
            else:

                ds['owned'] = False
                if ds['visibility'] in ('public', 'unlisted', 'shared'):
                    access_ok = True
                else:
                    access_ok = False

            # censor the session_token
            ds['session_token'] = 'redacted'

            # if lcformatdesc filepaths are in the ds, redact that as well
            if 'lcformatdescs' in ds:
                del ds['lcformatdescs']

            # if we're returning JSON
            if returnjson:

                if access_ok:

                    dsjson = json.dumps(ds)
                    dsjson = dsjson.replace('nan','null').replace('NaN','null')
                    self.set_header('Content-Type',
                                    'application/json; charset=UTF-8')
                    self.write(dsjson)

                    #
                    # end the request here
                    #
                    self.finish()

                    #
                    # once we finish with the user and this was a dataset
                    # preview or page request, check if there are any unmade
                    # dataset pages after the current page. if so, make up to 3
                    # more.
                    #
                    if ds['currpage'] == 1:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] + 1,
                                           ds['currpage'] + 4)
                            if(1 <= x <= ds['npages'])
                        ]

                    elif ds['currpage'] == ds['npages']:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 4,
                                           ds['currpage'] - 1)
                            if(1 <= x <= ds['npages'])
                        ]

                    else:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 3,
                                           ds['currpage'] + 3)
                            if (1 <= x <= ds['npages'])
                        ]


                    page_futures = []

                    for next_dspage in next_dspages:

                        if not os.path.exists(next_dspage[0]):

                            LOGGER.info(
                                'background queuing next '
                                'dataset page: %s for setid: %s' %
                                (next_dspage[1], setid)
                            )

                            page_futures.append(self.executor.submit(
                                datasets.sqlite_render_dataset_page,
                                self.basedir,
                                setid,
                                next_dspage[1]
                            ))

                    if len(page_futures) > 0:

                        background_dspage_results = yield page_futures
                        LOGGER.info('background dspage '
                                    'generation complete: %r' %
                                    background_dspage_results)

                else:

                    self.set_status(401)
                    self.write({
                        'status':'failed',
                        'message':"You don't have access to this dataset.",
                        'result':None
                    })
                    raise tornado.web.Finish()

            # otherwise, we'll return the dataset rendered page
            else:

                if access_ok:

                    # get the URL
                    if 'slug' in ds and ds['slug'] is not None:
                        slug = '/%s' % ds['slug']
                    else:
                        slug = ''

                    if 'X-Real-Host' in self.request.headers:
                        host = self.request.headers['X-Real-Host']
                    else:
                        host = self.request.host

                    set_url = '%s://%s/set/%s%s' % (
                        self.request.protocol,
                        host,
                        setid,
                        slug
                    )

                    self.render(
                        'dataset-async.html',
                        current_user=self.current_user,
                        page_title='LCC Dataset %s' % setid,
                        setid=setid,
                        set_url=set_url,
                        header=ds,
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),
                    )

                    #
                    # end the request here
                    #

                    #
                    # once we finish with the user and this was a dataset
                    # preview or page request, check if there are any unmade
                    # dataset pages after the current page. if so, make up to 3
                    # more.
                    #
                    if ds['currpage'] == 1:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] + 1,
                                           ds['currpage'] + 4)
                            if(1 <= x <= ds['npages'])
                        ]

                    elif ds['currpage'] == ds['npages']:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 4,
                                           ds['currpage'] - 1)
                            if(1 <= x <= ds['npages'])
                        ]

                    else:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 3,
                                           ds['currpage'] + 3)
                            if(1 <= x <= ds['npages'])
                        ]

                    page_futures = []
                    for next_dspage in next_dspages:

                        if not os.path.exists(next_dspage[0]):

                            LOGGER.info(
                                'background queuing next '
                                'dataset page: %s for setid: %s' %
                                (next_dspage[1], setid)
                            )
                            page_futures.append(self.executor.submit(
                                datasets.sqlite_render_dataset_page,
                                self.basedir,
                                setid,
                                next_dspage[1]
                            ))

                    if len(page_futures) > 0:

                        background_dspage_results = yield page_futures
                        LOGGER.info('background dspage '
                                    'generation complete: %r' %
                                    background_dspage_results)

                else:

                    self.set_status(401)
                    self.render(
                        'errorpage.html',
                        page_title='401 - Dataset is not accessible',
                        error_message=(
                            "You don't have permission to access this dataset."
                        ),
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box()
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

            #
            # decide if this dataset can be returned
            #

            # check if the current user is anonymous or not
            if (self.current_user['user_role'] in ('authenticated',
                                                   'staff','superuser') and
                self.current_user['user_id'] == ds['owner']):

                ds['owned'] = True
                access_ok = True

                if self.current_user['user_role'] in ('staff','superuser'):
                    ds['editable'] = True

            # otherwise, if the current user's session_token matches the
            # session_token used to create the dataset, they're the
            # owner.
            elif ( (self.current_user['user_role'] == 'anonymous') and
                   (self.current_user['session_token'] ==
                    ds['session_token']) ):

                ds['owned'] = True
                access_ok = True

            # if the current user is anonymous and the session tokens don't
            # match, check if the dataset is public or unlisted
            elif ( (self.current_user['user_role'] == 'anonymous') and
                   (self.current_user['session_token'] !=
                    ds['session_token']) ):

                ds['owned'] = False
                if ds['visibility'] in ('public', 'unlisted'):
                    access_ok = True
                else:
                    access_ok = False

            # superusers and staff can see all datasets
            elif (self.current_user['user_role'] in ('superuser','staff')):

                access_ok = True
                ds['editable'] = True

                if self.current_user['user_id'] == ds['owner']:
                    ds['owned'] = True
                else:
                    ds['owned'] = False

            # otherwise, this is a dataset not owned by the current user
            else:

                ds['owned'] = False
                if ds['visibility'] in ('public', 'unlisted', 'shared'):
                    access_ok = True
                else:
                    access_ok = False

            # censor the session_token
            ds['session_token'] = 'redacted'

            # if lcformatdesc filepaths are in the ds, redact that as well
            if 'lcformatdescs' in ds:
                del ds['lcformatdescs']

            # if we're returning JSON
            if returnjson:

                if access_ok:

                    dsjson = json.dumps(ds)
                    dsjson = dsjson.replace('nan','null').replace('NaN','null')
                    self.set_header('Content-Type',
                                    'application/json; charset=UTF-8')
                    self.write(dsjson)

                    #
                    # end the request here
                    #
                    self.finish()

                    #
                    # once we finish with the user and this was a dataset
                    # preview or page request, check if there are any unmade
                    # dataset pages after the current page. if so, make up to 3
                    # more.
                    #
                    if ds['currpage'] == 1:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] + 1,
                                           ds['currpage'] + 4)
                            if(1 <= x <= ds['npages'])
                        ]

                    elif ds['currpage'] == ds['npages']:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 4,
                                           ds['currpage'] - 1)
                            if(1 <= x <= ds['npages'])
                        ]

                    else:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 3,
                                           ds['currpage'] + 3)
                            if(1 <= x <= ds['npages'])
                        ]

                    page_futures = []
                    for next_dspage in next_dspages:

                        if not os.path.exists(next_dspage[0]):

                            LOGGER.info(
                                'background queuing next '
                                'dataset page: %s for setid: %s' %
                                (next_dspage[1], setid)
                            )
                            page_futures.append(self.executor.submit(
                                datasets.sqlite_render_dataset_page,
                                self.basedir,
                                setid,
                                next_dspage[1]
                            ))

                    if len(page_futures) > 0:

                        background_dspage_results = yield page_futures
                        LOGGER.info('background dspage '
                                    'generation complete: %r' %
                                    background_dspage_results)

                else:

                    self.set_status(401)
                    self.write({
                        'status':'failed',
                        'message':"You don't have access to this dataset.",
                        'result':None
                    })
                    raise tornado.web.Finish()

            # otherwise, we'll return the dataset rendered page
            else:

                if access_ok:

                    # get the URL
                    if 'slug' in ds and ds['slug'] is not None:
                        slug = '/%s' % ds['slug']
                    else:
                        slug = ''

                    if 'X-Real-Host' in self.request.headers:
                        host = self.request.headers['X-Real-Host']
                    else:
                        host = self.request.host

                    set_url = '%s://%s/set/%s%s' % (
                        self.request.protocol,
                        host,
                        setid,
                        slug
                    )

                    self.render(
                        'dataset-async.html',
                        page_title='LCC Dataset %s' % setid,
                        current_user=self.current_user,
                        setid=setid,
                        set_url=set_url,
                        header=ds,
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),
                    )

                    #
                    # once we finish with the user and this was a dataset
                    # preview or page request, check if there are any unmade
                    # dataset pages after the current page. if so, make up to 3
                    # more.
                    #
                    if ds['currpage'] == 1:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] + 1,
                                           ds['currpage'] + 4)
                            if(1 <= x <= ds['npages'])
                        ]

                    elif ds['currpage'] == ds['npages']:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 4,
                                           ds['currpage'] - 1)
                            if(1 <= x <= ds['npages'])
                        ]

                    else:

                        next_dspages = [
                            (os.path.join(self.basedir,
                                          'datasets',
                                          'dataset-%s-rows-page%s.pkl' %
                                          (setid,x)),
                             x)
                            for x in range(ds['currpage'] - 3,
                                           ds['currpage'] + 3)
                            if(1 <= x <= ds['npages'])
                        ]

                    page_futures = []
                    for next_dspage in next_dspages:

                        if not os.path.exists(next_dspage[0]):

                            LOGGER.info(
                                'background queuing next '
                                'dataset page: %s for setid: %s' %
                                (next_dspage[1], setid)
                            )
                            page_futures.append(self.executor.submit(
                                datasets.sqlite_render_dataset_page,
                                self.basedir,
                                setid,
                                next_dspage[1]
                            ))

                    if len(page_futures) > 0:

                        background_dspage_results = yield page_futures
                        LOGGER.info('background dspage '
                                    'generation complete: %r' %
                                    background_dspage_results)

                else:

                    self.set_status(401)
                    self.render(
                        'errorpage.html',
                        page_title='401 - Dataset is not accessible',
                        error_message=(
                            "You don't have permission to access this dataset."
                        ),
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box()
                    )
                    raise tornado.web.Finish()


        # if we somehow get here, everything is broken
        else:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            if returnjson:

                self.set_status(404)
                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                self.finish()

            else:

                self.set_status(404)
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

        Used to handle changes to the dataset metadata. The actual object lists
        are currently immutable so a dataset is a permanent record of a
        search. This might change in the future.

        This will check the user actually owns the dataset and then check if the
        changes requested can be made.

        anonymous and above can edit:

        - visibility

        authenticated and above can also edit:

        - name -> restricted to 280 characters
        - desc -> restricted to 1024 characters
        - citation -> restricted to 1024 characters

        staff and above can also:

        - 'delete' the dataset

        The dataset CSV will not be regenerated because we're lazy.

        FIXME: implement dataset sharedwith changes once we figure that out on
        the backend.

        '''

        # get the user info
        if not self.current_user:

            message = (
                "Unknown user. Cannot act on the dataset."
            )

            self.write({'status':'failed',
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()


        if not self.keycheck['status'] == 'ok':

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':"Sorry, you don't have access."
            }
            self.write(retdict)
            raise tornado.web.Finish()


        #
        # get the dataset ID from the provided URL
        #
        if setid is None or len(setid) == 0:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()


        # get the setid if it's fine
        setid = xhtml_escape(setid)

        # get the action
        action = self.get_argument('action',default=None)

        if action is None or len(action) == 0:

            message = (
                "No action specified."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()

        # get the action if it's fine
        action = xhtml_escape(action)

        if action not in ('edit',
                          'change_owner',
                          'change_visibility',
                          # 'change_sharedwith',  # FIXME: implement this later
                          'delete'):

            message = (
                "Unknown action specified."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()


        # get the action payload
        payload = self.get_argument('update', default=None)

        if payload is None or len(payload) == 0:

            message = (
                "No action payload specified."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()

        try:
            payload = json.loads(payload)
        except Exception as e:
            message = (
                "Could not deserialize the action payload."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            raise tornado.web.Finish()


        # get some useful stuff for the user
        incoming_userid = self.current_user['user_id']
        incoming_role = self.current_user['user_role']
        incoming_session_token = self.current_user['session_token']

        # get the associated backend function in datasets.py

        #
        # handle a dataset edit
        #
        if action == 'edit':

            try:

                ds_updated = yield self.executor.submit(
                    datasets.sqlite_edit_dataset,
                    self.basedir,
                    setid,
                    updatedict=payload,
                    incoming_userid=incoming_userid,
                    incoming_role=incoming_role,
                    incoming_session_token=incoming_session_token
                )

                if ds_updated:

                    message = (
                        "Dataset edit successful."
                    )

                    self.write({'status':'ok',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

                else:

                    message = (
                        "Dataset edit failed."
                    )

                    self.write({'status':'failed',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

            except Exception as e:

                LOGGER.exception(
                    'could not edit dataset: %s, user_id = %s, '
                    ' role = %s, session_token = %s'
                    % (setid, incoming_userid,
                       incoming_role, incoming_session_token)
                )
                message = (
                    "Dataset edit failed."
                )

                self.write({'status':'failed',
                            'date':datetime.utcnow().isoformat(),
                            'result':ds_updated,
                            'message':message})
                self.finish()

        #
        # handle a dataset edit
        #
        elif action == 'change_owner':

            try:

                ds_updated = yield self.executor.submit(
                    datasets.sqlite_change_dataset_owner,
                    self.basedir,
                    setid,
                    new_owner_userid=payload['new_owner_userid'],
                    incoming_userid=incoming_userid,
                    incoming_role=incoming_role,
                    incoming_session_token=incoming_session_token
                )

                if ds_updated:

                    message = (
                        "Dataset ownership change successful."
                    )

                    self.write({'status':'ok',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

                else:

                    message = (
                        "Dataset ownership change failed."
                    )

                    self.write({'status':'failed',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

            except Exception as e:

                LOGGER.exception(
                    'could not change dataset ownership: %s, user_id = %s, '
                    ' role = %s, session_token = %s'
                    % (setid, incoming_userid,
                       incoming_role, incoming_session_token)
                )
                message = (
                    "Dataset ownership change failed."
                )

                self.write({'status':'failed',
                            'date':datetime.utcnow().isoformat(),
                            'result':ds_updated,
                            'message':message})
                self.finish()

        #
        # handle a dataset visibility change
        #
        elif action == 'change_visibility':

            try:

                ds_updated = yield self.executor.submit(
                    datasets.sqlite_change_dataset_visibility,
                    self.basedir,
                    setid,
                    new_visibility=payload['new_visibility'],
                    incoming_userid=incoming_userid,
                    incoming_role=incoming_role,
                    incoming_session_token=incoming_session_token
                )

                if ds_updated:

                    message = (
                        "Dataset visibility change successful."
                    )

                    self.write({'status':'ok',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

                else:

                    message = (
                        "Dataset visibility change failed."
                    )

                    self.write({'status':'failed',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

            except Exception as e:

                LOGGER.exception(
                    'could not change dataset visibility: %s, user_id = %s, '
                    ' role = %s, session_token = %s'
                    % (setid, incoming_userid,
                       incoming_role, incoming_session_token)
                )
                message = (
                    "Dataset visibility change failed."
                )

                self.write({'status':'failed',
                            'date':datetime.utcnow().isoformat(),
                            'result':ds_updated,
                            'message':message})
                self.finish()

        #
        # handle a dataset visibility change
        #
        elif action == 'delete':

            try:

                ds_updated = yield self.executor.submit(
                    datasets.sqlite_delete_dataset,
                    self.basedir,
                    setid,
                    incoming_userid=incoming_userid,
                    incoming_role=incoming_role,
                    incoming_session_token=incoming_session_token
                )

                if ds_updated:

                    message = (
                        "Dataset delete successful."
                    )

                    self.write({'status':'ok',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

                else:

                    message = (
                        "Dataset delete failed."
                    )

                    self.write({'status':'failed',
                                'date':datetime.utcnow().isoformat(),
                                'result':ds_updated,
                                'message':message})
                    self.finish()

            except Exception as e:

                LOGGER.exception(
                    'could not delete dataset: %s, user_id = %s, '
                    ' role = %s, session_token = %s'
                    % (setid, incoming_userid,
                       incoming_role, incoming_session_token)
                )
                message = (
                    "Dataset delete failed."
                )

                self.write({'status':'failed',
                            'date':datetime.utcnow().isoformat(),
                            'result':ds_updated,
                            'message':message})
                self.finish()

        #
        # any other action is an automatic fail
        #
        else:

            message = (
                "Could not understand the action to be performed."
            )

            self.write({'status':'failed',
                        'date':datetime.utcnow().isoformat(),
                        'result':None,
                        'message':message})
            self.finish()



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
                   fernetkey,
                   ratelimit,
                   cachedir):
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
        self.ferneter = Fernet(fernetkey)
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.ratelimit = ratelimit
        self.cachedir = cachedir


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
