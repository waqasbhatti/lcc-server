#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''indexserver_handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) -
                             Apr 2018

These are Tornado handlers for the indexserver.

'''

####################
## SYSTEM IMPORTS ##
####################

import os
import os.path
import logging
import numpy as np
from datetime import datetime

# for generating encrypted token information
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
        elif isinstance(obj, datetime):
            return obj.isoformat()
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

from tornado.escape import xhtml_escape, squeeze
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
import markdown


###################
## LOCAL IMPORTS ##
###################

from .. import __version__
from ..backend import datasets
from ..backend import dbsearch

from .basehandler import BaseHandler


#####################
## MAIN INDEX PAGE ##
#####################

class IndexHandler(BaseHandler):
    '''This handles the index page.

    This page shows the current project.

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
                   cachedir,
                   footprint_svg):
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
        self.footprint_svg = footprint_svg



    def get(self):
        '''This handles GET requests to the index page.

        '''

        self.render(
            'index.html',
            flash_messages=self.render_flash_messages(),
            user_account_box=self.render_user_account_box(),
            page_title='%s - LCC Server' % self.siteinfo['project'],
            lccserver_version=__version__,
            siteinfo=self.siteinfo,
            current_user=self.current_user,
            footprint_svg=self.footprint_svg
        )



###################
## DOCS HANDLERS ##
###################

def doc_render_worker(docpage,
                      basedir,
                      serverindex,
                      siteindex):
    '''This is a worker that renders Markdown to HTML markup.

    Works in a background Executor.

    serverindex and siteindex are the dicts containing server and site doc page
    titles and doc page names.

    '''

    # check for shady doc pages
    if '.' in docpage:
        return None, None
    if '/' in docpage:
        return None, None
    if len(docpage) != len(squeeze(docpage).strip().replace(' ','')):
        return None, None


    # find the doc page requested
    if docpage in serverindex:
        page_title = serverindex[docpage]
        doc_md_file = os.path.join(os.path.dirname(__file__),
                                   '..',
                                   'server-docs',
                                   '%s.md' % docpage)
    elif docpage in siteindex:
        page_title = siteindex[docpage]
        doc_md_file = os.path.join(basedir,'docs',
                                   '%s.md' % docpage)

    # if the doc page is not found in either index, then it doesn't exist
    else:
        return None, None

    # check for some more shenanigans
    if not os.path.exists(doc_md_file):
        return None, None

    doc_md_dir_abspath = os.path.dirname(os.path.abspath(doc_md_file))

    if docpage in serverindex:
        doc_dir_abspath = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
                         '..',
                         'server-docs')
        )
    elif docpage in siteindex:
        doc_dir_abspath = os.path.abspath(os.path.join(basedir,'docs'))

    if (doc_md_dir_abspath != doc_dir_abspath):
        return None, None


    # we'll open in 'r' mode since we want unicode for markdown
    with open(doc_md_file,'r') as infd:
        doc_markdown = infd.read()

    LOGGER.info('read %s for requested docs page: %s...' %
                (doc_md_file, docpage))

    # render the markdown to HTML
    doc_html = markdown.markdown(
        doc_markdown,
        output_format='html5',
        extensions=['markdown.extensions.extra',
                    'markdown.extensions.codehilite',
                    'markdown.extensions.toc',
                    'markdown.extensions.tables'],
        extension_configs={
            'markdown.extensions.codehilite':{
                'guess_lang': False
            },
            'markdown.extensions.toc':{
                'anchorlink': True
            },
        }
    )

    return doc_html, page_title



class DocsHandler(BaseHandler):
    '''This handles the docs index page and all other docs requests.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   serverdocs,
                   sitedocs,
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

        #
        # these are the doc index JSONs parsed into dicts
        #

        # this is the lcc-server doc index
        self.server_docindex = serverdocs

        # this is the site-specific documentation index
        self.site_docindex = sitedocs

        # this generates the server url for use in documentation
        if 'X-Real-Host' in self.request.headers:
            self.req_hostname = self.request.headers['X-Real-Host']
        else:
            self.req_hostname = self.request.host

        self.server_url = "%s://%s" % (
            self.request.protocol,
            self.req_hostname,
        )


    @tornado.web.removeslash
    @gen.coroutine
    def get(self, docpage):
        '''This handles GET requests for docs

        '''

        if not docpage or len(docpage) == 0:

            self.render('docs-index.html',
                        page_title="Documentation index",
                        serverdocs=self.server_docindex,
                        sitedocs=self.site_docindex,
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box())

        # get a specific documentation page
        elif docpage and len(docpage) > 0:

            docpage = xhtml_escape(docpage).lower()

            try:
                rendered, page_title = yield self.executor.submit(
                    doc_render_worker,
                    docpage,
                    self.basedir,
                    self.server_docindex,
                    self.site_docindex
                )

                if rendered and page_title:

                    # this is because the rendering doesn't seem to figure out
                    # that there's a template tag left in. FIXME: figure out how
                    # to do this cleanly
                    rendered = rendered.replace('{{ server_url }}',
                                                self.server_url)
                    self.render('docs-page.html',
                                page_title=page_title,
                                page_content=rendered,
                                lccserver_version=__version__,
                                siteinfo=self.siteinfo,
                                flash_messages=self.render_flash_messages(),
                                user_account_box=self.render_user_account_box())

                else:

                    self.set_status(404)

                    self.render(
                        'errorpage.html',
                        page_title='404 - no docs available',
                        error_message=('Could not find a docs page '
                                       'for the requested item.'),
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box()
                    )

            except Exception as e:

                LOGGER.exception('failed to render doc page: %s' % docpage)
                self.set_status(404)

                self.render(
                    'errorpage.html',
                    page_title='404 - no docs available',
                    error_message=('Could not find a docs page '
                                   'for the requested item.'),
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo,
                    flash_messages=self.render_flash_messages(),
                    user_account_box=self.render_user_account_box()
                )



#############################
## COLLECTION LIST HANDLER ##
#############################

class CollectionListHandler(BaseHandler):
    '''
    This handles the collections list API.

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

        # this is used to render collection descriptions to HTML
        self.markdowner = markdown.Markdown(
            output_format='html5',
            extensions=['markdown.extensions.extra',
                        'markdown.extensions.tables'],
        )



    @gen.coroutine
    def get(self):
        '''This gets the list of collections currently available.

        '''

        collections = yield self.executor.submit(
            dbsearch.sqlite_list_collections,
            self.basedir,
            incoming_userid=self.current_user['user_id'],
            incoming_role=self.current_user['user_role'],
        )

        if collections is not None:

            collection_info = collections['info']
            collection_info['description'] = list(
                collection_info['description']
            )

            # get the descriptions and turn them into markdown if needed
            for collind, coll, desc in zip(
                    range(len(collection_info['collection_id'])),
                    collection_info['collection_id'],
                    collection_info['description']
            ):
                if desc and desc.startswith('#!MKD '):
                    try:
                        desc = self.markdowner.convert(desc[6:])
                        collection_info['description'][collind] = desc
                    except Exception as e:
                        LOGGER.warning('markdown convert failed '
                                       'for description for collection: %s' %
                                       coll)
                        desc = desc[6:]
                        collection_info['description'][collind] = desc
                    self.markdowner.reset()

            all_columns = collections['columns']
            all_indexed_columns = collections['indexedcols']
            all_fts_columns = collections['ftscols']

            # censor some bits
            del collection_info['kdtree_pkl_path']
            del collection_info['object_catalog_path']

            # we'll reform the lcformatdesc path so it can be downloaded
            # directly from the LCC server
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

        # if no collections are found for the incoming_userid and role, return
        # an error
        else:

            returndict = {
                'status':'failed',
                'result':{'available_columns':[],
                          'available_indexed_columns':[],
                          'available_fts_columns':[],
                          'collections':None},
                'message':(
                    'Sorry, no collections viewable by '
                    'the current user were found.'
                )
            }
            self.write(returndict)
            self.finish()



##########################
## DATASET LIST HANDLER ##
##########################

class DatasetListHandler(BaseHandler):
    '''
    This handles the dataset list API.

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
    def get(self):
        '''This gets the list of datasets currently available.

        '''

        try:
            nrecent = self.get_argument('nsets')
            nrecent = int(xhtml_escape(nrecent))
        except Exception as e:
            nrecent = 25

        setfilter = self.get_argument('filter', default=None)
        if setfilter:
            setfilter = xhtml_escape(setfilter)
            setfilter = setfilter[:1024]

        useronly = self.get_argument('useronly', default=None)
        if useronly is not None:
            useronly = xhtml_escape(useronly.strip())
            if useronly == 'true':
                useronly = True
            else:
                useronly = False
        else:
            useronly = False

        dataset_info = yield self.executor.submit(
            datasets.sqlite_list_datasets,
            self.basedir,
            setfilter=setfilter,
            useronly=useronly,
            nrecent=nrecent,
            require_status='complete',
            incoming_userid=self.current_user['user_id'],
            incoming_role=self.current_user['user_role']
        )

        # if there aren't any datasets yet, return immediately
        if dataset_info['result'] is None:

            self.write(dataset_info)
            raise tornado.web.Finish()

        # we'll have to censor stuff here as well
        dataset_list = []

        for dataset in dataset_info['result']:

            if isinstance(dataset, dict):

                # this should always be there
                try:
                    dataset_fpath = dataset['dataset_fpath']
                    dataset_fpath = dataset_fpath.replace(
                        os.path.join(self.basedir,'datasets'),
                        '/d'
                    )
                except Exception as e:
                    dataset_fpath = None

                try:
                    dataset_csv = dataset['dataset_csv']
                    dataset_csv = dataset_csv.replace(
                        os.path.join(self.basedir,'datasets'),
                        '/d'
                    )
                except Exception as e:
                    dataset_csv = None

                try:
                    lczip_fpath = dataset['lczip_fpath']
                    lczip_fpath = lczip_fpath.replace(
                        os.path.join(self.basedir,'products'),
                        '/p'
                    )
                except Exception as e:
                    lczip_fpath = None

                # update this listing with the URLs of the products
                dataset['dataset_fpath'] = dataset_fpath
                dataset['dataset_csv'] = dataset_csv
                dataset['lczip_fpath'] = lczip_fpath

                # update this listing to indicate if the current user is the
                # owner of this dataset. this only works for authenticated
                # users.
                if (self.current_user['user_id'] not in (2,3) and
                    self.current_user['user_id'] == dataset['dataset_owner']):

                    dataset['owned'] = True
                    append_to_list = True

                # otherwise, if the current user's session_token matches the
                # session_token used to create the dataset, they're the
                # owner. this will only hold true until the session token
                # expires, which is in 7 days from when they first hit the
                # site. this should be OK. if people want more than 7 days of
                # history, they can sign up.
                elif ( (self.current_user['user_id'] == 2) and
                       (self.current_user['session_token'] ==
                        dataset['dataset_sessiontoken']) ):

                    dataset['owned'] = True
                    append_to_list = True

                elif ( (self.current_user['user_id'] == 2) and
                       (self.current_user['session_token'] !=
                        dataset['dataset_sessiontoken']) ):

                    dataset['owned'] = False
                    if dataset['dataset_visibility'] != 'public':
                        append_to_list = False
                    else:
                        append_to_list = True

                # show all datasets if the user is a superuser or admin
                elif self.current_user['user_role'] in ('superuser','staff'):

                    dataset['owned'] = (self.current_user['user_id'] ==
                                        dataset['dataset_owner'])
                    append_to_list = True

                # otherwise, this is a dataset not owned by the current user
                else:

                    dataset['owned'] = False
                    if dataset['dataset_visibility'] != 'public':
                        append_to_list = False
                    else:
                        append_to_list = True

                # censor the session token
                dataset['dataset_sessiontoken'] = 'redacted'

                if append_to_list:
                    dataset_list.append(dataset)

        dataset_info['result'] = dataset_list

        self.write(dataset_info)
        self.finish()



    @gen.coroutine
    def post(self):
        '''This gets the list of datasets currently available.

        '''

        if not self.keycheck['status'] == 'ok':

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':"Sorry, you don't have access."
            }
            self.write(retdict)
            raise tornado.web.Finish()


        try:
            nrecent = self.get_argument('nsets')
            nrecent = int(xhtml_escape(nrecent))
        except Exception as e:
            nrecent = 25

        setfilter = self.get_argument('datasetsearch', default=None)
        if setfilter and len(setfilter.strip()) > 0:
            setfilter = xhtml_escape(setfilter)
        else:
            setfilter = None

        dataset_info = yield self.executor.submit(
            datasets.sqlite_list_datasets,
            self.basedir,
            setfilter=setfilter,
            nrecent=nrecent,
            require_status='complete',
            incoming_userid=self.current_user['user_id'],
            incoming_role=self.current_user['user_role']
        )

        # if there aren't any datasets yet, return immediately
        if dataset_info['result'] is None:

            self.write(dataset_info)
            raise tornado.web.Finish()

        # we'll have to censor stuff here as well
        dataset_list = []

        for dataset in dataset_info['result']:

            if isinstance(dataset, dict):

                # this should always be there
                try:
                    dataset_fpath = dataset['dataset_fpath']
                    dataset_fpath = dataset_fpath.replace(
                        os.path.join(self.basedir,'datasets'),
                        '/d'
                    )
                except Exception as e:
                    dataset_fpath = None

                try:
                    dataset_csv = dataset['dataset_csv']
                    dataset_csv = dataset_csv.replace(
                        os.path.join(self.basedir,'datasets'),
                        '/d'
                    )
                except Exception as e:
                    dataset_csv = None

                try:
                    lczip_fpath = dataset['lczip_fpath']
                    lczip_fpath = lczip_fpath.replace(
                        os.path.join(self.basedir,'products'),
                        '/p'
                    )
                except Exception as e:
                    lczip_fpath = None

                # update this listing with the URLs of the products
                dataset['dataset_fpath'] = dataset_fpath
                dataset['dataset_csv'] = dataset_csv
                dataset['lczip_fpath'] = lczip_fpath

                # update this listing to indicate if the current user is the
                # owner of this dataset. this only works for authenticated
                # users.
                if (self.current_user['user_id'] not in (2,3) and
                    self.current_user['user_id'] == dataset['dataset_owner']):

                    dataset['owned'] = True
                    append_to_list = True

                # otherwise, if the current user's session_token matches the
                # session_token used to create the dataset, they're the
                # owner. this will only hold true until the session token
                # expires, which is in 7 days from when they first hit the
                # site. this should be OK. if people want more than 7 days of
                # history, they can sign up.
                elif ( (self.current_user['user_id'] == 2) and
                       (self.current_user['session_token'] ==
                        dataset['dataset_sessiontoken']) ):

                    dataset['owned'] = True
                    append_to_list = True

                elif ( (self.current_user['user_id'] == 2) and
                       (self.current_user['session_token'] !=
                        dataset['dataset_sessiontoken']) ):

                    dataset['owned'] = False
                    if dataset['dataset_visibility'] != 'public':
                        append_to_list = False
                    else:
                        append_to_list = True

                # show all datasets if the user is a superuser or admin
                elif self.current_user['user_role'] in ('superuser','staff'):

                    dataset['owned'] = (self.current_user['user_id'] ==
                                        dataset['dataset_owner'])
                    append_to_list = True

                # otherwise, this is a dataset not owned by the current user
                else:

                    dataset['owned'] = False
                    if dataset['dataset_visibility'] != 'public':
                        append_to_list = False
                    else:
                        append_to_list = True

                # censor the session token
                dataset['dataset_sessiontoken'] = 'redacted'

                if append_to_list:
                    dataset_list.append(dataset)

        dataset_info['result'] = dataset_list

        self.write(dataset_info)
        self.finish()
