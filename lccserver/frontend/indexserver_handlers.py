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
from datetime import datetime, timedelta


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
from tornado import gen

import markdown
import secrets
import itsdangerous

###################
## LOCAL IMPORTS ##
###################

from .. import __version__
from ..backend import datasets
datasets.set_logger_parent(__name__)
from ..backend import dbsearch


#####################
## MAIN INDEX PAGE ##
#####################

class IndexHandler(tornado.web.RequestHandler):
    '''This handles the index page.

    This page shows the current project.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo

    def get(self):
        '''This handles GET requests to the index page.

        '''

        self.render(
            'index.html',
            page_title='LCC Server',
            lccserver_version=__version__,
            siteinfo=self.siteinfo
        )


######################
## API KEY HANDLERS ##
######################

class APIKeyHandler(tornado.web.RequestHandler):
    '''This handles API key generation

    '''

    def initialize(self, apiversion, signer, fernet):
        '''
        handles initial setup.

        '''
        self.apiversion = apiversion
        self.signer = signer
        self.fernet = fernet


    @gen.coroutine
    def get(self):
        '''This doesn't actually run the query.

        It is used to generate a token to be used in place of an XSRF token for
        the POST functions (or possibly other API enabled functions later). This
        is mostly useful for direct API request since we will not enable POST
        with XSRF. So for an API enabled service, the workflow on first hit is:

        - /api/key GET to receive a token

        Then one can run any /api/<method> with the following in the header:

        Authorization: Bearer <token>

        keys expire in 1 day and contain:

        ip: the remote IP address
        ver: the version of the API
        token: a random hex
        expiry: the ISO format date of expiry

        '''

        # using the signer, generate a key
        expires = '%sZ' % (datetime.utcnow() +
                           timedelta(seconds=86400.0)).isoformat()
        key = {'ip':self.request.remote_ip,
               'ver':self.apiversion,
               'token':secrets.token_urlsafe(10),
               'expiry':expires}
        signed = self.signer.dumps(key)
        LOGGER.warning('new API key generated for %s, '
                       'expires on: %s, typeof: %s' % (key['ip'],
                                                       expires,
                                                       type(signed)))

        retdict = {
            'status':'ok',
            'message':'key expires: %s' % key['expiry'],
            'result':{'key': signed,
                      'expires':expires}
        }

        self.write(retdict)
        self.finish()



class APIAuthHandler(tornado.web.RequestHandler):
    '''This handles API key authentication

    '''

    def initialize(self, apiversion, signer, fernet):
        '''
        handles initial setup.

        '''
        self.apiversion = apiversion
        self.signer = signer
        self.fernet = fernet


    @gen.coroutine
    def get(self):
        '''This is used to check if an API key is valid.

        '''

        try:

            key = self.get_argument('key', default=None)

            if not key:

                LOGGER.error('no key was provided')

                retdict = {
                    'status':'failed',
                    'message':'no key was provided to authenticate',
                    'result':None
                }

                self.set_status(400)
                self.write(retdict)
                self.finish()

            else:

                #
                # if we have a key
                #
                key = xhtml_escape(key)
                uns = self.signer.loads(key, max_age=86400.0)

                # match the remote IP and API version
                keyok = ((self.request.remote_ip == uns['ip']) and
                         (self.apiversion == uns['ver']))

                if not keyok:

                    if 'X-Real-Host' in self.request.headers:
                        self.req_hostname = self.request.headers['X-Real-Host']
                    else:
                        self.req_hostname = self.request.host

                    newkey_url = "%s://%s/api/key" % (
                        self.request.protocol,
                        self.req_hostname,
                    )

                    LOGGER.error('API key is valid, but '
                                 'IP or API version mismatch')

                    retdict = {
                        'status':'failed',
                        'message':('API key invalid for current LCC API '
                                   'version: %s or your '
                                   'IP address has changed. '
                                   'Get an up-to-date key from %s' %
                                   (self.apiversion, newkey_url)),
                        'result':None
                    }

                    self.set_status(401)
                    self.write(retdict)
                    self.finish()

                else:

                    LOGGER.warning('successful API key auth: %r' % uns)

                    retdict = {
                        'status':'ok',
                        'message':('API key verified successfully. '
                                   'Expires: %s' %
                                   uns['expiry']),
                        'result':{'expires':uns['expiry']},
                    }

                    self.write(retdict)
                    self.finish()

        except itsdangerous.SignatureExpired:

            LOGGER.exception('API key "%s" from %s has expired' %
                             (key, self.request.remote_ip))

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host

            newkey_url = "%s://%s/api/key" % (
                self.request.protocol,
                self.req_hostname,
            )

            retdict = {
                'status':'failed',
                'message':('API key has expired. '
                           'Get a new one from %s' % newkey_url),
                'result':None
            }

            self.status(401)
            self.write(retdict)
            self.finish()

        except itsdangerous.BadSignature:

            LOGGER.exception('API key "%s" from %s did not pass verification' %
                             (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':'API key could not be verified or has expired.',
                'result':None
            }

            self.set_status(401)
            self.write(retdict)
            self.finish()

        except Exception as e:

            LOGGER.exception('API key "%s" from %s did not pass verification' %
                             (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':('API key was not provided, '
                           'could not be verified, '
                           'or has expired.'),
                'result':None
            }

            self.set_status(401)
            self.write(retdict)
            self.finish()


#########################################
## DOCS HANDLING FUNCTIONS AND CLASSES ##
#########################################

def doc_render_worker(docpage,
                      basedir,
                      serverindex,
                      siteindex):
    '''This is a worker that renders Markdown to HTML markup.

    Works in a background Executor.

    serverindex and siteindex are the dicts containing server and site doc page
    titles and doc page names.

    '''

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

    LOGGER.info('opening %s for docs page: %s...' % (doc_md_file, docpage))

    # we'll open in 'r' mode since we want unicode for markdown
    with open(doc_md_file,'r') as infd:
        doc_markdown = infd.read()

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



class DocsHandler(tornado.web.RequestHandler):
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
                   siteinfo):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo

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
                        siteinfo=self.siteinfo)

        # get a specific documentation page
        elif docpage and len(docpage) > 0:

            docpage = xhtml_escape(docpage)

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
                                siteinfo=self.siteinfo)

                else:

                    self.set_status(404)

                    self.render(
                        'errorpage.html',
                        page_title='404 - no docs available',
                        error_message=('Could not find a docs page '
                                       'for the requested item.'),
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo
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
                    siteinfo=self.siteinfo
                )




class CollectionListHandler(tornado.web.RequestHandler):
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
                   signer,
                   fernet):
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
            self.basedir
        )

        collection_info = collections['info']
        collection_info['description'] = list(collection_info['description'])

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



class DatasetListHandler(tornado.web.RequestHandler):
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
                   signer,
                   fernet):
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


    @gen.coroutine
    def get(self):
        '''This gets the list of datasets currently available.

        '''

        try:
            nrecent = self.get_argument('nsets')
            nrecent = int(xhtml_escape(nrecent))
        except Exception as e:
            nrecent = 25

        dataset_info = yield self.executor.submit(
            datasets.sqlite_list_datasets,
            self.basedir,
            nrecent=nrecent,
            require_status='complete',
            require_ispublic=True
        )

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

                # update this listing
                dataset['dataset_fpath'] = dataset_fpath
                dataset['dataset_csv'] = dataset_csv
                dataset['lczip_fpath'] = lczip_fpath

                dataset_list.append(dataset)

        dataset_info['result'] = dataset_list

        self.write(dataset_info)
        self.finish()
