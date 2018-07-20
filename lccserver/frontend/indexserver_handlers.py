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

import markdown

###################
## LOCAL IMPORTS ##
###################

from ..objectsearch import datasets
datasets.set_logger_parent(__name__)
from ..objectsearch import dbsearch


#####################
## HANDLER CLASSES ##
#####################


class IndexHandler(tornado.web.RequestHandler):
    '''This handles the index page.

    This page shows the current project.

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


    def get(self):
        '''This handles GET requests to the index page.

        '''

        self.render(
            'index.html',
            page_title='LCC Server',
        )



class DocsHandler(tornado.web.RequestHandler):
    '''This handles the docs index page and all other docs requests.

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

        # look up the docindex JSON
        docindexfile = os.path.join(self.docspath, 'doc-index.json')

        # this is used to match docpage to title and path of the docpage
        with open(docindexfile,'r') as infd:
            self.docindex = json.load(infd)


    def get(self, docpage):
        '''This handles GET requests for docs

        '''

        LOGGER.info(repr(docpage))

        # get a specific documentation page
        if docpage and len(docpage) > 0:

            # get the Markdown file from the docpage specifier

            docpage = xhtml_escape(docpage)
            doc_md_file = os.path.join(self.docspath, '%s.md' % docpage)

            if os.path.exists(doc_md_file):

                with open(doc_md_file,'r') as infd:
                    doc_markdown = infd.read()

                # render the markdown to HTML
                doc_html = markdown.markdown(doc_markdown)

                # get the docpage's title
                page_title = self.docindex[docpage]

                self.render(
                    'docs-page.html',
                    page_title=page_title,
                    page_content=doc_html,
                )

            else:

                error_message = ("No docs page found matching: '%s'." % docpage)
                self.render('errorpage.html',
                            page_title='404 - Page not found',
                            error_message=error_message)

        # otherwise get the documentation index
        else:

            doc_md_file = os.path.join(self.docspath, 'index.md')

            with open(doc_md_file,'r') as infd:
                doc_markdown = infd.read()

            # render the markdown to HTML
            doc_html = markdown.markdown(doc_markdown)

            # get the docpage's title
            page_title = self.docindex['index']

            self.render(
                'docs-page.html',
                page_title=page_title,
                page_content=doc_html,
            )



class CollectionListHandler(tornado.web.RequestHandler):
    '''
    This handles the collections list API.

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
        '''This gets the list of collections currently available.

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



class DatasetListHandler(tornado.web.RequestHandler):
    '''
    This handles the collections list API.

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
        '''This gets the list of datasets currently available.

        '''

        dataset_info = yield self.executor.submit(
            datasets.sqlite_list_datasets,
            self.basedir
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
                except:
                    dataset_fpath = None

                try:
                    lczip_fpath = dataset['lczip_fpath']
                    lczip_fpath = lczip_fpath.replace(
                        os.path.join(self.basedir,'products'),
                        '/p'
                    )
                except:
                    lczip_fpath = None

                try:
                    cpzip_fpath = dataset['cpzip_fpath']
                    cpzip_fpath = cpzip_fpath.replace(
                        os.path.join(self.basedir,'products'),
                        '/p'
                    )
                except:
                    cpzip_fpath = None

                try:
                    pfzip_fpath = dataset['pfzip_fpath']
                    pfzip_fpath = pfzip_fpath.replace(
                        os.path.join(self.basedir,'products'),
                        '/p'
                    )
                except:
                    pfzip_fpath = None

                # update this listing
                dataset['dataset_fpath'] = dataset_fpath
                dataset['lczip_fpath'] = lczip_fpath
                dataset['cpzip_fpath'] = cpzip_fpath
                dataset['pfzip_fpath'] = pfzip_fpath

                dataset_list.append(dataset)

        dataset_info['result'] = dataset_list

        self.write(dataset_info)
        self.finish()
