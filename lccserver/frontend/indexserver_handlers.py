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
try:
    import cPickle as pickle
except:
    import pickle
import base64
import logging
import time as utime

try:
    from cStringIO import StringIO as strio
except:
    from io import BytesIO as strio

import numpy as np
from numpy import ndarray

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
            page_title='LCC server',
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

        # get a specific documentation page
        if docpage is not None:

            # get the Markdown file from the docpage specifier

            docpage = xhtml_escape(docpage)
            doc_md_file = os.path.join(self.docspath, '%s.md' % docpage)

            if os.path.exists(doc_md_file):

                with open(doc_md_file,'r') as infd:
                    doc_markdown = infd.read()

                # render the markdown to HTML
                doc_html = doc_markdown

                # get the docpage's title
                page_title = self.docindex[docpage]['title']

                self.render(
                    'doc-page.html',
                    page_title=page_title,
                    page_content=doc_html,
                )

        # otherwise get the documentation index
        else:

            # we pass the doc index dict to the template directly
            self.render(
                'doc-index.html',
                page_title='docs index',
                doc_index=self.docindex,
            )



class AboutPageHandler(tornado.web.RequestHandler):
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
            'about.html',
            page_title='About the LCC server',
        )



class CollectionsListHandler(tornado.web.RequestHandler):
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




    def get(self):
        '''This gets the list of collections currently available.

        Returns a JSON that forms a table with stuff like so:

        collection name, collection long title, collection description,
        collection center RA, collection center DEC collection RA extent,
        collection DEC extent, number of objects in collection, collection
        column names and descriptions, collection index columns, collection FTS
        index columns

        the collection name maps directly to a collection-dir where we can find
        the following files:

        - catalog-kdtree.pkl
        - catalog-objectinfo.sqlite

        the catalog-kdtree.pkl for each collection is loaded into memory by a
        searchserver.py instance. the catalog-objectinfo.sqlite is accessed
        async on demand by the searchserver.py instance.

        '''

        # this fires an async submit to the function:
        # ..objectsearch.collections.list_collections(). this returns a dict
        # containing the info above, this dict is returned to the frontend as
        # JSON
