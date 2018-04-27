#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''indexserver.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018
License: MIT - see the LICENSE file for the full text.

'''

#############
## LOGGING ##
#############

import logging
from datetime import datetime
from traceback import format_exc

# setup a logger
LOGGER = None
LOGMOD = __name__
DEBUG = False

def set_logger_parent(parent_name):
    globals()['LOGGER'] = logging.getLogger('%s.%s' % (parent_name, LOGMOD))

def LOGDEBUG(message):
    if LOGGER:
        LOGGER.debug(message)
    elif DEBUG:
        print('[%s - DBUG] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGINFO(message):
    if LOGGER:
        LOGGER.info(message)
    else:
        print('[%s - INFO] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGERROR(message):
    if LOGGER:
        LOGGER.error(message)
    else:
        print('[%s - ERR!] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGWARNING(message):
    if LOGGER:
        LOGGER.warning(message)
    else:
        print('[%s - WRN!] %s' % (
            datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            message)
        )

def LOGEXCEPTION(message):
    if LOGGER:
        LOGGER.exception(message)
    else:
        print(
            '[%s - EXC!] %s\nexception was: %s' % (
                datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
                message, format_exc()
            )
        )


#############
## IMPORTS ##
#############

import os
import os.path
import gzip
try:
    import cPickle as pickle
except:
    import pickle
import base64
import hashlib
import signal
import logging
import json
import time
import sys
import socket

# this handles async updates of the checkplot pickles so the UI remains
# responsive
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

# setup signal trapping on SIGINT
def recv_sigint(signum, stack):
    '''
    handler function to receive and process a SIGINT

    '''
    raise KeyboardInterrupt


#####################
## TORNADO IMPORTS ##
#####################

# experimental, probably will remove at some point
try:
    import asyncio
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass

import tornado.ioloop
import tornado.httpserver
import tornado.web
import tornado.options
from tornado.options import define, options


###########################
## DEFINING URL HANDLERS ##
###########################

from . import indexserver_handlers as handlers


###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
# indexserver  will serve on 12500-12509
# searchserver will serve on 12510-12519
# lcserver     will serve on 12520-12529

define('port',
       default=12500,
       help='Run on the given port.',
       type=int)

define('serve',
       default='127.0.0.1',
       help='Bind to given address and serve content.',
       type=str)

# the template path
define('assetpath',
       default=os.path.abspath(os.path.join(modpath,'templates')),
       help=('Sets the tornado template path.'),
       type=str)

# the assetpath
define('assetpath',
       default=os.path.abspath(os.path.join(modpath,'static')),
       help=('Sets the asset (server images, css, JS) path.'),
       type=str)

# basedir is the directory at the root where all LCC collections are stored this
# contains subdirs for each collection and a lcc-collections.sqlite file that
# contains info on all collections.
define('basedir',
       default=os.getcwd(),
       help=('The base directory of the light curve collections.'),
       type=str)

# docspath is a directory that contains a bunch of markdown files that are read
# by the lcc-server docshandler and then rendered to HTML using the usual
# templates
define('docspath',
       default=os.path.join(os.getcwd(), 'docs'),
       help=('Sets the documentation path for the lcc-server.'),
       type=str)
define('debugmode',
       default=0,
       help='start up in debug mode if set to 1.',
       type=int)
define('backgroundworkers',
       default=4,
       help=('number of background workers to use '),
       type=int)


############
### MAIN ###
############

def main():

    # parse the command line
    tornado.options.parse_command_line()

    DEBUG = True if options.debugmode == 1 else False

    # get a logger
    LOGGER = logging.getLogger(__name__)
    if DEBUG:
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.INFO)


    ###################
    ## SET UP CONFIG ##
    ###################

    MAXWORKERS = options.maxbackgroundworkers

    ASSETPATH = options.assetpath
    DOCSPATH = options.docspath

    CURRENTDIR = os.getcwd()
    COLLECTIONS_BASEDIR = options.basedir


    ####################################
    ## PERSISTENT BACKGROUND EXECUTOR ##
    ####################################

    EXECUTOR = ThreadPoolExecutor(MAXPROCS)

    ##################
    ## URL HANDLERS ##
    ##################


    HANDLERS = [
        # index page
        (r'/',
         handlers.IndexHandler,
         {'currentdir':CURRENTDIR,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':COLLECTIONS_BASEDIR}),
        # docs page index and other subdirs, renders markdown to HTML
        (r'/docs/(.*)',
         handlers.DocsHandler,
         {'currentdir':CURRENTDIR,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':COLLECTIONS_BASEDIR}),
        # about page
        (r'/about',
         handlers.AboutPageHandler,
         {'currentdir':CURRENTDIR,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':COLLECTIONS_BASEDIR}),
        # this returns a JSON list of the currently available LC collections
        (r'/api/collections',
         handlers.CollectionsListHandler,
         {'currentdir':CURRENTDIR,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':COLLECTIONS_BASEDIR}),
    ]


        app = tornado.web.Application(
        handlers=HANDLERS,
        static_path=ASSETPATH,
        template_path=ASSETPATH,
        static_url_prefix='/static/',
        compress_response=True,
        debug=DEBUG,
    )

    # start up the HTTP server and our application. xheaders = True turns on
    # X-Forwarded-For support so we can see the remote IP in the logs
    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)
