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
LOGMOD = __name__


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
import stat

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

# the address to listen on
define('serve',
       default='127.0.0.1',
       help='Bind to given address and serve content.',
       type=str)

# path to the cookie secrets file
define('secretfile',
       default=os.path.join(os.getcwd(), 'lccserver-secrets'),
       help=('The path to a text file containing a strong randomly '
             'generated token suitable for signing cookies.'),
       type=str)

# whether to run in debugmode or not
define('debugmode',
       default=0,
       help='start up in debug mode if set to 1.',
       type=int)

# number of background threads in the pool executor
define('backgroundworkers',
       default=4,
       help=('number of background workers to use '),
       type=int)

# the template path
define('templatepath',
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
# templates. By default, this is in a docs subdir of the current dir, but this
# should generally be in the basedir of the LCC collections directory.
define('docspath',
       default=os.path.join(os.getcwd(), 'docs'),
       help=('Sets the documentation path for the lcc-server.'),
       type=str)



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

    MAXWORKERS = options.backgroundworkers

    BASEDIR = os.path.abspath(options.basedir)
    TEMPLATEPATH = os.path.abspath(options.templatepath)
    ASSETPATH = os.path.abspath(options.assetpath)
    DOCSPATH = os.path.abspath(options.docspath)

    CURRENTDIR = os.path.abspath(os.getcwd())

    # handle the session secret to generate coookies with signatures
    if os.path.exists(options.secretfile):

        with open(options.secretfile,'r') as infd:
            SESSIONSECRET = infd.read().strip('\n')

    else:

        LOGGER.warning('no session secret file found. will make a new one: '
                       '%s' % options.secretfile)
        SESSIONSECRET = hashlib.sha512(os.urandom(20)).hexdigest()
        with open(options.secretfile,'w') as outfd:
            outfd.write(SESSIONSECRET)
        os.chmod(options.secretfile, 0o100600)


    ####################################
    ## PERSISTENT BACKGROUND EXECUTOR ##
    ####################################

    EXECUTOR = ThreadPoolExecutor(MAXWORKERS)

    ##################
    ## URL HANDLERS ##
    ##################

    HANDLERS = [
        # index page
        (r'/',
         handlers.IndexHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR}),
        # docs page index and other subdirs, renders markdown to HTML
        (r'/docs/?(.*)',
         handlers.DocsHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR}),
        # about page
        (r'/about',
         handlers.AboutPageHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR}),
        # this returns a JSON list of the currently available LC collections
        (r'/api/collections',
         handlers.CollectionsListHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'docspath':DOCSPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR}),
    ]

    ########################
    ## APPLICATION SET UP ##
    ########################

    app = tornado.web.Application(
        handlers=HANDLERS,
        static_path=ASSETPATH,
        template_path=TEMPLATEPATH,
        static_url_prefix='/static/',
        compress_response=True,
        cookie_secret=SESSIONSECRET,
        xsrf_cookies=True,
        debug=DEBUG,
    )

    # start up the HTTP server and our application. xheaders = True turns on
    # X-Forwarded-For support so we can see the remote IP in the logs
    http_server = tornado.httpserver.HTTPServer(app, xheaders=True)

    ######################
    ## start the server ##
    ######################

    # make sure the port we're going to listen on is ok
    # inspired by how Jupyter notebook does this
    portok = False
    serverport = options.port
    maxtries = 10
    thistry = 0
    while not portok and thistry < maxtries:
        try:
            http_server.listen(serverport, options.serve)
            portok = True
        except socket.error as e:
            LOGGER.warning('%s:%s is already in use, trying port %s' %
                           (options.serve, serverport, serverport + 1))
            serverport = serverport + 1

    if not portok:
        LOGGER.error('could not find a free port after %s tries, giving up' %
                     maxtries)
        sys.exit(1)

    LOGGER.info('started indexserver. listening on http://%s:%s' %
                (options.serve, serverport))

    # register the signal callbacks
    signal.signal(signal.SIGINT,recv_sigint)
    signal.signal(signal.SIGTERM,recv_sigint)

    # start the IOLoop and begin serving requests
    try:

        tornado.ioloop.IOLoop.instance().start()

    except KeyboardInterrupt:

        LOGGER.info('received Ctrl-C: shutting down...')
        tornado.ioloop.IOLoop.instance().stop()
        # close down the processpool

    EXECUTOR.shutdown()
    time.sleep(3)

# run the server
if __name__ == '__main__':
    main()