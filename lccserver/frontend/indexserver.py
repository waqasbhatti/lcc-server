#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''indexserver.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018
License: MIT - see the LICENSE file for the full text.

'''
#############
## LOGGING ##
#############

import logging

# setup a logger
LOGMOD = __name__


#############
## IMPORTS ##
#############

import os
import os.path
import hashlib
import signal
import time
import sys
import socket
import stat
import json

# this handles async background stuff
from concurrent.futures import ProcessPoolExecutor

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
    IOLOOP_SPEC = 'uvloop'
except Exception as e:
    HAVE_UVLOOP = False
    IOLOOP_SPEC = 'asyncio'

import tornado.ioloop
import tornado.httpserver
import tornado.web
import tornado.options
from tornado.options import define, options


########################
## OTHER USEFUL STUFF ##
########################

# for signing our API tokens
from itsdangerous import URLSafeTimedSerializer

# for generating encrypted token information
from cryptography.fernet import Fernet

###########################
## DEFINING URL HANDLERS ##
###########################

from . import indexserver_handlers as ih
from . import searchserver_handlers as sh
from . import dataserver_handlers as dh
from . import objectserver_handlers as oh


###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
# indexserver  will serve on 12500-12519 by default
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
       default=os.path.join(os.getcwd(), '.lccserver.secret'),
       help=('The path to a text file containing a strong randomly '
             'generated token suitable for signing cookies. Will be used as '
             'the filename basis for files containing a Fernet key for '
             'API authentication and a shared key for '
             'checkplotserver as well.'),
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

# this overrides the light curve directories that the server uses to find
# original format light curves.
define('uselcdir',
       default=None,
       help=('This overrides the light curve directories '
             'that the server uses to find original format '
             'light curves when it is converting them to the '
             'LCC CSV format'),
       type=str)

## this tells the indexserver about the backend checkplotservers
define('cpaddr',
       default='http://127.0.0.1:5225',
       help=('This tells the lcc-server the address of a '
             'running checkplotserver instance that might be '
             'used to get individual object info.'),
       type='str')



#########################
## SECRET KEY HANDLING ##
#########################

def get_secret_keys(tornado_options, logger):
    """
    This loads and generates secret keys.

    """
    # handle the session secret to generate coookies and itsdangerous tokens
    # with signatures
    if 'LCC_SESSIONSECRET' in os.environ:

        SESSIONSECRET = os.environ['LCC_SESSIONSECRET']
        if len(SESSIONSECRET) == 0:

            logger.error(
                'SESSIONSECRET from environ["LCC_SESSIONSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using SESSIONSECRET from environ["LCC_SESSIONSECRET"]'
        )

    elif os.path.exists(options.secretfile):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(options.secretfile)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('incorrect file permissions on %s '
                         '(needs chmod 600)' % options.secretfile)
            sys.exit(1)


        with open(options.secretfile,'r') as infd:
            SESSIONSECRET = infd.read().strip('\n')

        if len(SESSIONSECRET) == 0:

            logger.error(
                'SESSIONSECRET from file in current base directory '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using SESSIONSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'no session secret file found in '
            'current base directory and no LCC_SESSIONSECRET '
            'environment variable found. will make a new session '
            'secret file in current directory: %s' % options.secretfile
        )
        SESSIONSECRET = hashlib.sha512(os.urandom(32)).hexdigest()
        with open(options.secretfile,'w') as outfd:
            outfd.write(SESSIONSECRET)
        os.chmod(options.secretfile, 0o100600)


    # handle the fernet secret key to encrypt tokens sent out by itsdangerous
    fernet_secrets = options.secretfile + '-fernet'

    if 'LCC_FERNETSECRET' in os.environ:

        FERNETSECRET = os.environ['LCSERVER_FERNETSECRET']
        if len(FERNETSECRET) == 0:

            logger.error(
                'FERNETSECRET from environ["LCC_FERNETSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using FERNETSECRET from environ["LCC_FERNETSECRET"]'
        )

    elif os.path.exists(fernet_secrets):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(fernet_secrets)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('incorrect file permissions on %s '
                         '(needs chmod 600)' % fernet_secrets)
            sys.exit(1)


        with open(fernet_secrets,'r') as infd:
            FERNETSECRET = infd.read().strip('\n')

        if len(FERNETSECRET) == 0:

            logger.error(
                'FERNETSECRET from file in current base directory '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using FERNETSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'no fernet secret file found in '
            'current base directory and no LCC_FERNETSECRET '
            'environment variable found. will make a new fernet '
            'secret file in current directory: %s' % fernet_secrets
        )
        FERNETSECRET = Fernet.generate_key()
        with open(fernet_secrets,'wb') as outfd:
            outfd.write(FERNETSECRET)
        os.chmod(fernet_secrets, 0o100600)



    # handle the cpserver secret key to encrypt tokens sent out by itsdangerous
    cpserver_secrets = options.secretfile + '-cpserver'

    if 'CPSERVER_SHAREDSECRET' in os.environ:

        CPSERVERSECRET = os.environ['CPSERVER_SHAREDSECRET']
        if len(CPSERVERSECRET) == 0:

            logger.error(
                'CPSERVERSECRET from environ["CPSERVER_SHAREDSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using CPSERVERSECRET from environ["CPSERVER_SHAREDSECRET"]'
        )

    elif os.path.exists(cpserver_secrets):

        with open(cpserver_secrets,'r') as infd:
            CPSERVERSECRET = infd.read().strip('\n')

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(cpserver_secrets)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('incorrect file permissions on %s '
                         '(needs chmod 600)' % cpserver_secrets)
            sys.exit(1)


        if len(CPSERVERSECRET) == 0:

            logger.error(
                'CPSERVERSECRET from file in current base directory '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'using CPSERVERSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'no cpserver secret file found in '
            'current base directory and no CPSERVER_SHAREDSECRET '
            'environment variable found. will make a new cpserver '
            'secret file in current directory: %s' % cpserver_secrets
        )
        CPSERVERSECRET = Fernet.generate_key()
        with open(cpserver_secrets,'wb') as outfd:
            outfd.write(CPSERVERSECRET)
        os.chmod(cpserver_secrets, 0o100600)

    # return our signer object, fernet object and shared key for talking to
    # checkplotserver
    SIGNER = URLSafeTimedSerializer(SESSIONSECRET,
                                    salt='lcc-server-api')
    FERNET = Fernet(FERNETSECRET)

    return SESSIONSECRET, SIGNER, FERNET, CPSERVERSECRET



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

    ####################################
    ## SET THE GLOBAL VERSION STRINGS ##
    ####################################

    APIVERSION = 1


    ###################
    ## SET UP CONFIG ##
    ###################

    MAXWORKERS = options.backgroundworkers

    BASEDIR = os.path.abspath(options.basedir)
    TEMPLATEPATH = os.path.abspath(options.templatepath)
    ASSETPATH = os.path.abspath(options.assetpath)

    USELCDIR = options.uselcdir

    CURRENTDIR = os.path.abspath(os.getcwd())

    # get our secret keys
    SESSIONSECRET, SIGNER, FERNET, CPKEY = get_secret_keys(
        tornado.options,
        LOGGER
    )
    # get the address of the background checkplotserver instance
    CPADDR = options.cpaddr

    #
    # site docs
    #
    SITE_DOCSPATH = options.docspath
    SITE_STATIC = os.path.join(SITE_DOCSPATH,'static')
    with open(os.path.join(SITE_DOCSPATH, 'doc-index.json'),'r') as infd:
        SITE_DOCINDEX = json.load(infd)

    #
    # site specific info
    #
    siteinfojson = os.path.join(BASEDIR, 'site-info.json')
    with open(siteinfojson,'r') as infd:
        SITEINFO = json.load(infd)

    #
    # server docs
    #
    SERVER_DOCSPATH = os.path.abspath(os.path.join(modpath,
                                                   '..',
                                                   'server-docs'))
    SERVER_STATIC = os.path.join(SERVER_DOCSPATH, 'static')
    with open(os.path.join(SERVER_DOCSPATH,'doc-index.json'),'r') as infd:
        SERVER_DOCINDEX = json.load(infd)



    ####################################
    ## PERSISTENT BACKGROUND EXECUTOR ##
    ####################################

    EXECUTOR = ProcessPoolExecutor(MAXWORKERS)

    ##################
    ## URL HANDLERS ##
    ##################

    HANDLERS = [

        #################
        ## BASIC STUFF ##
        #################

        # index page
        (r'/',
         ih.IndexHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO}),

        # docs page index and other subdirs, renders markdown to HTML
        # (r'/docs/?(\S*)',
        (r'/docs/?(.*)/*',
         ih.DocsHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'serverdocs':SERVER_DOCINDEX,
          'sitedocs':SITE_DOCINDEX,
          'siteinfo':SITEINFO}),

        ###################################
        ## STATIC FILE DOWNLOAD HANDLERS ##
        ###################################

        # static files like images, etc associated with site docs
        (r'/doc-static/(.*)',
         tornado.web.StaticFileHandler,
         {'path':SITE_STATIC}),

        # static files like images, etc associated with server docs
        (r'/server-static/(.*)',
         tornado.web.StaticFileHandler,
         {'path':SERVER_STATIC}),

        # this handles static file downloads for collection info
        (r'/c/(.*)',
         tornado.web.StaticFileHandler,
         {'path':os.path.join(BASEDIR,'lccjsons')}),

        # this handles static file downloads for dataset pickles
        (r'/d/(.*)',
         tornado.web.StaticFileHandler,
         {'path':os.path.join(BASEDIR,'datasets')}),

        # this handles static file downloads for dataset products
        (r'/p/(.*)',
         tornado.web.StaticFileHandler,
         {'path':os.path.join(BASEDIR,'products')}),

        # this handles static file downloads for individual light curves
        (r'/l/(.*)',
         tornado.web.StaticFileHandler,
         {'path':os.path.join(BASEDIR,'csvlcs')}),


        ######################
        ## FIRST LEVEL APIS ##
        ######################

        # this returns an API key
        (r'/api/key',
         ih.APIKeyHandler,
         {'apiversion':APIVERSION,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this checks the API key to see if it's still valid
        (r'/api/auth',
         ih.APIAuthHandler,
         {'apiversion':APIVERSION,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this returns a JSON list of the currently available LC collections
        (r'/api/collections',
         ih.CollectionListHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this returns a JSON list of the currently available datasets
        (r'/api/datasets',
         ih.DatasetListHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'signer':SIGNER,
          'fernet':FERNET}),


        ##################################
        ## SEARCH API ENDPOINT HANDLERS ##
        ##################################

        # this is the basic column search API endpoint
        (r'/api/columnsearch',
         sh.ColumnSearchHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'uselcdir':USELCDIR,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this is the cone search API endpoint
        (r'/api/conesearch',
         sh.ConeSearchHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'uselcdir':USELCDIR,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this is the FTS search API endpoint
        (r'/api/ftsquery',
         sh.FTSearchHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'uselcdir':USELCDIR,
          'signer':SIGNER,
          'fernet':FERNET}),

        # this is the xmatch search API endpoint
        (r'/api/xmatch',
         sh.XMatchHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'uselcdir':USELCDIR,
          'signer':SIGNER,
          'fernet':FERNET}),


        ##############################################
        ## DATASET DISPLAY AND LIVE-UPDATE HANDLERS ##
        ##############################################

        # this is the dataset display API for a single dataset
        (r'/set/(\w+)',
         dh.DatasetHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'signer':SIGNER,
          'fernet':FERNET,
          'siteinfo':SITEINFO}),

        # this just shows all datasets in a big table
        (r'/datasets',
         dh.AllDatasetsHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO}),


        ################################################
        ## OBJECT INFORMATION FROM CHECKPLOT HANDLERS ##
        ################################################

        # the main objectinfo API - talks to checkplotserver
        (r'/api/object',
         oh.ObjectInfoHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'signer':SIGNER,
          'fernet':FERNET,
          'cpsharedkey':CPKEY,
          'cpaddress':CPADDR}),

        # renders objectinfo from API above to an HTML page for easy viewing
        (r'/obj/(\S+)/(\S+)',
         oh.ObjectInfoPageHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'signer':SIGNER,
          'fernet':FERNET,
          'cpsharedkey':CPKEY,
          'cpaddress':CPADDR,
          'siteinfo':SITEINFO}),

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
    LOGGER.info('background worker processes: %s, IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))
    LOGGER.info('the current base directory is: %s' % os.path.abspath(BASEDIR))

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
    time.sleep(2)

# run the server
if __name__ == '__main__':
    main()
