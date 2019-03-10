#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''indexserver.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018
License: MIT - see the LICENSE file for the full text.

'''
#############
## LOGGING ##
#############

import logging


#############
## IMPORTS ##
#############

import os
import os.path
import signal
import time
import sys
import socket
import json


# setup signal trapping on SIGINT
def _recv_sigint(signum, stack):
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
       type=str)

## this tells the testserver about the backend authnzerver
define('authnzerver',
       default='http://127.0.0.1:12600',
       help=('This tells the lcc-server the address of '
             'the local authentication and authorization server.'),
       type=str)

## this tells the testserver about the default session expiry time in days
define('sessionexpiry',
       default=7,
       help=('This tells the lcc-server the session-expiry time in days.'),
       type=int)


#
# worker set up for the pool
#
def setup_worker():
    '''This sets up the processpoolexecutor worker to ignore SIGINT.

    Makes for a cleaner shutdown.

    '''
    # unregister interrupt signals so they don't get to the worker
    # and the executor can kill them cleanly (hopefully)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


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

    ###########################
    ## DEFINING URL HANDLERS ##
    ###########################

    from . import indexserver_handlers as ih
    from . import searchserver_handlers as sh
    from . import dataserver_handlers as dh
    from . import objectserver_handlers as oh
    from . import auth_handlers as ah
    from . import admin_handlers as admin
    from .basehandler import AuthEnabledStaticHandler
    from ..authnzerver import authdb

    from ..utils import ProcExecutor


    ####################################
    ## SET THE GLOBAL VERSION STRINGS ##
    ####################################

    APIVERSION = 2


    ###################
    ## SET UP CONFIG ##
    ###################

    MAXWORKERS = options.backgroundworkers

    # various directories we need
    BASEDIR = os.path.abspath(options.basedir)
    TEMPLATEPATH = os.path.abspath(options.templatepath)
    ASSETPATH = os.path.abspath(options.assetpath)
    USELCDIR = options.uselcdir
    CURRENTDIR = os.path.abspath(os.getcwd())

    # get the address of the background checkplotserver instance
    CPADDR = options.cpaddr

    # get our secret keys
    SESSIONSECRET = authdb.get_secret_token(
        'LCC_SESSIONSECRET',
        os.path.join(
            BASEDIR,
            '.lccserver.secret'
        ),
        LOGGER
    )
    FERNETSECRET = authdb.get_secret_token(
        'LCC_FERNETSECRET',
        os.path.join(
            BASEDIR,
            '.lccserver.secret-fernet'
        ),
        LOGGER
    )
    CPKEY = authdb.get_secret_token(
        'LCC_CPSERVERSECRET',
        os.path.join(
            BASEDIR,
            '.lccserver.secret-cpserver'
        ),
        LOGGER
    )

    #
    # site docs
    #
    SITE_DOCSPATH = os.path.join(BASEDIR,'docs')
    SITE_STATIC = os.path.join(SITE_DOCSPATH,'static')
    with open(os.path.join(SITE_DOCSPATH, 'doc-index.json'),'r') as infd:
        SITE_DOCINDEX = json.load(infd)


    #
    # find the collection footprint SVG if any and read it in.
    #
    footprint_svgf = os.path.join(BASEDIR,
                                  'docs',
                                  'static',
                                  'collection-footprints.svg')

    if os.path.exists(footprint_svgf):
        with open(footprint_svgf, 'r') as infd:
            footprint_svg = infd.readlines()
            footprint_svg = ''.join(footprint_svg[3:])
    else:
        footprint_svg = None

    #
    # site specific info
    #
    siteinfojson = os.path.join(BASEDIR, 'site-info.json')
    with open(siteinfojson,'r') as infd:
        SITEINFO = json.load(infd)

    # get the email info file if it exists
    if ('email_settings_file' in SITEINFO and
        os.path.exists(os.path.abspath(SITEINFO['email_settings_file']))):

        with open(SITEINFO['email_settings_file'],'r') as infd:
            email_settings = json.load(infd)

        if email_settings['email_server'] != "smtp.emailserver.org":
            SITEINFO.update(email_settings)

            LOGGER.info('Site info: email server to use: %s:%s.' %
                        (email_settings['email_server'],
                         email_settings['email_port']))
            LOGGER.info('Site info: email server sender to use: %s.' %
                        email_settings['email_sender'])

        else:
            LOGGER.warning('Site info: no email server is set up.')
            SITEINFO['email_server'] = None
            SITEINFO['email_sender'] = None
            SITEINFO['email_port'] = 25
            SITEINFO['email_user'] = None
            SITEINFO['email_pass'] = None
    else:
        LOGGER.warning('Site info: no email server is set up.')
        SITEINFO['email_server'] = None
        SITEINFO['email_sender'] = None
        SITEINFO['email_port'] = 25
        SITEINFO['email_user'] = None
        SITEINFO['email_pass'] = None


    # get the user login settings
    if SITEINFO['email_server'] is None:
        LOGGER.warning('Site info: '
                       'no email server set up, '
                       'user logins cannot be enabled.')
        SITEINFO['logins_allowed'] = False

    elif ('logins_allowed' in SITEINFO and
          SITEINFO['logins_allowed'] and
          SITEINFO['email_server'] is not None):
        LOGGER.info('Site info: user logins are allowed.')

    elif ('logins_allowed' in SITEINFO and (not SITEINFO['logins_allowed'])):
        LOGGER.warning('Site info: user logins are disabled.')

    else:
        SITEINFO['logins_allowed'] = False
        LOGGER.warning('Site info: '
                       'settings key "logins_allowed" not found, '
                       'disabling user logins.')

    # get the user signup and signin settings
    if SITEINFO['email_server'] is None:
        LOGGER.warning('Site info: '
                       'no email server set up, '
                       'user signups cannot be enabled.')
        SITEINFO['signups_allowed'] = False

    elif ('signups_allowed' in SITEINFO and
          SITEINFO['signups_allowed'] and
          SITEINFO['email_server'] is not None):
        LOGGER.info('Site info: user signups are allowed.')

    elif 'signups_allowed' in SITEINFO and not SITEINFO['signups_allowed']:
        LOGGER.warning('Site info: user signups are disabled.')

    else:
        SITEINFO['signups_allowed'] = False
        LOGGER.warning('Site info: '
                       'settings key "signups_allowed" not found, '
                       'disabling user signups.')

    #
    # server docs
    #
    SERVER_DOCSPATH = os.path.abspath(os.path.join(modpath,
                                                   '..',
                                                   'server-docs'))
    SERVER_STATIC = os.path.join(SERVER_DOCSPATH, 'static')
    with open(os.path.join(SERVER_DOCSPATH,'doc-index.json'),'r') as infd:
        SERVER_DOCINDEX = json.load(infd)



    #
    # authentication server options
    #
    AUTHNZERVER = options.authnzerver
    SESSION_EXPIRY = options.sessionexpiry

    #
    # rate limit options
    #
    RATELIMIT = SITEINFO['rate_limit_active']
    CACHEDIR = SITEINFO['cache_location']

    ###########################
    ## WORK AROUND APPLE BUG ##
    ###########################

    # here, we have to initialize networking in the main thread
    # before forking for MacOS. see:
    # https://bugs.python.org/issue30385#msg293958
    # if this doesn't work, Python will segfault.
    # the workaround noted in the report is to launch
    # lcc-server like so:
    # env no_proxy='*' indexserver
    if sys.platform == 'darwin':
        import requests
        try:
            requests.get('http://captive.apple.com/hotspot-detect.html')
        except Exception as e:
            requests.get(CPADDR)


    ####################################
    ## PERSISTENT BACKGROUND EXECUTOR ##
    ####################################

    EXECUTOR = ProcExecutor(max_workers=MAXWORKERS,
                            initializer=setup_worker,
                            initargs=())


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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'footprint_svg':footprint_svg}),

        #########################
        ## ADMIN RELATED PAGES ##
        #########################

        # this is the main admin page
        (r'/admin',
         admin.AdminIndexHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the site settings update handler
        (r'/admin/site',
         admin.SiteSettingsHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'sitestatic':SITE_STATIC}),

        # this is the email settings update handler
        (r'/admin/email',
         admin.EmailSettingsHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'sitestatic':SITE_STATIC}),

        # this is the user info update handler
        (r'/admin/users',
         admin.UserAdminHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'basedir':BASEDIR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'sitestatic':SITE_STATIC}),

        ########################
        ## AUTH RELATED PAGES ##
        ########################

        # this is the login page
        (r'/users/login',
         ah.LoginHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the logout page
        (r'/users/logout',
         ah.LogoutHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the new user page
        (r'/users/new',
         ah.NewUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the verification page for verifying email addresses
        (r'/users/verify',
         ah.VerifyUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is step 1 page for forgotten passwords
        (r'/users/forgot-password-step1',
         ah.ForgotPassStep1Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the verification page for verifying email addresses
        (r'/users/forgot-password-step2',
         ah.ForgotPassStep2Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is the verification page for verifying email addresses
        (r'/users/password-change',
         ah.ChangePassHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is user-prefs page
        (r'/users/home',
         ah.UserHomeHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this is user-delete page
        (r'/users/delete',
         ah.DeleteUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        ################
        ## DOCS PAGES ##
        ################

        # the /api endpoint redirects to /docs/api
        (r'/api',tornado.web.RedirectHandler,{'url':'/docs/api'}),

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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

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
         AuthEnabledStaticHandler,
         {'path':os.path.join(BASEDIR,'datasets'),
          'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'apiversion':APIVERSION,}),

        # this handles static file downloads for dataset products
        (r'/p/(.*)',
         AuthEnabledStaticHandler,
         {'path':os.path.join(BASEDIR,'products'),
          'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'apiversion':APIVERSION,}),

        # this handles static file downloads for individual light curves
        (r'/l/(.*)',
         AuthEnabledStaticHandler,
         {'path':os.path.join(BASEDIR,'csvlcs'),
          'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR,
          'apiversion':APIVERSION,}),


        ######################
        ## FIRST LEVEL APIS ##
        ######################

        # this returns an API key
        (r'/api/key',
         ah.APIKeyHandler,
         {'apiversion':APIVERSION,
          'authnzerver':AUTHNZERVER,
          'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this checks the API key to see if it's still valid
        (r'/api/verify',
         ah.APIVerifyHandler,
         {'apiversion':APIVERSION,
          'authnzerver':AUTHNZERVER,
          'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this returns a JSON list of the currently available LC collections
        (r'/api/collections',
         ih.CollectionListHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this returns a JSON list of the currently available datasets
        (r'/api/datasets',
         ih.DatasetListHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),


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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

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
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),


        ##############################################
        ## DATASET DISPLAY AND LIVE-UPDATE HANDLERS ##
        ##############################################

        # this is the dataset display API for a single dataset
        (r'/set/(\w+)/?\S*',
         dh.DatasetHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # this just shows all datasets in a big table
        (r'/datasets',
         dh.AllDatasetsHandler,
         {'currentdir':CURRENTDIR,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),


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
          'cpsharedkey':CPKEY,
          'cpaddress':CPADDR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

        # renders objectinfo from API above to an HTML page for easy viewing
        (r'/obj/(\S+)/(\S+)',
         oh.ObjectInfoPageHandler,
         {'currentdir':CURRENTDIR,
          'apiversion':APIVERSION,
          'templatepath':TEMPLATEPATH,
          'assetpath':ASSETPATH,
          'executor':EXECUTOR,
          'basedir':BASEDIR,
          'cpsharedkey':CPKEY,
          'cpaddress':CPADDR,
          'siteinfo':SITEINFO,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'fernetkey':FERNETSECRET,
          'ratelimit':RATELIMIT,
          'cachedir':CACHEDIR}),

    ]

    ########################
    ## APPLICATION SET UP ##
    ########################

    app = tornado.web.Application(
        static_path=ASSETPATH,
        handlers=HANDLERS,
        template_path=TEMPLATEPATH,
        static_url_prefix='/static/',
        compress_response=True,
        cookie_secret=SESSIONSECRET,
        xsrf_cookies=True,
        xsrf_cookie_kwargs={'samesite':'Lax'},
        debug=DEBUG,
    )

    # FIXME: consider using this instead of handlers=HANDLERS above.
    # http://www.tornadoweb.org/en/stable/guide/security.html#dns-rebinding
    # FIXME: how does this work for X-Real-Ip and X-Forwarded-Host?
    # if options.serve == '127.0.0.1':
    #     app.add_handlers(r'(localhost|127\.0\.0\.1)', HANDLERS)
    # else:
    #     fqdn = socket.getfqdn()
    #     ip = options.serve.replace('.','\.')
    #     app.add_handlers(r'({fqdn}|{ip})'.format(fqdn=fqdn,ip=ip), HANDLERS)

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

    LOGGER.info('Started indexserver. listening on http://%s:%s' %
                (options.serve, serverport))
    LOGGER.info('Background worker processes: %s, IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))
    LOGGER.info('The current base directory is: %s' % os.path.abspath(BASEDIR))

    # register the signal callbacks
    signal.signal(signal.SIGINT,_recv_sigint)
    signal.signal(signal.SIGTERM,_recv_sigint)

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
