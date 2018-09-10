#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''testserver.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Apr 2018
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


###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
# testserver  will serve on 12500-12519 by default
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

## this tells the testserver about the backend checkplotservers
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

    from lccserver.frontend import auth_handlers as ah
    from lccserver.authnzerver import authdb

    ###################
    ## SET UP CONFIG ##
    ###################

    MAXWORKERS = options.backgroundworkers

    # various directories we need
    BASEDIR = os.path.abspath(options.basedir)
    TEMPLATEPATH = os.path.abspath(options.templatepath)
    ASSETPATH = os.path.abspath(options.assetpath)

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

    AUTHNZERVER = options.authnzerver
    SESSION_EXPIRY = options.sessionexpiry

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

        if email_settings['email_server'] != "smtp.example.email.server.org":
            SITEINFO.update(email_settings)

            LOGGER.info('email server to use: %s:%s' %
                        (email_settings['email_server'],
                         email_settings['email_port']))
            LOGGER.info('email server sender to use: %s' %
                        email_settings['email_sender'])

        else:
            LOGGER.warning('no email server is set up')
            SITEINFO['email_server'] = None
    else:
        LOGGER.warning('no email server is set up')
        SITEINFO['email_server'] = None


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

        # this is the index page
        (r'/',
         ah.IndexHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the login page
        (r'/users/login',
         ah.LoginHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the logout page
        (r'/users/logout',
         ah.LogoutHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the new user page
        (r'/users/new',
         ah.NewUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the verification page for verifying email addresses
        (r'/users/verify',
         ah.VerifyUserHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is step 1 page for forgotten passwords
        (r'/users/forgot-password-step1',
         ah.ForgotPassStep1Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the verification page for verifying email addresses
        (r'/users/forgot-password-step2',
         ah.ForgotPassStep2Handler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

        # this is the verification page for verifying email addresses
        (r'/users/password-change',
         ah.ChangePassHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),


        # this is an example protected page for the user containing their prefs
        (r'/users/home',
         ah.UserHomeHandler,
         {'fernetkey':FERNETSECRET,
          'executor':EXECUTOR,
          'authnzerver':AUTHNZERVER,
          'session_expiry':SESSION_EXPIRY,
          'siteinfo':SITEINFO}),

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
        debug=DEBUG,
        login_url='/users/login',
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

    LOGGER.info('Started testserver. listening on http://%s:%s' %
                (options.serve, serverport))
    LOGGER.info('Background worker processes: %s, IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))
    LOGGER.info('The current base directory is: %s' % os.path.abspath(BASEDIR))

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
