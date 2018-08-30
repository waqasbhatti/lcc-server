#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''main.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This is the main file for the authnzerver, a simple authorization and
authentication server backed by SQLite and Tornado for use with the lcc-server.

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
import stat
import os.path
import socket
import sys
import signal
import time

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
import multiprocessing as mp
import getpass

from cryptography.fernet import Fernet

###################
## LOCAL IMPORTS ##
###################

from ..utils import ProcExecutor

##############
## HANDLERS ##
##############

from .handlers import AuthHandler, EchoHandler
from . import tables

###############################
### APPLICATION SETUP BELOW ###
###############################

modpath = os.path.abspath(os.path.dirname(__file__))

# define our commandline options

# the port to serve on
# indexserver  will serve on 12600-12604 by default
define('port',
       default=12600,
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

# basedir is the directory at the root where all LCC collections are stored this
# contains subdirs for each collection and a lcc-collections.sqlite file that
# contains info on all collections.
define('basedir',
       default=os.getcwd(),
       help=('The base directory of the light curve collections.'),
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

# path to the authentication database file
define('authdb',
       default=os.path.join(os.path.abspath(os.getcwd()), '.authdb.sqlite'),
       help=("The path to a local SQLite database used for "
             "storing authentication data. If this doesn't exist, "
             "it will be created."),
       type=str)


#######################
## UTILITY FUNCTIONS ##
#######################

def get_fernet_secret(tornado_options, logger):
    """
    This loads and generates the required Fernet secret key:


    """
    # handle the fernet secret key to encrypt tokens sent out by itsdangerous
    fernet_secrets = tornado_options.secretfile + '-fernet'

    if 'LCC_FERNETSECRET' in os.environ:

        FERNETSECRET = os.environ['LCSERVER_FERNETSECRET']
        if len(FERNETSECRET) == 0:

            logger.error(
                'FERNETSECRET from environ["LCC_FERNETSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'Using FERNETSECRET from environ["LCC_FERNETSECRET"]'
        )

    elif os.path.exists(fernet_secrets):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(fernet_secrets)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('Incorrect file permissions on %s '
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
            'Using FERNETSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'No Fernet secret file found in '
            'current base directory and no LCC_FERNETSECRET '
            'environment variable found.'
        )
        logger.info('Will make a new Fernet '
                    'secret file: %s' % fernet_secrets)

        FERNETSECRET = Fernet.generate_key()
        with open(fernet_secrets,'wb') as outfd:
            outfd.write(FERNETSECRET)
        os.chmod(fernet_secrets, 0o100600)

    return FERNETSECRET



def get_session_secret(tornado_options, logger):
    """
    This loads and generates the required session secret key:


    """
    # handle the session secret key to encrypt tokens sent out by itsdangerous
    session_secrets = tornado_options.secretfile

    if 'LCC_SESSIONSECRET' in os.environ:

        SESSIONSECRET = os.environ['LCSERVER_SESSIONSECRET']
        if len(SESSIONSECRET) == 0:

            logger.error(
                'SESSIONSECRET from environ["LCC_SESSIONSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'Using SESSIONSECRET from environ["LCC_SESSIONSECRET"]'
        )

    elif os.path.exists(session_secrets):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(session_secrets)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('Incorrect file permissions on %s '
                         '(needs chmod 600)' % session_secrets)
            sys.exit(1)


        with open(session_secrets,'r') as infd:
            SESSIONSECRET = infd.read().strip('\n')

        if len(SESSIONSECRET) == 0:

            logger.error(
                'SESSIONSECRET from file in current base directory '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'Using SESSIONSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'No Session secret file found in '
            'current base directory and no LCC_SESSIONSECRET '
            'environment variable found.'
        )
        logger.info('Will make a new Session '
                    'secret file: %s' % session_secrets)

        SESSIONSECRET = Fernet.generate_key()
        with open(session_secrets,'wb') as outfd:
            outfd.write(SESSIONSECRET)
        os.chmod(session_secrets, 0o100600)

    return SESSIONSECRET



def get_cpserver_secret(tornado_options, logger):
    """
    This loads and generates the required CPSERVER secret key:


    """
    # handle the cpserver secret key to encrypt tokens sent out by itsdangerous
    cpserver_secrets = tornado_options.secretfile + '-cpserver'

    if 'LCC_CPSERVERSECRET' in os.environ:

        CPSERVERSECRET = os.environ['LCSERVER_CPSERVERSECRET']
        if len(CPSERVERSECRET) == 0:

            logger.error(
                'CPSERVERSECRET from environ["LCC_CPSERVERSECRET"] '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'Using CPSERVERSECRET from environ["LCC_CPSERVERSECRET"]'
        )

    elif os.path.exists(cpserver_secrets):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(cpserver_secrets)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            logger.error('Incorrect file permissions on %s '
                         '(needs chmod 600)' % cpserver_secrets)
            sys.exit(1)


        with open(cpserver_secrets,'r') as infd:
            CPSERVERSECRET = infd.read().strip('\n')

        if len(CPSERVERSECRET) == 0:

            logger.error(
                'CPSERVERSECRET from file in current base directory '
                'is either empty or not valid, will not continue'
            )
            sys.exit(1)

        logger.info(
            'Using CPSERVERSECRET from file in current base directory'
        )

    else:

        logger.warning(
            'No CPSERVER secret file found in '
            'current base directory and no LCC_CPSERVERSECRET '
            'environment variable found.'
        )
        logger.info('Will make a new CPSERVER '
                    'secret file: %s' % cpserver_secrets)

        CPSERVERSECRET = Fernet.generate_key()
        with open(cpserver_secrets,'wb') as outfd:
            outfd.write(CPSERVERSECRET)
        os.chmod(cpserver_secrets, 0o100600)

    return CPSERVERSECRET



def create_authentication_database(authdb_path):
    '''This will make a new authentication database.

    Does NOT return engine or metadata, because those are opened in the
    background workers.

    '''

    tables.create_auth_db(authdb_path,
                          echo=False,
                          returnconn=False)



def setup_auth_worker(authdb_path,
                      fernet_secret):
    '''This stores secrets and the auth DB path in the worker loop's context.

    The worker will then open the DB and set up its Fernet instance by itself.

    '''
    # unregister interrupt signals so they don't get to the worker
    # and the executor can kill them cleanly (hopefully)
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    currproc = mp.current_process()
    currproc.auth_db_path = authdb_path
    currproc.fernet_secret = fernet_secret


def close_authentication_database():

    '''This is used to close the authentication database when the worker loop
    exits.

    '''

    currproc = mp.current_process()
    if getattr(currproc, 'table_meta', None):
        del currproc.table_meta

    if getattr(currproc, 'connection', None):
        currproc.connection.close()
        del currproc.connection

    if getattr(currproc, 'engine', None):
        currproc.engine.dispose()
        del currproc.engine

    print('Shutting down database engine in process: %s' % currproc.name,
          file=sys.stdout)


##########
## MAIN ##
##########
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

    # also fire the cpserver and lccserver secret functions so these get
    # generated before the checkplotserver or indexserver launch
    get_cpserver_secret(options, LOGGER)
    get_session_secret(options, LOGGER)
    FERNETSECRET = get_fernet_secret(options, LOGGER)

    # create our authentication database if it doesn't exist
    if not os.path.exists(options.authdb):

        LOGGER.info('Making new authentication database...')

        # generate the initial DB
        create_authentication_database(options.authdb)

        # generate the admin users and initial DB info
        u, p = tables.initial_authdb_inserts(options.authdb)

        LOGGER.warning('')
        LOGGER.warning('Generated new admin user: %s' % u)
        LOGGER.warning('Generated new admin pass: %s' % p)
        LOGGER.warning('')

    else:
        LOGGER.info('Using existing authentication database...')

    #
    # this is the background executor we'll pass over to the handler
    #
    executor = ProcExecutor(max_workers=MAXWORKERS,
                            initializer=setup_auth_worker,
                            initargs=(options.authdb,
                                      FERNETSECRET),
                            finalizer=close_authentication_database)

    # we only have one actual endpoint, the other one is for testing
    handlers = [
        (r'/', AuthHandler,
         {'authdb':options.authdb,
          'fernet_secret':FERNETSECRET,
          'executor':executor}),
    ]

    if DEBUG:
        # put in the echo handler for debugging
        handlers.append(
            (r'/echo', EchoHandler,
             {'authdb':options.authdb,
              'fernet_secret':FERNETSECRET,
              'executor':executor})
        )

    ########################
    ## APPLICATION SET UP ##
    ########################

    app = tornado.web.Application(
        debug=DEBUG,
        autoreload=False,  # this sometimes breaks Executors so disable it
    )

    # try to guard against the DNS rebinding attack
    # http://www.tornadoweb.org/en/stable/guide/security.html#dns-rebinding
    app.add_handlers(r'(localhost|127\.0\.0\.1)',
                     handlers)

    # start up the HTTP server and our application
    http_server = tornado.httpserver.HTTPServer(app)


    ######################
    ## start the server ##
    ######################

    # register the signal callbacks
    signal.signal(signal.SIGINT, recv_sigint)
    signal.signal(signal.SIGTERM, recv_sigint)

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
        LOGGER.error('Could not find a free port after %s tries, giving up' %
                     maxtries)
        sys.exit(1)

    LOGGER.info('Started authnzerver. listening on http://%s:%s' %
                (options.serve, serverport))
    LOGGER.info('Background worker processes: %s. IOLoop in use: %s' %
                (MAXWORKERS, IOLOOP_SPEC))


    # start the IOLoop and begin serving requests
    try:

        loop = tornado.ioloop.IOLoop.current()
        loop.start()

    except KeyboardInterrupt:

        LOGGER.info('Received Ctrl-C: shutting down...')

        # close down the processpool
        executor.shutdown()
        time.sleep(2)

        tornado.ioloop.IOLoop.instance().stop()



# run the server
if __name__ == '__main__':
    main()
