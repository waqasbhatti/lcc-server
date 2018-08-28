#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''main.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This is the main file for the authnzerver, a simple authorization and
authentication server backed by SQLite and Tornado for use with the lcc-server.

Here's the plan:

- make a Tornado auth server

- this will listen ONLY on a localhost HTTP port

- we will have a shared secret to authenticate incoming messages (loaded in
  order of environ > local file)

- the frontend will use AsyncHTTPClient to talk to this using POST requests only

- all messages will be Fernet encrypted using the PSK so no user info leaks in
  transit. we'll add the option of running this over TLS sockets using Tornado's
  native support for this.

The auth server will provide:

- session-add, session-delete, session-check

- user-add, user-check, user-delete, user-edit (user-add will add an arbitrary
  JSON dict to the users table in the auth-db.sqlite file)

- group-add, group-check, group-delete

- permissions-add, permissions-delete, permissions-edit, permissions-check for
  permissions rules that can be applied (this should integrate with django-rules
  so we can use that for checking object-level permissions)

The auth server will:

- launch background process workers using ProcessPoolExecutor

- run the SQL queries in the background workers (need to figure out how to
  replicate the functionality of initializers in Py3.7. Maybe use a class as the
  function object, set up __init__ to launch the SQL connection on the first
  __call__ based on the provided SQLite file path and use __call__ subsequently
  to make queries as needed.

The auth-server message schema (after decryption) is:

REQ: {
  "reqid": <random request ID>,
  "request": <request type>,
  "payload": <JSON payload>
}

RESP: {
  "status":<status>,
  "reqid":<same request ID as request>,
  "response": <JSON payload of response>,
  "message": <narrative message>
}

status is one of:

success, error

The signature will be checked on each request. If it doesn't match, we'll
return:

{"status": "error", "response": null, "message": "auth key mismatch"}

auth-server request types:

"session-add": this should contain an expiry time limit, the user's IP address,
the user's Header

- This will return a session token encrypted and signed by Fernet containing the
  expiry, IP, header, and a random secret generated using Fernet. This session
  token will be stored in the auth-db.sqlite file in the sessions table with the
  expiry time.

"session-delete": this should contain the session ID to delete. Will return
success if OK, error if session not found.

"session-check": will check if the provided session token exists and has not yet
expired.

...

TODO: Fill in details later.

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


# for generating encrypted token information
from cryptography.fernet import Fernet


##############
## HANDLERS ##
##############

from . import handlers


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

# TODO: finish this based on the outline
