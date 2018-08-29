#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

These are handlers for the authnzerver.

'''

#############
## LOGGING ##
#############

import logging

# get a logger
LOGGER = logging.getLogger(__name__)


#############
## IMPORTS ##
#############

import json
import ipaddress
import base64

import multiprocessing as mp


import tornado.web
from tornado import gen
import tornado.ioloop

from cryptography.fernet import Fernet, InvalidToken

from sqlalchemy.sql import select

from . import tables


#########################
## REQ/RESP VALIDATION ##
#########################

def check_host(remote_ip):
    '''
    This just returns False if the remote_ip != 127.0.0.1

    '''
    try:
        return (ipaddress.ip_address(remote_ip) ==
                ipaddress.ip_address('127.0.0.1'))
    except ValueError:
        return False


def decrypt_request(requestbody_base64, fernet_key):
    '''
    This decrypts the incoming request.

    '''

    frn = Fernet(fernet_key)

    try:

        request_bytes = base64.b64decode(requestbody_base64)
        decrypted = frn.decrypt(request_bytes)
        return json.loads(decrypted)

    except InvalidToken:

        LOGGER.error('invalid request could not be decrypted')
        return None

    except Exception as e:

        LOGGER.exception('could not understand incoming request')
        return None


def encrypt_response(response_dict, fernet_key):
    '''
    This encrypts the outgoing response.

    '''

    frn = Fernet(fernet_key)

    json_bytes = json.dumps(response_dict).encode()
    json_encrypted_bytes = frn.encrypt(json_bytes)
    response_base64 = base64.b64encode(json_encrypted_bytes)
    return response_base64


#####################################
## AUTH REQUEST HANDLING FUNCTIONS ##
#####################################

def auth_echo(payload):
    '''
    This just echoes back the payload.

    '''

    # this checks if the database connection is live
    currproc = mp.current_process()

    engine = getattr(currproc, 'engine', None)

    if not engine:

        currproc.engine, currproc.connection, currproc.table_meta = (
            tables.get_auth_db(
                currproc.auth_db_path,
                echo=False
            )
        )

    permissions = currproc.table_meta.tables['permissions']
    s = select([permissions])
    result = currproc.engine.execute(s)
    # add the result to the outgoing payload
    serializable_result = list(dict(x) for x in result)
    payload['dbtest'] = serializable_result
    result.close()

    LOGGER.info('responding from process: %s' % currproc.name)
    return payload


def auth_session_add(payload):
    '''
    This generates a new session token.

    '''


def auth_session_check(payload):
    '''
    This checks if session token exists.

    '''


def auth_session_invalidate(payload):
    '''
    This removes a session token.

    '''


#
# this maps request types -> request functions to execute
#
request_functions = {
    'echo':auth_echo,
    'session-add':auth_session_add,
    'session-check':auth_session_check,
    'session-invalidate':auth_session_invalidate,
}


#############
## HANDLER ##
#############


class EchoHandler(tornado.web.RequestHandler):
    '''
    This just echos back whatever we send.

    Useful to see if the encryption is working as intended.

    '''

    def initialize(self,
                   authdb,
                   fernet_secret,
                   executor):
        '''
        This sets up stuff.

        '''

        self.authdb = authdb
        self.fernet_secret = fernet_secret
        self.executor = executor


    # @gen.coroutine
    async def post(self):
        '''
        Handles the incoming POST request.

        '''

        ipcheck = check_host(self.request.remote_ip)

        if not ipcheck:
            raise tornado.web.HTTPError(status_code=400)

        payload = decrypt_request(self.request.body, self.fernet_secret)
        if not payload:
            raise tornado.web.HTTPError(status_code=401)

        # if we successfully got past host and decryption validation, then
        # process the request
        try:

            loop = tornado.ioloop.IOLoop.current()

            response_dict = await loop.run_in_executor(
                self.executor,
                request_functions[payload['request']],
                payload
            )

            if response_dict is not None:
                encrypted_base64 = encrypt_response(
                    response_dict,
                    self.fernet_secret
                )

                self.set_header('content-type','text/plain; charset=UTF-8')
                self.write(encrypted_base64)
                self.finish()

            else:
                raise tornado.web.HTTPError(status_code=401)

        except Exception as e:

            LOGGER.exception('failed to understand request')
            raise tornado.web.HTTPError(status_code=400)



class AuthHandler(tornado.web.RequestHandler):
    '''
    This handles the actual auth requests.

    '''

    def post(self):
        '''
        Handles the incoming POST request.

        '''
