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
import numpy as np
from datetime import datetime

class FrontendEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.isoformat()
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

import ipaddress
import base64

import multiprocessing as mp


import tornado.web
import tornado.ioloop

from cryptography.fernet import Fernet, InvalidToken

from sqlalchemy.sql import select

from . import authdb
from . import actions


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
            authdb.get_auth_db(
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


#
# this maps request types -> request functions to execute
#
request_functions = {
    # session actions
    'session-new':actions.auth_session_new,
    'session-exists':actions.auth_session_exists,
    'session-delete':actions.auth_session_delete,
    'session-setinfo':actions.auth_session_set_extrainfo,
    'user-login':actions.auth_user_login,
    'user-logout':actions.auth_user_logout,
    'user-passcheck': actions.auth_password_check,
    # user actions
    'user-new':actions.create_new_user,
    'user-changepass':actions.change_user_password,
    'user-delete':actions.delete_user,
    'user-list':actions.list_users,
    'user-edit':actions.edit_user,
    'user-resetpass':actions.verify_password_reset,
    # email actions
    'user-signup-email':actions.send_signup_verification_email,
    'user-verify-email':actions.verify_user_email_address,
    'user-forgotpass-email':actions.send_forgotpass_verification_email,
    # apikey actions
    'apikey-new':actions.issue_new_apikey,
    'apikey-verify':actions.verify_apikey,
}


#############
## HANDLER ##
#############


class EchoHandler(tornado.web.RequestHandler):
    '''
    This just echoes back whatever we send.

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

        if payload['request'] != 'echo':
            LOGGER.error("this handler can only echo things. "
                         "invalid request: %s" % payload['request'])
            raise tornado.web.HTTPError(status_code=400)

        # if we successfully got past host and decryption validation, then
        # process the request
        try:

            loop = tornado.ioloop.IOLoop.current()
            response_dict = await loop.run_in_executor(
                self.executor,
                auth_echo,
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

        if payload['request'] == 'echo':
            LOGGER.error("this handler can't echo things.")
            raise tornado.web.HTTPError(status_code=400)

        # if we successfully got past host and decryption validation, then
        # process the request
        try:

            # get the request ID
            # this is an integer
            reqid = payload['reqid']
            if reqid is None or reqid == 0:
                raise ValueError("no request ID provided")

            # run the function associated with the request type
            loop = tornado.ioloop.IOLoop.current()
            response = await loop.run_in_executor(
                self.executor,
                request_functions[payload['request']],
                payload['body']
            )

            response_dict = {"success": response['success'],
                             "reqid": reqid,
                             "response":response,
                             "message": response['messages']}

            encrypted_base64 = encrypt_response(
                response_dict,
                self.fernet_secret
            )

            self.set_header('content-type','text/plain; charset=UTF-8')
            self.write(encrypted_base64)
            self.finish()

        except Exception as e:

            LOGGER.exception('failed to understand request')
            raise tornado.web.HTTPError(status_code=400)
