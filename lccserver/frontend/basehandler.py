#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''basehandler.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Sep 2018

This is the handler from which all other LCC-Server handlers inherit. It knows
how to authenticate a user.

'''

####################
## SYSTEM IMPORTS ##
####################

import logging
import numpy as np
from datetime import datetime, timedelta
import random
from textwrap import dedent as twd

######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
# - datetime
import json

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


#############
## LOGGING ##
#############

# get a logger
LOGGER = logging.getLogger(__name__)


#####################
## TORNADO IMPORTS ##
#####################

import tornado.web
from base64 import b64encode, b64decode
from tornado import gen

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from cryptography.fernet import Fernet, InvalidToken

###################
## LOCAL IMPORTS ##
###################

from lccserver import __version__

#######################
## UTILITY FUNCTIONS ##
#######################

def decrypt_response(response_base64, fernetkey):
    '''
    This decrypts the incoming response from authnzerver.

    '''

    frn = Fernet(fernetkey)

    try:

        response_bytes = b64decode(response_base64)
        decrypted = frn.decrypt(response_bytes)
        return json.loads(decrypted)

    except InvalidToken:

        LOGGER.error('invalid response could not be decrypted')
        return None

    except Exception as e:

        LOGGER.exception('could not understand incoming response')
        return None


def encrypt_request(request_dict, fernetkey):
    '''
    This encrypts the outgoing request to authnzerver.

    '''

    frn = Fernet(fernetkey)
    json_bytes = json.dumps(request_dict).encode()
    json_encrypted_bytes = frn.encrypt(json_bytes)
    request_base64 = b64encode(json_encrypted_bytes)
    return request_base64


########################
## BASE HANDLER CLASS ##
########################

class BaseHandler(tornado.web.RequestHandler):


    def initialize(self,
                   authnzerver,
                   fernetkey,
                   executor,
                   session_expiry,
                   siteinfo):
        '''
        This just sets up some stuff.

        '''

        self.authnzerver = authnzerver
        self.fernetkey = fernetkey
        self.executor = executor
        self.session_expiry = session_expiry
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.siteinfo = siteinfo


    def render_flash_messages(self):
        '''
        This renders any flash messages to a Bootstrap alert.

        '''

        if self.flash_messages:

            flash_msg = twd(
                '''\
                <div class="alert alert-warning mt-2" role="alert">
                {flash_messages}
                </div>'''.format(
                    flash_messages='<br>'.join(json.loads(self.flash_messages))
                )
            )
            return flash_msg
        else:
            return ''


    @gen.coroutine
    def prepare(self):
        '''This async talks to the authnzerver to get info on the current user.

        1. check the lccserver_session cookie and see if it's not expired.

        2. if can get cookie, talk to authnzerver to get the session info and
           populate the self.current_user variable with the session dict.

        3. if cannot get cookie, then ask authnzerver for a new session token by
           giving it the remote_ip, client_header, and an expiry date. set the
           cookie with this session token and set self.current_user variable
           with session dict.

        '''

        # FIXME: cookie annoyances secure=True won't work if
        # you're developing on localhost samesite=True is not
        # supported by Python yet
        if self.request.remote_ip != '127.0.0.1':
            self.csecure = True
        else:
            self.csecure = False

        # check the session cookie
        session_token = self.get_secure_cookie(
            'lccserver_session',
            max_age_days=self.session_expiry
        )

        # get the flash messages if any
        self.flash_messages = self.get_secure_cookie(
            'lccserver_flashmsg'
        )

        # clear the cookie so we can re-use it later
        self.clear_cookie('lccserver_flashmsg')

        LOGGER.info('session_token = %s' % session_token)

        if session_token is not None:

            reqtype = 'session-exists'
            reqbody = {
                'session_token': session_token
            }
            reqid = random.randint(0,5000)
            req = {'request':reqtype,
                   'body':reqbody,
                   'reqid':reqid}
            encrypted_req = yield self.executor.submit(
                encrypt_request,
                req,
                self.fernetkey
            )
            auth_req = HTTPRequest(
                self.authnzerver,
                method='POST',
                body=encrypted_req
            )
            encrypted_resp = yield self.httpclient.fetch(
                auth_req, raise_error=False
            )

            if encrypted_resp.code != 200:

                LOGGER.error('could not talk to the backend authnzerver. '
                             'Will fail this request.')
                raise tornado.web.HTTPError(statuscode=401)

            else:

                respdict = yield self.executor.submit(
                    decrypt_response,
                    encrypted_resp.body,
                    self.fernetkey
                )
                LOGGER.info(respdict)

                response = respdict['response']

                # if the session lookup succeeded, we're OK
                if respdict['success']:
                    self.current_user = response['session_info']
                else:

                    # if the session lookup failed, then delete the cookie and
                    # redirect to the home page
                    self.current_user = None
                    self.clear_cookie('lccserver_session')
                    self.redirect('/')

        # if the session token is not set, then set the secure cookie
        else:

            # ask authnzerver for a session cookie
            reqtype = 'session-new'
            reqbody = {
                'ip_address': self.request.remote_ip,
                'client_header':self.request.headers['User-Agent'],
                'user_id':2,
                'expires':(
                    datetime.utcnow() + timedelta(days=self.session_expiry)
                ),
                'extra_info_json':{},
            }
            reqid = random.randint(0,5000)
            req = {'request':reqtype,
                   'body':reqbody,
                   'reqid':reqid}
            encrypted_req = yield self.executor.submit(
                encrypt_request,
                req,
                self.fernetkey
            )
            auth_req = HTTPRequest(
                self.authnzerver,
                method='POST',
                body=encrypted_req
            )
            encrypted_resp = yield self.httpclient.fetch(
                auth_req,
                raise_error=False
            )

            if encrypted_resp.code != 200:

                LOGGER.error('could not talk to the backend authnzerver. '
                             'Will fail this request.')
                raise tornado.web.HTTPError(statuscode=401)

            else:

                respdict = yield self.executor.submit(
                    decrypt_response,
                    encrypted_resp.body,
                    self.fernetkey
                )
                LOGGER.info(respdict)

                response = respdict['response']

                if respdict['success']:

                    # get the expiry date from the response
                    cookie_expiry = datetime.strptime(
                        response['expires'].replace('Z',''),
                        '%Y-%m-%dT%H:%M:%S.%f'
                    )
                    expires_days = cookie_expiry - datetime.utcnow()
                    expires_days = expires_days.days

                    LOGGER.info(
                        'new session cookie for %s expires at %s, in %s days' %
                        (response['session_token'],
                         response['expires'],
                         expires_days)
                    )

                    self.set_secure_cookie(
                        'lccserver_session',
                        response['session_token'],
                        expires_days=expires_days,
                        httponly=True,
                        secure=self.csecure
                    )
                else:

                    LOGGER.error('could not talk to the backend authnzerver. '
                                 'Will fail this request.')
                    raise tornado.web.HTTPError(statuscode=401)



###########################
## VARIOUS TEST HANDLERS ##
###########################

class IndexHandler(BaseHandler):
    '''
    This is a test handler inheriting from the base handler to provide auth.

    '''


    @gen.coroutine
    def get(self):
        '''
        This just checks if the cookie was set correctly and sessions work.

        '''

        current_user = self.current_user

        if current_user:
            self.write(current_user)
        else:
            self.write('No cookie set yet.')
        self.finish()



class LoginHandler(BaseHandler):
    '''
    This is a test handler inheriting from the base handler to provide auth.

    '''


    @gen.coroutine
    def get(self):
        '''
        This just checks if the cookie was set correctly and sessions work.

        '''

        current_user = self.current_user


        # if we have a session token ready, then prepare to log in
        if current_user:

            # if we're already logged in, redirect to the index page
            # FIXME: in the future, this may redirect to /users/home
            if ((current_user['user_role'] in
                 ('authenticated', 'staff', 'superuser')) and
                (current_user['user_id'] != 2)):

                LOGGER.warning('user is already logged in')
                self.redirect('/')

            # if we're anonymous and we want to login, show the login page
            elif ((current_user['user_role'] == 'anonymous') and
                  (current_user['user_id'] == 2)):

                self.render('login.html',
                            flash_messages=self.render_flash_messages(),
                            page_title="Sign in",
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

            # anything else is probably the locked user, turn them away
            else:

                self.set_status(403)
                self.write('You are not authorized to view this page.')
                self.finish()

        # redirect us to the login page again so the anonymous session cookie
        # gets set correctly. FIXME: does this make sense?
        else:
            self.redirect('/')


    @gen.coroutine
    def post(self):
        '''
        This handles user login by talking to the authnzerver.

        '''

        # get the current user
        current_user = self.current_user

        # get the provided email and password
        try:
            email = self.get_argument('email')
            password = self.get_argument('password')
        except Exception as e:
            LOGGER.error('email and password are both required.')
            raise tornado.web.HTTPError(statuscode=400)

        # talk to the authnzerver to login this user

        reqtype = 'user-login'
        reqbody = {
            'session_token': current_user['session_token'],
            'email':email,
            'password':password
        }
        reqid = random.randint(0,5000)
        req = {'request':reqtype,
               'body':reqbody,
               'reqid':reqid}
        encrypted_req = yield self.executor.submit(
            encrypt_request,
            req,
            self.fernetkey
        )
        auth_req = HTTPRequest(
            self.authnzerver,
            method='POST',
            body=encrypted_req
        )
        encrypted_resp = yield self.httpclient.fetch(
            auth_req, raise_error=False
        )

        if encrypted_resp.code != 200:

            LOGGER.error('could not talk to the backend authnzerver. '
                         'Will fail this request.')
            raise tornado.web.HTTPError(statuscode=401)

        else:

            respdict = yield self.executor.submit(
                decrypt_response,
                encrypted_resp.body,
                self.fernetkey
            )
            LOGGER.info(respdict)

            response = respdict['response']

            # if the login succeeded, we'll ask the authnzerver for a new
            # session token corresponding to this user_id and set the new
            # lccserver_session cookie
            if respdict['success']:

                # ask authnzerver for a session cookie
                reqtype = 'session-new'
                reqbody = {
                    'ip_address': self.request.remote_ip,
                    'client_header':self.request.headers['User-Agent'],
                    'user_id':response['user_id'],
                    'expires':(
                        datetime.utcnow() + timedelta(days=self.session_expiry)
                    ),
                    'extra_info_json':{},
                }
                reqid = random.randint(0,5000)
                req = {'request':reqtype,
                       'body':reqbody,
                       'reqid':reqid}
                encrypted_req = yield self.executor.submit(
                    encrypt_request,
                    req,
                    self.fernetkey
                )
                auth_req = HTTPRequest(
                    self.authnzerver,
                    method='POST',
                    body=encrypted_req
                )
                encrypted_resp = yield self.httpclient.fetch(
                    auth_req,
                    raise_error=False
                )

                if encrypted_resp.code != 200:

                    LOGGER.error('could not talk to the backend authnzerver. '
                                 'Will fail this request.')
                    raise tornado.web.HTTPError(statuscode=401)

                else:

                    respdict = yield self.executor.submit(
                        decrypt_response,
                        encrypted_resp.body,
                        self.fernetkey
                    )
                    LOGGER.info(respdict)

                    response = respdict['response']

                    if respdict['success']:

                        # get the expiry date from the response
                        cookie_expiry = datetime.strptime(
                            response['expires'].replace('Z',''),
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )
                        expires_days = cookie_expiry - datetime.utcnow()
                        expires_days = expires_days.days

                        LOGGER.info(
                            'new session cookie for %s expires '
                            'at %s, in %s days' %
                            (response['session_token'],
                             response['expires'],
                             expires_days)
                        )

                        self.set_secure_cookie(
                            'lccserver_session',
                            response['session_token'],
                            expires_days=expires_days,
                            httponly=True,
                            secure=self.csecure
                        )

                        # redirect to the home page
                        self.redirect('/')

                    else:

                        LOGGER.error(
                            'could not talk to the backend authnzerver. '
                            'Will fail this request.'
                        )
                        raise tornado.web.HTTPError(statuscode=401)


            # if the user login did not succeed, redirect to /users/login with
            # flash message returned by the authnzerver. we'll set a new session
            # token
            else:

                # set the flash messages cookie with the failure messages
                self.set_secure_cookie('lccserver_flashmsg',
                                       json.dumps(response['messages']),
                                       expires_days=None)

                # ask authnzerver for a session cookie for the anonymous user
                reqtype = 'session-new'
                reqbody = {
                    'ip_address': self.request.remote_ip,
                    'client_header':self.request.headers['User-Agent'],
                    'user_id':2,
                    'expires':(
                        datetime.utcnow() + timedelta(days=self.session_expiry)
                    ),
                    'extra_info_json':{},
                }
                reqid = random.randint(0,5000)
                req = {'request':reqtype,
                       'body':reqbody,
                       'reqid':reqid}
                encrypted_req = yield self.executor.submit(
                    encrypt_request,
                    req,
                    self.fernetkey
                )
                auth_req = HTTPRequest(
                    self.authnzerver,
                    method='POST',
                    body=encrypted_req
                )
                encrypted_resp = yield self.httpclient.fetch(
                    auth_req,
                    raise_error=False
                )

                if encrypted_resp.code != 200:

                    LOGGER.error('could not talk to the backend authnzerver. '
                                 'Will fail this request.')
                    raise tornado.web.HTTPError(statuscode=401)

                else:

                    respdict = yield self.executor.submit(
                        decrypt_response,
                        encrypted_resp.body,
                        self.fernetkey
                    )
                    LOGGER.info(respdict)

                    response = respdict['response']

                    if respdict['success']:

                        # get the expiry date from the response
                        cookie_expiry = datetime.strptime(
                            response['expires'].replace('Z',''),
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )
                        expires_days = cookie_expiry - datetime.utcnow()
                        expires_days = expires_days.days

                        LOGGER.info(
                            'new session cookie for %s expires '
                            'at %s, in %s days' %
                            (response['session_token'],
                             response['expires'],
                             expires_days)
                        )

                        self.set_secure_cookie(
                            'lccserver_session',
                            response['session_token'],
                            expires_days=expires_days,
                            httponly=True,
                            secure=self.csecure
                        )

                        # redirect back to us so we can show the message
                        self.redirect('/users/login')

                    else:

                        LOGGER.error(
                            'could not talk to the backend authnzerver. '
                            'Will fail this request.'
                        )
                        raise tornado.web.HTTPError(statuscode=401)




class LogoutHandler(BaseHandler):

    @gen.coroutine
    def post(self):
        '''
        This handles user logout by talking to the authnzerver.

        '''

        current_user = self.current_user

        # ask the authnzerver for confirmation of this user's logout



class NewUserHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This shows the sign-up page.

        '''

        current_user = self.current_user


    @gen.coroutine
    def post(self):
        '''
        This handles user sign-up by talking to the authnzerver.

        '''

        current_user = self.current_user

        # ask the authnzerver for confirmation



class VerifyUserHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This handles user email verification by talking to the authnzerver.

        '''

        current_user = self.current_user

        # ask the authnzerver for confirmation of this user's logout



class UserHomeHandler(BaseHandler):
    '''
    This shows the user's home page and handles any preferences changes.

    '''


    @gen.coroutine
    @tornado.web.authenticated
    def get(self):
        '''
        This justs checks if the cookie was set correctly and sessions work.

        '''

        current_user = self.current_user

        if current_user:
            self.write(current_user)
        else:
            self.write('No cookie set yet.')
        self.finish()


    @gen.coroutine
    @tornado.web.authenticated
    def post(self):
        '''
        This justs checks if the cookie was set correctly and sessions work.

        '''

        current_user = self.current_user

        if current_user:
            self.write(current_user)
        else:
            self.write('No cookie set yet.')
        self.finish()
