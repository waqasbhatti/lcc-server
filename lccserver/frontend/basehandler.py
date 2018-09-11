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
        self.ferneter = Fernet(fernetkey)
        self.executor = executor
        self.session_expiry = session_expiry
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.siteinfo = siteinfo


    def save_flash_messages(self, messages, alert_type):
        '''
        This saves the flash messages to a secure cookie.

        '''

        if isinstance(messages,list):

            outmsg = json.dumps({
                'text':messages,
                'type':alert_type
            })

        elif isinstance(messages,str):

            outmsg = json.dumps({
                'text':[messages],
                'type':alert_type
            })

        else:
            outmsg = ''

        self.set_secure_cookie(
            'lccserver_messages',
            outmsg,
            httponly=True,
            secure=self.csecure
        )



    def render_flash_messages(self):
        '''
        This renders any flash messages to a Bootstrap alert.

        alert_type is one of: warning, danger, info, primary, secondary, success

        '''

        if self.flash_messages:

            messages = json.loads(self.flash_messages)
            message_text = messages['text']
            alert_type = messages['type']

            flash_msg = twd(
                '''\
                <div class="mt-2 alert alert-{alert_type}
                alert-dismissible fade show" role="alert">
                {flash_messages}
                <button type="button"
                class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
                </button>
                </div>'''.format(
                    flash_messages='<br>'.join(message_text),
                    alert_type=alert_type,
                )
            )
            return flash_msg
        else:
            return ''


    def render_blocked_message(self):
        '''
        This renders the template indicating that the user is blocked.

        '''
        self.set_status(403)
        self.render(
            'errorpage.html',
            error_message=(
                "Sorry, it appears that you're not authorized to "
                "view the page you were trying to get to. "
                "If you believe this is in error, please contact "
                "the admins of this LCC-Server instance."
            ),
            page_title="403 - You cannot access this page.",
            siteinfo=self.siteinfo,
            lccserver_version=__version__
        )


    def render_user_account_box(self):
        '''
        This renders the user login/logout box.

        '''

        current_user = self.current_user

        # the user is not logged in - so the anonymous session is in play
        if current_user and current_user['user_id'] == 2:

            user_account_box = twd(
                '''\
                <div class="user-signin-box">
                <a class="nav-item nav-link"
                title="Sign in to your LCC-Server account"
                href="/users/login">
                Sign in
                </a>
                </div>
                <div class="user-signup-box">
                <a class="nav-item nav-link"
                title="Sign up for an LCC-Server account"
                href="/users/new">
                Sign up
                </a>
                </div>
                '''
            )

        # normal authenticated user
        elif current_user and current_user['user_id'] > 3:

            user_account_box = twd(
                '''\
                <div class="user-prefs-box">
                <a class="nav-item nav-link user-prefs-link"
                title="Change user preferences"
                href="/users/home">
                {current_user}
                </a>
                </div>
                <div class="user-signout-box">
                <button type="submit" class="btn btn-secondary btn-sm">
                Sign out
                </button>
                </div>
                '''
            ).format(current_user=current_user['email'])


        # the first superuser
        elif current_user and current_user['user_id'] == 1:

            user_account_box = twd(
                '''\
                <div class="superuser-admin-box">
                <a class="nav-item nav-link admin-portal-link"
                title="LCC-Server admin portal"
                href="/admin">
                Admin
                </a>
                </div>
                <div class="user-prefs-box">
                <a class="nav-item nav-link user-prefs-link"
                title="Change user preferences"
                href="/users/home">
                {current_user}
                </a>
                </div>
                <div class="user-signout-box">
                <button type="submit" class="btn btn-secondary btn-sm">
                Sign out
                </button>
                </div>
                '''
            ).format(current_user=current_user['email'])

        # anything should not be possible because the user is locked
        else:

            user_account_box = ''

        return user_account_box



    @gen.coroutine
    def authnzerver_request(self,
                            request_type,
                            request_body):
        '''
        This talks to the authnzerver.

        '''

        reqid = random.randint(0,10000)

        req = {'request':request_type,
               'body':request_body,
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

            return False, None, None

        else:

            respdict = yield self.executor.submit(
                decrypt_response,
                encrypted_resp.body,
                self.fernetkey
            )
            LOGGER.info(respdict)

            success = respdict['success']
            response = respdict['response']
            messages = respdict['response']['messages']

            return success, response, messages



    @gen.coroutine
    def new_session_token(self,
                          user_id=2,
                          expires_days=7,
                          extra_info=None):
        '''
        This is a shortcut function to request a new session token.

        Also sets the lccserver_session cookie.

        '''

        # ask authnzerver for a session cookie
        ok, resp, msgs = yield self.authnzerver_request(
            'session-new',
            {'ip_address': self.request.remote_ip,
             'client_header': self.request.headers['User-Agent'],
             'user_id': user_id,
             'expires': (datetime.utcnow() +
                         timedelta(days=expires_days)),
             'extra_info_json':extra_info}
        )

        if ok:

            # get the expiry date from the response
            cookie_expiry = datetime.strptime(
                resp['expires'].replace('Z',''),
                '%Y-%m-%dT%H:%M:%S.%f'
            )
            expires_days = cookie_expiry - datetime.utcnow()
            expires_days = expires_days.days

            LOGGER.info(
                'new session cookie for %s expires at %s, in %s days' %
                (resp['session_token'],
                 resp['expires'],
                 expires_days)
            )

            self.set_secure_cookie(
                'lccserver_session',
                resp['session_token'],
                expires_days=expires_days,
                httponly=True,
                secure=self.csecure,
            )

            return resp['session_token']

        else:

            self.current_user = None
            self.clear_all_cookies()
            LOGGER.error('could not talk to the backend authnzerver. '
                         'Will fail this request.')
            raise tornado.web.HTTPError(statuscode=401)



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

        # FIXME: cookie secure=True won't work if
        # you're developing on localhost
        # will probably have to go through the whole local CA nonsense

        # FIXME: cookie samesite=True is not supported by Python yet
        # https://bugs.python.org/issue29613
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
            'lccserver_messages'
        )
        # clear the cookie so we can re-use it later
        self.clear_cookie('lccserver_messages')

        LOGGER.info('session_token = %s' % session_token)

        # if a session token is found in the cookie, we'll see who it belongs to
        if session_token is not None:

            ok, resp, msgs = yield self.authnzerver_request(
                'session-exists',
                {'session_token': session_token}
            )

            # if we found the session successfully, set the current_user
            # attribute for this request
            if ok:

                self.current_user = resp['session_info']
                self.user_id = self.current_user['user_id']
                self.user_role = self.current_user['user_role']

            else:
                # if the session token provided did not match any existing
                # session in the DB, we'll clear all the cookies and redirect
                # the user to the front page so they can start over.
                self.current_user = None
                self.clear_all_cookies()
                self.redirect('/')

        # if the session token is not set, then create a new session
        else:

            new_session_token = yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry,
                extra_info={}
            )
