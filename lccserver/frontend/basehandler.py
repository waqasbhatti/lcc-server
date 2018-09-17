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
import threading
from base64 import b64encode, b64decode
import email.utils
import os.path
import hashlib
import stat
import os
import mimetypes

import itsdangerous
from cryptography.fernet import Fernet, InvalidToken


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
from tornado.escape import utf8
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.web import _time_independent_equals, HTTPError
from tornado import gen
from tornado import httputil, iostream
from tornado.log import gen_log

###################
## LOCAL IMPORTS ##
###################

from lccserver import __version__
from lccserver.backend import dbsearch, datasets


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

        # anything else will be shown the usual box because either the user is
        # locked or this is a complete new session
        else:

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

        TODO: add in the use of api keys

        1. If the Authorization: Bearer <token> pattern is present in the
           header, assume that we're using API key authentication.

        2. If using the provided API key, check if it's unexpired and is
           associated with a valid user account.

        3. If it is, go ahead and populate the self.current_user with the user
           information. The API key will be used as the session_token.

        4. If it's not and we've assumed that we're using the API key method,
           fail the request.

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

        # check if there's an authorization header in the request
        authorization = self.request.headers.get('Authorization')

        # if there's no authorization header in the request,
        # we'll assume that we're using normal session tokens
        if not authorization:

            # check the session cookie
            session_token = self.get_secure_cookie(
                'lccserver_session',
                max_age_days=self.session_expiry
            )

            # get the flash messages if any
            self.flash_messages = self.get_secure_cookie(
                'lccserver_messages'
            )
            # clear the lccserver_messages cookie so we can re-use it later
            self.clear_cookie('lccserver_messages')

            # if a session token is found in the cookie, we'll see who it
            # belongs to
            if session_token is not None:

                LOGGER.info('session_token = %s' % session_token)

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
                    # session in the DB, we'll clear all the cookies and
                    # redirect the user to the front page so they can start
                    # over.
                    self.current_user = None
                    self.clear_all_cookies()
                    self.redirect('/')

            # if the session token is not set, then create a new session
            else:

                yield self.new_session_token(
                    user_id=2,
                    expires_days=self.session_expiry,
                    extra_info={}
                )

        # if using the API Key
        else:

            apikey_info = self.check_apikey()

            if apikey_info['status'] != 'success':

                self.write(apikey_info)
                self.finish()

            # FIXME: here, if the apikey expiry check succeeds, we'll need to
            # set the session token appropriately. we'll need an
            # apikey-session-new command for the authnzerver. this should take
            # in the provided apikey and set it as the session_token in the
            # sessions table.

            # FIXME: we might also need a apikey-check command for authnzerver
            # to look up apikeys and get their user IDs (although we should
            # probably encode this info into the API key itself and then encrypt
            # it)


    def on_finish(self):
        '''
        This just cleans up the httpclient.

        '''

        self.httpclient.close()



    def check_apikey(self):
        '''
        This checks the API key.

        '''
        try:

            authorization = self.request.headers.get('Authorization')

            if authorization:

                key = authorization.split()[1].strip()

                # the key lifetime is the same as that for the session
                uns = self.signer.loads(
                    key,
                    max_age=self.session_expiry*86400.0
                )

                # match the remote IP and API version
                keyok = ((self.request.remote_ip == uns['ip']) and
                         (self.apiversion == uns['ver']))

            else:

                LOGGER.error('no Authorization header key found')
                retdict = {
                    'status':'failed',
                    'message':('No credentials provided or '
                               'they could not be parsed safely'),
                    'result':None
                }

                self.set_status(401)
                return retdict


            if not keyok:

                if 'X-Real-Host' in self.request.headers:
                    self.req_hostname = self.request.headers['X-Real-Host']
                else:
                    self.req_hostname = self.request.host

                newkey_url = "%s://%s/api/key" % (
                    self.request.protocol,
                    self.req_hostname,
                )

                LOGGER.error('API key is valid, but IP or API version mismatch')
                retdict = {
                    'status':'failed',
                    'message':('API key invalid for current LCC API '
                               'version: %s or your IP address has changed. '
                               'Get an up-to-date key from %s' %
                               (self.apiversion, newkey_url)),
                    'result':None
                }

                self.set_status(401)
                return retdict

            # SUCCESS HERE ONLY
            else:

                LOGGER.warning('successful API key auth: %r' % uns)

                retdict = {
                    'status':'ok',
                    'message':('API key verified successfully. Expires: %s' %
                               uns['expiry']),
                    'result':{'expiry':uns['expiry']},
                }

                return retdict

        except itsdangerous.SignatureExpired:

            LOGGER.error('API key "%s" from %s has expired' %
                         (key, self.request.remote_ip))

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host

            newkey_url = "%s://%s/api/key" % (
                self.request.protocol,
                self.req_hostname,
            )

            retdict = {
                'status':'failed',
                'message':('API key has expired. '
                           'Get a new one from %s' % newkey_url),
                'result':None
            }

            self.set_status(401)
            return retdict

        except itsdangerous.BadSignature:

            LOGGER.error('API key "%s" from %s did not pass verification' %
                         (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':'API key could not be verified or has expired.',
                'result':None
            }

            self.set_status(401)
            return retdict

        except Exception as e:

            LOGGER.exception('API key "%s" from %s did not pass verification' %
                             (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':('API key was not provided, '
                           'could not be verified, '
                           'or has expired.'),
                'result':None
            }

            self.set_status(401)
            return retdict



    def tornado_check_xsrf_cookie(self):
        '''This is the original Tornado XSRF token checker.

        From: http://www.tornadoweb.org
              /en/stable/_modules/tornado/web.html
              #RequestHandler.check_xsrf_cookie

        Modified a bit to not immediately raise 403s since we want to return
        JSON all the time.

        '''

        token = (self.get_argument("_xsrf", None) or
                 self.request.headers.get("X-Xsrftoken") or
                 self.request.headers.get("X-Csrftoken"))

        if not token:

            retdict = {
                'status':'failed',
                'message':("'_xsrf' argument missing from POST'"),
                'result':None
            }

            self.set_status(401)
            return retdict

        _, token, _ = self._decode_xsrf_token(token)
        _, expected_token, _ = self._get_raw_xsrf_token()

        if not token:

            retdict = {
                'status':'failed',
                'message':("'_xsrf' argument missing from POST"),
                'result':None
            }

            self.set_status(401)
            return retdict


        if not _time_independent_equals(utf8(token),
                                        utf8(expected_token)):

            retdict = {
                'status':'failed',
                'message':("XSRF cookie does not match POST argument"),
                'result':None
            }

            self.set_status(401)
            return retdict



    def check_xsrf_cookie(self):
        '''This overrides the usual Tornado XSRF checker.

        We use this because we want the same endpoint to support POSTs from an
        API or from the browser.

        '''

        xsrf_auth = (self.get_argument("_xsrf", None) or
                     self.request.headers.get("X-Xsrftoken") or
                     self.request.headers.get("X-Csrftoken"))

        if xsrf_auth:
            LOGGER.info('using tornado XSRF auth...')
            self.keycheck = self.tornado_check_xsrf_cookie()
        else:
            LOGGER.info('using API Authorization header auth...')
            self.keycheck = self.check_apikey()


######################################
## STATIC HANDLER WITH AUTH ENABLED ##
######################################

class AuthEnabledStaticHandler(BaseHandler):
    '''This mostly copies over the StaticFileHandler from Tornado but adds auth.

    We're not subclassing it because of warnings that it's not advisable to do
    so.

    Tornado's license:

    #
    # Copyright 2009 Facebook
    #
    # Licensed under the Apache License, Version 2.0 (the "License"); you may
    # not use this file except in compliance with the License. You may obtain
    # a copy of the License at
    #
    #     http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    # WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    # License for the specific language governing permissions and limitations
    # under the License.

    Original StaticFileHandler docstring:

    A `StaticFileHandler` is configured automatically if you pass the
    ``static_path`` keyword argument to `Application`.  This handler
    can be customized with the ``static_url_prefix``, ``static_handler_class``,
    and ``static_handler_args`` settings.
    To map an additional path to this handler for a static data directory
    you would add a line to your application like::
        application = web.Application([
            (r"/content/(.*)", web.StaticFileHandler, {"path": "/var/www"}),
        ])
    The handler constructor requires a ``path`` argument, which specifies the
    local root directory of the content to be served.
    Note that a capture group in the regex is required to parse the value for
    the ``path`` argument to the get() method (different than the constructor
    argument above); see `URLSpec` for details.
    To serve a file like ``index.html`` automatically when a directory is
    requested, set ``static_handler_args=dict(default_filename="index.html")``
    in your application settings, or add ``default_filename`` as an initializer
    argument for your ``StaticFileHandler``.
    To maximize the effectiveness of browser caching, this class supports
    versioned urls (by default using the argument ``?v=``).  If a version
    is given, we instruct the browser to cache this file indefinitely.
    `make_static_url` (also available as `RequestHandler.static_url`) can
    be used to construct a versioned url.
    This handler is intended primarily for use in development and light-duty
    file serving; for heavy traffic it will be more efficient to use
    a dedicated static file server (such as nginx or Apache).  We support
    the HTTP ``Accept-Ranges`` mechanism to return partial content (because
    some browsers require this functionality to be present to seek in
    HTML5 audio or video).
    **Subclassing notes**
    This class is designed to be extensible by subclassing, but because
    of the way static urls are generated with class methods rather than
    instance methods, the inheritance patterns are somewhat unusual.
    Be sure to use the ``@classmethod`` decorator when overriding a
    class method.  Instance methods may use the attributes ``self.path``
    ``self.absolute_path``, and ``self.modified``.
    Subclasses should only override methods discussed in this section;
    overriding other methods is error-prone.  Overriding
    ``StaticFileHandler.get`` is particularly problematic due to the
    tight coupling with ``compute_etag`` and other methods.
    To change the way static urls are generated (e.g. to match the behavior
    of another server or CDN), override `make_static_url`, `parse_url_path`,
    `get_cache_time`, and/or `get_version`.
    To replace all interaction with the filesystem (e.g. to serve
    static content from a database), override `get_content`,
    `get_content_size`, `get_modified_time`, `get_absolute_path`, and
    `validate_absolute_path`.
    .. versionchanged:: 3.1
       Many of the methods for subclasses were added in Tornado 3.1.

    '''


    CACHE_MAX_AGE = 86400 * 365 * 10  # 10 years

    _static_hashes = {}  # type: typing.Dict
    _lock = threading.Lock()  # protects _static_hashes

    def initialize(
            self,
            path,
            currentdir,
            templatepath,
            assetpath,
            executor,
            basedir,
            siteinfo,
            authnzerver,
            session_expiry,
            fernetkey,
            default_filename=None,
    ):

        self.root = path
        self.default_filename = default_filename
        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo
        self.authnzerver = authnzerver
        self.session_expiry = session_expiry
        self.fernetkey = fernetkey
        self.httpclient = AsyncHTTPClient(force_instance=True)


    @classmethod
    def reset(cls):
        with cls._lock:
            cls._static_hashes = {}

    def head(self, path):
        return self.get(path, include_body=False)

    @gen.coroutine
    def get(self, path, include_body=True):

        # if the path includes 'csvlc', check the ownership of the object
        if 'csvlc' in path:

            objectid = os.path.basename(path).replace('-csvlc.gz','')
            collection = os.path.dirname(path).replace('-','_')

            object_lookup = yield self.executor.submit(
                dbsearch.sqlite_fulltext_search,
                self.basedir,
                '"%s"' % objectid,
                lcclist=[collection],
                incoming_userid=self.current_user['user_id'],
                incoming_role=self.current_user['user_role']
            )

            object_access_check = len(object_lookup[collection]['result']) > 0
            LOGGER.info('object_access_check = %s' % object_access_check)

            if not object_access_check:
                raise HTTPError(
                    401, "You are not authorized to access the file: %s",
                    path
                )

        # if the path includes 'lightcurves', check the ownership of the dataset
        elif 'lightcurves' in path:

            setid = os.path.basename(
                path
            ).replace('lightcurves-','').replace('.zip','')

            dataset_access_check = yield self.executor.submit(
                datasets.sqlite_check_dataset_access,
                setid,
                'view',
                incoming_userid=self.current_user['user_id'],
                incoming_role=self.current_user['user_role'],
                database=os.path.join(self.basedir, 'lcc-datasets.sqlite')
            )
            LOGGER.info('dataset_access_check = %s' % dataset_access_check)

            if not dataset_access_check:
                raise HTTPError(
                    401, "You are not authorized to access the file: %s",
                    path
                )

        # if the path includes 'dataset', check the ownership of the dataset
        elif 'dataset' in path:

            setid = os.path.basename(
                path
            ).split('-')[1].replace('.pkl.gz','').replace('.csv','')

            dataset_access_check = yield self.executor.submit(
                datasets.sqlite_check_dataset_access,
                setid,
                'view',
                incoming_userid=self.current_user['user_id'],
                incoming_role=self.current_user['user_role'],
                database=os.path.join(self.basedir, 'lcc-datasets.sqlite')
            )
            LOGGER.info('dataset_access_check = %s' % dataset_access_check)

            if not dataset_access_check:
                raise HTTPError(
                    401, "You are not authorized to access the file: %s",
                    path
                )

        # log if everything works as expected
        LOGGER.info(
            'user: %s auth OK to download: %s' %
            (self.current_user['user_id'], path)
        )

        # Set up our path instance variables.
        self.path = self.parse_url_path(path)
        del path  # make sure we don't refer to path instead of self.path again
        absolute_path = self.get_absolute_path(self.root, self.path)
        self.absolute_path = self.validate_absolute_path(
            self.root, absolute_path)
        if self.absolute_path is None:
            return

        self.modified = self.get_modified_time()
        self.set_headers()

        if self.should_return_304():
            self.set_status(304)
            return

        request_range = None
        range_header = self.request.headers.get("Range")
        if range_header:
            # As per RFC 2616 14.16, if an invalid Range header is specified,
            # the request will be treated as if the header didn't exist.
            request_range = httputil._parse_request_range(range_header)

        size = self.get_content_size()
        if request_range:
            start, end = request_range
            if (start is not None and start >= size) or end == 0:
                # As per RFC 2616 14.35.1, a range is not satisfiable only: if
                # the first requested byte is equal to or greater than the
                # content, or when a suffix with length 0 is specified
                self.set_status(416)  # Range Not Satisfiable
                self.set_header("Content-Type", "text/plain")
                self.set_header("Content-Range", "bytes */%s" % (size, ))
                return
            if start is not None and start < 0:
                start += size
            if end is not None and end > size:
                # Clients sometimes blindly use a large range to limit their
                # download size; cap the endpoint at the actual file size.
                end = size
            # Note: only return HTTP 206 if less than the entire range has been
            # requested. Not only is this semantically correct, but Chrome
            # refuses to play audio if it gets an HTTP 206 in response to
            # ``Range: bytes=0-``.
            if size != (end or size) - (start or 0):
                self.set_status(206)  # Partial Content
                self.set_header("Content-Range",
                                httputil._get_content_range(start, end, size))
        else:
            start = end = None

        if start is not None and end is not None:
            content_length = end - start
        elif end is not None:
            content_length = end
        elif start is not None:
            content_length = size - start
        else:
            content_length = size
        self.set_header("Content-Length", content_length)

        if include_body:
            content = self.get_content(self.absolute_path, start, end)
            if isinstance(content, bytes):
                content = [content]
            for chunk in content:
                try:
                    self.write(chunk)
                    yield self.flush()
                except iostream.StreamClosedError:
                    return
        else:
            assert self.request.method == "HEAD"

    def compute_etag(self):
        """Sets the ``Etag`` header based on static url version.
        This allows efficient ``If-None-Match`` checks against cached
        versions, and sends the correct ``Etag`` for a partial response
        (i.e. the same ``Etag`` as the full file).
        .. versionadded:: 3.1
        """
        version_hash = self._get_cached_version(self.absolute_path)
        if not version_hash:
            return None
        return '"%s"' % (version_hash, )

    def set_headers(self):
        """Sets the content and caching headers on the response.
        .. versionadded:: 3.1
        """
        self.set_header("Accept-Ranges", "bytes")
        self.set_etag_header()

        if self.modified is not None:
            self.set_header("Last-Modified", self.modified)

        content_type = self.get_content_type()
        if content_type:
            self.set_header("Content-Type", content_type)

        cache_time = self.get_cache_time(self.path, self.modified,
                                         content_type)
        if cache_time > 0:
            self.set_header("Expires", datetime.utcnow() +
                            timedelta(seconds=cache_time))
            self.set_header("Cache-Control", "max-age=" + str(cache_time))

        self.set_extra_headers(self.path)

    def should_return_304(self):
        """Returns True if the headers indicate that we should return 304.
        .. versionadded:: 3.1
        """
        # If client sent If-None-Match, use it, ignore If-Modified-Since
        if self.request.headers.get('If-None-Match'):
            return self.check_etag_header()

        # Check the If-Modified-Since, and don't send the result if the
        # content has not been modified
        ims_value = self.request.headers.get("If-Modified-Since")
        if ims_value is not None:
            date_tuple = email.utils.parsedate(ims_value)
            if date_tuple is not None:
                if_since = datetime(*date_tuple[:6])
                if if_since >= self.modified:
                    return True

        return False

    @classmethod
    def get_absolute_path(cls, root, path):
        """Returns the absolute location of ``path`` relative to ``root``.
        ``root`` is the path configured for this `StaticFileHandler`
        (in most cases the ``static_path`` `Application` setting).
        This class method may be overridden in subclasses.  By default
        it returns a filesystem path, but other strings may be used
        as long as they are unique and understood by the subclass's
        overridden `get_content`.
        .. versionadded:: 3.1
        """
        abspath = os.path.abspath(os.path.join(root, path))
        return abspath

    def validate_absolute_path(self, root, absolute_path):
        """Validate and return the absolute path.
        ``root`` is the configured path for the `StaticFileHandler`,
        and ``path`` is the result of `get_absolute_path`
        This is an instance method called during request processing,
        so it may raise `HTTPError` or use methods like
        `RequestHandler.redirect` (return None after redirecting to
        halt further processing).  This is where 404 errors for missing files
        are generated.
        This method may modify the path before returning it, but note that
        any such modifications will not be understood by `make_static_url`.
        In instance methods, this method's result is available as
        ``self.absolute_path``.
        .. versionadded:: 3.1
        """
        # os.path.abspath strips a trailing /.
        # We must add it back to `root` so that we only match files
        # in a directory named `root` instead of files starting with
        # that prefix.
        root = os.path.abspath(root)
        if not root.endswith(os.path.sep):
            # abspath always removes a trailing slash, except when
            # root is '/'. This is an unusual case, but several projects
            # have independently discovered this technique to disable
            # Tornado's path validation and (hopefully) do their own,
            # so we need to support it.
            root += os.path.sep
        # The trailing slash also needs to be temporarily added back
        # the requested path so a request to root/ will match.
        if not (absolute_path + os.path.sep).startswith(root):
            raise HTTPError(403, "%s is not in root static directory",
                            self.path)
        if (os.path.isdir(absolute_path) and
                self.default_filename is not None):
            # need to look at the request.path here for when path is empty
            # but there is some prefix to the path that was already
            # trimmed by the routing
            if not self.request.path.endswith("/"):
                self.redirect(self.request.path + "/", permanent=True)
                return
            absolute_path = os.path.join(absolute_path, self.default_filename)
        if not os.path.exists(absolute_path):
            raise HTTPError(404)
        if not os.path.isfile(absolute_path):
            raise HTTPError(403, "%s is not a file", self.path)
        return absolute_path

    @classmethod
    def get_content(cls, abspath, start=None, end=None):
        """Retrieve the content of the requested resource which is located
        at the given absolute path.
        This class method may be overridden by subclasses.  Note that its
        signature is different from other overridable class methods
        (no ``settings`` argument); this is deliberate to ensure that
        ``abspath`` is able to stand on its own as a cache key.
        This method should either return a byte string or an iterator
        of byte strings.  The latter is preferred for large files
        as it helps reduce memory fragmentation.
        .. versionadded:: 3.1
        """
        with open(abspath, "rb") as file:
            if start is not None:
                file.seek(start)
            if end is not None:
                remaining = end - (start or 0)
            else:
                remaining = None
            while True:
                chunk_size = 64 * 1024
                if remaining is not None and remaining < chunk_size:
                    chunk_size = remaining
                chunk = file.read(chunk_size)
                if chunk:
                    if remaining is not None:
                        remaining -= len(chunk)
                    yield chunk
                else:
                    if remaining is not None:
                        assert remaining == 0
                    return

    @classmethod
    def get_content_version(cls, abspath):
        """Returns a version string for the resource at the given path.
        This class method may be overridden by subclasses.  The
        default implementation is a hash of the file's contents.
        .. versionadded:: 3.1
        """
        data = cls.get_content(abspath)
        hasher = hashlib.md5()
        if isinstance(data, bytes):
            hasher.update(data)
        else:
            for chunk in data:
                hasher.update(chunk)
        return hasher.hexdigest()

    def _stat(self):
        if not hasattr(self, '_stat_result'):
            self._stat_result = os.stat(self.absolute_path)
        return self._stat_result

    def get_content_size(self):
        """Retrieve the total size of the resource at the given path.
        This method may be overridden by subclasses.
        .. versionadded:: 3.1
        .. versionchanged:: 4.0
           This method is now always called, instead of only when
           partial results are requested.
        """
        stat_result = self._stat()
        return stat_result[stat.ST_SIZE]

    def get_modified_time(self):
        """Returns the time that ``self.absolute_path`` was last modified.
        May be overridden in subclasses.  Should return a `~datetime.datetime`
        object or None.
        .. versionadded:: 3.1
        """
        stat_result = self._stat()
        modified = datetime.utcfromtimestamp(
            stat_result[stat.ST_MTIME])
        return modified

    def get_content_type(self):
        """Returns the ``Content-Type`` header to be used for this request.
        .. versionadded:: 3.1
        """
        mime_type, encoding = mimetypes.guess_type(self.absolute_path)
        # per RFC 6713, use the appropriate type for a gzip compressed file
        if encoding == "gzip":
            return "application/gzip"
        # As of 2015-07-21 there is no bzip2 encoding defined at
        # http://www.iana.org/assignments/media-types/media-types.xhtml
        # So for that (and any other encoding), use octet-stream.
        elif encoding is not None:
            return "application/octet-stream"
        elif mime_type is not None:
            return mime_type
        # if mime_type not detected, use application/octet-stream
        else:
            return "application/octet-stream"

    def set_extra_headers(self, path):
        """For subclass to add extra headers to the response"""
        pass

    def get_cache_time(self, path, modified, mime_type):
        """Override to customize cache control behavior.
        Return a positive number of seconds to make the result
        cacheable for that amount of time or 0 to mark resource as
        cacheable for an unspecified amount of time (subject to
        browser heuristics).
        By default returns cache expiry of 10 years for resources requested
        with ``v`` argument.
        """
        return self.CACHE_MAX_AGE if "v" in self.request.arguments else 0

    @classmethod
    def make_static_url(cls, settings, path, include_version=True):
        """Constructs a versioned url for the given path.
        This method may be overridden in subclasses (but note that it
        is a class method rather than an instance method).  Subclasses
        are only required to implement the signature
        ``make_static_url(cls, settings, path)``; other keyword
        arguments may be passed through `~RequestHandler.static_url`
        but are not standard.
        ``settings`` is the `Application.settings` dictionary.  ``path``
        is the static path being requested.  The url returned should be
        relative to the current host.
        ``include_version`` determines whether the generated URL should
        include the query string containing the version hash of the
        file corresponding to the given ``path``.
        """
        url = settings.get('static_url_prefix', '/static/') + path
        if not include_version:
            return url

        version_hash = cls.get_version(settings, path)
        if not version_hash:
            return url

        return '%s?v=%s' % (url, version_hash)

    def parse_url_path(self, url_path):
        """Converts a static URL path into a filesystem path.
        ``url_path`` is the path component of the URL with
        ``static_url_prefix`` removed.  The return value should be
        filesystem path relative to ``static_path``.
        This is the inverse of `make_static_url`.
        """
        if os.path.sep != "/":
            url_path = url_path.replace("/", os.path.sep)
        return url_path

    @classmethod
    def get_version(cls, settings, path):
        """Generate the version string to be used in static URLs.
        ``settings`` is the `Application.settings` dictionary and ``path``
        is the relative location of the requested asset on the filesystem.
        The returned value should be a string, or ``None`` if no version
        could be determined.
        .. versionchanged:: 3.1
           This method was previously recommended for subclasses to override;
           `get_content_version` is now preferred as it allows the base
           class to handle caching of the result.
        """
        abs_path = cls.get_absolute_path(settings['static_path'], path)
        return cls._get_cached_version(abs_path)

    @classmethod
    def _get_cached_version(cls, abs_path):
        with cls._lock:
            hashes = cls._static_hashes
            if abs_path not in hashes:
                try:
                    hashes[abs_path] = cls.get_content_version(abs_path)
                except Exception:
                    gen_log.error("Could not open static file %r", abs_path)
                    hashes[abs_path] = None
            hsh = hashes.get(abs_path)
            if hsh:
                return hsh
        return None
