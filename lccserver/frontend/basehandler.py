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




###########################
## VARIOUS TEST HANDLERS ##
###########################

class IndexHandler(BaseHandler):
    '''
    This is a test handler inheriting from the base handler to provide auth.

    '''


    @gen.coroutine
    def get(self):
        '''This just checks if the cookie was set correctly and sessions work.

        FIXME: this will eventually be the usual LCC-Server page but with some
        bits added in for login/logout etc at the top right.

        '''

        current_user = self.current_user

        if current_user:
            self.redirect('/users/home')
        else:
            self.redirect('/users/login')



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
                            page_title="Sign in to your account",
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

            # anything else is probably the locked user, turn them away
            else:

                self.render_blocked_message()

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
            self.save_flash_messages(
                "A valid email address and password are both required.",
                "warning"
            )
            self.redirect('/users/login')

        # talk to the authnzerver to login this user

        reqtype = 'user-login'
        reqbody = {
            'session_token': current_user['session_token'],
            'email':email,
            'password':password
        }

        ok, resp, msgs = yield self.authnzerver_request(
            reqtype, reqbody
        )

        # if login did not succeed, then set the flash messages and redirect
        # back to /users/login
        if not ok:

            # we have to get a new session with the same user ID (anon)
            new_session = yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry
            )

            LOGGER.error(' '.join(msgs))
            self.save_flash_messages(msgs, "warning")
            self.redirect('/users/login')

        # if login did succeed, redirect to the home page.
        else:

            # we have to get a new session with the same user ID (anon)
            new_session = yield self.new_session_token(
                user_id=resp['user_id'],
                expires_days=self.session_expiry
            )

            self.redirect('/users/home')




class LogoutHandler(BaseHandler):

    @gen.coroutine
    def post(self):
        '''
        This handles user logout by talking to the authnzerver.

        '''

        current_user = self.current_user

        if (current_user and current_user['user_id'] not in (2,3) and
            current_user['is_active'] and current_user['email_verified']):

            # tell the authnzerver to delete this session
            ok, resp, msgs = yield self.authnzerver_request(
                'session-delete',
                {'session_token':current_user['session_token']}
            )

            new_session = yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry
            )
            self.save_flash_messages(
                'You have signed out of your account. Have a great day!',
                "primary"
            )
            self.redirect('/users/login')

        else:

            self.save_flash_messages(
                'You are not signed in, so you cannot sign out.',
                "warning"
            )
            self.redirect('/users/login')



class NewUserHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This shows the sign-up page.

        '''

        current_user = self.current_user

        # if we have a session token ready, then prepare to log in
        if current_user:

            # if we're already logged in, redirect to the index page
            # FIXME: in the future, this may redirect to /users/home
            if ((current_user['user_role'] in
                 ('authenticated', 'staff', 'superuser')) and
                (current_user['user_id'] not in (2,3))):

                LOGGER.warning('user is already logged in')
                self.redirect('/')

            # if we're anonymous and we want to login, show the login page
            elif ((current_user['user_role'] == 'anonymous') and
                  (current_user['user_id'] == 2)):

                self.render('signup.html',
                            flash_messages=self.render_flash_messages(),
                            page_title="Sign up for an account",
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

            # anything else is probably the locked user, turn them away
            else:

                self.render_blocked_message()

        # redirect us to the login page again so the anonymous session cookie
        # gets set correctly. FIXME: does this make sense?
        else:
            self.redirect('/')


    @gen.coroutine
    def post(self):
        '''
        This handles user sign-up by talking to the authnzerver.

        The verification email request to the authnzerver should contain an IP
        address and browser header so we can include this info in the
        verification request (and geolocate the IP address if possible).

        '''

        current_user = self.current_user

        # get the provided email and password
        try:

            email = self.get_argument('email')
            password = self.get_argument('password')

        except Exception as e:

            LOGGER.error('email and password are both required.')
            self.save_flash_messages(
                "An email address and strong password are both required.",
                "warning"
            )
            self.redirect('/users/new')

        # talk to the authnzerver to login this user
        ok, resp, msgs = yield self.authnzerver_request(
            'user-new',
            {'session_token':current_user['session_token'],
             'email':email,
             'password':password}
        )

        if not ok:

            new_session = yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry,
            )
            self.save_flash_messages(msgs,"warning")
            self.redirect('/users/new')

        # if we succeeded, we need to go through the user verification process
        else:

            # FIXME: if the sign up succeeded, we will redirect to
            # /users/verify, which will just tell the user that we sent a
            # verification email to them with a sign up code (this will be
            # generated using Fernet or itsdangerous). On /users/verify, we'll
            # have a form to fill in the email address (this will be a readonly
            # item), password, and the token. Once the form POSTs OK, we will
            # log the user in.

            # we generate a new session token with the user's new user ID, and
            # set the lccserver_session cookie. We then redirect to
            # /users/verify, which will read the cookie, match the session token
            # with an existing session, look up the user info. If it sees that
            # the user is not active and verification email was sent, it will
            # set up the form to ask the user for their verification code
            # (following the procedure above)
            new_session = yield self.new_session_token(
                user_id=resp['user_id'],
                expires_days=self.session_expiry,
            )

            # FIXME: we need a background request to authnzerver to make it send
            # a verification email
            self.save_flash_messages(
                "Thanks for signing up! We've sent a verification "
                "request to your email address. "
                "Please complete user registration by "
                "entering the code you received.",
                "primary"
            )

            self.redirect('/users/verify')



class VerifyUserHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This handles user email verification by talking to the authnzerver.

        '''

        current_user = self.current_user

        # only proceed to verification if this is a valid verification request
        if (current_user and
            current_user['user_id'] > 3 and
            not current_user['is_active'] and
            not current_user['email_verified']):

            # we'll render the verification form.
            self.render('verify.html',
                        email_address=current_user['email'],
                        flash_messages=self.render_flash_messages(),
                        page_title="Verify your sign up request",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        else:

            # tell the user that their verification request is invalid
            # and redirect them to the login page
            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')


    @gen.coroutine
    def post(self):
        '''This handles submission of the user verification form.

        We first check if the verification code is valid and unexpired. If it
        is, we'll check the user's email and password. If they're also valid,
        we'll log them in.

        If the verification is not valid, redirect to /users/verify again with a
        new session token with the same user_id and tell the user that their
        code is not valid.

        TODO: finish this

        '''



class ForgotPassStep1Handler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This handles user forgotten passwords.

        '''

        current_user = self.current_user

        # only proceed to password reset if the user is anonymous
        if (current_user and current_user['user_id'] == 2):

            # we'll render the verification form.
            self.render('passreset-step1.html',
                        email_address=current_user['email'],
                        flash_messages=self.render_flash_messages(),
                        page_title="Reset your password",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # otherwise, tell the user that their password forgotten request is
        # invalid and redirect them to the login page
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



    @gen.coroutine
    def post(self):
        '''This handles submission of the password reset step 1 form.

        Fires the request to authnzerver to send a verification email. Then
        redirects to step 2 of the form.

        The verification email request to the authnzerver should contain an IP
        address and browser header so we can include this info in the
        verification request (and geolocate the IP address if possible).

        TODO: finish this

        '''



class ForgotPassStep2Handler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''
        This shows the choose new password form.

        '''

        current_user = self.current_user

        # only proceed to password reset if the user is anonymous
        if (current_user and current_user['user_id'] == 2):

            # we'll render the verification form.
            self.render('passreset-step1.html',
                        email_address=current_user['email'],
                        flash_messages=self.render_flash_messages(),
                        page_title="Reset your password",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # otherwise, tell the user that their password forgotten request is
        # invalid and redirect them to the login page
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



    @gen.coroutine
    def post(self):
        '''This handles submission of the password reset step 2 form.

        If the authnzerver accepts the new password, redirects to the
        /users/login page.

        TODO: finish this

        '''


class ChangePassHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        '''This handles password change request from a logged-in only user.

        When we navigate to this page, we'll immediately send a verification
        code to the user's email address. They must enter it into the form we
        show to continue.

        The verification email request to the authnzerver should contain an IP
        address and browser header so we can include this info in the
        verification request (and geolocate the IP address if possible).

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            current_user['user_id'] not in (2,3) and
            current_user['is_active'] and
            current_user['email_verified']):

            # we'll render the verification form.
            self.render('passchange.html',
                        email_address=current_user['email'],
                        flash_messages=self.render_flash_messages(),
                        page_title="Change your password",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # otherwise, tell the user that their password forgotten request is
        # invalid and redirect them to the login page
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



    @gen.coroutine
    def post(self):
        '''This handles submission of the password change request form.

        If the authnzerver accepts the new password, redirects to the
        /users/home or / page.

        FIXME: or should this log the user out and force them to sign back in?
        TODO: finish this

        '''




class UserHomeHandler(BaseHandler):

    '''
    This shows the user's home page and handles any preferences changes.

    '''

    @gen.coroutine
    @tornado.web.authenticated
    def get(self):
        '''
        This just shows the prefs and user home page.

        '''

        current_user = self.current_user

        if (current_user and
            current_user['is_active'] and
            current_user['user_id'] not in (2,3)):

            self.render(
                'userhome.html',
                current_user=current_user,
                flash_messages=self.render_flash_messages(),
                page_title="User home page",
                lccserver_version=__version__,
                siteinfo=self.siteinfo
            )

        else:
            self.save_flash_messages(
                "Please sign in to proceed.",
                "primary"
            )

            self.redirect('/users/login')


    @gen.coroutine
    @tornado.web.authenticated
    def post(self):
        '''This is an AJAX endpoint for any prefs changes and API key generation
        requests.

        '''

        current_user = self.current_user

        if current_user:
            self.write(current_user)
        else:
            self.write('No cookie set yet.')
        self.finish()
