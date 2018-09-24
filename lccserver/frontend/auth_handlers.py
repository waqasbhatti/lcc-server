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
import secrets

######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
# - datetime
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

#############
## LOGGING ##
#############

# get a logger
LOGGER = logging.getLogger(__name__)


#####################
## TORNADO IMPORTS ##
#####################

import tornado.web
from tornado import gen
from tornado.escape import xhtml_escape


###################
## LOCAL IMPORTS ##
###################

from cryptography.fernet import InvalidToken
from lccserver import __version__
from lccserver.frontend.basehandler import BaseHandler


###########################
## VARIOUS AUTH HANDLERS ##
###########################

class LoginHandler(BaseHandler):
    '''
    This handles /users/login.

    '''


    @gen.coroutine
    def get(self):
        '''
        This shows the login form.

        '''

        if not self.current_user:
            self.redirect('/users/login')

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
            elif (current_user['user_role'] == 'anonymous'):

                self.render('login.html',
                            flash_messages=self.render_flash_messages(),
                            user_account_box=self.render_user_account_box(),
                            page_title="Sign in to your account",
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

            # anything else is probably the locked user, turn them away
            else:
                self.render_blocked_message()



    @gen.coroutine
    def post(self):
        '''
        This handles the POST of the login form.

        '''

        if not self.current_user:
            self.redirect('/users/login')

        # get the current user
        current_user = self.current_user

        # get the provided email and password
        try:

            email = xhtml_escape(self.get_argument('email'))
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
            yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry
            )

            LOGGER.error(' '.join(msgs))
            self.save_flash_messages(msgs, "warning")
            self.redirect('/users/login')

        # if login did succeed, redirect to the home page.
        else:

            # we have to get a new session with the same user ID (anon)
            yield self.new_session_token(
                user_id=resp['user_id'],
                expires_days=self.session_expiry
            )

            self.redirect('/')



class LogoutHandler(BaseHandler):
    '''
    This handles /user/logout.

    '''

    @gen.coroutine
    def post(self):
        '''
        This handles the POST request to /users/logout.

        '''

        if not self.current_user:
            self.redirect('/')

        current_user = self.current_user

        if (current_user and current_user['user_id'] not in (2,3) and
            current_user['is_active'] and current_user['email_verified']):

            # tell the authnzerver to delete this session
            ok, resp, msgs = yield self.authnzerver_request(
                'session-delete',
                {'session_token':current_user['session_token']}
            )

            yield self.new_session_token(
                user_id=2,
                expires_days=self.session_expiry
            )
            self.save_flash_messages(
                'You have signed out of your account. Have a great day!',
                "primary"
            )
            self.redirect('/')

        else:

            self.save_flash_messages(
                'You are not signed in, so you cannot sign out.',
                "warning"
            )
            self.redirect('/')



class NewUserHandler(BaseHandler):
    '''
    This handles /users/new.

    '''

    @gen.coroutine
    def get(self):
        '''
        This shows the sign-up page.

        '''

        if not self.current_user:
            self.redirect('/users/new')

        current_user = self.current_user

        # if we have a session token ready, then prepare to log in
        if current_user:

            # if we're already logged in, redirect to the index page
            if current_user['user_role'] in ('authenticated',
                                             'staff',
                                             'superuser'):

                LOGGER.warning(
                    'user %s is already logged in '
                    'but tried to sign up for a new account' %
                    current_user['user_id']
                )
                self.save_flash_messages(
                    "You have an LCC-Server account and are already logged in.",
                    "warning"
                )

                self.redirect('/')

            # if we're anonymous and we want to login, show the signup page
            elif (current_user['user_role'] == 'anonymous'):

                self.render('signup.html',
                            flash_messages=self.render_flash_messages(),
                            user_account_box=self.render_user_account_box(),
                            page_title="Sign up for an account",
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

            # anything else is probably the locked user, turn them away
            else:

                self.render_blocked_message()



    @gen.coroutine
    def post(self):
        '''This handles the POST request to /users/new.

        '''

        if not self.current_user:
            self.redirect('/users/new')

        current_user = self.current_user

        # get the provided email and password
        try:

            email = xhtml_escape(self.get_argument('email'))
            password = self.get_argument('password')

        except Exception as e:

            LOGGER.error('email and password are both required.')
            self.save_flash_messages(
                "An email address and strong password are both required.",
                "warning"
            )
            self.redirect('/users/new')

        # talk to the authnzerver to sign this user up
        ok, resp, msgs = yield self.authnzerver_request(
            'user-new',
            {'session_token':current_user['session_token'],
             'email':email,
             'password':password}
        )

        # FIXME: don't generate a new sesion token here yet
        # # generate a new anon session token in any case
        # new_session = yield self.new_session_token(
        #     user_id=2,
        #     expires_days=self.session_expiry,
        # )

        # if the sign up request is successful, send the email
        if ok:

            #
            # send the background request to authnzerver to send an email
            #

            # get the email info from site-info.json
            smtp_sender = self.siteinfo['email_sender']
            smtp_user = self.siteinfo['email_user']
            smtp_pass = self.siteinfo['email_pass']
            smtp_server = self.siteinfo['email_server']
            smtp_port = self.siteinfo['email_port']

            # generate a fernet verification token that is timestamped. we'll
            # give it 15 minutes to expire and decrypt it using:
            # self.ferneter.decrypt(token, ttl=15*60)
            fernet_verification_token = self.ferneter.encrypt(
                secrets.token_urlsafe(32).encode()
            )

            # get this LCC-Server's base URL
            lccserver_baseurl = '%s://%s' % (self.request.protocol,
                                             self.request.host)

            ok, resp, msgs = yield self.authnzerver_request(
                'user-signup-email',
                {'email_address':email,
                 'lccserver_baseurl':lccserver_baseurl,
                 'session_token':current_user['session_token'],
                 'smtp_server':smtp_server,
                 'smtp_sender':smtp_sender,
                 'smtp_user':smtp_user,
                 'smtp_pass':smtp_pass,
                 'smtp_server':smtp_server,
                 'smtp_port':smtp_port,
                 'fernet_verification_token':fernet_verification_token,
                 'created_info':resp}
            )

            if ok:

                self.save_flash_messages(
                    "Thanks for signing up! We've sent a verification "
                    "request to your email address. "
                    "Please complete user registration by "
                    "entering the code you received.",
                    "primary"
                )
                self.redirect('/users/verify')

            # FIXME: if the backend breaks here, the user is left in limbo
            # what to do?
            else:

                LOGGER.error('failed to send an email. %r' % msgs)
                self.save_flash_messages(msgs,'warning')
                self.redirect('/users/new')


        # if the sign up request fails, tell the user we've sent an email but do
        # nothing
        else:

            self.save_flash_messages(
                "Thanks for signing up! We've sent a verification "
                "request to your email address. "
                "Please complete user registration by "
                "entering the code you received.",
                "primary"
            )
            self.redirect('/users/verify')



class VerifyUserHandler(BaseHandler):
    '''
    This handles /users/verify.

    '''

    @gen.coroutine
    def get(self):
        '''
        This shows the user verification form.

        '''

        if not self.current_user:
            self.redirect('/users/verify')

        current_user = self.current_user

        # only proceed to verification if the user is not logged in as an actual
        # user
        if current_user and current_user['user_role'] == 'anonymous':

            # we'll render the verification form.
            self.render('verify.html',
                        email_address=current_user['email'],
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),
                        page_title="Verify your sign up request",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # if the user is already logged in, then redirect them back to their
        # home page
        else:

            # tell the user that their verification request is invalid
            # and redirect them to the login page
            self.save_flash_messages(
                "You have an account and are already logged in.",
                "warning"
            )
            self.redirect('/users/home')


    @gen.coroutine
    def post(self):
        '''This handles POST of the user verification form.

        '''

        if not self.current_user:
            self.redirect('/users/verify')

        current_user = self.current_user

        try:

            email = xhtml_escape(self.get_argument('email'))
            password = self.get_argument('password')
            verification = xhtml_escape(self.get_argument('verificationcode'))

            # check the verification code to see if it's valid
            self.ferneter.decrypt(verification.encode(), ttl=15*60)

            LOGGER.info('%s: decrypted verification token OK and unexpired' %
                        email)

            # if all looks OK, verify the email address
            verified_ok, resp, msgs = yield self.authnzerver_request(
                'user-verify-email',
                {'email':email},
            )

            # if we successfully set the user is_active = True, then we'll log
            # them in by checking the provided email address and password
            if verified_ok:

                login_ok, resp, msgs = yield self.authnzerver_request(
                    'user-login',
                    {'session_token':current_user['session_token'],
                     'email':email,
                     'password':password}
                )

                if login_ok:

                    # this is saved so we can change the ownership of the anon
                    # user's current datasets.
                    current_session_token = self.current_user['session_token']

                    # FIXME: change the ownership for all of the datasets that
                    # the user made with their current session_token
                    LOGGER.warning(
                        'changing ownership of datasets made '
                        'by anonymous user with session_token to '
                        'their new user_id = %s' % current_session_token
                    )

                    # generate a new session token matching the user_id
                    # when we login successfully
                    yield self.new_session_token(
                        user_id=resp['user_id'],
                        expires_days=self.session_expiry
                    )
                    self.save_flash_messages(
                        "Thanks for verifying your email address! "
                        "Your account is fully activated and "
                        "you're now logged in.",
                        "primary"
                    )

                    # redirect to their home page
                    self.redirect('/users/home')

                else:

                    yield self.new_session_token()

                    self.save_flash_messages(
                        "Sorry, there was a problem verifying "
                        "your account sign up. "
                        "Please try again or contact us if this doesn't work.",
                        "warning"
                    )
                    self.redirect('/users/verify')

            else:

                yield self.new_session_token()

                self.save_flash_messages(
                    "Sorry, there was a problem verifying "
                    "your account sign up. "
                    "Please try again or contact us if this doesn't work.",
                    "warning"
                )
                self.redirect('/users/verify')


        except InvalidToken as e:

            yield self.new_session_token()

            self.save_flash_messages(
                "Sorry, there was a problem verifying your account sign up. "
                "Please try again or contact us if this doesn't work.",
                "warning",
            )
            LOGGER.exception(
                'verification token did not match for account: %s' %
                email
            )

            self.redirect('/users/verify')

        except Exception as e:

            yield self.new_session_token()

            LOGGER.exception(
                'could not verify user sign up: %s' % email
            )

            self.save_flash_messages(
                "Sorry, there was a problem verifying your account sign up. "
                "Please try again or contact us if this doesn't work.",
                "warning"
            )
            self.redirect('/users/verify')



class ForgotPassStep1Handler(BaseHandler):
    '''
    This handles /users/forgot-password-step1.

    '''

    @gen.coroutine
    def get(self):
        '''
        This shows the email address request form for forgotten passwords.

        '''

        if not self.current_user:
            self.redirect('/users/forgot-password-step1')

        current_user = self.current_user

        # only proceed to password reset if the user is anonymous
        if (current_user and current_user['user_role'] == 'anonymous'):

            # we'll render the verification form.
            self.render('passreset-step1.html',
                        email_address=current_user['email'],
                        user_account_box=self.render_user_account_box(),
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

        TODO: finish this

        '''

        if not self.current_user:
            self.redirect('/users/forgot-password-step1')

        current_user = self.current_user

        # only proceed to password reset if the user is anonymous
        if (current_user and current_user['user_role'] == 'anonymous'):

            # get the user's email
            email_address = self.get_argument('email', default=None)

            if not email_address or len(email_address.strip()) == 0:

                self.save_flash_messages(
                    "No email address was provided or we couldn't validate it. "
                    "Please try again.",
                    'warning'
                )
                self.redirect('/users/forgot-password-step1')

            else:

                email_address = xhtml_escape(email_address)





class ForgotPassStep2Handler(BaseHandler):
    '''
    This handles /users/forgot-password-step2.

    '''

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
                        user_account_box=self.render_user_account_box(),
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
    '''
    This handles /users/password-change.

    '''

    @gen.coroutine
    def get(self):
        '''This handles password change request from a logged-in only user.

        When we navigate to this page, we'll immediately send a verification
        code to the user's email address. They must enter it into the form we
        show to continue.

        The verification email request to the authnzerver should contain an IP
        address and browser header so we can include this info in the
        verification request (and geolocate the IP address if possible).

        TODO: finish this.

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            current_user['user_id'] not in (2,3) and
            current_user['is_active'] and
            current_user['email_verified']):

            # TODO: flash message to inform the user we've sent a verification
            # code to their email address.

            #
            # TODO: actually send the email here
            #

            # then, we'll render the verification form.
            self.render('passchange.html',
                        email_address=current_user['email'],
                        user_account_box=self.render_user_account_box(),
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
    This handles /users/home.

    '''

    @gen.coroutine
    @tornado.web.authenticated
    def get(self):
        '''This just shows the prefs and user home page.

        Should also show all of the user's recent datasets (along with the
        search queries).

        '''

        current_user = self.current_user

        if (current_user and
            current_user['is_active'] and
            current_user['user_role'] in ('authenticated','staff','superuser')):

            self.render(
                'userhome.html',
                current_user=current_user,
                user_account_box=self.render_user_account_box(),
                flash_messages=self.render_flash_messages(),
                page_title="User home page",
                lccserver_version=__version__,
                siteinfo=self.siteinfo,
                cookie_expires_days=self.session_expiry,
                cookie_secure='true' if self.csecure else 'false'
            )

        else:
            self.save_flash_messages(
                "Please sign in to proceed.",
                "warning"
            )
            self.redirect('/users/login')


    @gen.coroutine
    @tornado.web.authenticated
    def post(self):
        '''This is an AJAX endpoint for any prefs changes and API key generation
        requests.

        TODO: define some useful options.

        TODO: finish this.

        '''

        current_user = self.current_user

        if current_user:
            self.write(current_user)
        else:
            self.write('No cookie set yet.')
        self.finish()
