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
from datetime import datetime

from cryptography.fernet import Fernet, InvalidToken

######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
# - datetime
import json
import numpy as np

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
from tornado.httpclient import AsyncHTTPClient


###################
## LOCAL IMPORTS ##
###################

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
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

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

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()


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
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

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
            server_baseurl = '%s://%s' % (self.request.protocol,
                                          self.request.host)

            ok, resp, msgs = yield self.authnzerver_request(
                'user-signup-email',
                {'email_address':email,
                 'server_baseurl':server_baseurl,
                 'server_name':'LCC-Server',
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
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

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
            self.redirect('/')

        current_user = self.current_user

        # only proceed to password reset if the user is anonymous
        if (current_user and current_user['user_role'] == 'anonymous'):

            # we'll render the verification form.
            self.render('passreset-step1.html',
                        email_address=current_user['email'],
                        user_account_box=self.render_user_account_box(),
                        flash_messages=self.render_flash_messages(),
                        page_title="Reset your password - Step 1",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # otherwise, tell the user that their password forgotten request is
        # invalid and redirect them to the login page
        else:

            self.save_flash_messages(
                "You are currently logged in. If you've forgotten your "
                "password, log out, then come back to the "
                "forgot password form.",
                "primary"
            )
            self.redirect('/users/home')


    @gen.coroutine
    def post(self):
        '''This handles submission of the password reset step 1 form.

        Fires the request to authnzerver to send a verification email. Then
        redirects to step 2 of the form.

        '''
        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()


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

                try:

                    email_address = xhtml_escape(email_address)

                    # get the email info from site-info.json
                    smtp_sender = self.siteinfo['email_sender']
                    smtp_user = self.siteinfo['email_user']
                    smtp_pass = self.siteinfo['email_pass']
                    smtp_server = self.siteinfo['email_server']
                    smtp_port = self.siteinfo['email_port']

                    # generate a fernet verification token that is
                    # timestamped. we'll give it 15 minutes to expire and
                    # decrypt it using: self.ferneter.decrypt(token, ttl=15*60)
                    fernet_verification_token = self.ferneter.encrypt(
                        secrets.token_urlsafe(32).encode()
                    )

                    # get this LCC-Server's base URL
                    server_baseurl = '%s://%s' % (self.request.protocol,
                                                  self.request.host)

                    ok, resp, msgs = yield self.authnzerver_request(
                        'user-forgotpass-email',
                        {'email_address':email_address,
                         'fernet_verification_token':fernet_verification_token,
                         'server_baseurl':server_baseurl,
                         'server_name':'LCC-Server',
                         'session_token':current_user['session_token'],
                         'smtp_server':smtp_server,
                         'smtp_sender':smtp_sender,
                         'smtp_user':smtp_user,
                         'smtp_pass':smtp_pass,
                         'smtp_server':smtp_server,
                         'smtp_port':smtp_port}
                    )

                    if ok:

                        self.save_flash_messages(
                            "We've sent a verification token "
                            "to your email address on file. "
                            "Use that to fill in this form.",
                            'warning'
                        )
                        LOGGER.info('email sent to %s for forgot password' %
                                    email_address)
                        self.redirect('/users/forgot-password-step2')

                    # if the email send step fails, show the next step anyway
                    else:

                        LOGGER.error(
                            'email could not be sent '
                            'to %s for forgot password' %
                            email_address
                        )

                        self.save_flash_messages(
                            "We've sent a verification token "
                            "to your email address on file. "
                            "Use that to fill in this form.",
                            'warning'
                        )
                        self.redirect('/users/forgot-password-step2')

                except Exception as e:

                    self.save_flash_messages(
                        "An email address is required.",
                        "warning"
                    )
                    self.redirect('/users/forgot-password-step1')

        else:

            self.save_flash_messages(
                "You are currently logged in. If you've forgotten your "
                "password, log out, then come back to the "
                "forgot password form.",
                "primary"
            )
            self.redirect('/users/home')



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
        if (current_user and current_user['user_role'] == 'anonymous'):

            # we'll render the verification form.
            self.render('passreset-step2.html',
                        user_account_box=self.render_user_account_box(),
                        flash_messages=self.render_flash_messages(),
                        page_title="Reset your password - Step 2",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # otherwise, tell the user that their password forgotten request is
        # invalid and redirect them to the login page
        else:

            self.save_flash_messages(
                "You are currently logged in. If you've forgotten your "
                "password, log out, then come back to the "
                "forgot password form.",
                "primary"
            )
            self.redirect('/users/home')



    @gen.coroutine
    def post(self):
        '''This handles submission of the password reset step 2 form.

        If the authnzerver accepts the new password, redirects to the
        /users/login page.

        '''
        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

        try:

            verification = self.get_argument('verificationcode')
            email_address = xhtml_escape(self.get_argument('email'))
            new_password = self.get_argument('password')
            session_token = self.current_user['session_token']

            # check the verification code to see if it's valid
            self.ferneter.decrypt(verification.encode(), ttl=15*60)
            LOGGER.info('%s: decrypted verification token OK and unexpired' %
                        email_address)

            # check the new password by sending an authnzerver request
            ok, resp, msgs = yield self.authnzerver_request(
                'user-resetpass',
                {'email_address':email_address,
                 'new_password':new_password,
                 'session_token':session_token}
            )

            # if request OK and password is validated, redirect to the login
            # form
            if ok:

                self.save_flash_messages(
                    "Your password was successfully updated. Please sign in "
                    "to continue.",
                    'primary'
                )

                self.redirect('/users/login')

            else:

                LOGGER.error(msgs)

                self.save_flash_messages(
                    ["We couldn't validate your password reset request. ",
                     ("You may not have entered the correct "
                      "email for this account "
                      "and an acceptable password. "
                      "Passwords must be at "
                      "least 12 characters long. "),
                     ("The verification token you received was valid for "
                      "15 minutes only, so it may have expired.")],
                    "warning"
                )
                self.redirect('/users/forgot-password-step2')

        except Exception as e:

            self.save_flash_messages(
                ["We couldn't validate your password reset request. ",
                 ("You may not have entered the correct email for this account "
                  "and an acceptable password. Passwords must be at "
                  "least 12 characters long. "),
                 ("The verification token you received was valid for "
                  "15 minutes only, so it may have expired.")],
                "warning"
            )
            self.redirect('/users/forgot-password-step2')



class ChangePassHandler(BaseHandler):
    '''
    This handles /users/password-change.

    '''

    @gen.coroutine
    def get(self):
        '''This handles password change request from a logged-in only user.

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            (current_user['user_role'] in
             ('authenticated','staff','superuser')) and
            current_user['is_active'] and
            current_user['email_verified']):

            # then, we'll render the verification form.
            self.render('passchange.html',
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
        /users/home page.

        '''

        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()


        if ((self.current_user) and
            (self.current_user['is_active']) and
            (self.current_user['user_role'] in ('authenticated',
                                                'superuser',
                                                'staff')) and
            (self.current_user['email_verified'])):

            try:

                email_address = xhtml_escape(self.current_user['email'])
                current_password = self.get_argument('currpassword')
                new_password = self.get_argument('newpassword')

                change_ok, resp, msgs = yield self.authnzerver_request(
                    'user-changepass',
                    {'user_id':self.current_user['user_id'],
                     'email': email_address,
                     'current_password':current_password,
                     'new_password':new_password}
                )

                if change_ok:

                    self.save_flash_messages(
                        "Your password has been changed successfully.",
                        'primary',
                    )
                    self.redirect('/users/home')

                else:

                    self.save_flash_messages(
                        msgs,
                        'warning',
                    )
                    self.redirect('/users/password-change')


            except Exception as e:

                self.save_flash_messages(
                    "We could not validate the password change form. "
                    "All fields are required.",
                    "warning"
                )
                self.redirect('/users/password-change')

        # unknown users get sent back to /
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



class ChangeEmailStep1Handler(BaseHandler):
    '''This handles /users/email-change-step1.

    The first step asks the user to type in their current email address, their
    password, and their new email address. We will then send emails to both
    their old and new addresses with different verification tokens if the
    current email and password match to what's on file. If the old email and
    password don't match what's on file, we'll show an error.

    The second step asks for both of these verification tokens. If they match
    the ones we sent out, we'll change the user's email address and redirect
    them to /users/home.

    FIXME: this will involve storing new and old emails and the verification
    tokens sent to either of them in the session extra_info_json column. Need to
    write an authnzerver function for that and use it here. Step 2 can then look
    up the session's info, grab the verification tokens from there, then wipe
    them. Will write a function that does `update_session_extra_info()` or
    some-such and attach it to the `session-edit` authnzerver request.

    '''

    @gen.coroutine
    def get(self):
        '''This handles an email address change request
        from a logged-in only user.

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            (current_user['user_role'] in
             ('authenticated','staff','superuser')) and
            current_user['is_active'] and
            current_user['email_verified']):

            # then, we'll render the verification form.
            self.render('emailchange-step1.html',
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
        '''This handles submission of the email change step-1 form.

        '''

        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()


        if ((self.current_user) and
            (self.current_user['is_active']) and
            (self.current_user['user_role'] in ('authenticated',
                                                'superuser',
                                                'staff')) and
            (self.current_user['email_verified'])):

            try:

                curr_email = xhtml_escape(self.get_argument('curremail'))
                password = self.get_argument('password')
                new_email = self.get_argument('newemail')

                verify_ok, resp, msgs = yield self.authnzerver_request(
                    'user-changeemail',
                    {'user_id':self.current_user['user_id'],
                     'current_email': curr_email,
                     'password':password,
                     'new_email':new_email}
                )

                if verify_ok:

                    self.save_flash_messages(
                        "We have sent emails to your current AND "
                        "new email addresses containing verification tokens. "
                        "Please enter these below to verify your email "
                        "address change.",
                        'primary',
                    )
                    self.redirect('/users/email-change-step2')

                else:

                    self.save_flash_messages(
                        msgs,
                        'warning',
                    )
                    self.redirect('/users/email-change-step1')


            except Exception as e:

                self.save_flash_messages(
                    "We could not validate the email change request form. "
                    "All fields are required.",
                    "warning"
                )
                self.redirect('/users/email-change-step1')

        # unknown users get sent back to /
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



class ChangeEmailStep2Handler(BaseHandler):
    '''
    This handles /users/email-change-step2.

    '''

    @gen.coroutine
    def get(self):
        '''This handles an email address change request
        from a logged-in only user.

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            (current_user['user_role'] in
             ('authenticated','staff','superuser')) and
            current_user['is_active'] and
            current_user['email_verified']):


            # FIXME: get the user's new and old emails from the session
            # extra_info_json column and render them to the form.
            old_email_address = 'blah'
            new_email_address = 'blah'

            # then, we'll render the verification form.
            self.render('emailchange-step2.html',
                        old_email_address=old_email_address,
                        new_email_address=new_email_address,
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
        '''This handles submission of the email change step-2 form.


        '''

        if not self.current_user:
            self.redirect('/')

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()


        if ((self.current_user) and
            (self.current_user['is_active']) and
            (self.current_user['user_role'] in ('authenticated',
                                                'superuser',
                                                'staff')) and
            (self.current_user['email_verified'])):

            try:

                old_email_token = xhtml_escape(
                    self.get_argument('oldemailtoken')
                )
                new_email_token = xhtml_escape(
                    self.get_argument('newemailtoken')
                )

                # ask authnzerver to check the verification tokens associated
                # with this session and the two emails. this will wipe the
                # tokens if it succeeds. if it doesn't succeed, the tokens
                # expire in 15 minutes anyway.
                change_ok, resp, msgs = yield self.authnzerver_request(
                    'user-verifyemailchange',
                    {'user_id':self.current_user['user_id'],
                     'old_email_token': old_email_token,
                     'new_email_token': new_email_token,
                     'session_token':self.current_user['session_token']}
                )

                if change_ok:

                    self.save_flash_messages(
                        "Your email address has been changed successfully.",
                        'primary',
                    )
                    self.redirect('/users/home')

                else:

                    self.save_flash_messages(
                        msgs,
                        'warning',
                    )
                    self.redirect('/users/email-change-step2')


            except Exception as e:

                self.save_flash_messages(
                    "We could not validate the email change form. "
                    "All fields are required.",
                    "warning"
                )
                self.redirect('/users/email-change-step1')

        # unknown users get sent back to /
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



class DeleteUserHandler(BaseHandler):
    '''
    This handles /users/delete.

    '''

    @gen.coroutine
    def get(self):
        '''This handles a user delete.

        Only shown if the user is logged in.

        Must enter email address and password.

        '''

        current_user = self.current_user

        # only proceed to password change if the user is active and logged in
        if (current_user and
            (current_user['user_role'] in
             ('authenticated','staff')) and
            current_user['is_active'] and
            current_user['email_verified']):

            # then, we'll render the verification form.
            self.render('delete.html',
                        user_account_box=self.render_user_account_box(),
                        flash_messages=self.render_flash_messages(),
                        page_title="Delete your account",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)

        # superuser accounts cannot be deleted from the web interface
        elif (current_user and
              (current_user['user_role'] == 'superuser') and
              current_user['is_active'] and
              current_user['email_verified']):

            self.save_flash_messages(
                "You have a superuser account. This cannot be deleted "
                "from the web interface. Use the CLI instead.",
                'warning'
            )
            self.redirect('/users/home')

        # otherwise, tell the user that their delete request is invalid
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
        '''This handles submission of the delete user form.

        - check if the user signing in is valid and password is valid
        - check if the user being deleted is the one that submitted the form

        - delete the user from the users table (this should kill their sessions
          too)
        - delete all lccserver_* cookies
        - redirect to /

        '''
        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

        if ((self.current_user) and
            (self.current_user['is_active']) and
            (self.current_user['user_role'] in ('authenticated',
                                                'superuser',
                                                'staff')) and
            (self.current_user['email_verified'])):

            try:

                email_address = xhtml_escape(self.get_argument('email'))
                password = self.get_argument('password')

                if email_address != self.current_user['email']:

                    self.save_flash_messages(
                        "We could not verify your email address "
                        "or password.",
                        'warning',
                    )
                    self.redirect('/users/delete')

                else:

                    delete_ok, resp, msgs = yield self.authnzerver_request(
                        'user-delete',
                        {'user_id':self.current_user['user_id'],
                         'email': email_address,
                         'password': password},
                    )

                    if delete_ok:

                        # make double-sure the current session is dead
                        sessdel_ok, resp, msgs = yield self.authnzerver_request(
                            'session-delete',
                            {'session_token':(
                                self.current_user['session_token']
                            )},
                        )

                        self.save_flash_messages(
                            "Your account has been deleted successfully.",
                            'danger',
                        )

                        self.clear_all_cookies()
                        self.redirect('/')

                    else:

                        self.save_flash_messages(
                            msgs,
                            'warning',
                        )
                        self.redirect('/users/delete')

            except Exception as e:

                self.save_flash_messages(
                    "We could not validate the password change form. "
                    "All fields are required.",
                    "warning"
                )
                self.redirect('/users/delete')

        # unknown users get sent back to /
        else:

            self.save_flash_messages(
                "Sign in with your existing account credentials. "
                "If you do not have a user account, "
                "please <a href=\"/users/new\">sign up</a>.",
                "primary"
            )
            self.redirect('/users/login')



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
                page_title="User home",
                lccserver_version=__version__,
                siteinfo=self.siteinfo,
                cookie_expires_days=self.session_expiry,
                cookie_secure='true' if self.csecure else 'false',
            )

        else:

            self.save_flash_messages(
                "Please sign in to proceed.",
                "warning"
            )
            self.redirect('/users/login')


    @gen.coroutine
    def post(self):
        '''This handles submission of the edit user form on the userhome page.

        '''
        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'session')):

            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()

        if ((self.current_user) and
            (self.current_user['is_active']) and
            (self.current_user['user_role'] in ('authenticated',
                                                'superuser',
                                                'staff')) and
            (self.current_user['email_verified'])):

            current_user_id = self.current_user['user_id']
            current_user_role = self.current_user['user_role']
            current_sessiontoken = self.current_user['session_token']

            try:

                # TODO: add other editeable things here as we make them
                # available
                updated_fullname = xhtml_escape(
                    self.get_argument('updated_fullname')
                )

                updatedict = {
                    'full_name':updated_fullname,
                }

                reqtype = 'user-edit'
                target_userid = current_user_id
                reqbody = {
                    'session_token': current_sessiontoken,
                    'user_id':current_user_id,
                    'user_role':current_user_role,
                    'target_userid':target_userid,
                    'update_dict':updatedict
                }

                ok, resp, msgs = yield self.authnzerver_request(
                    reqtype, reqbody
                )

                # if edit did not succeed, complain
                if not ok:

                    LOGGER.warning('edit_user: %r initiated by '
                                   'user_id: %s failed for '
                                   'user_id: %s' % (list(updatedict.keys()),
                                                    current_user_id,
                                                    target_userid))
                    LOGGER.error(' '.join(msgs))

                    self.set_status(400)
                    retdict = {
                        'status':'failed',
                        'result':None,
                        'message':("Sorry, editing user information failed.")
                    }
                    self.write(retdict)
                    raise tornado.web.Finish()

                # if login did succeed, return the updated info
                else:

                    LOGGER.warning('edit_user: %r initiated by '
                                   'user_id: %s successful for '
                                   'user_id: %s' % (list(updatedict.keys()),
                                                    current_user_id,
                                                    target_userid))

                    retdict = {
                        'status':'ok',
                        'result':resp['user_info'],
                        'message':("Edit to user information successful.")
                    }
                    self.write(retdict)
                    self.finish()

            except Exception as e:

                LOGGER.exception('failed to update user information.')

                self.set_status(400)
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':("Invalid input provided for "
                               "user edit.")
                }
                self.write(retdict)
                raise tornado.web.Finish()

        # anything else is probably the locked user, turn them away
        else:
            self.set_status(403)
            retdict = {
                'status':'failed',
                'result':None,
                'message':("Sorry, you don't have access. "
                           "API keys are not allowed for this endpoint.")
            }
            self.write(retdict)
            raise tornado.web.Finish()




######################
## API KEY HANDLING ##
######################

class APIKeyHandler(BaseHandler):
    '''This handles API key generation

    '''

    def initialize(self,
                   apiversion,
                   authnzerver,
                   fernetkey,
                   executor,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''
        self.apiversion = apiversion
        self.authnzerver = authnzerver
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.executor = executor
        self.session_expiry = session_expiry
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.siteinfo = siteinfo
        self.ratelimit = ratelimit
        self.cachedir = cachedir


    @gen.coroutine
    def get(self):
        '''This generates an API key.

        Then one can run any /api/<method> with the following in the header:

        Authorization: Bearer <token>

        keys expire in 1 day and contain:

        ip: the remote IP address
        ver: the version of the API
        token: a random hex
        expiry: the ISO format date of expiry

        '''

        # redirect completely unknown clients
        if not self.current_user:
            self.redirect('/')

        client_header = self.current_user['client_header']
        ip_address = self.current_user['ip_address']
        user_id = self.current_user['user_id']
        user_role = self.current_user['user_role']
        expires_days = self.session_expiry
        session_token = self.current_user['session_token']

        # send this info to the backend to store and make an API key dict
        ok, resp, msgs = yield self.authnzerver_request(
            'apikey-new',
            {'user_id':user_id,
             'user_role':user_role,
             'expires_days':expires_days,
             'ip_address':ip_address,
             'client_header':client_header,
             'session_token':session_token,
             'apiversion':self.apiversion}
        )

        # when we get back the API key dict, encode to bytes, then
        # Fernet encrypt+sign it.
        if ok:

            apikey_bytes = resp['apikey'].encode()
            apikey_encrypted_signed = self.ferneter.encrypt(
                apikey_bytes
            )

            retdict = {
                'status':'ok',
                'result':{
                    'apikey':apikey_encrypted_signed,
                    'expires':'%sZ' % resp['expires'],
                },
                'message':('API key generated successfully. Expires: %sZ'
                           % resp['expires'])
            }

            self.write(retdict)
            self.finish()

        else:

            LOGGER.error(msgs)
            retdict = {
                'status':'failed',
                'result':None,
                'message':(
                    'API key could not be generated because of a backend error.'
                )
            }

            self.write(retdict)
            self.finish()



class APIVerifyHandler(BaseHandler):
    '''This handles API key verification.

    '''

    def initialize(self,
                   apiversion,
                   authnzerver,
                   fernetkey,
                   executor,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir):
        '''
        handles initial setup.

        '''
        self.apiversion = apiversion
        self.authnzerver = authnzerver
        self.fernetkey = fernetkey
        self.ferneter = Fernet(fernetkey)
        self.executor = executor
        self.session_expiry = session_expiry
        self.httpclient = AsyncHTTPClient(force_instance=True)
        self.siteinfo = siteinfo
        self.ratelimit = ratelimit
        self.cachedir = cachedir



    @gen.coroutine
    def post(self):
        '''This is used to check if an API key is valid.

        This transparently uses the BaseHandler's POST API key verification.

        '''

        if ((not self.keycheck['status'] == 'ok') or
            (not self.xsrf_type == 'apikey')):

            self.set_status(401)
            retdict = {
                'status':'failed',
                'result': None,
                'message': self.keycheck['message']
            }
            self.write(retdict)
            self.finish()

        else:

            retdict = {
                'status':'ok',
                'result': None,
                'message': self.keycheck['message']
            }
            self.write(retdict)
            self.finish()
