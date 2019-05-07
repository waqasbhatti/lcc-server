#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''admin_handlesr.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Mar 2019

This contains handlers for the admin interface.

'''

####################
## SYSTEM IMPORTS ##
####################

import os.path
import os

import logging
from datetime import datetime

from cryptography.fernet import Fernet

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


####################
## ADMIN HANDLERS ##
####################


class AdminIndexHandler(BaseHandler):
    '''
    This handles /admin.

    '''

    def initialize(self,
                   fernetkey,
                   executor,
                   authnzerver,
                   basedir,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir):
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
        self.ratelimit = ratelimit
        self.cachedir = cachedir
        self.basedir = basedir

        # initialize this to None
        # we'll set this later in self.prepare()
        self.current_user = None

        # apikey verification info
        self.apikey_verified = False
        self.apikey_info = None


    @gen.coroutine
    def get(self):
        '''
        This shows the admin page.

        '''

        if not self.current_user:
            self.redirect('/users/login')

        current_user = self.current_user

        # only allow in staff and superuser roles
        if current_user and current_user['user_role'] in ('staff', 'superuser'):

            # ask the authnzerver for a user list
            reqtype = 'user-list'
            reqbody = {'user_id': None}

            ok, resp, msgs = yield self.authnzerver_request(
                reqtype, reqbody
            )

            if not ok:

                LOGGER.error('no user list returned from authnzerver')
                user_list = []

            else:
                user_list = resp['user_info']

            self.render('admin.html',
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),
                        page_title="LCC-Server admin",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo,
                        userlist=user_list)


        # anything else is probably the locked user, turn them away
        else:
            self.render_blocked_message()



class SiteSettingsHandler(BaseHandler):
    '''
    This handles /admin/site.

    '''
    def initialize(self,
                   fernetkey,
                   executor,
                   authnzerver,
                   basedir,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir,
                   sitestatic):
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
        self.ratelimit = ratelimit
        self.cachedir = cachedir
        self.basedir = basedir
        self.sitestatic = sitestatic

        # initialize this to None
        # we'll set this later in self.prepare()
        self.current_user = None

        # apikey verification info
        self.apikey_verified = False
        self.apikey_info = None


    @gen.coroutine
    def post(self):
        '''This handles the POST to /admin/site and
        updates the site-settings.json file on disk.

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

        # only allow in superuser roles
        if current_user and current_user['user_role'] == 'superuser':

            try:

                # get the form inputs
                projectname = xhtml_escape(
                    self.get_argument('projectname')
                ).strip()
                projectlink = xhtml_escape(
                    self.get_argument('projectlink')
                ).strip()
                institution = xhtml_escape(
                    self.get_argument('institution')
                ).strip()
                institutionlink = xhtml_escape(
                    self.get_argument('institutionlink')
                ).strip()
                institutionlogo = xhtml_escape(
                    self.get_argument('institutionlogo')
                ).strip()
                department = xhtml_escape(
                    self.get_argument('department')
                ).strip()
                departmentlink = xhtml_escape(
                    self.get_argument('departmentlink')
                ).strip()

                if (institutionlogo.lower() == 'none' or
                    institutionlogo.lower() == 'null' or
                    len(institutionlogo) == 0):
                    institutionlogo = None

                else:
                    logo_file_check = os.path.exists(
                        os.path.join(
                            self.sitestatic,
                            institutionlogo
                        )
                    )
                    if not logo_file_check:
                        institutionlogo = None


                updatedict = {
                    "project": projectname,
                    "project_link": projectlink,
                    "department": department,
                    "department_link": departmentlink,
                    "institution": institution,
                    "institution_link": institutionlink,
                    "institution_logo": institutionlogo,
                }

                # update the siteinfo dict
                self.siteinfo.update(updatedict)

                # update the site-info.json file on disk
                siteinfojson = os.path.join(
                    self.basedir,
                    'site-info.json'
                )

                try:

                    with open(siteinfojson,'r') as infd:
                        siteinfo_disk = json.load(infd)

                    siteinfo_disk.update(updatedict)

                except Exception as e:
                    LOGGER.error(
                        'site-info.json file does not '
                        'exist or has disappeared! A new site-info.json '
                        'will be written to the basedir.'
                    )
                    siteinfo_disk = updatedict
                    siteinfo_disk['email_setting_file'] = (
                        '.lccserver.secret-email'
                    )
                    siteinfo_disk['signups_allowed'] = False
                    siteinfo_disk['logins_allowed'] = True
                    siteinfo_disk['rate_limit_active'] = True
                    siteinfo_disk['cache_location'] = (
                        '/tmp/lccserver_cache'
                    )


                LOGGER.warning(
                    'updating site-info.json from admin-site-update-form'
                )

                with open(siteinfojson,'w') as outfd:
                    json.dump(siteinfo_disk, outfd, indent=4)

                returndict = {
                    'status':'ok',
                    'message':('Site settings successfully updated. '
                               'Reload this page to see the '
                               'new footer if it was updated.'),
                    'result':updatedict
                }

                self.write(returndict)
                self.finish()

            except Exception as e:

                LOGGER.exception('failed to update site-info.json')

                self.set_status(400)
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':("Invalid input provided for site-settings form.")
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



class EmailSettingsHandler(BaseHandler):
    '''
    This handles /admin/email.

    '''

    def initialize(self,
                   fernetkey,
                   executor,
                   authnzerver,
                   basedir,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir,
                   sitestatic):
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
        self.ratelimit = ratelimit
        self.cachedir = cachedir
        self.basedir = basedir
        self.sitestatic = sitestatic

        # initialize this to None
        # we'll set this later in self.prepare()
        self.current_user = None

        # apikey verification info
        self.apikey_verified = False
        self.apikey_info = None


    @gen.coroutine
    def post(self):
        '''This handles the POST to /admin/email and
        updates the site-settings.json file on disk.

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

        # only allow in superuser roles
        if current_user and current_user['user_role'] == 'superuser':

            try:

                # get the form inputs
                loginval = xhtml_escape(
                    self.get_argument('loginradio')
                ).strip().lower()
                signupval = xhtml_escape(
                    self.get_argument('signupradio')
                ).strip().lower()
                emailsender = xhtml_escape(
                    self.get_argument('emailsender')
                ).strip()
                emailserver = xhtml_escape(
                    self.get_argument('emailserver')
                ).strip().lower()
                emailport = abs(int(
                    xhtml_escape(
                        self.get_argument('emailport')
                    ).strip()
                ))
                emailuser = xhtml_escape(
                    self.get_argument('emailuser')
                ).strip()
                emailpass = self.get_argument('emailpass').strip()


                if loginval == 'login-allowed':
                    loginval = True
                elif loginval == 'login-disallowed':
                    loginval = False
                else:
                    loginval = False

                if signupval == 'signup-allowed':
                    signupval = True
                elif signupval == 'signup-disallowed':
                    signupval = False
                else:
                    signupval = False

                # make sure to check if the email settings are valid if signups
                # are enabled
                if signupval and (len(emailsender) == 0 or
                                  len(emailserver) == 0 or
                                  emailserver == 'smtp.emailserver.org' or
                                  emailport == 0 or
                                  len(emailuser) == 0 or
                                  len(emailpass) == 0):

                    LOGGER.error('invalid items in the '
                                 'admin-email-update-form')

                    self.set_status(400)
                    retdict = {
                        'status':'failed',
                        'result':None,
                        'message':("Invalid input in the "
                                   "email settings form. "
                                   "All fields are required "
                                   "if new user sign-ups are "
                                   "to be enabled.")
                    }
                    self.write(retdict)
                    raise tornado.web.Finish()


                updatedict = {
                    "logins_allowed": loginval,
                    "signups_allowed": signupval,
                }

                emailupdatedict = {
                    "email_sender": emailsender,
                    "email_server": emailserver,
                    "email_port": emailport,
                    "email_user": emailuser,
                    "email_pass": emailpass,
                }

                # update the siteinfo dict
                self.siteinfo.update(updatedict)

                # update the site-info.json file on disk
                siteinfojson = os.path.join(
                    self.basedir,
                    'site-info.json'
                )
                with open(siteinfojson,'r') as infd:
                    siteinfo_disk = json.load(infd)

                # update site-info.json only with values of logins-allowed,
                # signups-allowed
                siteinfo_disk.update(updatedict)

                LOGGER.warning(
                    'updating site-info.json from admin-email-update-form'
                )

                with open(siteinfojson,'w') as outfd:
                    json.dump(siteinfo_disk, outfd, indent=4)

                # update the email-settings in the siteinfo dict and the
                # email-settings file next
                self.siteinfo.update(emailupdatedict)

                email_settings_file = os.path.join(
                    self.basedir,
                    self.siteinfo['email_settings_file']
                )

                if not os.path.exists(email_settings_file):

                    LOGGER.error('no email settings file found '
                                 'at expected path indicated '
                                 'in site-info.json. Making a new one...')

                    with open(email_settings_file,'w') as outfd:
                        json.dump(emailupdatedict, outfd, indent=4)

                else:

                    # make sure we can write to the email settings file
                    os.chmod(email_settings_file, 0o100600)

                    with open(email_settings_file,'r') as infd:
                        emailsettings_disk = json.load(infd)

                    emailsettings_disk.update(emailupdatedict)

                    LOGGER.warning(
                        'updating email settings file '
                        'from admin-email-update-form'
                    )

                    with open(email_settings_file,'w') as outfd:
                        json.dump(emailsettings_disk, outfd, indent=4)


                # set email settings file permissions back to readonly
                os.chmod(email_settings_file, 0o100400)

                updatedict.update(emailupdatedict)

                returndict = {
                    'status':'ok',
                    'message':('Email and user sign-up/sign-in '
                               'settings successfully updated.'),
                    'result':updatedict
                }

                self.write(returndict)
                self.finish()

            except Exception as e:

                LOGGER.exception('failed to update site-info.json')

                self.set_status(400)
                retdict = {
                    'status':'failed',
                    'result':None,
                    'message':("Invalid input provided for "
                               "email-settings form.")
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



class UserAdminHandler(BaseHandler):
    '''
    This handles /admin/users.

    Called from /admin/users by superusers only to update:

    email, full_name, is_active, user_role

    '''

    def initialize(self,
                   fernetkey,
                   executor,
                   authnzerver,
                   basedir,
                   session_expiry,
                   siteinfo,
                   ratelimit,
                   cachedir,
                   sitestatic):
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
        self.ratelimit = ratelimit
        self.cachedir = cachedir
        self.basedir = basedir
        self.sitestatic = sitestatic

        # initialize this to None
        # we'll set this later in self.prepare()
        self.current_user = None

        # apikey verification info
        self.apikey_verified = False
        self.apikey_info = None


    @gen.coroutine
    def post(self):
        '''This handles the POST to /admin/users.

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

        if current_user and current_user['user_role'] in ('authenticated',
                                                          'staff',
                                                          'superuser'):

            current_user_id = current_user['user_id']
            current_user_role = current_user['user_role']
            current_sessiontoken = current_user['session_token']

            try:

                updated_email = self.get_argument('updated_email')
                updated_fullname = self.get_argument('updated_fullname')
                updated_role = self.get_argument('updated_role')

                reqtype = 'user-edit'

                if current_user_role == 'superuser':

                    if updated_role == 'locked':
                        is_active = False
                    else:
                        is_active = True

                    updatedict = {
                        'email':updated_email,
                        'full_name':updated_fullname,
                        'user_role':updated_role,
                        'is_active':is_active,
                    }

                    target_userid = int(
                        xhtml_escape(
                            self.get_argument('target_userid')
                        )
                    )

                    reqbody = {
                        'session_token': current_sessiontoken,
                        'user_id':current_user_id,
                        'user_role':current_user_role,
                        'target_userid':target_userid,
                        'update_dict':updatedict,
                    }

                else:

                    updatedict = {
                        'email':updated_email,
                        'full_name':updated_fullname,
                    }

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
