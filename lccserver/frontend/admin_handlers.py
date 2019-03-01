#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''admin_handlesr.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Mar 2019

This contains handlers for the admin interface.

'''

####################
## SYSTEM IMPORTS ##
####################

import os.path

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

            self.render('admin.html',
                        flash_messages=self.render_flash_messages(),
                        user_account_box=self.render_user_account_box(),
                        page_title="LCC-Server admin",
                        lccserver_version=__version__,
                        siteinfo=self.siteinfo)


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
                with open(siteinfojson,'r') as infd:
                    siteinfo_disk = json.load(infd)

                siteinfo_disk.update(self.siteinfo)

                LOGGER.warning(
                    'updating site-info.json from admin-site-update-form'
                )

                with open(siteinfojson,'w') as outfd:
                    json.dump(siteinfo_disk, outfd)

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

            # handle the POST
            # replace below as appropriate
            pass

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

    '''

    @gen.coroutine
    def post(self):
        '''This handles the POST to /admin/users and
        updates the authdb by talking to the authnzerver.

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

            # handle the POST
            # replace below as appropriate
            pass

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
