#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''admin_actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive admin related actions (listing users, editing
users, change user roles).

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

try:

    from datetime import datetime, timezone, timedelta
    utc = timezone.utc

except Exception as e:

    from datetime import datetime, timedelta, tzinfo
    ZERO = timedelta(0)

    class UTC(tzinfo):
        """UTC"""

        def utcoffset(self, dt):
            return ZERO

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return ZERO

    utc = UTC()

import multiprocessing as mp

from sqlalchemy import select, desc

from . import authdb
from .actions_session import auth_session_exists


##################
## LISTING USERS ##
###################

def list_users(payload,
               raiseonfail=False,
               override_authdb_path=None):
    '''
    This lists users.

    Parameters
    ----------

    payload : dict
        This is the input payload dict. Required items:

        - user_id: int or None. If None, all users will be returned

    raiseonfail : bool
        If True, will raise an Exception if something goes wrong.

    override_authdb_path : str or None
        If given as a str, is the alternative path to the auth DB.

    Returns
    -------

    dict
        The dict returned is of the form::

            {'success': True or False,
             'user_info': list of dicts, one per user,
             'messages': list of str messages if any}

        The dicts per user will contain the following items::

            {'user_id','full_name', 'email',
             'is_active','created_on','user_role',
             'last_login_try','last_login_success'}

    '''

    if 'user_id' not in payload:
        LOGGER.error('no user_id provided')

        return {
            'success':False,
            'user_info':None,
            'messages':["No user_id provided."],
        }

    user_id = payload['user_id']

    try:

        # this checks if the database connection is live
        currproc = mp.current_process()
        engine = getattr(currproc, 'engine', None)

        if override_authdb_path:
            currproc.auth_db_path = override_authdb_path

        if not engine:
            currproc.engine, currproc.connection, currproc.table_meta = (
                authdb.get_auth_db(
                    currproc.auth_db_path,
                    echo=raiseonfail
                )
            )

        users = currproc.table_meta.tables['users']

        if user_id is None:

            s = select([
                users.c.user_id,
                users.c.full_name,
                users.c.email,
                users.c.is_active,
                users.c.last_login_try,
                users.c.last_login_success,
                users.c.created_on,
                users.c.user_role,
            ]).order_by(desc(users.c.created_on)).select_from(users)

        else:

            s = select([
                users.c.user_id,
                users.c.full_name,
                users.c.email,
                users.c.is_active,
                users.c.last_login_try,
                users.c.last_login_success,
                users.c.created_on,
                users.c.user_role,
            ]).order_by(desc(users.c.created_on)).select_from(users).where(
                users.c.user_id == user_id
            )

        result = currproc.connection.execute(s)
        rows = result.fetchall()
        result.close()

        try:

            serialized_result = [dict(x) for x in rows]

            return {
                'success':True,
                'user_info':serialized_result,
                'messages':["User look up successful."],
            }

        except Exception as e:

            if raiseonfail:
                raise

            return {
                'success':False,
                'user_info':None,
                'messages':["User look up failed."],
            }

    except Exception as e:

        if raiseonfail:
            raise

        LOGGER.warning('user info not found or '
                       'could not check if it exists')

        return {
            'success':False,
            'user_info':None,
            'messages':["User look up failed."],
        }



def edit_user(payload,
              raiseonfail=False,
              override_authdb_path=None):
    '''
    This edit users.

    Parameters
    ----------

    payload : dict
        This is the input payload dict. Required items:

        - user_id: int, user ID of an admin user or == target_userid
        - user_role: str, == 'superuser' or == target_userid user_role
        - session_token: str, session token of admin or target_userid token
        - target_userid: int, the user to edit
        - update_dict: dict, the update dict

    raiseonfail : bool
        If True, will raise an Exception if something goes wrong.

    override_authdb_path : str or None
        If given as a str, is the alternative path to the auth DB.

    Returns
    -------

    dict
        The dict returned is of the form::

            {'success': True or False,
             'user_info': dict, with new user info,
             'messages': list of str messages if any}

        Only these items can be edited::

            {'full_name', 'email', <- by user and superuser
             'is_active','user_role', <- by superuser only}

    '''

    for key in ('user_id','user_role','session_token','target_userid',
                'update_dict'):

        if key not in payload:

            LOGGER.error('no %s provided' % key)
            return {
                'success':False,
                'user_info':None,
                'messages':["No user_id provided."],
            }

    user_id = payload['user_id']
    user_role = payload['user_role']
    session_token = payload['session_token']
    target_userid = payload['target_userid']
    update_dict = payload['update_dict']

    try:

        # this checks if the database connection is live
        currproc = mp.current_process()
        engine = getattr(currproc, 'engine', None)

        if override_authdb_path:
            currproc.auth_db_path = override_authdb_path

        if not engine:
            currproc.engine, currproc.connection, currproc.table_meta = (
                authdb.get_auth_db(
                    currproc.auth_db_path,
                    echo=raiseonfail
                )
            )


        # check if the user_id == target_userid
        # if so, check if session_token is valid and belongs to user_id



        users = currproc.table_meta.tables['users']

        if user_id is None:

            s = select([
                users.c.user_id,
                users.c.full_name,
                users.c.email,
                users.c.is_active,
                users.c.last_login_try,
                users.c.last_login_success,
                users.c.created_on,
                users.c.user_role,
            ]).order_by(desc(users.c.created_on)).select_from(users)

        else:

            s = select([
                users.c.user_id,
                users.c.full_name,
                users.c.email,
                users.c.is_active,
                users.c.last_login_try,
                users.c.last_login_success,
                users.c.created_on,
                users.c.user_role,
            ]).order_by(desc(users.c.created_on)).select_from(users).where(
                users.c.user_id == user_id
            )

        result = currproc.connection.execute(s)
        rows = result.fetchall()
        result.close()

        try:

            serialized_result = [dict(x) for x in rows]

            return {
                'success':True,
                'user_info':serialized_result,
                'messages':["User look up successful."],
            }

        except Exception as e:

            if raiseonfail:
                raise

            return {
                'success':False,
                'user_info':None,
                'messages':["User look up failed."],
            }

    except Exception as e:

        if raiseonfail:
            raise

        LOGGER.warning('user info not found or '
                       'could not check if it exists')

        return {
            'success':False,
            'user_info':None,
            'messages':["User look up failed."],
        }
