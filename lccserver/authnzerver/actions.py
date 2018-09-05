#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive auth actions.

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
import ipaddress
import secrets
import multiprocessing as mp
from datetime import datetime
import base64

from . import authdb


###################################
## USER LOGIN HANDLING FUNCTIONS ##
###################################

def auth_user_login(payload,
                    override_authdb_path=None,
                    raiseonfail=False):
    '''This logs a user in.

    override_authdb_path allows testing without an executor.

    payload keys required:

    valid session_token, email, password

    If user auth succeeds, invalidates the current session token, generates a
    new one, then returns:

    True, session_token

    else invalidates the current session token and generates a new one, then
    returns:

    False, session_token

    If the user doesn't match any existing ones, we'll run the password hash
    check against the hashed password of the dummyuser (which is unknowable
    unless someone other than us has access to the DB). This should avoid
    leaking the presence or absence of specific users via timing attacks.

    We require a valid and unexpired session_token to enable user login. The
    frontend indexserver takes care of getting this from the timed-expiry
    Secure, HTTPonly, SameSite cookie. If it's expired or invalid, then the
    frontend will send None as session_token and we won't allow login until the
    session token has been renewed (which happens right after this request).

    query:

    select
      a.user_id,
      a.session_token,
      a.email,
      a.password,
      a.is_active,
      a.user_role,
      a.last_login_try,
      a.last_login_success
    from
      users a join sessions b on (a.user_id = b.user_id)
    where
      a.email in (provided_email, 'dummyuser@localhost')

    '''

    # break immediately if the payload doesn't match
    for item in ('email', 'password', 'session_token'):
        if item not in payload:
            return False

    # get the dummy user's password from the DB

    # now we'll check if the session exists

    # if it doesn't, hash the dummy password twice, generate a new session token
    # and return False, session_token

    # if it does, we'll proceed to checking the password for the provided email

    # if the user exists or doesn't exist, hash the password against the dummy
    # user

    # if the user exists, hash the password against the provided password. if
    # not, hash the password again against the dummy user

    # check if it matches, if True: delete existing session token, create a new
    # one, and return True, session_token

    # if it doesn't match, delete existing session token, create a new one and
    # return False, session_token



def auth_user_logout(payload,
                     override_authdb_path=None,
                     raiseonfail=False):
    '''This logs out a user.

    override_authdb_path allows testing without an executor.

    payload keys required:

    valid session_token

    Deletes the session token from the session store. On the next request
    (redirect from POST /auth/logout to GET /), the frontend will issue a new
    one.

    '''



################################
## SESSION HANDLING FUNCTIONS ##
################################

def auth_session_new(payload,
                     override_authdb_path=None,
                     raiseonfail=False):
    '''
    This generates a new session token.

    override_authdb_path allows testing without an executor.

    Request payload keys required:

    ip_address, client_header, user_id, expires, extra_info_json

    Returns:

    a 32 byte session token in base64 from secrets.token_urlsafe(32)

    '''

    # fail immediately if the required payload items are not present
    for item in ('ip_address',
                 'client_header',
                 'user_id',
                 'expires',
                 'extra_info_json'):
        if item not in payload:
            return False

    try:

        validated_ip = str(ipaddress.ip_address(payload['ip_address']))

        # set the userid to anonuser@localhost if no user is provided
        if not payload['user_id']:
            user_id = 'anonuser@localhost'

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

        # generate a session token
        session_token = secrets.token_urlsafe(32)

        payload['session_token'] = session_token
        payload['created'] = datetime.utcnow()

        # get the insert object from sqlalchemy
        sessions = currproc.table_meta.tables['sessions']
        insert = sessions.insert().values(**payload)
        result = currproc.connection.execute(insert)
        result.close()

        return session_token

    except Exception as e:
        LOGGER.exception('could not create a new session')

        if raiseonfail:
            raise

        return False



def auth_session_exists(payload,
                        raiseonfail=False,
                        override_authdb_path=None):
    '''
    This checks if the provided session token exists.

    Request payload keys required:

    session_key

    Returns:

    All sessions columns for session_key if token exists and has not expired.
    None if token has expired. Will also call auth_session_delete.

    '''

    if 'session_token' not in payload:
        LOGGER.error('no session token provided')
        return False

    session_token = payload['session_token']

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

        sessions = currproc.table_meta.tables['sessions']
        select = sessions.select().where(
            sessions.c.session_token == session_token
        )
        result = currproc.connection.execute(select)

        try:
            serialized_result = [dict(x) for x in result]
            return serialized_result
        except Exception as e:
            LOGGER.exception(
                'no existing session info for token: %s' % session_token
            )
            return False

    except Exception as e:

        LOGGER.exception('session token not found or '
                         'could not check if it exists')

        if raiseonfail:
            raise

        return False



def auth_session_delete(payload,
                        raiseonfail=False,
                        override_authdb_path=None):
    '''
    This removes a session token.

    Request payload keys required:

    session_key

    Returns:

    Deletes the row corresponding to the session_key in the sessions table.

    '''

    if 'session_token' not in payload:
        LOGGER.error('no session token provided')
        return False

    session_token = payload['session_token']

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

        sessions = currproc.table_meta.tables['sessions']
        delete = sessions.delete().where(
            sessions.c.session_token == session_token
        )
        result = currproc.connection.execute(delete)
        result.close()

        return result.rowcount

    except Exception as e:

        LOGGER.exception('could not create a new session')

        if raiseonfail:
            raise

        return False



###################
## ROLE HANDLING ##
###################


###################
## USER HANDLING ##
###################


#########################
## PERMISSION HANDLING ##
#########################
