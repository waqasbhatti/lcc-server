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
                    echo=False
                )
            )

        # generate a session token
        session_token = secrets.token_urlsafe(32)

        # get the insert object from sqlalchemy
        sessions = currproc.table_meta.tables['sessions']
        insert = sessions.insert().values(
            session_token=session_token,
            ip_address=validated_ip,
            client_header=payload['client_header'],
            user_id=user_id,
            expires=payload['expires'],
            extra_info_json=json.dumps(payload['extra_info_json']),
        )
        result = currproc.connection.execute(insert)
        result.close()

        return session_token

    except Exception as e:
        LOGGER.exception('could not create a new session')

        if raiseonfail:
            raise

        return False



def auth_session_exists(payload):
    '''
    This checks if the provided session token exists.

    Request payload keys required:

    session_key

    Returns:

    All sessions columns for session_key if token exists and has not expired.
    None if token has expired. Will also call auth_session_delete.

    '''


def auth_session_delete(payload):
    '''
    This removes a session token.

    Request payload keys required:

    session_key

    Returns:

    Deletes the row corresponding to the session_key in the sessions table.

    '''


###################
## ROLE HANDLING ##
###################


###################
## USER HANDLING ##
###################


#########################
## PERMISSION HANDLING ##
#########################
