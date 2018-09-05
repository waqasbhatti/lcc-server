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

import ipaddress
import secrets
import multiprocessing as mp
from datetime import datetime

from sqlalchemy import select

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

    login flow:

    session cookie get -> check session exists -> check user login -> old
    session delete (no matter what) -> new session create (with actual user_id
    and other info now included if successful or same user_id = anon if not
    successful) -> done

    '''

    # check broken
    request_ok = True
    for item in ('email', 'password', 'session_token'):
        if item not in payload:
            request_ok = False
            break


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

    #
    # check if the request is OK
    #

    # if it isn't, then hash the dummy user's password twice and
    # return False, None
    if not request_ok:

        # always get the dummy user's password from the DB
        dummy_results = users.select([
            users.c.password
        ]).where(users.c.user_id == 3)
        dummy_password = dummy_results.fetchone()['password']
        dummy_results.close()
        authdb.password_context.verify('nope',
                                       dummy_password)
        # always get the dummy user's password from the DB
        dummy_results = users.select([
            users.c.password
        ]).where(users.c.user_id == 3)
        dummy_password = dummy_results.fetchone()['password']
        dummy_results.close()
        authdb.password_context.verify('nope',
                                       dummy_password)

        return False

    # otherwise, now we'll check if the session exists
    else:

        session_info = auth_session_exists(
            {'session_token':payload['session_token']},
            raiseonfail=raiseonfail,
            override_authdb_path=override_authdb_path
        )

        # if it doesn't, hash the dummy password twice, insert new session token
        # and return False, session_token
        if not session_info:

            # always get the dummy user's password from the DB
            dummy_results = users.select([
                users.c.password
            ]).where(users.c.user_id == 3)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)
            # always get the dummy user's password from the DB
            dummy_results = users.select([
                users.c.password
            ]).where(users.c.user_id == 3)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)

            return False

        # if it does, we'll proceed to checking the password for the provided
        # email
        else:

            # always get the dummy user's password from the DB
            dummy_results = users.select([
                users.c.password
            ]).where(users.c.user_id == 3)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)

            # look up the provided user
            user_results = users.select([
                users.c.user_id,
                users.c.password
            ]).where(users.c.email == payload['email'])
            user_info = user_results.fetchone()
            user_results.close()

            if user_info:

                pass_ok = authdb.password_context.verify(
                    payload['password'],
                    user_info['password']
                )

            else:

                authdb.password_context.verify('nope',
                                               dummy_password)
                pass_ok = False


            if not pass_ok:

                # delete the current session from the database
                auth_session_delete(
                    {'session_token':payload['session_token']},
                    raiseonfail=raiseonfail,
                    override_authdb_path=override_authdb_path
                )

                return False

            # if password verification succeeeded, we'll delete the existing
            # session token. The frontend will take this new session token and
            # set the cookie on it
            else:

                # delete the current session from the database
                auth_session_delete(
                    {'session_token':payload['session_token']},
                    raiseonfail=raiseonfail,
                    override_authdb_path=override_authdb_path
                )

                return user_info['user_id']



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

    # check if the session token exists
    session_ok = auth_session_exists(payload,
                                     override_authdb_path=override_authdb_path,
                                     raiseonfail=raiseonfail)

    if session_ok:

        deleted = auth_session_delete(payload,
                                      override_authdb_path=override_authdb_path,
                                      raiseonfail=raiseonfail)
        return deleted

    else:

        return False




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
        payload['ip_address'] = validated_ip

        # set the userid to anonuser@localhost if no user is provided
        if not payload['user_id']:
            payload['user_id'] = 2

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
        users = currproc.table_meta.tables['users']
        s = select([
            users.c.user_id,
            users.c.full_name,
            users.c.email,
            users.c.email_verified,
            users.c.emailverify_sent_datetime,
            users.c.is_active,
            users.c.last_login_try,
            users.c.last_login_success,
            users.c.created_on,
            users.c.user_role,
            sessions.c.session_token,
            sessions.c.ip_address,
            sessions.c.client_header,
            sessions.c.created,
            sessions.c.expires,
            sessions.c.extra_info_json
        ]).select_from(users.join(sessions)).where(
            (sessions.c.session_token == session_token) &
            (sessions.c.expires > datetime.utcnow())
        )
        result = currproc.connection.execute(s)
        rows = result.fetchone()
        result.close()

        try:

            serialized_result = dict(rows)
            return serialized_result
        except Exception as e:
            LOGGER.warning(
                'no existing and unexpired session info for token: %s'
                % session_token
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
