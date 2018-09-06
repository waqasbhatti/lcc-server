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
from datetime import datetime, timedelta
import socket

from tornado.escape import squeeze
from sqlalchemy import select
from fuzzywuzzy.fuzz import UQRatio

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

    login flow for frontend:

    session cookie get -> check session exists -> check user login -> old
    session delete (no matter what) -> new session create (with actual user_id
    and other info now included if successful or same user_id = anon if not
    successful) -> done

    The frontend MUST unset the cookie as well.

    FIXME: update (and fake-update) the Users table with the last_login_try and
    last_login_success.

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

        # dummy session request
        session_info = auth_session_exists(
            {'session_token':'nope'},
            raiseonfail=raiseonfail,
            override_authdb_path=override_authdb_path
        )

        # always get the dummy user's password from the DB
        dummy_sel = select([
            users.c.password
        ]).select_from(users).where(users.c.user_id == 3)
        dummy_results = currproc.connection.execute(dummy_sel)

        dummy_password = dummy_results.fetchone()['password']
        dummy_results.close()
        authdb.password_context.verify('nope',
                                       dummy_password)
        # always get the dummy user's password from the DB
        dummy_sel = select([
            users.c.password
        ]).select_from(users).where(users.c.user_id == 3)
        dummy_results = currproc.connection.execute(dummy_sel)
        dummy_password = dummy_results.fetchone()['password']
        dummy_results.close()
        authdb.password_context.verify('nope',
                                       dummy_password)

        # run a fake session delete
        auth_session_delete({'session_token':'nope'},
                            raiseonfail=raiseonfail,
                            override_authdb_path=override_authdb_path)

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
            dummy_sel = select([
                users.c.password
            ]).select_from(users).where(users.c.user_id == 3)
            dummy_results = currproc.connection.execute(dummy_sel)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)
            # always get the dummy user's password from the DB
            dummy_sel = select([
                users.c.password
            ]).select_from(users).where(users.c.user_id == 3)
            dummy_results = currproc.connection.execute(dummy_sel)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)

            # run a fake session delete and generate a new session
            auth_session_delete(
                {'session_token':'nope'},
                raiseonfail=raiseonfail,
                override_authdb_path=override_authdb_path
            )

            return False

        # if the session token does exist, we'll proceed to checking the
        # password for the provided email
        else:

            # always get the dummy user's password from the DB
            dummy_sel = select([
                users.c.password
            ]).select_from(users).where(users.c.user_id == 3)
            dummy_results = currproc.connection.execute(dummy_sel)
            dummy_password = dummy_results.fetchone()['password']
            dummy_results.close()
            authdb.password_context.verify('nope',
                                           dummy_password)

            # look up the provided user
            user_sel = select([
                users.c.user_id,
                users.c.password,
                users.c.is_active,
                users.c.user_role,
            ]).select_from(users).where(users.c.email == payload['email'])
            user_results = currproc.connection.execute(user_sel)
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

            # run a session delete on the provided token. the frontend will
            # always re-ask for a new session token on the next request after
            # login if it fails or succeeds.
            auth_session_delete(
                {'session_token':payload['session_token']},
                raiseonfail=raiseonfail,
                override_authdb_path=override_authdb_path
            )

            if not pass_ok:

                return False

            # if password verification succeeeded, check if the user can
            # actually log in (i.e. their account is not locked or is not
            # inactive)
            else:

                if (user_info['is_active'] and
                    user_info['user_role'] != 'locked'):
                    return user_info['user_id']
                else:
                    return False



def auth_user_logout(payload,
                     override_authdb_path=None,
                     raiseonfail=False):
    '''This logs out a user.

    override_authdb_path allows testing without an executor.

    payload keys required:

    valid session_token, user_id

    Deletes the session token from the session store. On the next request
    (redirect from POST /auth/logout to GET /), the frontend will issue a new
    one.

    The frontend MUST unset the cookie as well.

    '''

    # check if the session token exists
    session = auth_session_exists(payload,
                                  override_authdb_path=override_authdb_path,
                                  raiseonfail=raiseonfail)

    if session:

        # check the user ID
        if payload['user_id'] == session['user_id']:

            deleted = auth_session_delete(
                payload,
                override_authdb_path=override_authdb_path,
                raiseonfail=raiseonfail
            )
            if deleted > 0:
                return session['user_id']
            else:
                LOGGER.error(
                    'something went wrong when '
                    'trying to remove session ID: %s, '
                    'for user ID: %s' % (payload['session_token'],
                                         payload['user_id'])
                )

        else:
            LOGGER.error(
                'tried to log out but session token = %s '
                'and user_id = %s do not match. '
                'expected user_id = %s' % (payload['session_token'],
                                           payload['user_id'],
                                           session['user_id']))
            return False

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

def change_user_role(payload,
                     raiseonfail=False,
                     override_authdb_path=None):
    '''
    This changes a user's role.

    Not used except for by superusers to promote people to other roles.

    '''
    # TODO: finish this



###################
## USER HANDLING ##
###################

def validate_input_password(email,
                            password,
                            min_length=12,
                            max_match_threshold=20):
    '''This validates user input passwords.

    1. must be at least min_length characters (we'll truncate the password at
       1024 characters since we don't want to store entire novels)
    2. must not match within max_match_threshold of their email
    3. must not match within max_match_threshold of the site's FQDN
    4. must not have a single case-folded character take up more than 20% of the
       length of the password
    5. must not be completely numeric

    '''

    messages = []

    # we'll ignore any repeated white space and fail immediately if the password
    # is all white space
    if len(squeeze(password.strip())) < min_length:

        LOGGER.warning('password for new account: %s is too short' % email)
        messages.append('Your password is too short. '
                        'It must have at least %s characters.' % min_length)
        passlen_ok = False
    else:
        passlen_ok = True

    # check the fuzzy match against the FQDN and email address
    fqdn = socket.getfqdn()
    fqdn_match = UQRatio(password, fqdn)
    email_match = UQRatio(password, email)

    fqdn_ok = fqdn_match < max_match_threshold
    email_ok = email_match < max_match_threshold

    if not fqdn_ok or not email_ok:
        LOGGER.warning('password for new account: %s matches FQDN '
                       '(similarity: %s) or their email address '
                       '(similarity: %s)' % (email, fqdn_match, email_match))
        messages.append('Your password is too similar to either '
                        'the domain name of this LCC-Server or your '
                        'own email address.')

    # next, check if the password is complex enough
    histogram = {}
    for char in password:
        if char.lower() not in histogram:
            histogram[char.lower()] = 1
        else:
            histogram[char.lower()] = histogram[char.lower()] + 1

    hist_ok = True

    for h in histogram:
        if (histogram[h]/len(password)) > 0.2:
            hist_ok = False
            LOGGER.warning('one character is more than '
                           '0.2 x length of the password')
            messages.append(
                'Your password is not complex enough. '
                'One or more characters appear appear too frequently.'
            )
            break

    # check if the password is all numeric
    if password.isdigit():
        numeric_ok = False
        messages.append('Your password cannot be all numbers.')
    else:
        numeric_ok = True

    return (
        (passlen_ok and email_ok and fqdn_ok and hist_ok and numeric_ok),
        messages
    )



def create_new_user(payload,
                    min_pass_length=12,
                    max_similarity=30,
                    raiseonfail=False,
                    override_authdb_path=None):
    '''This makes a new user.

    payload keys: email, password

    Returns the user_id and email if successful.

    The emailverify_sent_datetime is set to the current time. The initial
    account's is_active is set to False and user_role is set to 'locked'.

    The email verification token sent by the frontend expires in 2 hours. If the
    user doesn't get to it by then, they'll have to wait at least 24 hours until
    another one can be sent.

    If the email address already exists in the database, then either the user
    has forgotten that they have an account or someone else is being
    annoying. In this case, if is_active is True, we'll tell the user that we've
    sent an email but won't do anything. If is_active is False and
    emailverify_sent_datetime is at least 24 hours in the past, we'll send a new
    email verification email and update the emailverify_sent_datetime. In this
    case, we'll just tell the user that we've sent the email but won't tell them
    if their account exists.

    Only after the user verifies their email, is_active will be set to True and
    user_role will be set to 'authenticated'.

    '''

    if 'email' not in payload:
        return {'user_added':False,
                'user_email':None,
                'user_id':None,
                'send_verification':False,
                'messages':[]}

    if 'password' not in payload:
        return {'user_added':False,
                'user_email':None,
                'user_id':None,
                'send_verification':False,
                'messages':[]}

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

    input_password = payload['password'][:1024]

    # hash the user's password
    hashed_password = authdb.password_context.hash(input_password)

    # validate the input password to see if it's OK
    # do this here to make sure the password hash completes at least once
    passok, messages = validate_input_password(
        payload['email'],
        input_password,
        min_length=min_pass_length,
        max_match_threshold=max_similarity
    )

    if not passok:
        return {'user_added':False,
                'user_email':None,
                'user_id':None,
                'send_verification':False,
                'messages':messages}

    # insert stuff into the user's table, set is_active = False, user_role =
    # 'locked', the emailverify_sent_datetime to datetime.utcnow()

    try:

        ins = users.insert(
            {'password':hashed_password,
             'email':payload['email'],
             'email_verified':False,
             'is_active':False,
             'emailverify_sent_datetime':datetime.utcnow(),
             'created_on':datetime.utcnow(),
             'user_role':'locked'}
        )

        result = currproc.connection.execute(ins)
        result.close()
        user_added = True

        LOGGER.info('new user created: %s' % payload['email'])

    # this will catch stuff like people trying to sign up again with their email
    # address
    except Exception as e:

        LOGGER.warning('could not create a new user with '
                       'email: %s probably because they exist already'
                       % payload['email'])
        user_added = False


    # get back the user ID
    sel = select([
        users.c.email,
        users.c.user_id,
        users.c.is_active,
        users.c.emailverify_sent_datetime,
    ]).select_from(users).where(
        users.c.email == payload['email']
    )
    result = currproc.connection.execute(sel)
    rows = result.fetchone()
    result.close()

    # if the user was added successfully, tell the frontend all is good and to
    # send a verification email
    if user_added and rows:

        LOGGER.info('new user ID: %s for email: %s, is_active = %s'
                    % (rows['user_id'], rows['email'], rows['is_active']))
        messages.append(
            'User account created. Please verify your email address to log in.'
        )

        return {'user_added':True,
                'user_email':rows['email'],
                'user_id':rows['user_id'],
                'send_verification':True,
                'messages':messages}

    # if the user wasn't added successfully, then they exist in the DB already
    elif (not user_added) and rows:

        LOGGER.warning(
            'attempt to create new user with existing email: %s'
            % payload['email']
        )

        # check the timedelta between now and the emailverify_sent_datetime
        verification_timedelta = (datetime.utcnow() -
                                  rows['emailverify_sent_datetime'])

        # this sets whether we should resend the verification email
        resend_verification = (
            not(rows['is_active']) and
            (verification_timedelta > timedelta(hours=24))
        )
        LOGGER.warning(
            'existing user_id = %s, '
            'is active = %s, '
            'email verification originally sent at = %sZ, '
            'will resend verification = %s' %
            (rows['user_id'],
             rows['is_active'],
             rows['emailverify_sent_datetime'].isoformat(),
             resend_verification)
        )

        messages.append(
            'User account created. Please verify your email address to log in.'
        )
        return {'user_added':True,
                'user_email':rows['email'],
                'user_id':rows['user_id'],
                'send_verification':resend_verification,
                'messages':messages}

    # otherwise, the user wasn't added successfully and they don't already exist
    # in the database so something else went wrong.
    else:

        messages.append(
            'User account created. Please verify your email address to log in.'
        )
        return {'user_added':False,
                'user_email':None,
                'user_id':None,
                'send_verification':False,
                'messages':messages}



def verify_user_email_address(payload,
                              raiseonfail=False,
                              override_authdb_path=None):
    '''This verifies the email address of the user.

    payload must have the following keys: email, user_id

    This is called by the frontend after it verifies that the token challenge to
    verify the user's email succeeded and has not yet expired. This will set the
    user_role to 'authenticated' and the is_active column to True.

    '''

    if 'email' not in payload:
        return None, False, 'locked'
    if 'user_id' not in payload:
        return None, False, 'locked'

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

    # update the table for this user
    upd = users.update(
    ).where(
        users.c.user_id == payload['user_id']
    ).where(
        users.c.is_active == False
    ).where(
        users.c.email == payload['email']
    ).values({
        'is_active':True,
        'email_verified':True,
        'user_role':'authenticated'
    })
    result = currproc.connection.execute(upd)

    sel = select([
        users.c.user_id,
        users.c.is_active,
        users.c.user_role,
    ]).select_from(users).where(
        (users.c.user_id == payload['user_id'])
    )
    result = currproc.connection.execute(sel)
    rows = result.fetchone()

    if rows:
        result.close()
        return rows['user_id'], rows['is_active'], rows['user_role']
    else:
        return payload['user_id'], False, 'locked'



def delete_user(payload,
                raiseonfail=False,
                override_authdb_path=None):
    '''
    This deletes the user.

    This can only be called by the user themselves or the superuser.

    This will immediately invalidate all sessions corresponding to this user.

    Superuser accounts cannot be deleted.

    '''
    if 'email' not in payload:
        return {
            'deleted': False,
            'user_id':None,
            'email':None,
        }
    if 'user_id' not in payload:
        return {
            'deleted':False,
            'user_id':None,
            'email':None
        }

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
    sessions = currproc.table_meta.tables['sessions']

    delete = users.delete().where(
        users.c.user_id == payload['user_id']
    ).where(
        users.c.email == payload['email']
    )
    result = currproc.connection.execute(delete)
    result.close()

    sel = select([
        users.c.user_id,
        users.c.email,
        sessions.c.session_token
    ]).select_from(
        users.join(sessions)
    ).where(
        users.c.user_id == payload['user_id']
    )

    result = currproc.connection.execute(sel)
    rows = result.fetchall()

    if rows and len(rows) > 0:
        return {
            'deleted': False,
            'user_id':payload['user_id'],
            'email':payload['email']
        }
    else:
        return {
            'deleted': True,
            'user_id':payload['user_id'],
            'email':payload['email']
        }



def change_user_password(payload,
                         raiseonfail=False,
                         override_authdb_path=None,
                         min_pass_length=12,
                         max_similarity=30):
    '''This changes the user's password.

    This requires a successful email verification challenge. We'll use this both
    for forgotten passwords and actual password change requests.

    This will immediately invalidate the current session after the password is
    changed so the user has to login with their new password.

    '''
    if 'email' not in payload:
        return {
            'changed':False,
            'user_id':None,
            'email':None,
            'messages':['No email provided.'],
        }
    if 'password' not in payload:
        return {
            'changed':False,
            'user_id':None,
            'email':None,
            'messages':['No new password provided.'],
        }
    if 'user_id' not in payload:
        return {
            'changed':False,
            'user_id':None,
            'email':None,
            'messages':['No user id provided.'],
        }

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

    # verify the new password is OK
    input_password = payload['password'][:1024]

    # hash the user's password
    hashed_password = authdb.password_context.hash(input_password)

    # validate the input password to see if it's OK
    # do this here to make sure the password hash completes at least once
    passok, messages = validate_input_password(
        payload['email'],
        input_password,
        min_length=min_pass_length,
        max_match_threshold=max_similarity
    )

    if passok:

        # update the table for this user
        upd = users.update(
        ).where(
            users.c.user_id == payload['user_id']
        ).where(
            users.c.is_active == True
        ).where(
            users.c.email == payload['email']
        ).values({
            'password': hashed_password
        })
        result = currproc.connection.execute(upd)

        sel = select([
            users.c.password,
        ]).select_from(users).where(
            (users.c.user_id == payload['user_id'])
        )
        result = currproc.connection.execute(sel)
        rows = result.fetchone()
        result.close()

        if rows and rows['password'] == hashed_password:
            messages.append('Password changed successfully.')
            return {
                'changed':True,
                'user_id':payload['user_id'],
                'email':payload['email'],
                'messages':messages
            }

        else:
            messages.append('Password could not be changed.')
            return {
                'changed':False,
                'user_id':payload['user_id'],
                'email':payload['email'],
                'messages':messages
            }

    else:
        messages.append('Password could not be changed.')
        return {
            'changed':False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages': messages
        }


#########################
## PERMISSION HANDLING ##
#########################
