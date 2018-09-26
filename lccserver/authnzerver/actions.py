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
import json

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

import ipaddress
import secrets
import multiprocessing as mp
import socket
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
import smtplib
import time

from tornado.escape import squeeze
from sqlalchemy import select
from fuzzywuzzy.fuzz import UQRatio

from . import authdb



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

            return {
                'success':False,
                'session_token':None,
                'expires':None,
                'messages':["Invalid session initiation request. "
                            "Missing some parameters."]
            }

    try:

        validated_ip = str(ipaddress.ip_address(payload['ip_address']))
        payload['ip_address'] = validated_ip

        # set the userid to anonuser@localhost if no user is provided
        if not payload['user_id']:
            payload['user_id'] = 2

        # check if the payload expires key is a string and not a datetime.time
        # and reform it to a datetime if necessary
        if isinstance(payload['expires'],str):

            # this is assuming UTC
            payload['expires'] = datetime.strptime(
                payload['expires'].replace('Z',''),
                '%Y-%m-%dT%H:%M:%S.%f'
            )

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

        return {
            'success':True,
            'session_token':session_token,
            'expires':payload['expires'].isoformat(),
            'messages':["Generated session_token successfully. "
                        "Session initiated."]
        }

    except Exception as e:
        LOGGER.exception('could not create a new session')

        if raiseonfail:
            raise

        return {
            'success':False,
            'session_token':None,
            'expires':None,
            'messages':["Could not create a new session."],
        }



def auth_session_exists(payload,
                        raiseonfail=False,
                        override_authdb_path=None):
    '''
    This checks if the provided session token exists.

    Request payload keys required:

    session_token

    Returns:

    All sessions columns for session_key if token exists and has not expired.
    None if token has expired. Will also call auth_session_delete.

    '''

    if 'session_token' not in payload:
        LOGGER.error('no session token provided')

        return {
            'success':False,
            'session_info':None,
            'messages':["No session token provided."],
        }

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

            return {
                'success':True,
                'session_info':serialized_result,
                'messages':["Session look up successful."],
            }

        except Exception as e:

            return {
                'success':False,
                'session_info':None,
                'messages':["Session look up failed."],
            }

    except Exception as e:

        LOGGER.warning('session token not found or '
                       'could not check if it exists')

        return {
            'success':False,
            'session_info':None,
            'messages':["Session look up failed."],
        }



def auth_session_delete(payload,
                        raiseonfail=False,
                        override_authdb_path=None):
    '''
    This removes a session token.

    Request payload keys required:

    session_token

    Returns:

    Deletes the row corresponding to the session_key in the sessions table.

    '''

    if 'session_token' not in payload:
        LOGGER.error('no session token provided')

        return {
            'success':False,
            'messages':["No session token provided."],
        }

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

        return {
            'success':True,
            'messages':["Session deleted successfully."],
        }

    except Exception as e:

        LOGGER.exception('could not delete the session')

        if raiseonfail:
            raise

        return {
            'success':False,
            'messages':["Session could not be deleted."],
        }


def auth_kill_old_sessions(
        session_expiry_days=7,
        raiseonfail=False,
        override_authdb_path=None
):
    '''
    This kills all expired sessions.

    payload is:

    {'session_expiry_days': session older than this number will be removed}

    '''

    expires_days = session_expiry_days
    earliest_date = datetime.utcnow() - timedelta(days=expires_days)

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

    sel = select(
        [sessions.c.session_token,
         sessions.c.created,
         sessions.c.expires]
    ).select_from(
        sessions
    ).where(sessions.c.expires < earliest_date)

    result = currproc.connection.execute(sel)
    rows = result.fetchall()
    result.close()

    if len(rows) > 0:

        LOGGER.warning('will kill %s sessions older than %sZ' %
                       (len(rows), earliest_date.isoformat()))

        delete = sessions.delete().where(
            sessions.c.expires < earliest_date
        )
        result = currproc.connection.execute(delete)
        result.close()

        return {
            'success':True,
            'messages':["%s sessions older than %sZ deleted." %
                        (len(rows),
                         earliest_date.isoformat())]
        }

    else:

        LOGGER.warning(
            'no sessions older than %sZ found to delete. returning...' %
            earliest_date.isoformat()
        )
        return {
            'success':False,
            'messages':['no sessions older than %sZ found to delete' %
                        earliest_date.isoformat()]
        }




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

    # if it isn't, then hash the dummy user's password twice
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

        return {
            'success':False,
            'user_id':None,
            'messages':['No session token provided.']
        }

    # otherwise, now we'll check if the session exists
    else:

        session_info = auth_session_exists(
            {'session_token':payload['session_token']},
            raiseonfail=raiseonfail,
            override_authdb_path=override_authdb_path
        )

        # if it doesn't, hash the dummy password twice
        if not session_info['success']:

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
            auth_session_delete(
                {'session_token':'nope'},
                raiseonfail=raiseonfail,
                override_authdb_path=override_authdb_path
            )

            return {
                'success':False,
                'user_id':None,
                'messages':['No session token provided.']
            }

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
                    payload['password'][:1024],
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

                return {
                    'success':False,
                    'user_id':None,
                    'messages':["Sorry, that user ID and "
                                "password combination didn't work."]
                }

            # if password verification succeeeded, check if the user can
            # actually log in (i.e. their account is not locked or is not
            # inactive)
            else:

                # if the user account is active and unlocked, proceed.
                # the frontend will take this user_id and ask for a new session
                # token with it.
                if (user_info['is_active'] and
                    user_info['user_role'] != 'locked'):

                    return {
                        'success':True,
                        'user_id': user_info['user_id'],
                        'messages':["Login successful."]
                    }

                # if the user account is locked, return a failure
                else:

                    return {
                        'success':False,
                        'user_id': user_info['user_id'],
                        'messages':["Sorry, that user ID and "
                                    "password combination didn't work."]
                    }



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

    if session['success']:

        # check the user ID
        if payload['user_id'] == session['session_info']['user_id']:

            deleted = auth_session_delete(
                payload,
                override_authdb_path=override_authdb_path,
                raiseonfail=raiseonfail
            )

            if deleted['success']:

                return {
                    'success':True,
                    'user_id': session['session_info']['user_id'],
                    'messages':["Logout successful."]
                }

            else:

                LOGGER.error(
                    'something went wrong when '
                    'trying to remove session ID: %s, '
                    'for user ID: %s' % (payload['session_token'],
                                         payload['user_id'])
                )
                return {
                    'success':False,
                    'user_id':payload['user_id'],
                    'messages':["Logout failed. Invalid "
                                "session_token for user_id."]
                }

        else:
            LOGGER.error(
                'tried to log out but session token = %s '
                'and user_id = %s do not match. '
                'expected user_id = %s' % (payload['session_token'],
                                           payload['user_id'],
                                           session['user_id']))
            return {
                'success':False,
                'user_id':payload['user_id'],
                'messages':["Logout failed. Invalid session_token for user_id."]
            }

    else:

        return {
            'success':False,
            'user_id':payload['user_id'],
            'messages':["Logout failed. Invalid "
                        "session_token for user_id."]
        }



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



#######################
## PASSWORD HANDLING ##
#######################

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



def change_user_password(payload,
                         raiseonfail=False,
                         override_authdb_path=None,
                         min_pass_length=12,
                         max_similarity=30):
    '''This changes the user's password.

    '''
    if 'user_id' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':['Invalid password change request. '
                        'Some args are missing.'],
        }
    if 'email' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':['Invalid password change request. '
                        'Some args are missing.'],
        }
    if 'current_password' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':['Invalid password change request. '
                        'Some args are missing.'],
        }
    if 'new_password' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':['Invalid password change request. '
                        'Some args are missing.'],
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

    # get the current password
    sel = select([
        users.c.password,
    ]).select_from(users).where(
        (users.c.user_id == payload['user_id'])
    )
    result = currproc.connection.execute(sel)
    rows = result.fetchone()
    result.close()

    current_password = payload['current_password'][:1024]
    new_password = payload['new_password'][:1024]

    pass_check = authdb.password_context.verify(current_password,
                                                rows['password'])

    if not pass_check:
        return {
            'success':False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':['Your current password did '
                        'not match the stored password.']
        }

    # check if the new hashed password is the same as the old hashed password,
    # meaning that the new password is just the old one
    same_check = authdb.password_context.verify(new_password,
                                                rows['password'])
    if same_check:
        return {
            'success':False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':['Your new password cannot '
                        'be the same as your old password.']
        }

    # hash the user's password
    hashed_password = authdb.password_context.hash(new_password)

    # validate the input password to see if it's OK
    # do this here to make sure the password hash completes at least once
    # verify the new password is OK
    passok, messages = validate_input_password(
        payload['email'],
        new_password,
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
                'success':True,
                'user_id':payload['user_id'],
                'email':payload['email'],
                'messages':messages
            }

        else:
            messages.append('Password could not be changed.')
            return {
                'success':False,
                'user_id':payload['user_id'],
                'email':payload['email'],
                'messages':messages
            }

    else:
        messages.append("The new password you entered is insecure. "
                        "It must be at least 12 characters long and "
                        "be sufficiently complex.")
        return {
            'success':False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages': messages
        }



###################
## USER HANDLING ##
###################

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
        return {
            'success':False,
            'user_email':None,
            'user_id':None,
            'send_verification':False,
            'messages':["Invalid user creation request."]
        }

    if 'password' not in payload:
        return {
            'success':False,
            'user_email':None,
            'user_id':None,
            'send_verification':False,
            'messages':["Invalid user creation request."]
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
        return {
            'success':False,
            'user_email':payload['email'],
            'user_id':None,
            'send_verification':False,
            'messages':messages
        }

    # insert stuff into the user's table, set is_active = False, user_role =
    # 'locked', the emailverify_sent_datetime to datetime.utcnow()

    try:

        ins = users.insert({
            'password':hashed_password,
            'email':payload['email'],
            'email_verified':False,
            'is_active':False,
            'emailverify_sent_datetime':datetime.utcnow(),
            'created_on':datetime.utcnow(),
            'user_role':'locked',
            'last_updated':datetime.utcnow(),
        })

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

        return {
            'success':True,
            'user_email':rows['email'],
            'user_id':rows['user_id'],
            'send_verification':True,
            'messages':messages
        }

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
        return {
            'success':False,
            'user_email':rows['email'],
            'user_id':rows['user_id'],
            'send_verification':resend_verification,
            'messages':messages
        }

    # otherwise, the user wasn't added successfully and they don't already exist
    # in the database so something else went wrong.
    else:

        messages.append(
            'User account created. Please verify your email address to log in.'
        )
        return {
            'success':False,
            'user_email':None,
            'user_id':None,
            'send_verification':False,
            'messages':messages
        }



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
            'success': False,
            'user_id':None,
            'email':None,
            'messages':["Invalid user deletion request."],
        }
    if 'user_id' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':["Invalid user deletion request."],
        }
    if 'password' not in payload:
        return {
            'success':False,
            'user_id':None,
            'email':None,
            'messages':["Invalid user deletion request."],
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

    # check if the incoming email address actually belongs to the user making
    # the request
    sel = select([
        users.c.user_id,
        users.c.email,
        users.c.password,
    ]).select_from(
        users
    ).where(
        users.c.user_id == payload['user_id']
    )
    result = currproc.connection.execute(sel)
    row = result.fetchone()

    if (not row) or (row['email'] != payload['email']):

        return {
            'success': False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':["We could not verify your email address or password."]
        }

    # check if the user's password is valid and matches the one on record
    pass_ok = authdb.password_context.verify(
        payload['password'][:1024],
        row['password']
    )

    if not pass_ok:
        return {
            'success': False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':["We could not verify your email address or password."]
        }


    delete = users.delete().where(
        users.c.user_id == payload['user_id']
    ).where(
        users.c.email == payload['email']
    ).where(
        users.c.user_role != 'superuser'
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
            'success': False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':["Could not delete user from DB."]
        }
    else:
        return {
            'success': True,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':["User successfully deleted from DB."]
        }



####################
## SENDING EMAILS ##
####################

SIGNUP_VERIFICATION_EMAIL_SUBJECT = (
    '[LCC-Server] Please verify your account sign up request'
)
SIGNUP_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the LCC-Server at: {lccserver_baseurl}.

We received an account sign up request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {lccserver_baseurl}/users/verify

to verify that you initiated this request. This code will expire in 15
minutes. You will also need to enter your email address and password
to log in.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
LCC-Server admins
{lccserver_baseurl}
'''


FORGOTPASS_VERIFICATION_EMAIL_SUBJECT = (
    '[LCC-Server] Please verify your password reset request'
)
FORGOTPASS_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the LCC-Server at: {lccserver_baseurl}.

We received a password reset request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {lccserver_baseurl}/users/forgot-password-step2

to verify that you initiated this request. This code will expire in 15
minutes.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
LCC-Server admins
{lccserver_baseurl}
'''


CHANGEPASS_VERIFICATION_EMAIL_SUBJECT = (
    '[LCC-Server] Please verify your password change request'
)
CHANGEPASS_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the LCC-Server at: {lccserver_baseurl}.

We received a password change request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {lccserver_baseurl}/users/password-change

to verify that you initiated this request. This code will expire in 15
minutes.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
LCC-Server admins
{lccserver_baseurl}
'''


def authnzerver_send_email(
        sender,
        subject,
        text,
        recipients,
        server,
        user,
        password,
        port=587
):
    '''
    This is a utility function to send email.

    '''

    msg = MIMEText(text)
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Message-Id'] = make_msgid()
    msg['Subject'] = subject
    msg['Date'] = formatdate(time.time())

    # next, we'll try to login to the SMTP server
    try:

        server = smtplib.SMTP(server, port)
        server_ehlo_response = server.ehlo()

        if server.has_extn('STARTTLS'):

            try:

                tls_start_response = server.starttls()
                tls_ehlo_response = server.ehlo()

                login_response = server.login(
                    user,
                    password
                )

                send_response = server.sendmail(
                    sender,
                    recipients,
                    msg.as_string()
                )

                quit_response = server.quit()
                return True

            except Exception as e:

                LOGGER.exception(
                    "could not send the email to %s, "
                    "subject: %s because of an exception"
                    % (recipients, subject)
                )
                quit_response = server.quit()
                return False
        else:

            LOGGER.error('email server: %s does not support TLS, '
                         'will not send an email.' % server)
            quit_response = server.quit()
            return False

    except Exception as e:

        LOGGER.exception(
            "could not send the email to %s, "
            "subject: %s because of an exception"
            % (recipients, subject)
        )
        quit_response = server.quit()
        return False



def send_signup_verification_email(payload,
                                   raiseonfail=False,
                                   override_authdb_path=None):
    '''This actually sends the verification email.

    The payload must contain:

    - the email_address
    - the current session token
    - the output dict from the create_new_user function as created_info

    - the username, password, address, and port for an SMTP server to use
      (these should be set in the site.json file from the frontend and the
      frontend should pass these to us)

    - a time-stamped Fernet verification token that is set to expire in 1 hour
      in payload['fernet_verification_token'].

    '''

    for key in ('email_address',
                'lccserver_baseurl',
                'session_token',
                'fernet_verification_token',
                'smtp_sender',
                'smtp_user',
                'smtp_pass',
                'smtp_server',
                'smtp_port',
                'created_info'):

        if key not in payload:
            return {
                'success':False,
                'user_id':None,
                'email_address':None,
                'verifyemail_sent_datetime':None,
                'messages':([
                    "Invalid verification email request."
                ])
            }

    # check if we don't need to send an email to this user
    if payload['created_info']['send_verification'] is False:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Not allowed to send an email verification request."
            ])
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

    # first, we'll verify the user was created successfully, their account is
    # currently set to inactive and their role is 'locked'. then, we'll verify
    # if the session token provided exists and get the IP address and the
    # browser identifier out of it.
    # look up the provided user
    user_sel = select([
        users.c.user_id,
        users.c.email,
        users.c.is_active,
        users.c.user_role,
    ]).select_from(users).where(users.c.email == payload['email_address'])
    user_results = currproc.connection.execute(user_sel)
    user_info = user_results.fetchone()
    user_results.close()

    if not user_info:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Invalid verification email request."
            ])
        }

    if user_info['is_active'] or user_info['user_role'] != 'locked':

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Not sending an email verification request to an existing user."
            ])
        }

    # check if the email address we're supposed to send to is the same as the
    # one for the user
    if payload['email_address'] != user_info['email']:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Not sending an email verification "
                "request to a nonexistent user."
            ])
        }


    # check the session
    session_info = auth_session_exists(
        {'session_token':payload['session_token']},
        raiseonfail=raiseonfail,
        override_authdb_path=override_authdb_path
    )

    if not session_info['success']:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Invalid verification email request."
            ])
        }

    # get the IP address and browser ID from the session
    ip_addr = session_info['session_info']['ip_address']
    browser = session_info['session_info']['client_header']

    # TODO: we'll use geoip to get the location of the person who initiated the
    # request.

    # generate the email message
    msgtext = SIGNUP_VERIFICATION_EMAIL_TEMPLATE.format(
        lccserver_baseurl=payload['lccserver_baseurl'],
        verification_code=payload['fernet_verification_token'],
        browser_identifier=browser.replace('_','.'),
        ip_address=ip_addr,
        user_email=payload['email_address'],
    )
    sender = 'LCC-Server admin <%s>' % payload['smtp_sender']
    recipients = [user_info['email']]

    # send the email
    email_sent = authnzerver_send_email(
        sender,
        SIGNUP_VERIFICATION_EMAIL_SUBJECT,
        msgtext,
        recipients,
        payload['smtp_server'],
        payload['smtp_user'],
        payload['smtp_pass'],
        port=payload['smtp_port']
    )

    if email_sent:

        emailverify_sent_datetime = datetime.utcnow()

        # finally, we'll update the users table with the actual
        # verifyemail_sent_datetime if sending succeeded.
        upd = users.update(
        ).where(
            users.c.user_id == payload['created_info']['user_id']
        ).where(
            users.c.is_active == False
        ).where(
            users.c.email == payload['created_info']['user_email']
        ).values({
            'emailverify_sent_datetime': emailverify_sent_datetime,
        })
        result = currproc.connection.execute(upd)
        result.close()

        return {
            'success':True,
            'user_id':user_info['user_id'],
            'email_address':user_info['email'],
            'verifyemail_sent_datetime':emailverify_sent_datetime,
            'messages':([
                "Email verification request sent successfully to %s"
                % recipients
            ])
        }

    else:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Could not send email to %s for the user verification request."
                % recipients
            ])
        }



def verify_user_email_address(payload,
                              raiseonfail=False,
                              override_authdb_path=None):
    '''This verifies the email address of the user.

    payload must have the following keys: email

    This is called by the frontend after it verifies that the token challenge to
    verify the user's email succeeded and has not yet expired. This will set the
    user_role to 'authenticated' and the is_active column to True.

    '''

    if 'email' not in payload:

        return {
            'success':False,
            'user_id':None,
            'is_active': False,
            'user_role':'locked',
            'messages':["Invalid email verification request."]
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

    # update the table for this user
    upd = users.update(
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
        (users.c.email == payload['email'])
    )
    result = currproc.connection.execute(sel)
    rows = result.fetchone()
    result.close()

    if rows:

        return {
            'success':True,
            'user_id':rows['user_id'],
            'is_active':rows['is_active'],
            'user_role':rows['user_role'],
            'messages':["Email verification request succeeded."]
        }

    else:

        return {
            'success':False,
            'user_id':payload['user_id'],
            'is_active':False,
            'user_role':'locked',
            'messages':["Email verification request failed."]
        }


##############################
## FORGOT PASSWORD HANDLING ##
##############################

def send_forgotpass_verification_email(payload,
                                       raiseonfail=False,
                                       override_authdb_path=None):
    '''This actually sends the verification email.

    The payload must contain:

    - the email_address
    - the session_token

    - the username, password, address, and port for an SMTP server to use
      (these should be set in the site.json file from the frontend and the
      frontend should pass these to us)

    - a time-stamped Fernet verification token that is set to expire in 1 hour
      in payload['fernet_verification_token'].

    '''

    for key in ('email_address',
                'fernet_verification_token',
                'lccserver_baseurl',
                'session_token',
                'smtp_sender',
                'smtp_user',
                'smtp_pass',
                'smtp_server',
                'smtp_port'):

        if key not in payload:
            return {
                'success':False,
                'user_id':None,
                'email_address':None,
                'forgotemail_sent_datetime':None,
                'messages':([
                    "Invalid verification email request."
                ])
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
    user_sel = select([
        users.c.user_id,
        users.c.email,
        users.c.is_active,
        users.c.user_role,
        users.c.emailforgotpass_sent_datetime,
    ]).select_from(users).where(
        users.c.email == payload['email_address']
    ).where(
        users.c.is_active == True
    ).where(
        users.c.user_role != 'locked'
    ).where(
        users.c.user_role != 'anonymous'
    )
    user_results = currproc.connection.execute(user_sel)
    user_info = user_results.fetchone()
    user_results.close()

    if not user_info:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'forgotemail_sent_datetime':None,
            'messages':([
                "Invalid password reset email request."
            ])
        }

    # see if the user is not locked or inactive
    if not (user_info['is_active'] and user_info['user_role'] != 'locked'):

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'forgotemail_sent_datetime':None,
            'messages':([
                "Invalid password reset email request."
            ])
        }

    # check the last time we sent a forgot password email to this user
    if user_info['emailforgotpass_sent_datetime'] is not None:

        check_elapsed = (
            datetime.utcnow() - user_info['emailforgotpass_sent_datetime']
        ) > timedelta(hours=24)

        if check_elapsed:
            send_email = True
        else:
            send_email = False

    # if we've never sent a forgot-password email before, it's OK to send it
    else:
        send_email = True

    if not send_email:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'forgotemail_sent_datetime':None,
            'messages':([
                "Invalid password reset email request."
            ])
        }


    # check the session
    session_info = auth_session_exists(
        {'session_token':payload['session_token']},
        raiseonfail=raiseonfail,
        override_authdb_path=override_authdb_path
    )

    if not session_info['success']:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Invalid verification email request."
            ])
        }


    #
    # finally! we'll process the email sending request
    #

    # get the IP address and browser ID from the session
    ip_addr = session_info['session_info']['ip_address']
    browser = session_info['session_info']['client_header']

    # TODO: we'll use geoip to get the location of the person who initiated the
    # request.

    # generate the email message
    msgtext = FORGOTPASS_VERIFICATION_EMAIL_TEMPLATE.format(
        lccserver_baseurl=payload['lccserver_baseurl'],
        verification_code=payload['fernet_verification_token'],
        browser_identifier=browser.replace('_','.'),
        ip_address=ip_addr,
        user_email=payload['email_address'],
    )
    sender = 'LCC-Server admin <%s>' % payload['smtp_sender']
    recipients = [user_info['email']]

    # send the email
    email_sent = authnzerver_send_email(
        sender,
        FORGOTPASS_VERIFICATION_EMAIL_SUBJECT,
        msgtext,
        recipients,
        payload['smtp_server'],
        payload['smtp_user'],
        payload['smtp_pass'],
        port=payload['smtp_port']
    )

    if email_sent:

        emailforgotpass_sent_datetime = datetime.utcnow()

        # finally, we'll update the users table with the actual
        # verifyemail_sent_datetime if sending succeeded.
        upd = users.update(
        ).where(
            users.c.is_active == True
        ).where(
            users.c.email == payload['email_address']
        ).values({
            'emailforgotpass_sent_datetime': emailforgotpass_sent_datetime,
        })
        result = currproc.connection.execute(upd)
        result.close()

        return {
            'success':True,
            'user_id':user_info['user_id'],
            'email_address':user_info['email'],
            'forgotemail_sent_datetime':emailforgotpass_sent_datetime,
            'messages':([
                "Password reset request sent successfully to %s"
                % recipients
            ])
        }

    else:

        return {
            'success':False,
            'user_id':None,
            'email_address':None,
            'verifyemail_sent_datetime':None,
            'messages':([
                "Could not send email to %s for the user password reset request."
                % recipients
            ])
        }



def verify_password_reset(payload,
                          raiseonfail=False,
                          override_authdb_path=None,
                          min_pass_length=12,
                          max_similarity=30):
    '''
    This verifies a password reset request.

    '''
    if 'email_address' not in payload:

        return {
            'success':False,
            'messages':["Email not provided."]
        }

    if 'new_password' not in payload:

        return {
            'success':False,
            'messages':["Password not provided."]
        }

    if 'session_token' not in payload:

        return {
            'success':False,
            'messages':["Session token not provided."]
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

    # check the session
    session_info = auth_session_exists(
        {'session_token':payload['session_token']},
        raiseonfail=raiseonfail,
        override_authdb_path=override_authdb_path
    )

    if not session_info['success']:

        return {
            'success':False,
            'messages':([
                "Invalid session token for password reset request."
            ])
        }

    sel = select([
        users.c.user_id,
        users.c.email,
        users.c.password,
    ]).select_from(
        users
    ).where(
        users.c.email == payload['email_address']
    )

    result = currproc.connection.execute(sel)
    user_info = result.fetchone()
    result.close()

    if not user_info or len(user_info) == 0:

        return {
            'success':False,
            'messages':([
                "Invalid user for password reset request."
            ])
        }

    # let's hash the new password against the current password
    new_password = payload['new_password'][:1024]

    pass_same = authdb.password_context.verify(
        new_password,
        user_info['password']
    )

    # don't fail here, but note that the user is re-using the password they
    # forgot. FIXME: should we actually fail here?
    if pass_same:
        LOGGER.warning('user %s is re-using their '
                       'password that they ostensibly forgot' %
                       user_info['email_address'])

    # hash the user's password
    hashed_password = authdb.password_context.hash(new_password)

    # validate the input password to see if it's OK
    # do this here to make sure the password hash completes at least once
    passok, messages = validate_input_password(
        payload['email_address'],
        new_password,
        min_length=min_pass_length,
        max_match_threshold=max_similarity
    )

    if not passok:

        return {
            'success':False,
            'messages':([
                "Invalid password for password reset request."
            ])
        }

    # if the password passes validation, hash it and store it
    else:

        # update the table for this user
        upd = users.update(
        ).where(
            users.c.user_id == user_info['user_id']
        ).where(
            users.c.is_active == True
        ).where(
            users.c.email == payload['email_address']
        ).values({
            'password': hashed_password
        })
        result = currproc.connection.execute(upd)

        sel = select([
            users.c.password,
        ]).select_from(users).where(
            (users.c.email == payload['email_address'])
        )
        result = currproc.connection.execute(sel)
        rows = result.fetchone()
        result.close()

        if rows and rows['password'] == hashed_password:
            messages.append('Password changed successfully.')
            return {
                'success':True,
                'messages':messages
            }

        else:
            messages.append('Password could not be changed.')
            return {
                'success':False,
                'messages':messages
            }

#########################
## PERMISSION HANDLING ##
#########################



######################
## API KEY HANDLING ##
######################

def issue_new_apikey(payload,
                     raiseonfail=False,
                     override_authdb_path=None):
    '''This issues a new API key.

    payload must have the following keys:

    user_id, user_role, expires_days, ip_address, client_header, session_token

    API keys are tied to an IP address and client header combination. This will
    return an signed and encrypted Fernet API key that contains the user_id,
    random token, IP address, client header, and expiry date in ISO
    format. We'll then be able to use this info to verify the API key without
    hitting the database all the time.

    '''

    for key in ('user_id',
                'user_role',
                'expires_days',
                'ip_address',
                'client_header',
                'session_token',
                'apiversion'):

        if key not in payload:
            return {
                'success':False,
                'apikey':None,
                'expires':None,
                'messages':["Some required keys are missing from payload."]
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

    # check the session
    session_info = auth_session_exists(
        {'session_token':payload['session_token']},
        raiseonfail=raiseonfail,
        override_authdb_path=override_authdb_path
    )

    if not session_info['success']:

        return {
            'success':False,
            'apikey':None,
            'expires':None,
            'messages':([
                "Invalid session token for password reset request."
            ])
        }

    session = session_info['session_info']

    # check if the session info matches what we have in the payload
    session_ok = (
        (session['user_id'] == payload['user_id']) and
        (session['ip_address'] == payload['ip_address']) and
        (session['client_header'] == payload['client_header']) and
        (session['user_role'] == payload['user_role'])
    )

    if not session_ok:

        return {
            'success':False,
            'apikey':None,
            'expires':None,
            'messages':([
                "DB session user_id, ip_address, client_header, "
                "user_role does not match provided session info."
            ])
        }

    #
    # finally, generate the API key
    #
    random_token = secrets.token_urlsafe(32)

    # we'll return this API key dict to the frontend so it can JSON dump it,
    # encode to bytes, then encrypt, then sign it, and finally send back to the
    # client
    issued = datetime.utcnow()
    expires = datetime.utcnow() + timedelta(days=payload['expires_days'])

    apikey_dict = {
        'ver':payload['apiversion'],
        'uid':payload['user_id'],
        'rol':payload['user_role'],
        'clt':payload['client_header'],
        'ipa':payload['ip_address'],
        'tkn':random_token,
        'iat':issued.isoformat(),
        'exp':expires.isoformat()
    }
    apikey_json = json.dumps(apikey_dict)

    # we'll also store this dict in the apikeys table
    apikeys = currproc.table_meta.tables['apikeys']

    # NOTE: we store only the random token. this will later be checked for
    # equality against the value stored in the API key dict['tkn'] when we send
    # in this API key for verification later
    ins = apikeys.insert({
        'apikey':random_token,
        'issued':issued,
        'expires':expires,
        'user_id':payload['user_id'],
        'session_token':payload['session_token'],
    })

    result = currproc.connection.execute(ins)
    result.close()

    #
    # return the API key to the frontend
    #
    messages = (
        "API key generated successfully for user_id = %s, expires: %s." %
        (payload['user_id'],
         expires.isoformat())
    )

    return {
        'success':True,
        'apikey':apikey_json,
        'expires':expires.isoformat(),
        'messages':([
            messages
        ])
    }



def verify_apikey(payload,
                  raiseonfail=False,
                  override_authdb_path=None):
    '''This checks if an API key is valid.

    payload requires the following keys:

    apikey dict, user_id, user_role, expires_days, ip_address, client_header,
    apiversion

    '''
    if 'apikey_dict' not in payload:
        return {
            'success':False,
            'messages':["Some required keys are missing from payload."]
        }
    apikey_dict = payload['apikey_dict']

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

    apikeys = currproc.table_meta.tables['apikeys']
    sel = select([
        apikeys.c.apikey,
        apikeys.c.expires,
    ]).select_from(apikeys).where(
        apikeys.c.apikey == apikey_dict['tkn']
    ).where(
        apikeys.c.user_id == apikey_dict['uid']
    )
    result = currproc.connection.execute(sel)
    row = result.fetchone()
    result.close()

    if row is not None and len(row) != 0:

        return {
            'success':True,
            'messages':[(
                "API key verified successfully. Expires: %s." %
                row['expires'].isoformat()
            )]
        }

    else:

        return {
            'success':False,
            'messages':[(
                "API key could not be verified."
            )]
        }
