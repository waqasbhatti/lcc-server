#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''actions_session.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive session-related auth actions.

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

import ipaddress
import secrets
import multiprocessing as mp

from sqlalchemy import select

from .. import authdb



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


def auth_session_set_extrainfo(payload,
                               raiseonfail=False,
                               override_authdb_path=None):
    '''This adds info the extra_info_json key of a session column.

    Parameters
    ----------

    payload : dict
        This should contain the following items:

        - session_token : str, the session token to update
        - extra_info : dict, the update dict to put into the extra_info_json

    raiseonfail : bool
        If True, and something goes wrong, this will raise an Exception instead
        of returning normally with a failure condition.

    override_authdb_path : str or None
        The SQLAlchemy database URL to use if not using the default auth DB.

    Returns
    -------

    dict
        Returns a dict containing the new session info dict.

    '''

    if 'session_token' not in payload:
        LOGGER.error('no session token provided')

        return {
            'success':False,
            'session_info':None,
            'messages':["No session token provided."],
        }
    if 'extra_info' not in payload:
        LOGGER.error('no extra info provided')

        return {
            'success':False,
            'session_info':None,
            'messages':["No extra info provided."],
        }

    session_token = payload['session_token']
    extra_info = payload['extra_info']

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

        upd = sessions.update(
        ).where(
            sessions.c.session_token == session_token
        ).values({'extra_info_json':extra_info})
        result = currproc.connection.execute(upd)

        s = select([
            sessions.c.session_token,
            sessions.c.ip_address,
            sessions.c.client_header,
            sessions.c.created,
            sessions.c.expires,
            sessions.c.extra_info_json
        ]).select_from(sessions).where(
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
                'messages':["Session extra_info update successful."],
            }

        except Exception as e:

            return {
                'success':False,
                'session_info':None,
                'messages':["Session extra_info update failed."],
            }

    except Exception as e:

        LOGGER.warning('Session token not found or '
                       'could not check if it exists')

        return {
            'success':False,
            'session_info':None,
            'messages':["Session extra_info update failed."],
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

def auth_password_check(payload,
                        override_authdb_path=None,
                        raiseonfail=False):
    '''This runs a password check given a session token and password.

    Used to gate high-security areas or operations that require re-verification
    of the password for a user's existing session.

    Parameters
    ----------

    payload : dict
        This is a dict containing the following items:

        - session_token
        - password

    override_authdb_path : str or None
        The SQLAlchemy database URL to use if not using the default auth DB.

    raiseonfail : bool
        If True, and something goes wrong, this will raise an Exception instead
        of returning normally with a failure condition.

    Returns
    -------

    dict
        Returns a dict containing the result of the password verification check.

    '''

    # check broken request
    request_ok = True
    for item in ('password', 'session_token'):
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

        return {
            'success':False,
            'user_id':None,
            'messages':['Invalid password verification request.']
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
            ]).select_from(
                users
            ).where(users.c.user_id == session_info['session_info']['user_id'])
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
                        'messages':["Verification successful."]
                    }

                # if the user account is locked, return a failure
                else:

                    return {
                        'success':False,
                        'user_id': user_info['user_id'],
                        'messages':["Sorry, that user ID and "
                                    "password combination didn't work."]
                    }



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
            ]).select_from(users).where(
                users.c.email == payload['email']
            ).where(
                users.c.is_active == True
            ).where(
                users.c.email_verified == True
            )
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
