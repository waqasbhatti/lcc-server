#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''actions_user.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive user account related auth actions.

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
import socket

from tornado.escape import squeeze
from sqlalchemy import select, desc
from fuzzywuzzy.fuzz import UQRatio

from .. import authdb
from .session import auth_session_exists


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
        users.c.user_role
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

    if row['user_role'] == 'superuser':
        LOGGER.error(
            "Can't delete superusers."
        )
        return {
            'success': False,
            'user_id':payload['user_id'],
            'email':payload['email'],
            'messages':["Can't delete superusers."]
        }

    # delete the user
    delete = users.delete().where(
        users.c.user_id == payload['user_id']
    ).where(
        users.c.email == payload['email']
    ).where(
        users.c.user_role != 'superuser'
    )
    result = currproc.connection.execute(delete)
    result.close()

    # delete the sessions as well
    delete = sessions.delete().where(
        sessions.c.user_id == payload['user_id']
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
