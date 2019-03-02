#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''actions_email.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive email-related auth actions.

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
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
import smtplib
import time

from sqlalchemy import select

from .. import authdb
from .session import auth_session_exists



####################
## SENDING EMAILS ##
####################

SIGNUP_VERIFICATION_EMAIL_SUBJECT = (
    '[{server_name}] Please verify your account sign up request'
)
SIGNUP_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the {server_name} at: {server_baseurl}.

We received an account sign up request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {server_baseurl}/users/verify

to verify that you initiated this request. This code will expire in 15
minutes. You will also need to enter your email address and password
to log in.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
{server_name} admins
{server_baseurl}
'''


FORGOTPASS_VERIFICATION_EMAIL_SUBJECT = (
    '[{server_name}] Please verify your password reset request'
)
FORGOTPASS_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the {server_name} at: {server_baseurl}.

We received a password reset request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {server_baseurl}/users/forgot-password-step2

to verify that you initiated this request. This code will expire in 15
minutes.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
{server_name} admins
{server_baseurl}
'''


CHANGEPASS_VERIFICATION_EMAIL_SUBJECT = (
    '[{server_name}] Please verify your password change request'
)
CHANGEPASS_VERIFICATION_EMAIL_TEMPLATE = '''\
Hello,

This is an automated message from the {server_name} at: {server_baseurl}.

We received a password change request for: {user_email}. This request
was initiated using the browser:

{browser_identifier}

from the IP address: {ip_address}.

Please enter this code:

{verification_code}

into the verification form at: {server_baseurl}/users/password-change

to verify that you initiated this request. This code will expire in 15
minutes.

If you do not recognize the browser and IP address above or did not
initiate this request, someone else may have used your email address
in error. Feel free to ignore this email.

Thanks,
{server_name} admins
{server_baseurl}
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
        server.ehlo()

        if server.has_extn('STARTTLS'):

            try:

                server.starttls()
                server.ehlo()

                server.login(
                    user,
                    password
                )

                server.sendmail(
                    sender,
                    recipients,
                    msg.as_string()
                )

                server.quit()
                return True

            except Exception as e:

                LOGGER.exception(
                    "could not send the email to %s, "
                    "subject: %s because of an exception"
                    % (recipients, subject)
                )
                server.quit()
                return False
        else:

            LOGGER.error('email server: %s does not support TLS, '
                         'will not send an email.' % server)
            server.quit()
            return False

    except Exception as e:

        LOGGER.exception(
            "could not send the email to %s, "
            "subject: %s because of an exception"
            % (recipients, subject)
        )
        server.quit()
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
                'server_baseurl',
                'server_name',
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
        server_baseurl=payload['server_baseurl'],
        server_name=payload['server_name'],
        verification_code=payload['fernet_verification_token'],
        browser_identifier=browser.replace('_','.'),
        ip_address=ip_addr,
        user_email=payload['email_address'],
    )
    sender = '%s admin <%s>' % (payload['server_name'],
                                payload['smtp_sender'])
    recipients = [user_info['email']]

    # send the email
    email_sent = authnzerver_send_email(
        sender,
        SIGNUP_VERIFICATION_EMAIL_SUBJECT.format(
            server_name=payload['server_name']
        ),
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
                'server_baseurl',
                'server_name',
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
        server_baseurl=payload['server_baseurl'],
        server_name=payload['server_name'],
        verification_code=payload['fernet_verification_token'],
        browser_identifier=browser.replace('_','.'),
        ip_address=ip_addr,
        user_email=payload['email_address'],
    )
    sender = '%s admin <%s>' % (payload['server_name'],
                                payload['smtp_sender'])
    recipients = [user_info['email']]

    # send the email
    email_sent = authnzerver_send_email(
        sender,
        FORGOTPASS_VERIFICATION_EMAIL_SUBJECT.format(
            server_name=payload['server_name']
        ),
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
                "Could not send email to %s for "
                "the user password reset request."
                % recipients
            ])
        }
