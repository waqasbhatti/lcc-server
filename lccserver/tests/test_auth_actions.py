'''test_auth_actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Sep 2018
License: MIT. See the LICENSE file for details.

This contains tests for the auth functions in authnzerver.actions.

'''

from lccserver.authnzerver import authdb, actions
import os.path
import os
from datetime import datetime, timedelta
import time
import secrets

import numpy as np
from numpy.testing import assert_allclose



def get_test_authdb():
    '''This just makes a new test auth DB for each test function.

    '''

    if os.path.exists('.authdb.sqlite'):
        os.remove('.authdb.sqlite')
    if os.path.exists('.authdb.sqlite-shm'):
        os.remove('.authdb.sqlite-shm')
    if os.path.exists('.authdb.sqlite-wal'):
        os.remove('.authdb.sqlite-wal')

    authdb.create_sqlite_auth_db('.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///.authdb.sqlite')



def test_create_user():
    '''
    This runs through various iterations of creating a user.

    '''
    get_test_authdb()

    # 1. dumb password
    payload = {'email':'testuser@test.org',
               'password':'password'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is False
    assert user_created['user_email'] is 'testuser@test.org'
    assert user_created['user_id'] is None
    assert user_created['send_verification'] is False
    assert ('Your password is too short. It must have at least 12 characters.'
            in user_created['messages'])
    assert ('Your password is too similar to either '
            'the domain name of this LCC-Server or your '
            'own email address.' in user_created['messages'])
    assert ('Your password is not complex enough. '
            'One or more characters appear appear too frequently.'
            in user_created['messages'])

    # 2. all numeric password
    payload = {'email':'testuser@test.org',
               'password':'239420349823904802398402375025'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is False
    assert user_created['user_email'] is 'testuser@test.org'
    assert user_created['user_id'] is None
    assert user_created['send_verification'] is False
    assert ('Your password cannot be all numbers.' in user_created['messages'])

    # 3. password == user name
    payload = {'email':'testuser@test.org',
               'password':'testuser'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is False
    assert user_created['user_email'] is 'testuser@test.org'
    assert user_created['user_id'] is None
    assert user_created['send_verification'] is False
    assert ('Your password is not complex enough. '
            'One or more characters appear appear too frequently.'
            in user_created['messages'])
    assert ('Your password is too similar to either '
            'the domain name of this LCC-Server or your '
            'own email address.' in user_created['messages'])

    # 4. password is OK
    payload = {'email':'testuser@test.org',
               'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser@test.org'
    assert user_created['user_id'] == 4
    assert user_created['send_verification'] is True
    assert ('User account created. Please verify your email address to log in.'
            in user_created['messages'])

    # 5. try to create a new user with an existing email address
    payload = {'email':'testuser@test.org',
               'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is False
    assert user_created['user_email'] == 'testuser@test.org'
    assert user_created['user_id'] == 4

    # we should not send a verification email because the user already has an
    # account or if the account is not active yet, the last verification email
    # was sent less than 24 hours ago
    assert user_created['send_verification'] is False
    assert ('User account created. Please verify your email address to log in.'
            in user_created['messages'])



def test_sessions():
    '''
    This tests session token generation, readback, deletion, and expiry.

    '''
    get_test_authdb()

    # session token payload
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.2.3.4',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation
    session_token1 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # check deletion
    deleted = actions.auth_session_delete(
        {'session_token':session_token1['session_token']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert deleted['success'] is True

    # check readback of deleted
    check = actions.auth_session_exists(
        {'session_token':session_token1['session_token']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert check['success'] is False

    # new session token payload
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.2.3.4',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation
    session_token2 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None
    assert session_token1['session_token'] != session_token2['session_token']

    # get items for session_token
    check = actions.auth_session_exists(
        {'session_token':session_token2['session_token']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    assert check['success'] is True

    for key in ('user_id','full_name','email','email_verified',
                'emailverify_sent_datetime','is_active',
                'last_login_try','last_login_success','created_on',
                'user_role','session_token','ip_address',
                'client_header','created','expires',
                'extra_info_json'):
        assert key in check['session_info']

    assert check['session_info']['user_id'] == 2
    assert check['session_info']['full_name'] is None
    assert check['session_info']['email'] == 'anonuser@localhost'
    assert check['session_info']['email_verified'] is True
    assert check['session_info']['is_active'] is True
    assert check['session_info']['last_login_try'] is None
    assert check['session_info']['last_login_success'] is None
    assert check['session_info']['user_role'] == 'anonymous'
    assert check['session_info'][
        'session_token'
    ] == session_token2['session_token']
    assert check['session_info']['ip_address'] == session_payload['ip_address']
    assert check['session_info'][
        'client_header'
    ] == session_payload['client_header']
    assert check['session_info']['expires'] == session_payload['expires']
    assert check['session_info'][
        'extra_info_json'
    ] == session_payload['extra_info_json']

    # new session token payload
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(seconds=5),
        'ip_address': '1.2.3.4',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation
    session_token3 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token3['success'] is True
    assert session_token3['session_token'] is not None
    assert session_token3['session_token'] != session_token2['session_token']

    # check readback when expired
    time.sleep(10.0)

    check = actions.auth_session_exists(
        {'session_token':session_token3['session_token']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    assert check['success'] is False



def test_login():
    '''
    This tests if we can log in successfully or fail correctly.

    '''

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser@test.org'
    assert user_created['user_id'] == 4
    assert ('User account created. Please verify your email address to log in.'
            in user_created['messages'])

    # create a new session token
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token1 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
    )

    assert emailverify['success'] is True
    assert emailverify['user_id'] == user_created['user_id']
    assert emailverify['is_active'] is True
    assert emailverify['user_role'] == 'authenticated'

    # now make a new session token
    session_payload = {
        'user_id':emailverify['user_id'],
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token2 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    assert login['success'] is True
    assert login['user_id'] == 4

    # try logging in now with the wrong password
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':'helloworld'},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert login['success'] is False


    # tests for no session token provided
    login = actions.auth_user_login(
        {'session_token':'correcthorsebatterystaple',
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert login['success'] is False



def test_login_timing():
    '''This tests obfuscating the presence/absence of users based on password
    checks.

    '''

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser@test.org'
    assert user_created['user_id'] == 4
    assert ('User account created. Please verify your email address to log in.'
            in user_created['messages'])

    # create a new session token
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token1 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
    )

    assert emailverify['success'] is True
    assert emailverify['user_id'] == user_created['user_id']
    assert emailverify['is_active'] is True
    assert emailverify['user_role'] == 'authenticated'

    # now make a new session token
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token2 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )

    assert login['success'] is True
    assert login['user_id'] == 4


    # basic tests for timing attacks

    # incorrect passwords
    incorrect_timings = []
    for _ in range(1000):

        # now make a new session token
        session_payload = {
            'user_id':2,
            'client_header':'Mozzarella Killerwhale',
            'expires':datetime.utcnow()+timedelta(hours=1),
            'ip_address': '1.1.1.1',
            'extra_info_json':{'pref_datasets_always_private':True}
        }

        # check creation of session
        session_token2 = actions.auth_session_new(
            session_payload,
            override_authdb_path='sqlite:///.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': user_payload['email'],
             'password':secrets.token_urlsafe(16)},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
        end = time.time()
        incorrect_timings.append(end - start)


    # correct passwords
    correct_timings = []
    for _ in range(1000):

        # now make a new session token
        session_payload = {
            'user_id':2,
            'client_header':'Mozzarella Killerwhale',
            'expires':datetime.utcnow()+timedelta(hours=1),
            'ip_address': '1.1.1.1',
            'extra_info_json':{'pref_datasets_always_private':True}
        }

        # check creation of session
        session_token2 = actions.auth_session_new(
            session_payload,
            override_authdb_path='sqlite:///.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': user_payload['email'],
             'password':user_payload['password']},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
        end = time.time()
        correct_timings.append(end - start)


    # wronguser passwords
    wronguser_timings = []
    for _ in range(1000):

        # now make a new session token
        session_payload = {
            'user_id':2,
            'client_header':'Mozzarella Killerwhale',
            'expires':datetime.utcnow()+timedelta(hours=1),
            'ip_address': '1.1.1.1',
            'extra_info_json':{'pref_datasets_always_private':True}
        }

        # check creation of session
        session_token2 = actions.auth_session_new(
            session_payload,
            override_authdb_path='sqlite:///.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': secrets.token_urlsafe(16),
             'password':secrets.token_urlsafe(16)},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
        end = time.time()
        wronguser_timings.append(end - start)


    # broken requests
    broken_timings = []
    for _ in range(1000):

        # now make a new session token
        session_payload = {
            'user_id':2,
            'client_header':'Mozzarella Killerwhale',
            'expires':datetime.utcnow()+timedelta(hours=1),
            'ip_address': '1.1.1.1',
            'extra_info_json':{'pref_datasets_always_private':True}
        }

        # check creation of session
        session_token2 = actions.auth_session_new(
            session_payload,
            override_authdb_path='sqlite:///.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':'correcthorsebatterystaple',
             'email': user_payload['email'],
             'password':user_payload['password']},
            override_authdb_path='sqlite:///.authdb.sqlite'
        )
        end = time.time()
        broken_timings.append(end - start)


    correct_timings = np.array(correct_timings)
    incorrect_timings = np.array(incorrect_timings)
    broken_timings = np.array(broken_timings)
    wronguser_timings = np.array(wronguser_timings)

    correct_median = np.median(correct_timings)
    incorrect_median = np.median(incorrect_timings)
    broken_median = np.median(broken_timings)
    wronguser_median = np.median(wronguser_timings)

    # should match within 5 milliseconds or so
    assert_allclose(correct_median, incorrect_median, atol=5.0e-3)
    assert_allclose(correct_median, broken_median, atol=5.0e-3)
    assert_allclose(correct_median, wronguser_median, atol=5.0e-3)



def test_login_logout():

    '''
    See if we can login and log out correctly.

    '''

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser@test.org'
    assert user_created['user_id'] == 4
    assert ('User account created. Please verify your email address to log in.'
            in user_created['messages'])


    # create a new session token
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token1 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///.authdb.sqlite',
            raiseonfail=True
        )
    )

    assert emailverify['success'] is True
    assert emailverify['user_id'] == user_created['user_id']
    assert emailverify['is_active'] is True
    assert emailverify['user_role'] == 'authenticated'

    # now make a new session token
    session_payload = {
        'user_id':2,
        'client_header':'Mozzarella Killerwhale',
        'expires':datetime.utcnow()+timedelta(hours=1),
        'ip_address': '1.1.1.1',
        'extra_info_json':{'pref_datasets_always_private':True}
    }

    # check creation of session
    session_token2 = actions.auth_session_new(
        session_payload,
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )

    assert login['success'] is True
    assert login['user_id'] == 4

    # make sure the session token we used to log in is gone
    # check if our session was deleted correctly
    session_still_exists = actions.auth_session_exists(
        {'session_token':session_token2['session_token']},
        override_authdb_path='sqlite:///.authdb.sqlite'
    )
    assert session_still_exists['success'] is False

    # start a new session with this user's user ID
    authenticated_session_token = actions.auth_session_new(
        {'user_id':login['user_id'],
         'client_header':'Mozzarella Killerwhale',
         'expires':datetime.utcnow()+timedelta(hours=1),
         'ip_address': '1.1.1.1',
         'extra_info_json':{'pref_datasets_always_private':True}},
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )

    # now see if we can log out
    logged_out = actions.auth_user_logout(
        {'session_token':authenticated_session_token['session_token'],
         'user_id': 4},
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )

    assert logged_out['success'] is True
    assert logged_out['user_id'] == 4

    # check if our session was deleted correctly
    session_still_exists = actions.auth_session_exists(
        {'session_token':authenticated_session_token},
        override_authdb_path='sqlite:///.authdb.sqlite',
        raiseonfail=True
    )

    assert session_still_exists['success'] is False