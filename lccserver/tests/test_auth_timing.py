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

import multiprocessing as mp

from pytest import mark

def get_test_authdb():
    '''This just makes a new test auth DB for each test function.

    '''

    authdb.create_sqlite_auth_db('test-timing.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-timing.authdb.sqlite')


@mark.skip(reason="timing is unreliable at the moment")
def test_login_timing():
    '''This tests obfuscating the presence/absence of users based on password
    checks.

    This may fail randomly if the testing service is under load.

    '''

    try:
        os.remove('test-timing.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-timing.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-timing.authdb.sqlite-wal')
    except Exception as e:
        pass

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser4@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///test-timing.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser4@test.org'
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
        override_authdb_path='sqlite:///test-timing.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-timing.authdb.sqlite'
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-timing.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-timing.authdb.sqlite'
    )

    assert login['success'] is True


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
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': user_payload['email'],
             'password':secrets.token_urlsafe(16)},
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
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
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': user_payload['email'],
             'password':user_payload['password']},
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
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
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':session_token2['session_token'],
             'email': secrets.token_urlsafe(16),
             'password':secrets.token_urlsafe(16)},
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
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
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
        )

        start = time.time()
        actions.auth_user_login(
            {'session_token':'correcthorsebatterystaple',
             'email': user_payload['email'],
             'password':user_payload['password']},
            override_authdb_path='sqlite:///test-timing.authdb.sqlite'
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

    # all timings should match within 7 milliseconds or so
    assert_allclose(correct_median, incorrect_median, atol=7.0e-3)
    assert_allclose(correct_median, broken_median, atol=7.0e-3)
    assert_allclose(correct_median, wronguser_median, atol=7.0e-3)

    currproc = mp.current_process()
    if getattr(currproc, 'table_meta', None):
        del currproc.table_meta

    if getattr(currproc, 'connection', None):
        currproc.connection.close()
        del currproc.connection

    if getattr(currproc, 'engine', None):
        currproc.engine.dispose()
        del currproc.engine

    try:
        os.remove('test-timing.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-timing.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-timing.authdb.sqlite-wal')
    except Exception as e:
        pass
