'''test_auth_actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Sep 2018
License: MIT. See the LICENSE file for details.

This contains tests for the auth functions in authnzerver.actions.

'''

from lccserver.authnzerver import authdb, actions
import os.path
import os
from datetime import datetime, timedelta
import multiprocessing as mp


def get_test_authdb():
    '''This just makes a new test auth DB for each test function.

    '''

    authdb.create_sqlite_auth_db('test-login.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-login.authdb.sqlite')



def test_login():
    '''
    This tests if we can log in successfully or fail correctly.

    '''

    try:
        os.remove('test-login.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-login.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-login.authdb.sqlite-wal')
    except Exception as e:
        pass

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser2@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser2@test.org'
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
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///test-login.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )

    assert login['success'] is True

    # try logging in now with the wrong password
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':'helloworld'},
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )
    assert login['success'] is False


    # tests for no session token provided
    login = actions.auth_user_login(
        {'session_token':'correcthorsebatterystaple',
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-login.authdb.sqlite'
    )
    assert login['success'] is False

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
        os.remove('test-login.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-login.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-login.authdb.sqlite-wal')
    except Exception as e:
        pass
