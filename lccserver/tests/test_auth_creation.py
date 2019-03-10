'''test_auth_actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Sep 2018
License: MIT. See the LICENSE file for details.

This contains tests for the auth functions in authnzerver.actions.

'''

from lccserver.authnzerver import authdb, actions
import os.path
import os
from datetime import datetime, timedelta
import time

import multiprocessing as mp



def get_test_authdb():
    '''This just makes a new test auth DB for each test function.

    '''

    authdb.create_sqlite_auth_db('test-creation.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-creation.authdb.sqlite')



def test_create_user():
    '''
    This runs through various iterations of creating a user.

    '''
    try:
        os.remove('test-creation.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-wal')
    except Exception as e:
        pass

    get_test_authdb()

    # 1. dumb password
    payload = {'email':'testuser@test.org',
               'password':'password'}
    user_created = actions.create_new_user(
        payload,
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        os.remove('test-creation.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-wal')
    except Exception as e:
        pass



def test_sessions():
    '''
    This tests session token generation, readback, deletion, and expiry.

    '''

    try:
        os.remove('test-creation.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-wal')
    except Exception as e:
        pass

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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # check deletion
    deleted = actions.auth_session_delete(
        {'session_token':session_token1['session_token']},
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
    )
    assert deleted['success'] is True

    # check readback of deleted
    check = actions.auth_session_exists(
        {'session_token':session_token1['session_token']},
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None
    assert session_token1['session_token'] != session_token2['session_token']

    # get items for session_token
    check = actions.auth_session_exists(
        {'session_token':session_token2['session_token']},
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
    )
    assert session_token3['success'] is True
    assert session_token3['session_token'] is not None
    assert session_token3['session_token'] != session_token2['session_token']

    # check readback when expired
    time.sleep(10.0)

    check = actions.auth_session_exists(
        {'session_token':session_token3['session_token']},
        override_authdb_path='sqlite:///test-creation.authdb.sqlite'
    )

    assert check['success'] is False

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
        os.remove('test-creation.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-creation.authdb.sqlite-wal')
    except Exception as e:
        pass
