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

    authdb.create_sqlite_auth_db('test-passcheck.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-passcheck.authdb.sqlite')



def test_passcheck():
    '''
    This tests if we can check the password for a logged-in user.

    '''

    try:
        os.remove('test-passcheck.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-passcheck.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-passcheck.authdb.sqlite-wal')
    except Exception as e:
        pass

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser-passcheck@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///test-passcheck.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser-passcheck@test.org'
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
        override_authdb_path='sqlite:///test-passcheck.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///test-passcheck.authdb.sqlite'
        )
    )

    assert emailverify['success'] is True
    assert emailverify['user_id'] == user_created['user_id']
    assert emailverify['is_active'] is True
    assert emailverify['user_role'] == 'authenticated'

    # now make a new session token to simulate a logged-in user
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
        override_authdb_path='sqlite:///test-passcheck.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    #
    # now run a password check
    #

    # correct password
    pass_check = actions.auth_password_check(
        {'session_token':session_token2['session_token'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-passcheck.authdb.sqlite',
        raiseonfail=True
    )
    assert pass_check['success'] is True
    assert pass_check['user_id'] == emailverify['user_id']

    # incorrect password
    pass_check = actions.auth_password_check(
        {'session_token':session_token2['session_token'],
         'password':'incorrectponylithiumfastener'},
        override_authdb_path='sqlite:///test-passcheck.authdb.sqlite',
        raiseonfail=True
    )
    assert pass_check['success'] is False
    assert pass_check['user_id'] is None

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
        os.remove('test-passcheck.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-passcheck.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-passcheck.authdb.sqlite-wal')
    except Exception as e:
        pass
