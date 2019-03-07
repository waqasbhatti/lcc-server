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

    authdb.create_sqlite_auth_db('test-loginlogout.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-loginlogout.authdb.sqlite')



def test_login_logout():

    '''
    See if we can login and log out correctly.

    '''

    try:
        os.remove('test-loginlogout.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-loginlogout.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-loginlogout.authdb.sqlite-wal')
    except Exception as e:
        pass


    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser3@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser3@test.org'
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
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # try logging in now with correct password
    login = actions.auth_user_login(
        {'session_token':session_token1['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )

    # this should fail because we haven't verified our email yet
    assert login['success'] is False

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
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
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    # and now try to log in again
    login = actions.auth_user_login(
        {'session_token':session_token2['session_token'],
         'email': user_payload['email'],
         'password':user_payload['password']},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )

    assert login['success'] is True

    # make sure the session token we used to log in is gone
    # check if our session was deleted correctly
    session_still_exists = actions.auth_session_exists(
        {'session_token':session_token2['session_token']},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite'
    )
    assert session_still_exists['success'] is False

    # start a new session with this user's user ID
    authenticated_session_token = actions.auth_session_new(
        {'user_id':login['user_id'],
         'client_header':'Mozzarella Killerwhale',
         'expires':datetime.utcnow()+timedelta(hours=1),
         'ip_address': '1.1.1.1',
         'extra_info_json':{'pref_datasets_always_private':True}},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )

    # now see if we can log out
    logged_out = actions.auth_user_logout(
        {'session_token':authenticated_session_token['session_token'],
         'user_id': login['user_id']},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )

    assert logged_out['success'] is True

    # check if our session was deleted correctly
    session_still_exists = actions.auth_session_exists(
        {'session_token':authenticated_session_token},
        override_authdb_path='sqlite:///test-loginlogout.authdb.sqlite',
        raiseonfail=True
    )

    assert session_still_exists['success'] is False

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
        os.remove('test-loginlogout.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-loginlogout.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-loginlogout.authdb.sqlite-wal')
    except Exception as e:
        pass
