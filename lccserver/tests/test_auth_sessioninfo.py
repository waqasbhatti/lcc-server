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

    authdb.create_sqlite_auth_db('test-sessioninfo.authdb.sqlite')
    authdb.initial_authdb_inserts('sqlite:///test-sessioninfo.authdb.sqlite')



def test_sessioninfo():
    '''
    This tests if we can add session info to a session dict.

    '''

    try:
        os.remove('test-sessioninfo.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-sessioninfo.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-sessioninfo.authdb.sqlite-wal')
    except Exception as e:
        pass

    get_test_authdb()

    # create the user
    user_payload = {'email':'testuser-sessioninfo@test.org',
                    'password':'aROwQin9L8nNtPTEMLXd'}
    user_created = actions.create_new_user(
        user_payload,
        override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite'
    )
    assert user_created['success'] is True
    assert user_created['user_email'] == 'testuser-sessioninfo@test.org'
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
        override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite'
    )
    assert session_token1['success'] is True
    assert session_token1['session_token'] is not None

    # verify our email
    emailverify = (
        actions.verify_user_email_address(
            {'email':user_payload['email'],
             'user_id': user_created['user_id']},
            override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite'
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
        override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite'
    )
    assert session_token2['success'] is True
    assert session_token2['session_token'] is not None

    #
    # now try to add info to the session
    #

    session_info_added = actions.auth_session_set_extrainfo(
        {'session_token':session_token2['session_token'],
         'extra_info':{'this':'is','a':'test'}},
        override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite',
        raiseonfail=True
    )

    assert session_info_added['success'] is True
    assert isinstance(
        session_info_added['session_info']['extra_info_json'],
        dict
    )
    assert session_info_added['session_info']['extra_info_json']['this'] == 'is'
    assert session_info_added['session_info']['extra_info_json']['a'] == 'test'

    # get back the new session info
    info_check = actions.auth_session_exists(
        {'session_token':session_token2['session_token']},
        override_authdb_path='sqlite:///test-sessioninfo.authdb.sqlite'
    )

    assert info_check['success'] is True
    assert isinstance(
        info_check['session_info']['extra_info_json'],
        dict
    )
    assert info_check['session_info']['extra_info_json']['this'] == 'is'
    assert info_check['session_info']['extra_info_json']['a'] == 'test'

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
        os.remove('test-sessioninfo.authdb.sqlite')
    except Exception as e:
        pass
    try:
        os.remove('test-sessioninfo.authdb.sqlite-shm')
    except Exception as e:
        pass
    try:
        os.remove('test-sessioninfo.authdb.sqlite-wal')
    except Exception as e:
        pass
