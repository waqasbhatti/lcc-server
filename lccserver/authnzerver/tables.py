#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''tables.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains SQLAlchemy models for the authnzerver.

'''

import os.path
from datetime import datetime, timedelta
import sqlite3
import secrets

from sqlalchemy import (
    Table, Column, Integer, String, Boolean, DateTime, ForeignKey, MetaData
)
from sqlalchemy import create_engine
from passlib.context import CryptContext


#############################
## PASSWORD HASHING POLICY ##
#############################

# https://passlib.readthedocs.io/en/stable/narr/quickstart.html
password_context = CryptContext(schemes=['argon2','bcrypt'],
                                deprecated='auto')


########################
## AUTHNZERVER TABLES ##
########################

AUTHDB_META = MetaData()

# this lists all possible roles in the system
# roles can be things like 'owner' or actual user group names
Roles = Table(
    'roles',
    AUTHDB_META,
    Column('name', String(length=100), primary_key=True, nullable=False),
    Column('desc', String(length=280), nullable=False)
)


# the sessions table storing client sessions
Sessions = Table(
    'sessions',
    AUTHDB_META,
    Column('session_token',String(), primary_key=True),
    Column('ip_address', String(length=280), nullable=False),
    # some annoying people send zero-length client-headers
    # we won't allow them to initiate a session
    Column('client_header', String(length=280), nullable=False),
    Column('user_id', Integer, ForeignKey("users.user_id", ondelete="CASCADE"),
           nullable=False),
    Column('created', DateTime(),
           default=datetime.utcnow,
           nullable=False, index=True),
    Column('expires', DateTime(), nullable=False, index=True),
    Column('extra_info_json', String()),
)


# this is the main users table
Users = Table(
    'users',
    AUTHDB_META,
    Column('user_id', Integer(), primary_key=True, nullable=False),
    Column('password', String(), nullable=False),
    Column('full_name', String(length=280), index=True),

    Column('email', String(length=280), nullable=False, index=True),
    Column('email_verified',Boolean(), default=False,
           nullable=False, index=True),
    Column('emailverify_sent_datetime', DateTime()),

    Column('is_active', Boolean(), default=False, nullable=False, index=True),

    # these two are separated so we can enforce a rate-limit on login tries
    Column('last_login_success', DateTime(), index=True),
    Column('last_login_try', DateTime(), onupdate=datetime.utcnow, index=True),

    Column('created_on', DateTime(),
           default=datetime.utcnow,
           nullable=False,index=True),
    Column('user_role', String(length=100),
           ForeignKey("roles.name"),
           nullable=False, index=True)
)


# user preferences - fairly freeform to allow extension
Preferences = Table(
    'preferences', AUTHDB_META,
    Column('pref_id', Integer, primary_key=True),
    Column('user_id', Integer,
           ForeignKey("users.user_id", ondelete="CASCADE"),
           nullable=False),
    Column('pref_name', String(length=100), nullable=False),
    Column('pref_value', String(length=280))
)


# API keys that are in use
APIKeys = Table(
    'apikeys',
    AUTHDB_META,
    Column('apikey', String(), primary_key=True, nullable=False),
    Column('issued', String(), nullable=False, default=datetime.utcnow),
    Column('expires', DateTime(), index=True, nullable=False),
    Column('user_id', Integer(),
           ForeignKey('users.user_id', ondelete="CASCADE"),
           nullable=False),
    Column('session_token', String(),
           ForeignKey('sessions.session_token', ondelete="CASCADE"),
           nullable=False)
)


######################################
## ROLES AND ASSOCIATED PERMISSIONS ##
######################################

# each item in the database will have these columns to make these work correctly
# owner -> userid of the object owner. superuser's ID = 1
# status -> one of 0 -> private, 1 -> shared, 2 -> public
# scope -> one of 0 -> owned item, 1 -> somebody else's item

# these are the basic permissions for roles
ROLE_PERMISSIONS = {
    'superuser':{
        'owned': {
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_private',
            'make_shared',
            'change_owner',
        },
        'others':{
            'public':{
                'view',
                'create',
                'delete',
                'edit',
                'make_private',
                'make_shared',
                'change_owner',
            },
            'shared':{
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_private',
                'change_owner',
            },
            'private':{
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_shared',
                'change_owner',
            },
        }
    },
    'staff':{
        'owned': {
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_private',
            'make_shared',
            'change_owner',
        },
        'others':{
            'public':{
                'view',
                'edit',
                'delete',
                'make_private',
                'make_shared',
                'change_owner',
            },
            'shared':{
                'view',
                'edit',
                'make_public',
                'make_private',
                'change_owner',
            },
            'private':{
                'view',
            },
        }
    },
    'authenticated':{
        'owned': {
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_private',
            'make_shared',
        },
        'others':{
            'public':{
                'view',
            },
            'shared':{
                'view',
                'edit',
            },
            'private':set({}),
        }
    },
    'anonymous':{
        'owned': {
            'view',
            'create',
        },
        'others':{
            'public':{
                'view',
            },
            'shared':set({}),
            'private':set({}),
        }
    },
    'locked':{
        'owned': set({}),
        'others':{
            'public':set({}),
            'shared':set({}),
            'private':set({}),
        }
    },
}

# these are intersected with each role's permissions above to form the final set
# of permissions available for each item
ITEM_PERMISSIONS = {
    'object':{
        'valid_permissions':{'view',
                             'create',
                             'edit',
                             'delete',
                             'make_public',
                             'make_shared',
                             'change_owner',
                             'make_private'},
        'valid_statuses':{'public',
                          'private',
                          'shared'},
        'invalid_roles':set({'locked'}),
    },
    'dataset':{
        'valid_permissions':{'view',
                             'create',
                             'edit',
                             'delete',
                             'make_public',
                             'make_shared',
                             'change_owner',
                             'make_private'},
        'valid_statuses':{'public',
                          'private',
                          'shared'},
        'invalid_roles':set({'locked'}),
    },
    'collection':{
        'valid_permissions':{'view',
                             'create',
                             'edit',
                             'delete',
                             'make_public',
                             'change_owner',
                             'make_shared',
                             'make_private'},
        'valid_statuses':{'public',
                          'private',
                          'shared'},
        'invalid_roles':set({'locked'}),
    },
    'apikey':{
        'valid_permissions':{'view',
                             'create',
                             'delete'},
        'valid_statuses':{'private'},
        'invalid_roles':set({'anonymous','locked'}),

    },
    'userprefs':{
        'valid_permissions':{'view',
                             'edit'},
        'valid_statuses':{'private'},
        'invalid_roles':set({'anonymous','locked'}),
    }
}


def get_item_permissions(role_name,
                         target_name,
                         target_status,
                         target_scope,
                         debug=False):
    '''Returns the possible permissions for a target given a role and target
    status.

    role is one of superuser, authenticated, anonymous, locked
    target is one of object, dataset, collection, apikey, userprefs
    target_status is one of public, private, shared
    target_scope is one of owned, others

    Returns a set. If the permissions don't make sense, returns an empty set, in
    which case access MUST be denied.

    '''

    if debug:
        print(
            'role_name = %s, target_name = %s, '
            'target_status = %s, target_scope = %s' %
            (role_name, target_name, target_status, target_scope)
        )

    try:
        target_valid_permissions = ITEM_PERMISSIONS[
            target_name
        ]['valid_permissions']
        target_valid_statuses = ITEM_PERMISSIONS[
            target_name
        ]['valid_statuses']
        target_invalid_roles = ITEM_PERMISSIONS[
            target_name
        ]['invalid_roles']

        if debug:
            print('%s valid_perms: %r' %
                  (target_name, target_valid_permissions))
            print('%s valid_statuses: %r' %
                  (target_name, target_valid_statuses))
            print('%s invalid_roles: %r' %
                  (target_name, target_invalid_roles))


        # if the role is not allowed into this target, return
        if role_name in target_invalid_roles:
            return set({})

        # if the target's status is not valid, return
        if target_status not in target_valid_statuses:
            return set({})

        # check the target's scope

        # if this target is owned by the user, then check target owned
        # permissions
        if target_scope == 'owned':
            role_permissions = ROLE_PERMISSIONS[role_name][target_scope]

        # otherwise, the target is not owned by the user, check scope
        # permissions for target status
        else:
            role_permissions = (
                ROLE_PERMISSIONS[role_name][target_scope][target_status]
            )

        # these are the final available permissions
        available_permissions = role_permissions.intersection(
            target_valid_permissions
        )

        if debug:
            print('role_permissions for status: %s, scope: %s: %r' %
                  (target_status, target_scope, role_permissions))
            print('available_permissions: %r' % available_permissions)

        return available_permissions

    except Exception as e:
        return set({})


#######################
## UTILITY FUNCTIONS ##
#######################

WAL_MODE_SCRIPT = '''\
pragma journal_mode = 'wal';
pragma journal_size_limit = 5242880;
'''

def create_auth_db(auth_db_path,
                   echo=False,
                   returnconn=True):
    '''
    This creates the auth DB.

    '''

    engine = create_engine('sqlite:///%s' % os.path.abspath(auth_db_path),
                           echo=echo)
    AUTHDB_META.create_all(engine)

    if returnconn:
        return engine, AUTHDB_META
    else:
        engine.dispose()
        del engine

    # at the end, we'll switch the auth DB to WAL mode to make it handle
    # concurrent operations a bit better
    db = sqlite3.connect(auth_db_path)
    cur = db.cursor()
    cur.executescript(WAL_MODE_SCRIPT)
    db.commit()
    db.close()



def get_auth_db(auth_db_path, echo=False):
    '''
    This just gets a connection to the auth DB.

    '''

    meta = MetaData()
    engine = create_engine('sqlite:///%s' % os.path.abspath(auth_db_path),
                           echo=echo)
    meta.bind = engine
    meta.reflect()

    return engine, engine.connect(), meta



def initial_authdb_inserts(auth_db_path,
                           superuser_email=None,
                           superuser_pass=None,
                           echo=False):
    '''
    This does initial set up of the auth DB.

    - adds an anonymous user
    - adds a superuser with:
      -  userid = UNIX userid
      - password = random 16 bytes)
    - sets up the initial permissions table

    Returns the superuser userid and password.

    '''

    engine, conn, meta = get_auth_db(auth_db_path,
                                     echo=echo)

    # get the roles table and fill it in
    roles = meta.tables['roles']
    res = conn.execute(roles.insert(),[
        {'name':'superuser',
         'desc':'Accounts that can do anything.'},
        {'name':'staff',
         'desc':'Users with basic admin privileges.'},
        {'name':'authenticated',
         'desc':'Logged in regular users.'},
        {'name':'anonymous',
         'desc':'The anonymous user role.'},
        {'name':'locked',
         'desc':'An account that has been disabled.'},
    ])
    res.close()

    # get the users table
    users = meta.tables['users']

    # make the superuser account
    if not superuser_email:
        superuser_email = '%s@localhost' % os.environ.get('USER',
                                                          default='superuser')
    if not superuser_pass:
        superuser_pass = secrets.token_urlsafe(16)
        superuser_pass_auto = True
    else:
        superuser_pass_auto = False

    hashed_password = password_context.hash(superuser_pass)

    result = conn.execute(
        users.insert().values([
            # the superuser
            {'password':hashed_password,
             'email':superuser_email,
             'email_verified':True,
             'is_staff':True,
             'is_active':True,
             'user_role':'superuser',
             'created_on':datetime.utcnow()},
            # the anonuser
            {'password':password_context.hash(secrets.token_urlsafe(32)),
             'email':'anonuser@localhost',
             'email_verified':True,
             'is_staff':False,
             'is_active':True,
             'user_role':'anonymous',
             'created_on':datetime.utcnow()},
            # the dummyuser to fail passwords for nonexistent users against
            {'password':password_context.hash(secrets.token_urlsafe(32)),
             'email':'dummyuser@localhost',
             'email_verified':True,
             'is_staff':False,
             'is_active':False,
             'user_role':'locked',
             'created_on':datetime.utcnow()},
        ])
    )
    result.close()

    if superuser_pass_auto:
        return superuser_email, superuser_pass
