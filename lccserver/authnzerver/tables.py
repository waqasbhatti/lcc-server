#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''tables.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains SQLAlchemy models for the authnzerver.

'''

import os.path
from datetime import datetime

from sqlalchemy import (
    Table, Column, Integer, String, Boolean, DateTime, ForeignKey, MetaData
)
from sqlalchemy import create_engine

import sqlite3

########################
## AUTHNZERVER TABLES ##
########################

AUTHDB_META = MetaData()

# the basic permissions table, which lists permissions
Permissions = Table(
    'permissions',
    AUTHDB_META,
    Column('name', String(length=100), primary_key=True, nullable=False),
    Column('desc', String(length=280), nullable=False)
)


# this lists all possible roles in the system
# roles can be things like 'owner' or actual user group names
Roles = Table(
    'roles',
    AUTHDB_META,
    Column('name', String(length=100), primary_key=True, nullable=False),
    Column('desc', String(length=280), nullable=False)
)


# this associates permissions with roles
PermissionsAndRoles = Table(
    'permissions_and_roles',
    AUTHDB_META,
    Column('permission_name', String(length=100),
           ForeignKey('permissions.name', ondelete="CASCADE"),
           nullable=False),
    Column('role_name', String(length=100),
           ForeignKey('roles.name', ondelete="CASCADE"),
           nullable=False)
)


# the sessions table storing client sessions
Sessions = Table(
    'sessions',
    AUTHDB_META,
    Column('session_key',String(), primary_key=True),
    Column('ip_address', String(length=280), nullable=False),
    Column('client_header', String(length=280), nullable=False),
    # this can't be null because there always be an anonymous user in the users
    # table so the anonymous sessions will be tied to that virtual user
    # FIXME: does this make sense? look at Django to see how they do it.
    Column('user_id', Integer,
           ForeignKey("users.user_id", ondelete="CASCADE"),
           nullable=False),
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
    Column('is_staff', Boolean(), default=False, nullable=False, index=True),
    Column('is_active', Boolean(), default=False, nullable=False, index=True),
    Column('is_superuser', Boolean(), default=False,
           nullable=False, index=True),
    Column('last_login', DateTime(),
           default=datetime.utcnow,
           onupdate=datetime.utcnow,
           nullable=False,
           index=True),
    Column('created_on', DateTime(),
           default=datetime.utcnow,
           nullable=False,index=True),
    Column('user_role', String(length=100),
           ForeignKey("roles.name"),
           nullable=False, index=True),
    Column('user_permissions', String(), nullable=False, index=True)
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
    Column('session_key', String(),
           ForeignKey('sessions.session_key', ondelete="CASCADE"),
           nullable=False)
)


######################################
## ROLES AND ASSOCIATED PERMISSIONS ##
######################################

# each item in the database will have these columns to make these work correctly
# owner -> userid of the object owner. superuser's ID = 1
# status -> one of 0 -> private, 1 -> shared, 2 -> public
# scope -> one of 0 -> owned item, 1 -> somebody else's item

# these are the basic permissions
PERMISSIONS = {
    'superuser':{
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
                'create',
                'delete',
                'edit',
                'make_public',
                'make_private',
                'make_shared',
            },
            'shared':{
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_private',
                'make_shared',
            },
            'private':{
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_private',
                'make_shared',
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
ITEM_SPECIFIC_PERMISSIONS = {
    'object':{
        'valid_permissions':{'view',
                             'create',
                             'edit',
                             'delete',
                             'make_public',
                             'make_shared',
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
        target_valid_permissions = ITEM_SPECIFIC_PERMISSIONS[
            target_name
        ]['valid_permissions']
        target_valid_statuses = ITEM_SPECIFIC_PERMISSIONS[
            target_name
        ]['valid_statuses']
        target_invalid_roles = ITEM_SPECIFIC_PERMISSIONS[
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
            role_permissions = PERMISSIONS[role_name][target_scope]

        # otherwise, the target is not owned by the user, check scope
        # permissions for target status
        else:
            role_permissions = (
                PERMISSIONS[role_name][target_scope][target_status]
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
                           permissions_model_json=None,
                           superuser_email=None,
                           superuser_pass=None,
                           echo=False):
    '''
    This does initial set up of the auth DB.

    - adds an anonymous user
    - adds a superuser with:
      -  userid = UNIX userid
      - password = random 20 characters)
    - sets up the initial permissions table

    Returns the superuser userid and password.

    '''
