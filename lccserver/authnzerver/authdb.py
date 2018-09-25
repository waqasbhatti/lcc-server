#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''tables.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains SQLAlchemy models for the authnzerver.

'''

import os.path
import os
import stat
from datetime import datetime
import sqlite3
import secrets
import getpass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import event
from sqlalchemy import (
    Table, Column, Integer, String, Text,
    Boolean, DateTime, ForeignKey, MetaData
)

from passlib.context import CryptContext

##########################
## JSON type for SQLite ##
##########################

# taken from:
# docs.sqlalchemy.org/en/latest/core/custom_types.html#marshal-json-strings

from sqlalchemy.types import TypeDecorator, TEXT
import json

class JSONEncodedDict(TypeDecorator):
    """Represents an immutable structure as a json-encoded string.

    Usage::

        JSONEncodedDict(255)

    """

    impl = TEXT

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value



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
    Column('extra_info_json', JSONEncodedDict()),
)


# this is the main users table
Users = Table(
    'users',
    AUTHDB_META,
    Column('user_id', Integer(), primary_key=True, nullable=False),
    Column('password', Text(), nullable=False),
    Column('full_name', String(length=280), index=True),
    Column('email', String(length=280), nullable=False, unique=True),
    Column('email_verified',Boolean(), default=False,
           nullable=False, index=True),
    Column('is_active', Boolean(), default=False, nullable=False, index=True),

    # these track when we last sent emails to this user
    Column('emailverify_sent_datetime', DateTime()),
    Column('emailforgotpass_sent_datetime', DateTime()),
    Column('emailchangepass_sent_datetime', DateTime()),

    # these two are separated so we can enforce a rate-limit on login tries
    Column('last_login_success', DateTime(), index=True),
    Column('last_login_try', DateTime(), index=True),
    # this is reset everytime a user logs in sucessfully. this is used to check
    # the number of failed tries since the last successful try. FIXME: can we
    # use this for throttling login attempts without leaking info?
    Column('failed_login_tries', Integer(), default=0),

    Column('created_on', DateTime(),
           default=datetime.utcnow,
           nullable=False,index=True),
    Column('last_updated', DateTime(),
           onupdate=datetime.utcnow,
           nullable=False,index=True),
    Column('user_role', String(length=100),
           ForeignKey("roles.name"),
           nullable=False, index=True)
)


# this is the groups table
# groups can only be created by authenticated users and above
Groups = Table(
    'groups', AUTHDB_META,
    Column('group_id', Integer, primary_key=True),
    Column('group_name',String(length=280), nullable=False),
    Column('visibility',String(length=100), nullable=False,
           default='public', index=True),
    Column('created_by', Integer, ForeignKey("users.user_id"),
           nullable=False),
    Column('is_active', Boolean(), default=False,
           nullable=False, index=True),
    Column('created_on', DateTime(),
           default=datetime.utcnow,
           nullable=False,index=True),
    Column('last_updated', DateTime(),
           onupdate=datetime.utcnow,
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
# FIXME: should API keys be deleted via CASCADE when the sessions are deleted?
APIKeys = Table(
    'apikeys',
    AUTHDB_META,
    Column('apikey', Text(), primary_key=True, nullable=False),
    Column('issued', DateTime(), nullable=False, default=datetime.utcnow),
    Column('expires', DateTime(), index=True, nullable=False),
    Column('user_id', Integer(),
           ForeignKey('users.user_id', ondelete="CASCADE"),
           nullable=False),
    Column('session_token', Text(),
           ForeignKey('sessions.session_token', ondelete="CASCADE"),
           nullable=False)
)


######################################
## ROLES AND ASSOCIATED PERMISSIONS ##
######################################

# these are the basic permissions for roles
ROLE_PERMISSIONS = {
    'superuser':{
        'limits':{
            'max_rows': 5000000,
            'max_reqs_60sec': 60000,
        },
        'can_own':{'dataset','object','collection','apikeys','preferences'},
        'for_owned': {
            'list',
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_unlisted',
            'make_private',
            'make_shared',
            'change_owner',
        },
        'for_others':{
            'public':{
                'list',
                'view',
                'create',
                'delete',
                'edit',
                'make_private',
                'make_unlisted',
                'make_shared',
                'change_owner',
            },
            'unlisted':{
                'list',
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_private',
                'make_shared',
                'change_owner',
            },
            'shared':{
                'list',
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_unlisted',
                'make_private',
                'change_owner',
            },
            'private':{
                'list',
                'view',
                'create',
                'delete',
                'edit',
                'make_public',
                'make_unlisted',
                'make_shared',
                'change_owner',
            },
        }
    },
    'staff':{
        'limits':{
            'max_rows': 1000000,
            'max_reqs_60sec': 60000,
        },
        'can_own':{'dataset','object','collection','apikeys','preferences'},
        'for_owned': {
            'list',
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_unlisted',
            'make_private',
            'make_shared',
            'change_owner',
        },
        'for_others':{
            'public':{
                'list',
                'view',
                'edit',
                'delete',
            },
            'unlisted':{
                'list',
                'view',
                'edit',
                'delete',
            },
            'shared':{
                'list',
                'view',
                'edit',
            },
            'private':{
                'list',
            },
        }
    },
    'authenticated':{
        'limits':{
            'max_rows': 500000,
            'max_reqs_60sec': 6000,
        },
        'can_own':{'dataset','apikeys','preferences'},
        'for_owned': {
            'list',
            'view',
            'create',
            'delete',
            'edit',
            'make_public',
            'make_unlisted',
            'make_private',
            'make_shared',
        },
        'for_others':{
            'public':{
                'list',
                'view',
            },
            'unlisted':{
                'view',
            },
            'shared':{
                'list',
                'view',
                'edit',
            },
            'private':set({}),
        }
    },
    'anonymous':{
        'limits':{
            'max_rows': 100000,
            'max_reqs_60sec': 600,
        },
        'can_own':{'dataset'},
        'for_owned': {
            'list',
            'view',
            'create',
            'make_private',
            'make_public',
            'make_unlisted',
        },
        'for_others':{
            'public':{
                'list',
                'view',
            },
            'unlisted':{
                'view'
            },
            'shared':set({}),
            'private':set({}),
        }
    },
    'locked':{
        'limits':{
            'max_rows': 0,
            'max_reqs_60sec': 0,
        },
        'can_own':set({}),
        'for_owned': set({}),
        'for_others':{
            'public':set({}),
            'unlisted':set({}),
            'shared':set({}),
            'private':set({}),
        }
    },
}

# these are intersected with each role's permissions above to form the final set
# of permissions available for each item
ITEM_PERMISSIONS = {
    'object':{
        'valid_permissions':{'list',
                             'view',
                             'create',
                             'edit',
                             'delete',
                             'change_owner',
                             'make_public',
                             'make_unlisted',
                             'make_shared',
                             'make_private'},
        'valid_visibilities':{'public',
                              'unlisted',
                              'private',
                              'shared'},
        'invalid_roles':set({'locked'}),
    },
    'dataset':{
        'valid_permissions':{'list',
                             'view',
                             'create',
                             'edit',
                             'delete',
                             'change_owner',
                             'make_public',
                             'make_unlisted',
                             'make_shared',
                             'make_private'},
        'valid_visibilities':{'public',
                              'unlisted',
                              'private',
                              'shared'},
        'invalid_roles':set({'locked'}),
    },
    'collection':{
        'valid_permissions':{'list',
                             'view',
                             'create',
                             'edit',
                             'delete',
                             'change_owner',
                             'make_public',
                             'make_unlisted',
                             'make_shared',
                             'make_private'},
        'valid_visibilities':{'public',
                              'unlisted',
                              'private',
                              'shared'},
        'invalid_roles':set({'locked'}),
    },
    'users':{
        'valid_permissions':{'list',
                             'view',
                             'edit',
                             'create',
                             'delete'},
        'valid_visibilities':{'private'},
        'invalid_roles':set({'authenticated','anonymous','locked'}),

    },
    'sessions':{
        'valid_permissions':{'list',
                             'view',
                             'delete'},
        'valid_visibilities':{'private'},
        'invalid_roles':set({'authenticated','anonymous','locked'}),

    },
    'apikeys':{
        'valid_permissions':{'list',
                             'view',
                             'create',
                             'delete'},
        'valid_visibilities':{'private'},
        'invalid_roles':set({'anonymous','locked'}),

    },
    'preferences':{
        'valid_permissions':{'list',
                             'view',
                             'edit'},
        'valid_visibilities':{'private'},
        'invalid_roles':set({'anonymous','locked'}),
    }
}


def get_item_permissions(role_name,
                         target_name,
                         target_visibility,
                         target_scope,
                         debug=False):
    '''Returns the possible permissions for a target given a role and target
    status.

    role is one of {superuser, authenticated, anonymous, locked}

    target_name is one of {object, dataset, collection, users,
                           apikeys, preferences, sessions}

    target_visibility is one of {public, private, shared}

    target_scope is one of {owned, others}

    Returns a set. If the permissions don't make sense, returns an empty set, in
    which case access MUST be denied.

    '''

    if debug:
        print(
            'role_name = %s\ntarget_name = %s\n'
            'target_visibility = %s\ntarget_scope = %s' %
            (role_name, target_name, target_visibility, target_scope)
        )

    try:
        target_valid_permissions = ITEM_PERMISSIONS[
            target_name
        ]['valid_permissions']
        target_valid_visibilities = ITEM_PERMISSIONS[
            target_name
        ]['valid_visibilities']
        target_invalid_roles = ITEM_PERMISSIONS[
            target_name
        ]['invalid_roles']

        if debug:
            print('%s valid_perms: %r' %
                  (target_name, target_valid_permissions))
            print('%s valid_visibilities: %r' %
                  (target_name, target_valid_visibilities))
            print('%s invalid_roles: %r' %
                  (target_name, target_invalid_roles))


        # if the role is not allowed into this target, return
        if role_name in target_invalid_roles:
            return set({})

        # if the target's status is not valid, return
        if target_visibility not in target_valid_visibilities:
            return set({})

        # check the target's scope

        # if this target is owned by the user, then check target owned
        # permissions
        if target_scope == 'for_owned':
            role_permissions = ROLE_PERMISSIONS[role_name][target_scope]

        # otherwise, the target is not owned by the user, check scope
        # permissions for target status
        else:
            role_permissions = (
                ROLE_PERMISSIONS[role_name][target_scope][target_visibility]
            )

        # these are the final available permissions
        available_permissions = role_permissions.intersection(
            target_valid_permissions
        )

        if debug:
            print("target role permissions: %r" % role_permissions)
            print('available actions for role: %r' % available_permissions)

        return available_permissions

    except Exception as e:
        return set({})


def check_user_access(userid=2,
                      role='anonymous',
                      action='view',
                      target_name='collection',
                      target_owner=1,
                      target_visibility='private',
                      target_sharedwith=None,
                      debug=False):
    '''
    This does a check for user access to a target.

    '''
    if debug:
        print('userid = %s\ntarget_owner = %s\nsharedwith_userids = %s' %
              (userid, target_owner, target_sharedwith))

    if role in ('superuser', 'staff'):

        shared_or_owned_ok = True

    elif target_visibility == 'private':

        shared_or_owned_ok = userid == target_owner

    elif target_visibility == 'shared':

        try:

            if target_sharedwith and target_sharedwith != '':

                sharedwith_userids = target_sharedwith.split(',')
                sharedwith_userids = [int(x) for x in sharedwith_userids]
                if debug:
                    print('sharedwith_userids = %s' % sharedwith_userids)
                shared_or_owned_ok = (
                    userid in sharedwith_userids or userid == target_owner
                )

                # anything shared with anonymous users is effectively shared for
                # everyone
                if 2 in sharedwith_userids:
                    shared_or_owned_ok = True

            else:
                shared_or_owned_ok = (
                    userid == target_owner
                )

        except Exception as e:
            shared_or_owned_ok = False

    # unlisted objects are OK to view
    elif target_visibility == 'unlisted':

        shared_or_owned_ok = True

    elif target_visibility == 'public':

        shared_or_owned_ok = True

    else:

        shared_or_owned_ok = False


    if debug:
        print('target shared or owned test passed = %s' % shared_or_owned_ok)

    target_may_be_owned_by_role = (
        target_name in ROLE_PERMISSIONS[role]['can_own']
    )

    if debug:
        print("target: '%s' may be owned by role: '%s' = %s" %
              (target_name, role, target_may_be_owned_by_role))

    # validate ownership of the target
    if (userid == target_owner and target_may_be_owned_by_role):
        perms = get_item_permissions(role,
                                     target_name,
                                     target_visibility,
                                     'for_owned',
                                     debug=debug)

    # if the target is not owned, then check if it's accessible under its scope
    # and visibility
    elif userid != target_owner:
        perms = get_item_permissions(role,
                                     target_name,
                                     target_visibility,
                                     'for_others',
                                     debug=debug)

    # if the target cannot be owned by the role, then fail
    else:
        perms = set({})

    if debug:
        print("user action: '%s', permitted actions: %s" % (action, perms))

    return ((action in perms) and shared_or_owned_ok)



def check_role_limits(role,
                      rows=None,
                      rate_60sec=None):
    '''
    This just returns the role limits.

    '''

    if rows is not None:
        return ROLE_PERMISSIONS[role]['limits']['max_rows'] <= rows
    elif rate_60sec is not None:
        return ROLE_PERMISSIONS[role]['limits']['max_reqs_60sec'] >= rate_60sec
    else:
        return ROLE_PERMISSIONS[role]['limits']


#######################
## UTILITY FUNCTIONS ##
#######################

WAL_MODE_SCRIPT = '''\
pragma journal_mode = 'wal';
pragma journal_size_limit = 5242880;
'''

def create_sqlite_auth_db(
        auth_db_path,
        echo=False,
        returnconn=False
):
    '''
    This creates the local SQLite auth DB.

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

    # set the permissions on the file appropriately
    os.chmod(auth_db_path, 0o100600)



def get_auth_db(auth_db_path, echo=False):
    '''
    This just gets a connection to the auth DB.

    '''

    # if this is an SQLite DB, make sure to check the auth DB permissions before
    # we load it so we can be sure no one else messes with it
    potential_file_path = auth_db_path.replace('sqlite:///','')

    if os.path.exists(potential_file_path):

        fileperm = oct(os.stat(potential_file_path)[stat.ST_MODE])

        if not (fileperm == '0100600' or fileperm == '0o100600'):
            raise IOError('incorrect permissions on auth DB, will not load it')

        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    engine = create_engine(auth_db_path, echo=echo)
    AUTHDB_META.bind = engine
    conn = engine.connect()

    return engine, conn, AUTHDB_META



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
        try:
            superuser_email = '%s@localhost' % getpass.getuser()
        except Exception as e:
            superuser_email = 'lccs_admin@localhost'

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
             'is_active':True,
             'user_role':'superuser',
             'created_on':datetime.utcnow(),
             'last_updated':datetime.utcnow()},
            # the anonuser
            {'password':password_context.hash(secrets.token_urlsafe(32)),
             'email':'anonuser@localhost',
             'email_verified':True,
             'is_active':True,
             'user_role':'anonymous',
             'created_on':datetime.utcnow(),
             'last_updated':datetime.utcnow()},
            # the dummyuser to fail passwords for nonexistent users against
            {'password':password_context.hash(secrets.token_urlsafe(32)),
             'email':'dummyuser@localhost',
             'email_verified':True,
             'is_active':False,
             'user_role':'locked',
             'created_on':datetime.utcnow(),
             'last_updated':datetime.utcnow()},
        ])
    )
    result.close()

    if superuser_pass_auto:
        return superuser_email, superuser_pass
    else:
        return superuser_email, None



def get_secret_token(token_environvar,
                     token_file,
                     logger):
    """
    This loads the specified token file from the environment or the token_file.


    """
    if token_environvar in os.environ:

        secret = os.environ[token_environvar]
        if len(secret.strip()) == 0:

            raise EnvironmentError(
                'Secret from environment is either empty or not valid.'
            )

        logger.info(
            'Using secret from environment.' % token_environvar
        )

    elif os.path.exists(token_file):

        # check if this file is readable/writeable by user only
        fileperm = oct(os.stat(token_file)[stat.ST_MODE])

        if not (fileperm == '0100400' or fileperm == '0o100400'):
            raise PermissionError('Incorrect file permissions on secret file '
                                  '(needs chmod 400)')


        with open(token_file,'r') as infd:
            secret = infd.read().strip('\n')

        if len(secret.strip()) == 0:

            raise ValueError(
                'Secret from specified secret file '
                'is either empty or not valid, will not continue'
            )

        logger.info(
            'Using secret from specified secret file.'
        )

    else:

        raise IOError(
            'Could not load secret from environment or the specified file.'
        )

    return secret
