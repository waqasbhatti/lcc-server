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


######################
## PERMISSIONS LIST ##
######################

# TODO: add an owner column on each dataset that references the user_id or
# anonymous.

permissions = [
    #
    # user model permissions
    #
    'apikeys.can_create',
    'apikeys.can_change',
    'apikeys.can_delete',
    'apikeys.can_view',
    'preferences.can_create',
    'preferences.can_change',
    'preferences.can_delete',
    'preferences.can_view',
    # FIXME: add other user permissions

    #
    # collection permissions
    #
    'collection.can_create',
    'collection.can_edit',
    'collection.can_delete',
    'collection.can_view',

    #
    # dataset permissions
    #
    # this is effectively a read-only mode for the server
    # if people can't create datasets, they can't run searches
    # so are restricted to only viewing existing datasets
    'dataset.can_create',
    'dataset.can_edit',
    'dataset.can_delete',
    'dataset.can_view',

    #
    # object permissions
    #
    'object.can_edit',
    'object.can_view',
]


######################################
## ROLES AND ASSOCIATED PERMISSIONS ##
######################################

# FIXME: fill this in


#######################
## UTILITY FUNCTIONS ##
#######################

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
    cur.executescript(
        'pragma journal_mode wal; pragma journal_size_limit = 5242880;'
    )
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
