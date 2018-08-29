#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''tables.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains SQLAlchemy models for the authnzerver.

'''

try:
    from datetime import datetime, timezone
    utc = timezone.utc
except Exception as e:
    from datetime import datetime, timedelta, tzinfo

    # we'll need to instantiate a tzinfo object because py2.7's datetime
    # doesn't have the super convenient timezone object (seriously)
    # https://docs.python.org/2/library/datetime.html#datetime.tzinfo.fromutc
    ZERO = timedelta(0)

    class UTC(tzinfo):
        """UTC"""

        def utcoffset(self, dt):
            return ZERO

        def tzname(self, dt):
            return "UTC"

        def dst(self, dt):
            return ZERO

    utc = UTC()

##########################################
## AUTHNZERVER TABLES FOR EXTERNAL AUTH ##
##########################################

from sqlalchemy import (
    Table, Column, Integer, String, Boolean, DateTime, ForeignKey, MetaData
)
from sqlalchemy import create_engine

metadata = MetaData()

Permissions = Table(
    'permissions',
    metadata,
    Column('name', String(length=100), primary_key=True, nullable=False),
    Column('desc', String(length=255), nullable=False)
)

# FIXME: add many-to-many for permissions foreign keys
Groups = Table(
    'groups',
    metadata,
    Column('name', String(length=100), primary_key=True, nullable=False),
    Column('desc', String(length=255), nullable=False)
)

# FIXME: add many-to-many for permissions foreign keys
Users = Table(
    'users',
    metadata,
    Column('uid', Integer(), primary_key=True, nullable=False),
    Column('email', String(length=255), nullable=False),
    Column('password', String(), nullable=False),
    Column('fullname', String(length=255)),
    Column('is_staff', Boolean(), default=False, nullable=False),
    Column('is_active', Boolean(), default=False, nullable=False),
    Column('is_superuser', Boolean(), default=False, nullable=False),
    Column('lastlogin', DateTime(),
           default=datetime.utcnow,
           onupdate=datetime.utcnow,
           nullable=False),
    Column('created', DateTime(),
           default=datetime.utcnow,
           nullable=False)
)

# the sessions table storing client sessions
# data_json contains for now: ipaddr, header, apikey if provided,
# maybe also contains a partition key used to route auth lookups to the correct
# DB instance (not sure about this, TBD...)
Sessions = Table(
    'sessions',
    metadata,
    Column('session_key',String(), primary_key=True),
    Column('data_json', String(), nullable=False),
    Column('expires', DateTime(), nullable=False),
)


#######################
## UTILITY FUNCTIONS ##
#######################

def create_auth_db(auth_db_path, echo=False):
    '''
    This creates the auth DB.

    '''

    engine = create_engine('sqlite:///%s' % auth_db_path, echo=echo)
    metadata.create_all(engine)
    return engine
