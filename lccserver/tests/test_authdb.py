#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''test_authdb.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This tests lccserver.authnzerver.authdb.py.

'''

import pytest

from lccserver.authnzerver import authdb


def test_check_user_access_default():
    '''This validates that check_user_access denies by default if called w/o
    kwargs.

    '''

    # default policy is deny
    assert authdb.check_user_access() is False



@pytest.mark.parametrize(
    "access,target,expected", [
        # anonymous -> self-owned public collection
        ((2, 'anonymous', 'list'),
         ('collection', 2, 'public', ''),
         True),
        ((2, 'anonymous', 'view'),
         ('collection', 2, 'public', ''),
         True),
        ((2, 'anonymous', 'create'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'edit'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('collection', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('collection', 2, 'public', ''),
         False),
        # anonymous -> public collection from others
        ((2, 'anonymous', 'list'),
         ('collection', 1, 'public', ''),
         True),
        ((2, 'anonymous', 'view'),
         ('collection', 1, 'public', ''),
         True),
        ((2, 'anonymous', 'create'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'edit'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('collection', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('collection', 1, 'public', ''),
         False),
        # anonymous -> shared collection from others
        ((2, 'anonymous', 'list'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'view'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'create'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'edit'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'delete'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_public'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_private'),
         ('collection', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('collection', 1, 'shared', '2'),
         False),
        # anonymous -> private collection from others
        ((2, 'anonymous', 'list'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'view'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'create'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'edit'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('collection', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('collection', 1, 'private', ''),
         False),
    ]
)
def test_check_anonymous_access_to_collection(access, target, expected):
    '''
    This checks user access.

    '''
    userid, role, action = access
    target_name, target_owner, target_visibility, target_sharedwith = target

    assert authdb.check_user_access(
        userid=userid,
        role=role,
        action=action,
        target_name=target_name,
        target_owner=target_owner,
        target_visibility=target_visibility,
        target_sharedwith=target_sharedwith
    ) is expected



@pytest.mark.parametrize(
    "access,target,expected", [
        # anonymous -> self-owned public collection
        ((2, 'anonymous', 'list'),
         ('dataset', 2, 'public', ''),
         True),
        ((2, 'anonymous', 'view'),
         ('dataset', 2, 'public', ''),
         True),
        ((2, 'anonymous', 'create'),
         ('dataset', 2, 'public', ''),
         True),
        ((2, 'anonymous', 'edit'),
         ('dataset', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('dataset', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('dataset', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('dataset', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('dataset', 2, 'public', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('dataset', 2, 'public', ''),
         False),
        # anonymous -> public dataset from others
        ((2, 'anonymous', 'list'),
         ('dataset', 1, 'public', ''),
         True),
        ((2, 'anonymous', 'view'),
         ('dataset', 1, 'public', ''),
         True),
        ((2, 'anonymous', 'create'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'edit'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('dataset', 1, 'public', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('dataset', 1, 'public', ''),
         False),
        # anonymous -> shared dataset from others
        ((2, 'anonymous', 'list'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'view'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'create'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'edit'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'delete'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_public'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'make_private'),
         ('dataset', 1, 'shared', '2'),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('dataset', 1, 'shared', '2'),
         False),
        # anonymous -> private dataset from others
        ((2, 'anonymous', 'list'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'view'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'create'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'edit'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'delete'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_public'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_shared'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'make_private'),
         ('dataset', 1, 'private', ''),
         False),
        ((2, 'anonymous', 'change_owner'),
         ('dataset', 1, 'private', ''),
         False),
    ]
)
def test_check_anonymous_access_to_dataset(access, target, expected):
    '''
    This checks user access.

    '''
    userid, role, action = access
    target_name, target_owner, target_visibility, target_sharedwith = target

    assert authdb.check_user_access(
        userid=userid,
        role=role,
        action=action,
        target_name=target_name,
        target_owner=target_owner,
        target_visibility=target_visibility,
        target_sharedwith=target_sharedwith
    ) is expected
