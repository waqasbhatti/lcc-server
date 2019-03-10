#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''actions.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains functions to drive auth actions.

'''

#############
## LOGGING ##
#############

import logging

# get a logger
LOGGER = logging.getLogger(__name__)


#############
## IMPORTS ##
#############

from .apikey import (
    issue_new_apikey,
    verify_apikey
)

from .admin import (
    list_users,
    edit_user,
)

from .email import (
    send_signup_verification_email,
    verify_user_email_address,
    send_forgotpass_verification_email,
    authnzerver_send_email,
)

from .session import (
    auth_session_new,
    auth_session_exists,
    auth_session_set_extrainfo,
    auth_session_delete,
    auth_password_check,
    auth_user_login,
    auth_user_logout,
    auth_kill_old_sessions,
)

from .user import (
    create_new_user,
    change_user_password,
    delete_user,
    verify_password_reset,
)
