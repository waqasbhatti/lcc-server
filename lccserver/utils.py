#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''utils.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT - see the LICENSE file for the full text.

This contains various utility functions and classes for lcc-server.

'''

# Use our custom ProcessPoolExecutor. This comes with a built-in finalizer
# function in addition to the initializer and initargs kwargs added in the
# Python 3.7 version. Should work in Python < 3.7 as well.
from lccserver.external.futures37.process import ProcessPoolExecutor
ProcExecutor = ProcessPoolExecutor
