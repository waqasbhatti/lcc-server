'''test_cli.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT. See the LICENSE file for details.

This contains tests for the cli and basically serves as an end-to-end test for
the lcc-server ingestion pipeline.

'''

import glob
import requests
import os
import os.path
import subprocess

from lccserver import cli
from lccserver.tests.setup_tests import get_lightcurves


def test_prepare_basedir():
    '''
    This tests if the basedir was created successfully.

    '''


def test_new_collection_directories():
    '''
    This tests new_collection_directories.

    '''


def test_convert_original_lightcurves():
    '''
    This tests convert_original_lightcurves.

    '''


def test_generated_lclist_catalog():
    '''
    This tests generate_augment_lclist_catalog.

    '''


def test_generate_catalog_kdtree():
    '''
    This tests generate_catalog_kdtree.

    '''


def test_generate_catalog_database():
    '''
    This tests generate_catalog_database.

    '''


def test_new_lcc_index_db():
    '''
    This tests new_lcc_index_db.

    '''


def test_new_lcc_datasets_db():
    '''
    This tests new_lcc_index_db.

    '''


def test_add_collection_lcc_index():
    '''
    This tests new_lcc_index_db.

    '''


def test_remove_collection_from_lcc_index():
    '''
    This tests new_lcc_index_db.

    '''


def test_start_lccserver():
    '''
    This tests if the LCC-Server starts and listens on the expected ports.

    '''


def test_lccserver_conesearch():
    '''
    This runs a cone-search and checks if it returns correctly.

    '''


def test_lccserver_columnsearch():
    '''
    This runs a cone-search and checks if it returns correctly.

    '''


def test_lccserver_ftsearch():
    '''
    This runs a cone-search and checks if it returns correctly.

    '''


def test_lccserver_xmatch():
    '''
    This runs a cone-search and checks if it returns correctly.

    '''
