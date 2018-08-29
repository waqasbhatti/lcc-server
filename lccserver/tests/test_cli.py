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

def get_lightcurves(outdir):
    '''
    This downloads test light curves if they're not already present.

    '''

    check_lcs = (
        os.path.exists(outdir) and
        len(glob.glob(os.path.join(outdir,
                                   'lcc-server-demo',
                                   'lightcurves',
                                   '*.csv'))) > 0
    )

    check_catalog = os.path.exists(
        os.path.join(
            outdir, 'lcc-server-demo','object-db.csv'
        )
    )

    if check_lcs and check_catalog:
        print('catalog and light curves already downloaded')
        return (os.path.join(outdir, 'lcc-server-demo','lightcurves'),
                os.path.join(outdir, 'lcc-server-demo', 'object-db.csv'))

    else:

        print('downloading light curves')
        req = requests.get('https://wbhatti.org/abfiles/lcc-server-demo.tar.gz',
                           timeout=10.0)
        req.raise_for_status()

        with open(os.path.join(outdir,'lcc-server-demo.tar.gz'),'wb') as outfd:
            for chunk in req.iter_content(chunk_size=1024*1024*4):
                outfd.write(chunk)
            print('done.')

        # untar the light curves
        currdir = os.getcwd()
        os.chdir(outdir)
        p = subprocess.run('tar xvf lcc-server-demo.tar.gz', shell=True)
        os.chdir(currdir)

        check_lcs = (
            os.path.exists(outdir) and
            len(glob.glob(os.path.join(outdir,
                                       'lcc-server-demo',
                                       'lightcurves',
                                       '*.csv'))) > 0
        )

        check_catalog = os.path.exists(
            os.path.join(
                outdir, 'lcc-server-demo','object-db.csv'
            )
        )

        if check_lcs and check_catalog:
            print('catalog and light curves downloaded successfully')
            return (os.path.join(outdir, 'lcc-server-demo','lightcurves'),
                    os.path.join(outdir, 'lcc-server-demo', 'object-db.csv'))
        else:
            raise FileNotFoundError('could not download the light curves')



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
