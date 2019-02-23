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

TEST_BUNDLE = 'https://wbhatti.org/abfiles/test-bundle-lccserver.tar.gz'
TEST_LCS = 'https://wbhatti.org/abfiles/lcc-server-demo.tar.gz'


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
    check_lcformat = os.path.exists(
        os.path.join(
            outdir, 'lcc-server-demo','lcformat-description.json'
        )
    )
    check_lcreadermodule = os.path.exists(
        os.path.join(
            outdir, 'lcc-server-demo','lcreadermodule.py'
        )
    )

    if check_lcs and check_catalog and check_lcformat and check_lcreadermodule:
        print('catalog and light curves already downloaded')
        return (os.path.join(outdir, 'lcc-server-demo','lightcurves'),
                os.path.join(outdir, 'lcc-server-demo', 'object-db.csv'))

    else:

        print('downloading light curves')
        req = requests.get(TEST_LCS,
                           timeout=10.0)
        req.raise_for_status()

        with open(os.path.join(outdir,'lcc-server-demo.tar.gz'),'wb') as outfd:
            for chunk in req.iter_content(chunk_size=1024*1024*4):
                outfd.write(chunk)
            print('done.')

        # untar the light curves
        currdir = os.getcwd()
        os.chdir(outdir)
        subprocess.run('tar xvf lcc-server-demo.tar.gz', shell=True)
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



def get_test_bundle(outdir):
    '''
    This downloads the full test bundle with:

    - original LCs
    - converted LCC CSV LCs
    - checkplot pickles

    - lcformat-description.json
    - lcreadermodule.py to read the light curves

    - lclist.pkl from lcproc.catalogs.make_lclist
    - lclist-catalog.pkl from abcat.add_cpinfo_to_lclist
    - catalog-kdtree.pkl from abcat.kdtree_from_lclist
    - catalog-objectinfo.sqlite from abcat.objectinfo_to_sqlite

    - LCC database: lcc-datasets.sqlite
    - LCC database: lcc-index.sqlite

    '''

    # this does a dumb check for existence of the target directory and
    # re-downloads if not present
    check_target = os.path.exists(os.path.join(outdir,'test-bundle-lccserver'))

    if not check_target:

        print('downloading test bundle')
        req = requests.get(
            TEST_BUNDLE,
            timeout=10.0
        )
        req.raise_for_status()

        with open(
                os.path.join(outdir,'test-bundle-lccserver.tar.gz'),'wb'
        ) as outfd:
            for chunk in req.iter_content(chunk_size=1024*1024*4):
                outfd.write(chunk)
            print('done.')

        # untar the light curves
        currdir = os.getcwd()
        os.chdir(outdir)
        subprocess.run('tar xvf test-bundle-lccserver.tar.gz', shell=True)
        os.chdir(currdir)

        # run a quick check to see if everything is where we expect
        check_lcs = (
            os.path.exists(outdir) and
            len(glob.glob(os.path.join(outdir,
                                       'test-bundle-lccserver',
                                       'lightcurves',
                                       '*.csv'))) > 0
        )

        check_catalog = os.path.exists(
            os.path.join(
                outdir, 'test-bundle-lccserver','object-db.csv'
            )
        )

        if check_lcs and check_catalog:
            print('LCC-Server test bundle downloaded successfully')
            return os.path.abspath(
                os.path.join(outdir, 'test-bundle-lccserver')
            )
        else:
            raise FileNotFoundError(
                'could not download the LCC-Server test bundle'
            )
