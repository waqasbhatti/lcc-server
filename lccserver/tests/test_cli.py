'''test_cli.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Aug 2018
License: MIT. See the LICENSE file for details.

This contains tests for the cli and basically serves as an end-to-end test for
the lcc-server ingestion pipeline.

'''

import glob
import os
import os.path
import stat
import shutil
import tempfile
import subprocess

from astrobase.hatsurveys import hatlc

from lccserver import cli
from lccserver.tests.setup_tests import get_lightcurves

from pytest import mark

def test_prepare_basedir():
    '''
    This tests if the basedir was created successfully.

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # check if the directory was created fine
    assert os.path.exists('./test-basedir') and os.path.isdir('./test-basedir')

    # check if all of the sub-directories were made fine
    basedir = os.path.abspath('./test-basedir')

    assert os.path.exists(os.path.join(basedir, 'csvlcs'))
    assert os.path.exists(os.path.join(basedir, 'datasets'))
    assert os.path.exists(os.path.join(basedir, 'docs'))
    assert os.path.exists(os.path.join(basedir, 'docs', 'static'))
    assert os.path.exists(os.path.join(basedir, 'lccjsons'))
    assert os.path.exists(os.path.join(basedir, 'products'))

    # check if the site-info.json file was copied over correctly
    site_json = os.path.join(basedir, 'site-info.json')
    assert (os.path.exists(site_json) and
            (oct(os.stat(site_json)[stat.ST_MODE]) == '0100600' or
             oct(os.stat(site_json)[stat.ST_MODE]) == '0o100600'))

    # check if there's a doc-index.json in the docs directory
    assert os.path.exists(os.path.join(basedir,'docs','doc-index.json'))

    # check if the citation.md and lcformat.md files exist in docs
    assert os.path.exists(os.path.join(basedir,'docs','lcformat.md'))
    assert os.path.exists(os.path.join(basedir,'docs','citation.md'))

    # check if the .lccserver-secret-* files were generated correctly
    email_secrets = os.path.join(basedir,'.lccserver.secret-email')
    admin_secrets = os.path.join(basedir,'.lccserver.admin-credentials')
    session_secrets = os.path.join(basedir,'.lccserver.secret')
    cpserver_secrets = os.path.join(basedir,'.lccserver.secret-cpserver')
    authnzrv_secrets = os.path.join(basedir,'.lccserver.secret-fernet')

    assert (os.path.exists(email_secrets) and
            (oct(os.stat(email_secrets)[stat.ST_MODE]) == '0100400' or
             oct(os.stat(email_secrets)[stat.ST_MODE]) == '0o100400'))

    assert (os.path.exists(admin_secrets) and
            (oct(os.stat(admin_secrets)[stat.ST_MODE]) == '0100400' or
             oct(os.stat(admin_secrets)[stat.ST_MODE]) == '0o100400'))

    assert (os.path.exists(session_secrets) and
            (oct(os.stat(session_secrets)[stat.ST_MODE]) == '0100400' or
             oct(os.stat(session_secrets)[stat.ST_MODE]) == '0o100400'))

    assert (os.path.exists(cpserver_secrets) and
            (oct(os.stat(cpserver_secrets)[stat.ST_MODE]) == '0100400' or
             oct(os.stat(cpserver_secrets)[stat.ST_MODE]) == '0o100400'))

    assert (os.path.exists(authnzrv_secrets) and
            (oct(os.stat(authnzrv_secrets)[stat.ST_MODE]) == '0100400' or
             oct(os.stat(authnzrv_secrets)[stat.ST_MODE]) == '0o100400'))

    # at the end, remove the test-basedir
    shutil.rmtree('./test-basedir')



def test_new_collection_directories():
    '''
    This tests new_collection_directories.

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'

    # check the various subdirs
    checkplot_subdir = os.path.join(basedir,collection,'checkplots')
    lightcurves_subdir = os.path.join(basedir,collection,'lightcurves')
    periodfinding_subdir = os.path.join(basedir,collection,'periodfinding')

    assert os.path.exists(checkplot_subdir)
    assert os.path.exists(lightcurves_subdir)
    assert os.path.exists(periodfinding_subdir)

    # check the light curve subdir symlink
    lccdir_symlink = os.path.join(basedir,'csvlcs',collection)
    assert os.path.exists(lccdir_symlink)
    assert os.path.islink(lccdir_symlink)
    assert (
        os.path.abspath(os.path.realpath(lccdir_symlink)) ==
        os.path.abspath(lightcurves_subdir)
    )

    # check the lcformat-description.json file
    lcformat_json = os.path.join(basedir,
                                 collection,
                                 'lcformat-description.json')
    assert os.path.exists(lcformat_json)

    # check the symlink in the lccjsons subdir to this JSON file
    lcformat_symlink = os.path.join(basedir,
                                    'lccjsons',
                                    collection,
                                    'lcformat-description.json')
    assert os.path.exists(lcformat_symlink)
    assert os.path.islink(lcformat_symlink)
    assert (
        os.path.abspath(os.path.realpath(lcformat_symlink)) ==
        os.path.abspath(lcformat_json)
    )

    # at the end, remove the test-basedir
    shutil.rmtree('./test-basedir')



def test_convert_original_lightcurves():
    '''
    This tests convert_original_lightcurves.

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'
    collection_subdir = os.path.join(basedir, collection)
    lightcurves_subdir = os.path.join(basedir, collection, 'lightcurves')

    with tempfile.TemporaryDirectory() as tempdir:

        # download light curves into a temporary directory
        get_lightcurves(tempdir)

        # copy over the lcformat-description.json, object-db.csv, and
        # lcreadermodule.py to collection_subdir
        dl_lcformat_json = os.path.join(tempdir,
                                        'lcc-server-demo',
                                        'lcformat-description.json')
        dl_readermodule = os.path.join(tempdir,
                                       'lcc-server-demo',
                                       'lcreadermodule.py')
        dl_objectdb = os.path.join(tempdir,
                                   'lcc-server-demo',
                                   'object-db.csv')

        shutil.copy(dl_lcformat_json, collection_subdir)
        shutil.copy(dl_readermodule, collection_subdir)
        shutil.copy(dl_objectdb, collection_subdir)

        # copy over the light curves to the lightcurves_subdir
        os.rmdir(lightcurves_subdir)
        dl_lcdir = os.path.join(tempdir, 'lcc-server-demo', 'lightcurves')
        shutil.copytree(dl_lcdir, lightcurves_subdir)

    # check if we have an lcformat-description.json, object-db.csv, and
    # lcreadermodule.py
    lcformat_json = os.path.join(collection_subdir,'lcformat-description.json')
    objectdb_csv = os.path.join(collection_subdir,'object-db.csv')
    lcreadermodule = os.path.join(collection_subdir,'lcreadermodule.py')

    assert os.path.exists(lcformat_json)
    assert os.path.exists(objectdb_csv)
    assert os.path.exists(lcreadermodule)

    # check if we have all 100 original LCs
    lcfiles = sorted(glob.glob(os.path.join(lightcurves_subdir,'*.csv')))
    assert len(lcfiles) == 100
    assert os.path.basename(lcfiles[0]) == 'HAT-215-0001809-lc.csv'
    assert os.path.basename(lcfiles[-1]) == 'HAT-265-0037533-lc.csv'

    # run convert_original_lightcurves on 5 LCs
    cli.convert_original_lightcurves(basedir,
                                     collection,
                                     max_lcs=5)

    # check if all of the original LCs have converted LCs
    assert os.path.exists(os.path.join(lightcurves_subdir,
                                       'HAT-215-0001809-csvlc.gz'))
    assert os.path.exists(os.path.join(lightcurves_subdir,
                                       'HAT-215-0004605-csvlc.gz'))
    assert os.path.exists(os.path.join(lightcurves_subdir,
                                       'HAT-215-0005039-csvlc.gz'))
    assert os.path.exists(os.path.join(lightcurves_subdir,
                                       'HAT-215-0005050-csvlc.gz'))
    assert os.path.exists(os.path.join(lightcurves_subdir,
                                       'HAT-215-0010422-csvlc.gz'))


    # check if the converted LC is readable
    lcd = hatlc.read_csvlc(os.path.join(lightcurves_subdir,
                                        'HAT-215-0001809-csvlc.gz'))
    assert lcd['objectid'] == 'HAT-215-0001809'
    assert lcd['objectinfo']['ndet'] == 11901
    assert lcd['columns'] == ['rjd',
                              'stf',
                              'xcc',
                              'ycc',
                              'aim_000',
                              'aim_001',
                              'aim_002',
                              'aie_000',
                              'aie_001',
                              'aie_002',
                              'aep_000',
                              'aep_001',
                              'aep_002']

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)



def test_generate_lc_catalog():
    '''
    This tests the generation of an LC catalog.

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'
    collection_subdir = os.path.join(basedir, collection)
    lightcurves_subdir = os.path.join(basedir, collection, 'lightcurves')

    with tempfile.TemporaryDirectory() as tempdir:

        # download light curves into a temporary directory
        get_lightcurves(tempdir)

        # copy over the lcformat-description.json, object-db.csv, and
        # lcreadermodule.py to collection_subdir
        dl_lcformat_json = os.path.join(tempdir,
                                        'lcc-server-demo',
                                        'lcformat-description.json')
        dl_readermodule = os.path.join(tempdir,
                                       'lcc-server-demo',
                                       'lcreadermodule.py')
        dl_objectdb = os.path.join(tempdir,
                                   'lcc-server-demo',
                                   'object-db.csv')

        shutil.copy(dl_lcformat_json, collection_subdir)
        shutil.copy(dl_readermodule, collection_subdir)
        shutil.copy(dl_objectdb, collection_subdir)

        # copy over the light curves to the lightcurves_subdir
        os.rmdir(lightcurves_subdir)
        dl_lcdir = os.path.join(tempdir, 'lcc-server-demo', 'lightcurves')
        shutil.copytree(dl_lcdir, lightcurves_subdir)

    # check if we have an lcformat-description.json, object-db.csv, and
    # lcreadermodule.py
    lcformat_json = os.path.join(collection_subdir,'lcformat-description.json')
    objectdb_csv = os.path.join(collection_subdir,'object-db.csv')
    lcreadermodule = os.path.join(collection_subdir,'lcreadermodule.py')

    assert os.path.exists(lcformat_json)
    assert os.path.exists(objectdb_csv)
    assert os.path.exists(lcreadermodule)

    # check if we have all 100 original LCs
    lcfiles = sorted(glob.glob(os.path.join(lightcurves_subdir,'*.csv')))
    assert len(lcfiles) == 100
    assert os.path.basename(lcfiles[0]) == 'HAT-215-0001809-lc.csv'
    assert os.path.basename(lcfiles[-1]) == 'HAT-265-0037533-lc.csv'

    # register the LC format
    from astrobase.lcproc import register_lcformat
    from lccserver.backend.abcat import get_lcformat_description

    lcform = get_lcformat_description(
        os.path.join(collection_subdir,'lcformat-description.json')
    )
    register_lcformat(
        lcform['parsed_formatinfo']['formatkey'],
        lcform['parsed_formatinfo']['fileglob'],
        ['rjd'],
        ['aep_000'],
        ['aie_000'],
        lcform['parsed_formatinfo']['readermodule'],
        lcform['parsed_formatinfo']['readerfunc'],
        readerfunc_kwargs=lcform['parsed_formatinfo']['readerfunc_kwargs'],
        normfunc_module=lcform['parsed_formatinfo']['normmodule'],
        normfunc=lcform['parsed_formatinfo']['normfunc'],
        normfunc_kwargs=lcform['parsed_formatinfo']['normfunc_kwargs'],
        magsarefluxes=lcform['magsarefluxes'],
        overwrite_existing=True
    )

    # make the light curve catalog
    from astrobase.lcproc.catalogs import make_lclist
    lc_catalog = make_lclist(
        lightcurves_subdir,
        os.path.join(collection_subdir,'lclist.pkl'),
        lcformat=lcform['formatkey']
    )
    assert os.path.exists(lc_catalog)

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)



def test_pfpickle_generation():
    '''
    This tests generation of period-finding result pickles for the LCs.

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'
    collection_subdir = os.path.join(basedir, collection)
    lightcurves_subdir = os.path.join(basedir, collection, 'lightcurves')

    with tempfile.TemporaryDirectory() as tempdir:

        # download light curves into a temporary directory
        get_lightcurves(tempdir)

        # copy over the lcformat-description.json, object-db.csv, and
        # lcreadermodule.py to collection_subdir
        dl_lcformat_json = os.path.join(tempdir,
                                        'lcc-server-demo',
                                        'lcformat-description.json')
        dl_readermodule = os.path.join(tempdir,
                                       'lcc-server-demo',
                                       'lcreadermodule.py')
        dl_objectdb = os.path.join(tempdir,
                                   'lcc-server-demo',
                                   'object-db.csv')

        shutil.copy(dl_lcformat_json, collection_subdir)
        shutil.copy(dl_readermodule, collection_subdir)
        shutil.copy(dl_objectdb, collection_subdir)

        # copy over the light curves to the lightcurves_subdir
        os.rmdir(lightcurves_subdir)
        dl_lcdir = os.path.join(tempdir, 'lcc-server-demo', 'lightcurves')
        shutil.copytree(dl_lcdir, lightcurves_subdir)

    # check if we have an lcformat-description.json, object-db.csv, and
    # lcreadermodule.py
    lcformat_json = os.path.join(collection_subdir,'lcformat-description.json')
    objectdb_csv = os.path.join(collection_subdir,'object-db.csv')
    lcreadermodule = os.path.join(collection_subdir,'lcreadermodule.py')

    assert os.path.exists(lcformat_json)
    assert os.path.exists(objectdb_csv)
    assert os.path.exists(lcreadermodule)

    # check if we have all 100 original LCs
    lcfiles = sorted(glob.glob(os.path.join(lightcurves_subdir,'*.csv')))
    assert len(lcfiles) == 100
    assert os.path.basename(lcfiles[0]) == 'HAT-215-0001809-lc.csv'
    assert os.path.basename(lcfiles[-1]) == 'HAT-265-0037533-lc.csv'

    # register the LC format
    from astrobase.lcproc import register_lcformat
    from lccserver.backend.abcat import get_lcformat_description
    lcform = get_lcformat_description(
        os.path.join(collection_subdir,'lcformat-description.json')
    )
    register_lcformat(
        lcform['parsed_formatinfo']['formatkey'],
        lcform['parsed_formatinfo']['fileglob'],
        ['rjd'],
        ['aep_000'],
        ['aie_000'],
        lcform['parsed_formatinfo']['readermodule'],
        lcform['parsed_formatinfo']['readerfunc'],
        readerfunc_kwargs=lcform['parsed_formatinfo']['readerfunc_kwargs'],
        normfunc_module=lcform['parsed_formatinfo']['normmodule'],
        normfunc=lcform['parsed_formatinfo']['normfunc'],
        normfunc_kwargs=lcform['parsed_formatinfo']['normfunc_kwargs'],
        magsarefluxes=lcform['magsarefluxes'],
        overwrite_existing=True
    )

    # generate period-finding pickles for the first 5 LCs
    from astrobase.lcproc.periodsearch import parallel_pf

    parallel_pf(
        lcfiles[:5],
        os.path.join(collection_subdir,
                     'periodfinding'),
        lcformat=lcform['formatkey'],
        pfmethods=('gls','pdm'),
        pfkwargs=({},{}),
        getblssnr=False
    )

    # check if all period-finding pickles exist
    pfpickle_subdir = os.path.join(collection_subdir,
                                   'periodfinding')

    pfpickles = sorted(glob.glob(os.path.join(pfpickle_subdir,
                                              'periodfinding*.pkl')))
    assert len(pfpickles) == 5
    assert (
        os.path.basename(pfpickles[0]) == 'periodfinding-HAT-215-0001809.pkl'
    )
    assert (
        os.path.basename(pfpickles[-1]) == 'periodfinding-HAT-215-0010422.pkl'
    )

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)



def test_augcat_kdtree_databases():
    '''This tests generation of a checkplot-info augmented LC catalog plus a
    KD-Tree for spatial matching for the LC collection.

    - makes pfpickles
    - removes all but the first five LCs in the test collection
    - makes an LC catalog for these
    - makes checkplot pickles for these plus the pfpickles
    - adds checkplot info to LC catalog -> augmented LC catalog
    - generates KD-Tree
    - generates objectinfo SQLite DB.
    - generates the LCC-Server databases and adds collection to them

    NOTE: it appears this crashes on MacOS 10.13+ when run as part of the full
    pytest suite. Doesn't crash when:
    `pytest test_cli.py::test_augcat_kdtree_databases` is called in isolation.
    Works fine both ways on Linux. Another WTF courtesy of Apple.

    Possibly related:

    - http://sealiesoftware.com/blog/archive/2017/6/5/Objective-C_and_fork_in_macOS_1013.html

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'
    collection_subdir = os.path.join(basedir, collection)
    lightcurves_subdir = os.path.join(basedir, collection, 'lightcurves')

    with tempfile.TemporaryDirectory() as tempdir:

        # download light curves into a temporary directory
        get_lightcurves(tempdir)

        # copy over the lcformat-description.json, object-db.csv, and
        # lcreadermodule.py to collection_subdir
        dl_lcformat_json = os.path.join(tempdir,
                                        'lcc-server-demo',
                                        'lcformat-description.json')
        dl_readermodule = os.path.join(tempdir,
                                       'lcc-server-demo',
                                       'lcreadermodule.py')
        dl_objectdb = os.path.join(tempdir,
                                   'lcc-server-demo',
                                   'object-db.csv')

        shutil.copy(dl_lcformat_json, collection_subdir)
        shutil.copy(dl_readermodule, collection_subdir)
        shutil.copy(dl_objectdb, collection_subdir)

        # copy over the light curves to the lightcurves_subdir
        os.rmdir(lightcurves_subdir)
        dl_lcdir = os.path.join(tempdir, 'lcc-server-demo', 'lightcurves')
        shutil.copytree(dl_lcdir, lightcurves_subdir)

    # check if we have an lcformat-description.json, object-db.csv, and
    # lcreadermodule.py
    lcformat_json = os.path.join(collection_subdir,'lcformat-description.json')
    objectdb_csv = os.path.join(collection_subdir,'object-db.csv')
    lcreadermodule = os.path.join(collection_subdir,'lcreadermodule.py')

    assert os.path.exists(lcformat_json)
    assert os.path.exists(objectdb_csv)
    assert os.path.exists(lcreadermodule)

    # check if we have all 100 original LCs
    lcfiles = sorted(glob.glob(os.path.join(lightcurves_subdir,'*.csv')))
    assert len(lcfiles) == 100
    assert os.path.basename(lcfiles[0]) == 'HAT-215-0001809-lc.csv'
    assert os.path.basename(lcfiles[-1]) == 'HAT-265-0037533-lc.csv'

    # register the LC format
    from astrobase.lcproc import register_lcformat
    from lccserver.backend.abcat import get_lcformat_description
    lcform = get_lcformat_description(
        os.path.join(collection_subdir,'lcformat-description.json')
    )
    register_lcformat(
        lcform['parsed_formatinfo']['formatkey'],
        lcform['parsed_formatinfo']['fileglob'],
        ['rjd'],
        ['aep_000'],
        ['aie_000'],
        lcform['parsed_formatinfo']['readermodule'],
        lcform['parsed_formatinfo']['readerfunc'],
        readerfunc_kwargs=lcform['parsed_formatinfo']['readerfunc_kwargs'],
        normfunc_module=lcform['parsed_formatinfo']['normmodule'],
        normfunc=lcform['parsed_formatinfo']['normfunc'],
        normfunc_kwargs=lcform['parsed_formatinfo']['normfunc_kwargs'],
        magsarefluxes=lcform['magsarefluxes'],
        overwrite_existing=True
    )

    # generate period-finding pickles for the first 5 LCs
    from astrobase.lcproc.periodsearch import parallel_pf

    parallel_pf(
        lcfiles[:5],
        os.path.join(collection_subdir,
                     'periodfinding'),
        lcformat=lcform['formatkey'],
        pfmethods=('gls',),
        pfkwargs=({},),
        getblssnr=False
    )

    # check if all period-finding pickles exist
    pfpickle_subdir = os.path.join(collection_subdir,
                                   'periodfinding')

    pfpickles = sorted(glob.glob(os.path.join(pfpickle_subdir,
                                              'periodfinding*.pkl')))
    assert len(pfpickles) == 5
    assert (
        os.path.basename(pfpickles[0]) == 'periodfinding-HAT-215-0001809.pkl'
    )
    assert (
        os.path.basename(pfpickles[-1]) == 'periodfinding-HAT-215-0010422.pkl'
    )

    # remove the rest of the LCs in prep for making LC catalog
    for lc in lcfiles[5:]:
        os.remove(lc)

    # make the light curve catalog
    from astrobase.lcproc.catalogs import make_lclist
    lc_catalog = make_lclist(
        lightcurves_subdir,
        os.path.join(collection_subdir,'lclist.pkl'),
        lcformat=lcform['formatkey']
    )
    assert os.path.exists(lc_catalog)

    # make checkplots now
    from astrobase.lcproc.checkplotgen import parallel_cp

    import sys
    if sys.platform == 'darwin':
        import requests
        try:
            requests.get('http://captive.apple.com/hotspot-detect.html',
                         timeout=5.0)
        except Exception as e:
            pass

    parallel_cp(
        pfpickles,
        os.path.join(collection_subdir,'checkplots'),
        os.path.join(collection_subdir, 'lightcurves'),
        fast_mode=10.0,
        lcfnamelist=lcfiles[:5],
        lclistpkl=lc_catalog,
        minobservations=49,
        lcformat=lcform['formatkey']
    )

    # check if the checkplots exist
    checkplot_subdir = os.path.join(collection_subdir,'checkplots')

    checkplotpkls = sorted(glob.glob(os.path.join(checkplot_subdir,
                                                  'checkplot-*.pkl')))
    assert len(checkplotpkls) == 5
    assert (
        os.path.basename(checkplotpkls[0]) ==
        'checkplot-HAT-215-0001809-aep_000.pkl'
    )
    assert (
        os.path.basename(checkplotpkls[-1]) ==
        'checkplot-HAT-215-0010422-aep_000.pkl'
    )

    # generate the augmented LC catalog
    augcat = cli.generate_augmented_lclist_catalog(
        basedir,
        collection,
        lc_catalog,
        'aep_000',
    )
    assert os.path.exists(augcat)

    # generate the KD-Tree
    kdt = cli.generate_catalog_kdtree(
        basedir,
        collection
    )
    assert os.path.exists(kdt)

    # generate the objectinfo SQLite file for this collection
    collection_db = cli.generate_catalog_database(
        basedir,
        collection,
        overwrite_existing=True
    )
    assert os.path.exists(collection_db)

    # generate the LCC index DB
    lcc_index_db = cli.new_lcc_index_db(basedir)
    assert os.path.exists(lcc_index_db)

    # generate the LCC datasets DB
    lcc_datasets_db = cli.new_lcc_datasets_db(basedir)
    assert os.path.exists(lcc_datasets_db)

    # add this collection to the LCC index DB
    cli.add_collection_to_lcc_index(basedir,
                                    collection)

    from lccserver.backend import dbsearch
    collection_info = dbsearch.sqlite_list_collections(
        basedir
    )
    assert collection in collection_info['info']['collection_id']

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)


@mark.xfail(
    reason='Fails randomly because authnzerver mysteriously hangs sometimes'
)
def test_lccserver_api():
    '''This tests if the LCC-Server starts, listens on the expected ports, and
    responds correctly to search queries.

    - runs all steps in test_lcc_server_databases
    - checks if the LCC-Server services start correctly
    - checks if GET localhost:12500/api/collections works, indicating the
      LCC-Server is responding correctly.
    - runs cone-search, fulltext-search, column-search, and xmatch-search

    '''

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make a basedir in the current directory
    cli.prepare_basedir('./test-basedir',
                        interactive=False)

    # make a new collection and its directories
    cli.new_collection_directories(
        './test-basedir',
        'test-collection'
    )

    basedir = './test-basedir'
    collection = 'test-collection'
    collection_subdir = os.path.join(basedir, collection)
    lightcurves_subdir = os.path.join(basedir, collection, 'lightcurves')

    with tempfile.TemporaryDirectory() as tempdir:

        # download light curves into a temporary directory
        get_lightcurves(tempdir)

        # copy over the lcformat-description.json, object-db.csv, and
        # lcreadermodule.py to collection_subdir
        dl_lcformat_json = os.path.join(tempdir,
                                        'lcc-server-demo',
                                        'lcformat-description.json')
        dl_readermodule = os.path.join(tempdir,
                                       'lcc-server-demo',
                                       'lcreadermodule.py')
        dl_objectdb = os.path.join(tempdir,
                                   'lcc-server-demo',
                                   'object-db.csv')

        shutil.copy(dl_lcformat_json, collection_subdir)
        shutil.copy(dl_readermodule, collection_subdir)
        shutil.copy(dl_objectdb, collection_subdir)

        # copy over the light curves to the lightcurves_subdir
        os.rmdir(lightcurves_subdir)
        dl_lcdir = os.path.join(tempdir, 'lcc-server-demo', 'lightcurves')
        shutil.copytree(dl_lcdir, lightcurves_subdir)

    # check if we have an lcformat-description.json, object-db.csv, and
    # lcreadermodule.py
    lcformat_json = os.path.join(collection_subdir,'lcformat-description.json')
    objectdb_csv = os.path.join(collection_subdir,'object-db.csv')
    lcreadermodule = os.path.join(collection_subdir,'lcreadermodule.py')

    assert os.path.exists(lcformat_json)
    assert os.path.exists(objectdb_csv)
    assert os.path.exists(lcreadermodule)

    # check if we have all 100 original LCs
    lcfiles = sorted(glob.glob(os.path.join(lightcurves_subdir,'*.csv')))
    assert len(lcfiles) == 100
    assert os.path.basename(lcfiles[0]) == 'HAT-215-0001809-lc.csv'
    assert os.path.basename(lcfiles[-1]) == 'HAT-265-0037533-lc.csv'

    # register the LC format
    from astrobase.lcproc import register_lcformat
    from lccserver.backend.abcat import get_lcformat_description
    lcform = get_lcformat_description(
        os.path.join(collection_subdir,'lcformat-description.json')
    )
    register_lcformat(
        lcform['parsed_formatinfo']['formatkey'],
        lcform['parsed_formatinfo']['fileglob'],
        ['rjd'],
        ['aep_000'],
        ['aie_000'],
        lcform['parsed_formatinfo']['readermodule'],
        lcform['parsed_formatinfo']['readerfunc'],
        readerfunc_kwargs=lcform['parsed_formatinfo']['readerfunc_kwargs'],
        normfunc_module=lcform['parsed_formatinfo']['normmodule'],
        normfunc=lcform['parsed_formatinfo']['normfunc'],
        normfunc_kwargs=lcform['parsed_formatinfo']['normfunc_kwargs'],
        magsarefluxes=lcform['magsarefluxes'],
        overwrite_existing=True
    )

    # generate period-finding pickles for the first 5 LCs
    from astrobase.lcproc.periodsearch import parallel_pf

    parallel_pf(
        lcfiles[:5],
        os.path.join(collection_subdir,
                     'periodfinding'),
        lcformat=lcform['formatkey'],
        pfmethods=('gls',),
        pfkwargs=({},),
        getblssnr=False
    )

    # check if all period-finding pickles exist
    pfpickle_subdir = os.path.join(collection_subdir,
                                   'periodfinding')

    pfpickles = sorted(glob.glob(os.path.join(pfpickle_subdir,
                                              'periodfinding*.pkl')))
    assert len(pfpickles) == 5
    assert (
        os.path.basename(pfpickles[0]) == 'periodfinding-HAT-215-0001809.pkl'
    )
    assert (
        os.path.basename(pfpickles[-1]) == 'periodfinding-HAT-215-0010422.pkl'
    )

    # remove the rest of the LCs in prep for making LC catalog
    for lc in lcfiles[5:]:
        os.remove(lc)

    # make the light curve catalog
    from astrobase.lcproc.catalogs import make_lclist
    lc_catalog = make_lclist(
        lightcurves_subdir,
        os.path.join(collection_subdir,'lclist.pkl'),
        lcformat=lcform['formatkey']
    )
    assert os.path.exists(lc_catalog)

    # make checkplots now
    from astrobase.lcproc.checkplotgen import parallel_cp

    import sys
    if sys.platform == 'darwin':
        import requests
        requests.get('http://captive.apple.com/hotspot-detect.html')

    parallel_cp(
        pfpickles,
        os.path.join(collection_subdir,'checkplots'),
        os.path.join(collection_subdir, 'lightcurves'),
        fast_mode=10.0,
        lcfnamelist=lcfiles[:5],
        lclistpkl=lc_catalog,
        minobservations=49,
        lcformat=lcform['formatkey']
    )

    # check if the checkplots exist
    checkplot_subdir = os.path.join(collection_subdir,'checkplots')

    checkplotpkls = sorted(glob.glob(os.path.join(checkplot_subdir,
                                                  'checkplot-*.pkl')))
    assert len(checkplotpkls) == 5
    assert (
        os.path.basename(checkplotpkls[0]) ==
        'checkplot-HAT-215-0001809-aep_000.pkl'
    )
    assert (
        os.path.basename(checkplotpkls[-1]) ==
        'checkplot-HAT-215-0010422-aep_000.pkl'
    )

    # generate the augmented LC catalog
    augcat = cli.generate_augmented_lclist_catalog(
        basedir,
        collection,
        lc_catalog,
        'aep_000',
    )
    assert os.path.exists(augcat)

    # generate the KD-Tree
    kdt = cli.generate_catalog_kdtree(
        basedir,
        collection
    )
    assert os.path.exists(kdt)

    # generate the objectinfo SQLite file for this collection
    collection_db = cli.generate_catalog_database(
        basedir,
        collection,
        overwrite_existing=True
    )
    assert os.path.exists(collection_db)

    # generate the LCC index DB
    lcc_index_db = cli.new_lcc_index_db(basedir)
    assert os.path.exists(lcc_index_db)

    # generate the LCC datasets DB
    lcc_datasets_db = cli.new_lcc_datasets_db(basedir)
    assert os.path.exists(lcc_datasets_db)

    # add this collection to the LCC index DB
    cli.add_collection_to_lcc_index(basedir,
                                    collection)

    from lccserver.backend import dbsearch
    collection_info = dbsearch.sqlite_list_collections(
        basedir
    )
    assert collection in collection_info['info']['collection_id']

    #
    # now run the LCC-server
    #

    # make sure to remove any existing indexserver, authnzerver, checkplotserver
    # procs
    kill_str = (
        "ps aux | grep %s | grep -v grep | "
        "awk '{ print $2 }' | xargs kill"
    )
    subprocess.run(kill_str % 'indexserver',shell=True,check=False)
    subprocess.run(kill_str % 'checkplotserver',shell=True,check=False)
    subprocess.run(kill_str % 'authnzerver',shell=True,check=False)

    # start the authnzerver
    import secrets
    cachedir = '/tmp/lccs-%s' % secrets.token_urlsafe(8)
    authnzerver_cmd = (
        "authnzerver --basedir='{basedir}' --cachedir='{cachedir}'"
    ).format(basedir=os.path.abspath(basedir),
             cachedir=cachedir)
    authnzerver_proc = subprocess.Popen(
        authnzerver_cmd,
        shell=True,
    )

    # start the checkplotserver
    checkplotserver_cmd = (
        "checkplotserver "
        "--standalone=1 "
        "--sharedsecret='%s'" % os.path.join(os.path.abspath(basedir),
                                             '.lccserver.secret-cpserver')
    )
    checkplotserver_proc = subprocess.Popen(
        checkplotserver_cmd,
        shell=True,
    )

    # start the indexserver
    indexserver_cmd = (
        "indexserver --basedir='%s' --port=12345" % os.path.abspath(basedir)
    )
    indexserver_proc = subprocess.Popen(
        indexserver_cmd,
        shell=True,
    )

    import time
    time.sleep(5)

    # remove any API keys for this test LCC server
    dl_apikey = os.path.expanduser(
        '~/.astrobase/lccs/apikey-http-localhost:12345'
    )
    if os.path.exists(dl_apikey):
        os.remove(dl_apikey)

    # hit the collections API to make sure the server is live
    import requests
    resp = requests.get('http://localhost:12345/api/collections',timeout=120.0)
    respjson = resp.json()

    # make sure our collection is present
    assert respjson['status'] == 'ok'
    assert collection in respjson['result']['collections']['collection_id']

    #
    # do tests of the API
    #
    from astrobase.services import lccs

    #
    # run a cone-search test around the position of the first object
    #
    res, csv, lcz = lccs.cone_search('http://localhost:12345',
                                     61.681917,
                                     30.444036,
                                     outdir=basedir)
    assert res['status'] == 'ok'
    assert res['result']['total_nmatches'] == 1
    assert os.path.exists(csv)
    assert os.path.exists(lcz)

    # get the dataset indicated
    ds = lccs.get_dataset('http://localhost:12345',
                          res['result']['setid'],
                          strformat=True)
    assert ds['rows'][0] == ['HAT-215-0001809',
                             'HAT-215-0001809',
                             '61.68192',
                             '30.44404',
                             '61.68192',
                             '30.44404',
                             '/l/test-collection/HAT-215-0001809-csvlc.gz',
                             '0.017',
                             '1',
                             'public',
                             'None',
                             'test_collection']

    #
    # run a fulltext search test for the objectid of the first object
    #
    res, csv, lcz = lccs.fulltext_search('http://localhost:12345',
                                         '"HAT-215-0001809"',
                                         outdir=basedir)
    assert res['status'] == 'ok'
    assert res['result']['total_nmatches'] == 1
    assert os.path.exists(csv)
    assert os.path.exists(lcz)

    # get the dataset indicated
    ds = lccs.get_dataset('http://localhost:12345',
                          res['result']['setid'],
                          strformat=True)
    assert ds['rows'][0][0] == 'HAT-215-0001809'

    #
    # run a column-search test
    #
    res, csv, lcz = lccs.column_search(
        'http://localhost:12345',
        'ndet lt 9000',
        outdir=basedir
    )
    assert res['status'] == 'ok'
    assert res['result']['total_nmatches'] == 3
    assert os.path.exists(csv)
    assert os.path.exists(lcz)

    # get the dataset indicated
    ds = lccs.get_dataset('http://localhost:12345',
                          res['result']['setid'],
                          strformat=True)
    assert [x[1] for x in ds['rows']] == ['HAT-215-0005050',
                                          'HAT-215-0010422',
                                          'HAT-215-0004605']

    #
    # run an xmatch test
    #
    with open(os.path.join(basedir,'text-xmatch-upload.txt'),'w') as outfd:
        outfd.write('object-a 67.39750 28.89177\n')
        outfd.write('object-b 67.82830 27.99300\n')
        outfd.write('object-c 67.37379 26.28145\n')

    res, csv, lcz = lccs.xmatch_search(
        'http://localhost:12345',
        os.path.join(basedir, 'text-xmatch-upload.txt'),
        outdir=basedir
    )
    assert res['status'] == 'ok'
    assert res['result']['total_nmatches'] == 3
    assert os.path.exists(csv)
    assert os.path.exists(lcz)

    # get the dataset indicated
    ds = lccs.get_dataset('http://localhost:12345',
                          res['result']['setid'],
                          strformat=True)
    assert [x[0] for x in ds['rows']] == ['HAT-215-0005050',
                                          'HAT-215-0010422',
                                          'HAT-215-0004605']

    #
    # test getting a list of all recent datasets
    #
    dslist = lccs.list_recent_datasets('http://localhost:12345')
    assert len(dslist) == 4

    #
    # test getting a collection list
    #
    lcclist = lccs.list_lc_collections('http://localhost:12345')
    assert collection in lcclist['collection_id']

    #
    # test looking up an object's info
    #
    from numpy.testing import assert_allclose

    obj = lccs.object_info('http://localhost:12345',
                           'HAT-215-0001809',
                           'test_collection')
    assert obj['objectid'] == 'HAT-215-0001809'
    assert obj['objectinfo']['gaiaid'] == '168910519611182592'
    assert obj['objectinfo']['ticid'] == '349454959'

    assert obj['pfmethods'] == ['0-gls']
    assert_allclose(
        obj['0-gls']['bestperiod'],
        1.0142220888196252,
        atol=1.0e-4
    )

    #
    # end of all API tests
    #

    # kill the servers
    indexserver_proc.terminate()
    checkplotserver_proc.terminate()
    authnzerver_proc.terminate()

    # remove any API keys for this test LCC server
    dl_apikey = os.path.expanduser(
        '~/.astrobase/lccs/apikey-http-localhost:12345'
    )
    if os.path.exists(dl_apikey):
        os.remove(dl_apikey)

    # remove the test-basedir
    shutil.rmtree('./test-basedir', ignore_errors=True)

    # make sure to remove any indexserver, authnzerver, checkplotserver procs
    kill_str = (
        "ps aux | grep %s | grep -v grep | "
        "awk '{ print $2 }' | xargs kill"
    )

    time.sleep(5)

    subprocess.run(kill_str % 'indexserver',shell=True,check=False)
    subprocess.run(kill_str % 'checkplotserver',shell=True,check=False)
    subprocess.run(kill_str % 'authnzerver',shell=True,check=False)
