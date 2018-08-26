#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''dataserver_handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) -
                             Apr 2018

These are Tornado handlers for the dataserver.

'''

####################
## SYSTEM IMPORTS ##
####################

import os
import os.path
import logging
import numpy as np


######################################
## CUSTOM JSON ENCODER FOR FRONTEND ##
######################################

# we need this to send objects with the following types to the frontend:
# - bytes
# - ndarray
import json

class FrontendEncoder(json.JSONEncoder):

    def default(self, obj):

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return obj.decode()
        elif isinstance(obj, complex):
            return (obj.real, obj.imag)
        elif (isinstance(obj, (float, np.float64, np.float_)) and
              not np.isfinite(obj)):
            return None
        elif isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        else:
            return json.JSONEncoder.default(self, obj)

# this replaces the default encoder and makes it so Tornado will do the right
# thing when it converts dicts to JSON when a
# tornado.web.RequestHandler.write(dict) is called.
json._default_encoder = FrontendEncoder()

#############
## LOGGING ##
#############

# get a logger
LOGGER = logging.getLogger(__name__)

#####################
## TORNADO IMPORTS ##
#####################

import tornado.ioloop
import tornado.httpserver
import tornado.web

from tornado.escape import xhtml_escape
from tornado import gen


###################
## LOCAL IMPORTS ##
###################

from .. import __version__
from ..backend import dbsearch
dbsearch.set_logger_parent(__name__)
from ..backend import datasets
datasets.set_logger_parent(__name__)


#############################
## DATASET DISPLAY HANDLER ##
#############################

class DatasetHandler(tornado.web.RequestHandler):
    '''
    This handles loading a dataset.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   signer,
                   fernet,
                   siteinfo):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.apiversion = apiversion
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.signer = signer
        self.fernet = fernet,
        self.siteinfo = siteinfo



    @gen.coroutine
    def get(self, setid):
        '''This runs the query.

        '''
        # get the returnjson argument
        try:
            returnjson = xhtml_escape(self.get_argument('json',default='0'))
            returnjson = True if returnjson == '1' else False
        except Exception as e:
            returnjson = False

        if returnjson:

            # get the returnjson argument
            try:
                strformat = xhtml_escape(self.get_argument('strformat',
                                                           default='0'))
                strformat = True if strformat == '1' else False
            except Exception as e:
                strformat = False

        else:
            strformat = False

        if setid is None or len(setid) == 0:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title='404 - no dataset by that name exists',
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)


        #
        # get the dataset ID from the provided URL
        #

        # get the setid
        setid = xhtml_escape(setid)

        # retrieve this dataset. we'll not provide returnjson to the backend
        # because we want to censor some things before sending them back
        ds = yield self.executor.submit(
            datasets.sqlite_get_dataset,
            self.basedir, setid,
        )

        # if there's no dataset at all, then return an error
        if ds is None:

            message = (
                "No dataset ID was provided or provided "
                "dataset ID: %s doesn't exist." % setid
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title='404 - Dataset %s not found' % setid,
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)


        # next, if the dataset is returned but status is 'broken'
        elif ds is not None and ds['status'] == 'broken':

            message = (
                "Provided dataset ID: %s doesn't exist." % setid
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                raise tornado.web.Finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title=('404 - Dataset %s missing or broken' %
                                        setid),
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

        # next, if the dataset is returned but status is 'in progress'
        elif ds is not None and ds['status'] == 'in progress':

            # if we're returning JSON
            if returnjson:

                jsondict = {
                    'setid':setid,
                    'created':'%sZ' % ds['created_on'],
                    'updated':'%sZ' % ds['last_updated'],
                    'status':'in progress',
                    'public':ds['ispublic'],
                    'searchtype':'not available yet...',
                    'searchargs':'not available yet...',
                    'collections':[],
                    'columns':[],
                    'nobjects':0,
                    'coldesc':None,
                    'rows':[],
                    'name':None,
                    'desc':None,
                    'dataset_pickle':None,
                    'dataset_csv':None,
                    'lczip':None,
                }

                dsjson = json.dumps(jsondict)
                dsjson = dsjson.replace('nan','null').replace('NaN','null')
                self.set_header('Content-Type',
                                'application/json; charset=UTF-8')
                self.write(dsjson)
                raise tornado.web.Finish()

            # otherwise, we'll return the dataset rendered page
            else:

                header = {
                    'setid':setid,
                    'created':'%sZ' % ds['created_on'],
                    'updated':'%sZ' % ds['last_updated'],
                    'status':'in progress',
                    'public':ds['ispublic'],
                    'searchtype':'not available yet...',
                    'searchargs':'not available yet...',
                    'collections':[],
                    'columns':[],
                    'nobjects':0,
                    'coldesc':None
                }

                self.render('dataset-async.html',
                            page_title='LCC Dataset %s' % setid,
                            setid=setid,
                            header=header,
                            setpickle=None,
                            setcsv=None,
                            setcsv_shasum=None,
                            lczip=None,
                            pfzip=None,
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)

                raise tornado.web.Finish()

        #
        # if everything went as planned, retrieve the data in specified format
        #
        elif ds is not None and ds['status'] == 'complete':

            # first, we'll censor some bits
            dataset_pickle = '/d/dataset-%s.pkl.gz' % setid
            ds['dataset_pickle'] = dataset_pickle

            if os.path.exists(os.path.join(self.basedir,
                                           'datasets',
                                           'dataset-%s.csv' % setid)):
                dataset_csv = '/d/dataset-%s.csv' % setid
                ds['dataset_csv'] = dataset_csv

            else:
                dataset_csv = None
                ds['dataset_csv'] = None


            if os.path.exists(ds['lczip']):
                dataset_lczip = ds['lczip'].replace(os.path.join(self.basedir,
                                                                 'products'),
                                                    '/p')
                ds['lczip'] = dataset_lczip
            else:
                ds['lczip'] = None


            # check if there are too many rows in the dataset and don't return
            # data if there more than 3000 rows
            if ds['nobjects'] > 3000:

                LOGGER.warning('more than 3000 objects '
                               'in dataset!')

                if returnjson:

                    # this automatically does the censoring LCs bit
                    jsondict, datarows = yield self.executor.submit(
                        datasets.generate_dataset_tablerows,
                        self.basedir, ds,
                        strformat=strformat,
                        giveupafter=3000
                    )

                    LOGGER.info('returning JSON for %s' % setid)

                    jsondict.update({
                        'rows':datarows,
                        'name':ds['name'],
                        'desc':ds['desc'],
                        'dataset_pickle':dataset_pickle,
                        'dataset_csv':dataset_csv,
                        'lczip':ds['lczip'],
                        'rowstatus':'showing only the top 3000 rows'
                    })

                    dsjson = json.dumps(jsondict)
                    dsjson = dsjson.replace('nan','null').replace('NaN','null')
                    self.set_header('Content-Type',
                                    'application/json; charset=UTF-8')
                    self.write(dsjson)
                    self.finish()

                else:

                    # this automatically does the censoring LCs bit
                    header = yield self.executor.submit(
                        datasets.generate_dataset_tablerows,
                        self.basedir, ds,
                        headeronly=True
                    )

                    self.render('dataset-async.html',
                                page_title='LCC Dataset %s' % setid,
                                setid=setid,
                                header=header,
                                setpickle=dataset_pickle,
                                setcsv=dataset_csv,
                                lczip=ds['lczip'],
                                lccserver_version=__version__,
                                siteinfo=self.siteinfo)


            # if there are less than 3000 objects, show all of them
            else:

                # if we're returning JSON, censor LC filenames and then return
                if returnjson:

                    # this automatically does the censoring LCs bit
                    jsondict, datarows = yield self.executor.submit(
                        datasets.generate_dataset_tablerows,
                        self.basedir, ds,
                        strformat=strformat
                    )

                    LOGGER.info('returning JSON for %s' % setid)

                    jsondict.update({
                        'rows':datarows,
                        'name':ds['name'],
                        'desc':ds['desc'],
                        'dataset_pickle':dataset_pickle,
                        'dataset_csv':dataset_csv,
                        'lczip':ds['lczip'],
                        'rowstatus':'showing all rows',
                    })

                    dsjson = json.dumps(jsondict)
                    dsjson = dsjson.replace('nan','null').replace('NaN','null')
                    self.set_header('Content-Type',
                                    'application/json; charset=UTF-8')
                    self.write(dsjson)
                    self.finish()

                # otherwise, we're going to render the dataset to a template
                else:

                    # this automatically does the censoring LCs bit
                    header = yield self.executor.submit(
                        datasets.generate_dataset_tablerows,
                        self.basedir, ds,
                        headeronly=True
                    )

                    self.render('dataset-async.html',
                                page_title='LCC Dataset %s' % setid,
                                setid=setid,
                                header=header,
                                setpickle=dataset_pickle,
                                setcsv=dataset_csv,
                                lczip=ds['lczip'],
                                lccserver_version=__version__,
                                siteinfo=self.siteinfo)

        # if we somehow get here, everything is broken
        else:

            message = (
                "No dataset ID was provided or that dataset ID doesn't exist."
            )

            if returnjson:

                self.write({'status':'failed',
                            'result':None,
                            'message':message})
                self.finish()

            else:

                self.render('errorpage.html',
                            error_message=message,
                            page_title='404 - no dataset by that name exists',
                            lccserver_version=__version__,
                            siteinfo=self.siteinfo)



class DatasetAJAXHandler(tornado.web.RequestHandler):
    '''
    This handles the AJAX for /set/ urls.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   signer,
                   fernet):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.apiversion = apiversion
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.signer = signer
        self.fernet = fernet


    @gen.coroutine
    def get(self, setid):
        '''This runs the query.

        '''

        collections = yield self.executor.submit(
            dbsearch.sqlite_list_collections,
            self.basedir
        )

        collection_info = collections['info']
        all_columns = collections['columns']
        all_indexed_columns = collections['indexedcols']
        all_fts_columns = collections['ftscols']

        # censor some bits
        del collection_info['kdtree_pkl_path']
        del collection_info['object_catalog_path']

        # we'll reform the lcformatdesc path so it can be downloaded directly
        # from the LCC server
        lcformatdesc = collection_info['lcformatdesc']
        lcformatdesc = [
            '/c%s' % (x.replace(self.basedir,'')) for x in lcformatdesc
        ]
        collection_info['lcformatdesc'] = lcformatdesc

        returndict = {
            'status':'ok',
            'result':{'available_columns':all_columns,
                      'available_indexed_columns':all_indexed_columns,
                      'available_fts_columns':all_fts_columns,
                      'collections':collection_info},
            'message':(
                'found %s collections in total' %
                len(collection_info['collection_id'])
            )
        }

        # return to sender
        self.write(returndict)
        self.finish()



#############################
## DATASET LISTING HANDLER ##
#############################

class AllDatasetsHandler(tornado.web.RequestHandler):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   siteinfo):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.executor = executor
        self.basedir = basedir
        self.siteinfo = siteinfo


    @gen.coroutine
    def get(self):
        '''This just lists all datasets.

        '''

        self.render('dataset-list.html',
                    page_title='All datasets',
                    lccserver_version=__version__,
                    siteinfo=self.siteinfo)
