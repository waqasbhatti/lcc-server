#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''searchserver_handlers.py - Waqas Bhatti (wbhatti@astro.princeton.edu) -
                             Apr 2018

These are Tornado handlers for the searchserver.

'''

####################
## SYSTEM IMPORTS ##
####################

import os
import os.path
import logging
import numpy as np
from datetime import datetime
import re

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

from tornado.escape import xhtml_escape, squeeze
from tornado import gen


###################
## LOCAL IMPORTS ##
###################

from ..objectsearch import dbsearch
dbsearch.set_logger_parent(__name__)
from ..objectsearch import datasets
datasets.set_logger_parent(__name__)

from astrobase.coordutils import hms_to_decimal, dms_to_decimal, \
    hms_str_to_tuple, dms_str_to_tuple



###########################
## SOME USEFUL CONSTANTS ##
###########################

# single object coordinate search
# ra dec radius
COORD_DEGSEARCH_REGEX = re.compile(
    r'^(\d{1,3}\.{0,1}\d*) (\-?\d{1,2}\.{0,1}\d*) ?(\d{1,2}\.{0,1}\d*)?'
)
COORD_HMSSEARCH_REGEX = re.compile(
    r'^(\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*) '
    '([+\-]?\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*) ?'
    '(\d{1,2}\.{0,1}\d*)?'
)

# multiple object search
# objectid ra dec, objectid ra dec, objectid ra dec, etc.
COORD_DEGMULTI_REGEX = re.compile(
    r'^(\S+) (\d{1,3}\.{0,1}\d*) (\-?\d{1,2}\.{0,1}\d*)$'
)
COORD_HMSMULTI_REGEX = re.compile(
    r'^(\S+) (\d{1,2}:\d{2}:\d{2}\.{0,1}\d*) '
    '([+\-]?\d{1,2}:\d{2}:\d{2}\.{0,1}\d*)$'
)



#############################
## SEARCH HELPER FUNCTIONS ##
#############################

def parse_coordstring(coordstring):
    '''
    This function parses a coordstring of the form:

    <ra> <dec> <radiusarcmin>

    '''
    searchstr = squeeze(coordstring).strip()

    # try all the regexes and see if one of them works
    degcoordtry = COORD_DEGSEARCH_REGEX.match(searchstr)
    hmscoordtry = COORD_HMSSEARCH_REGEX.match(searchstr)

    if degcoordtry:

        ra, dec, radius = degcoordtry.groups()

        try:
            ra, dec = float(ra), float(dec)
            if ((abs(ra) < 360.0) and (abs(dec) < 90.0)):
                if ra < 0:
                    ra = 360.0 + ra
                paramsok = True
                searchrad = float(radius)/60.0 if radius else 5.0/60.0
                radeg, decldeg, radiusdeg = ra, dec, searchrad

            else:
                paramsok = False
                radeg, decldeg, radiusdeg = None, None, None

        except Exception as e:

            LOGGER.error('could not parse search string: %s' % coordstring)
            paramsok = False
            radeg, decldeg, radiusdeg = None, None, None


    elif hmscoordtry:

        ra, dec, radius = hmscoordtry.groups()
        ra_tuple, dec_tuple = hms_str_to_tuple(ra), dms_str_to_tuple(dec)

        ra_hr, ra_min, ra_sec = ra_tuple
        dec_sign, dec_deg, dec_min, dec_sec = dec_tuple

        # make sure the coordinates are all legit
        if ((0 <= ra_hr < 24) and
            (0 <= ra_min < 60) and
            (0 <= ra_sec < 60) and
            (0 <= dec_deg < 90) and
            (0 <= dec_min < 60) and
            (0 <= dec_sec < 60)):

            ra_decimal = hms_to_decimal(ra_hr, ra_min, ra_sec)
            dec_decimal = dms_to_decimal(dec_sign, dec_deg, dec_min, dec_sec)

            paramsok = True
            searchrad = float(radius)/60.0 if radius else 5.0/60.0
            radeg, decldeg, radiusdeg = ra_decimal, dec_decimal, searchrad

        else:

            paramsok = False
            radeg, decldeg, radiusdeg = None, None, None

    else:

        paramsok = False
        radeg, decldeg, radiusdeg = None, None, None

    return paramsok, radeg, decldeg, radiusdeg



def parse_objectlist_item(objectline):
    '''This function parses a objectlist line that is of the following form:

    <objectid> <ra> <decl>

    '''

    searchstr = squeeze(objectline).strip()

    # try all the regexes and see if one of them works
    degcoordtry = COORD_DEGMULTI_REGEX.match(searchstr)
    hmscoordtry = COORD_HMSMULTI_REGEX.match(searchstr)

    if degcoordtry:

        objectid, ra, dec = degcoordtry.groups()

        try:
            ra, dec = float(ra), float(dec)
            if ((abs(ra) < 360.0) and (abs(dec) < 90.0)):
                if ra < 0:
                    ra = 360.0 + ra
                paramsok = True
                objid, radeg, decldeg = objectid, ra, dec

            else:
                paramsok = False
                objid, radeg, decldeg = None, None, None

        except Exception as e:

            LOGGER.error('could not parse object line: %s' % objectline)
            paramsok = False
            objid, radeg, decldeg = None, None, None

    elif hmscoordtry:

        objectid, ra, dec = hmscoordtry.groups()
        ra_tuple, dec_tuple = hms_str_to_tuple(ra), dms_str_to_tuple(dec)

        ra_hr, ra_min, ra_sec = ra_tuple
        dec_sign, dec_deg, dec_min, dec_sec = dec_tuple

        # make sure the coordinates are all legit
        if ((0 <= ra_hr < 24) and
            (0 <= ra_min < 60) and
            (0 <= ra_sec < 60) and
            (0 <= dec_deg < 90) and
            (0 <= dec_min < 60) and
            (0 <= dec_sec < 60)):

            ra_decimal = hms_to_decimal(ra_hr, ra_min, ra_sec)
            dec_decimal = dms_to_decimal(dec_sign, dec_deg, dec_min, dec_sec)

            paramsok = True
            objid, radeg, decldeg = objectid, ra_decimal, dec_decimal

        else:

            paramsok = False
            objid, radeg, decldeg = None, None, None

    else:

        paramsok = False
        objid, radeg, decldeg = None, None, None

    return paramsok, objid, radeg, decldeg



###########################
## COLUMN SEARCH HANDLER ##
###########################

class ColumnSearchHandler(tornado.web.RequestHandler):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
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



#########################
## CONE SEARCH HANDLER ##
#########################

class ConeSearchHandler(tornado.web.RequestHandler):
    '''
    This handles the cone search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
        '''This runs the query.

        URL: /api/conesearch?<params>

        required params
        ---------------

        coords: the coord string containing <ra> <dec> <searchradius>
                in either sexagesimal or decimal format

        result_ispublic: either 1 or 0

        optional params
        ---------------

        stream: either 1 or 0

        '''

        try:

            coordstr = xhtml_escape(self.get_argument('coords'))

            LOGGER.info(coordstr)

            coordok, center_ra, center_decl, radius_deg = parse_coordstring(
                coordstr
            )
            radius_arcmin = radius_deg*60.0

            result_ispublic = (
                True if int(self.get_argument('result_ispublic')) else False
            )

            # optional stream argument
            stream = self.get_argument('stream', default=None)
            if stream is None:
                stream = False
            else:
                stream = True if int(stream) else False

        except:

            LOGGER.exception('one or more of the required args are missing')
            retdict = {"status":"failed",
                       "result":None,
                       "message":"one or more of the required args are missing"}
            self.write(retdict)

            # we call this to end the request here (since self.finish() doesn't
            # actually stop executing statements)
            raise tornado.web.Finish()


        #
        # we'll use line-delimited JSON to respond
        #
        # FIXME: this should be selectable using an arg: stream=1 or 0

        # FIXME: we might want to end early for API requests and just return the
        # setid, from which one can get the URL for the dataset if it finishes
        # looks like we can use RequestHandler.on_finish() to do this, but we
        # need to figure out how to pass stuff to that function from here (maybe
        # a self.postprocess_items boolean will work?)

        # Q1. prepare the dataset
        setinfo = yield self.executor.submit(
            datasets.sqlite_prepare_dataset,
            self.basedir,
            ispublic=result_ispublic
        )

        setid, creationdt = setinfo

        # A1. we have a setid, send this back to the client
        retdict = {
            "message":"received a setid: %s" % setid,
            "status":"streaming",
            "result":{"setid":setid},  # add submit datetime, args, etc.
            "time":'%sZ' % datetime.utcnow().isoformat()
        }
        retdict = '%s\n' % json.dumps(retdict)
        self.set_header('Content-Type','application/json')
        self.write(retdict)
        yield self.flush()

        # Q2. execute the query
        query_result = yield self.executor.submit(
            dbsearch.sqlite_kdtree_conesearch,
            self.basedir,
            center_ra,
            center_decl,
            radius_arcmin,
            # extra kwargs here later
        )

        # A2. we have the query result, send back a query completed message
        if query_result is not None:

            nrows = 42

            retdict = {
                "message":"query complete, objects matched: %s" % nrows,
                "status":"streaming",
                "result":{
                    "setid":setid,
                    "nrows":nrows
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # Q3. make the dataset pickle and close out dataset row in the DB
            dspkl_setid = yield self.executor.submit(
                datasets.sqlite_new_dataset,
                self.basedir,
                setid,
                creationdt,
                query_result,
                ispublic=result_ispublic
            )

            # A3. we have the dataset pickle generated, send back an update
            dataset_pickle = "/d/dataset-%s.pkl.gz" % dspkl_setid
            retdict = {
                "message":("dataset pickle generation complete: %s" %
                           dataset_pickle),
                "status":"streaming",
                "result":{
                    "setid":dspkl_setid,
                    "dataset_pickle":dataset_pickle
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # Q4. collect light curve ZIP file
            lczip = yield self.executor.submit(
                datasets.sqlite_make_dataset_lczip,
                self.basedir,
                dspkl_setid,
                # FIXME: think about this (probably not required on actual LC
                # server)
                override_lcdir=os.path.join(self.basedir,
                                            'hatnet-keplerfield',
                                            'lightcurves')
            )

            # A4. we're done with collecting light curves
            lczip_url = '/p/%s' % os.path.basename(lczip)
            retdict = {
                "message":("dataset LC ZIP complete: %s" % lczip_url),
                "status":"streaming",
                "result":{
                    "setid":dspkl_setid,
                    "dataset_lczip":lczip_url
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            # A5. finish request by sending back the dataset URL
            dataset_url = "/set/%s" % dspkl_setid
            retdict = {
                "message":("dataset now ready: %s" % dataset_url),
                "status":"ok",
                "result":{
                    "setid":dspkl_setid,
                    "seturl":dataset_url
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            retdict = '%s\n' % json.dumps(retdict)
            self.write(retdict)
            yield self.flush()

            self.finish()

        else:

            retdict = {
                "status":"failed",
                "result":{
                    "setid":setid,
                    "nrows":0
                },
                "message":"query failed, no matching objects found"
            }
            self.write(retdict)
            self.finish()




#############################
## FULLTEXT SEARCH HANDLER ##
#############################

class FTSearchHandler(tornado.web.RequestHandler):
    '''
    This handles the FTS API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
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



###########################
## XMATCH SEARCH HANDLER ##
###########################

class XMatchHandler(tornado.web.RequestHandler):
    '''
    This handles the xmatch search API.

    '''

    def initialize(self,
                   currentdir,
                   templatepath,
                   assetpath,
                   docspath,
                   executor,
                   basedir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir


    @gen.coroutine
    def get(self):
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
