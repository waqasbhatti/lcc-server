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
from datetime import datetime, timedelta
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
import tornado.ioloop

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
    r'^(\d{1,3}\.{0,1}\d*) ([+\-]?\d{1,2}\.{0,1}\d*) ?(\d{1,2}\.{0,1}\d*)?'
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
    r'^(\S+) (\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*) '
    '([+\-]?\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*)$'
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

    # try HHMMSS first because we get false positives on some HH MM SS items in
    # degcoordtry
    if hmscoordtry:

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

    elif degcoordtry:

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


    else:

        paramsok = False
        radeg, decldeg, radiusdeg = None, None, None

    return paramsok, radeg, decldeg, radiusdeg



def parse_objectlist_item(objectline):
    '''This function parses a objectlist line that is of the following form:

    <objectid> <ra> <decl>

    This is used for the xmatch function

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
                   basedir,
                   uselcdir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir
        self.uselcdir = uselcdir



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
                   basedir,
                   uselcdir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir
        self.uselcdir = uselcdir



    @gen.coroutine
    def get(self):
        '''This runs the query.

        URL: /api/conesearch?<params>

        required params
        ---------------

        coords: the coord string containing <ra> <dec> <searchradius>
                in either sexagesimal or decimal format

        result_ispublic: either 1 or 0

        '''
        LOGGER.info('request arguments: %r' % self.request.arguments)

        try:

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host


            # REQUIRED: coords
            coordstr = xhtml_escape(self.get_argument('coords'))
            coordok, center_ra, center_decl, radius_deg = parse_coordstring(
                coordstr
            )

            if not coordok:

                LOGGER.error('could not parse the input coordinate string')
                retdict = {"status":"failed",
                           "result":None,
                           "message":"could not parse the input coords string"}
                self.write(retdict)
                raise tornado.web.Finish()

            # get the other arguments for the server

            # OPTIONAL: result_ispublic
            self.result_ispublic = (
                True if int(xhtml_escape(self.get_argument('result_ispublic')))
                else False
            )

            # OPTIONAL: columns
            getcolumns = self.get_arguments('columns[]')

            if getcolumns is not None:
                getcolumns = list(set([xhtml_escape(x) for x in getcolumns]))
            else:
                getcolumns = None


            # OPTIONAL: collections
            lcclist = self.get_arguments('collections[]')

            if lcclist is not None:

                lcclist = list(set([xhtml_escape(x) for x in lcclist]))
                if 'all' in lcclist:
                    lcclist.remove('all')
                if len(lcclist) == 0:
                    lcclist = None

            else:
                lcclist = None


            # OPTIONAL: extraconditions
            # FIXME: in a bit
            extraconditions = self.get_arguments('filters[]')

            extraconditions = None

            #
            # now we've collected all the parameters for
            # sqlite_kdtree_conesearch
            #

        # if something goes wrong parsing the args, bail out immediately
        except:

            LOGGER.exception(
                'one or more of the required args are missing or invalid'
            )
            retdict = {
                "status":"failed",
                "result":None,
                "message":("one or more of the "
                           "required args are missing or invalid")
            }
            self.write(retdict)

            # we call this to end the request here (since self.finish() doesn't
            # actually stop executing statements)
            raise tornado.web.Finish()

        #
        # reform the radius_deg returned by parse_coordstring to radius_arcmin
        #
        radius_arcmin = radius_deg * 60.0
        if radius_arcmin > 60.0:
            radius_arcmin = 60.0

        LOGGER.info('********* PARSED ARGS *********')
        LOGGER.info('center_ra = %s, center_decl = %s, radius_arcmin = %s'
                    % (center_ra, center_decl, radius_arcmin))
        LOGGER.info('getcolumns = %s' % getcolumns)
        LOGGER.info('lcclist = %s' % lcclist)
        LOGGER.info('extraconditions = %s' % extraconditions)

        #
        # we'll use line-delimited JSON to respond
        #

        # we'll wait 60 seconds for the query to complete in the foreground. if
        # it doesn't complete by then, we'll close the connection and complete
        # everything in on_finish() handler.

        # Q1. prepare the dataset
        setinfo = yield self.executor.submit(
            datasets.sqlite_prepare_dataset,
            self.basedir,
            ispublic=self.result_ispublic,
        )

        self.setid, self.creationdt = setinfo

        # A1. we have a setid, send this back to the client
        retdict = {
            "message":(
                "query in run-queue. executing with set ID: %s..." % self.setid
            ),
            "status":"queued",
            "result":{
                "setid": self.setid,
                "api_service":"conesearch",
                "api_args":{
                    "coords":coordstr,
                    "result_ispublic":self.result_ispublic,
                },
            },  # add submit datetime, args, etc.
            "time":'%sZ' % datetime.utcnow().isoformat()
        }
        retdict = '%s\n' % json.dumps(retdict)
        self.set_header('Content-Type','application/json')
        self.write(retdict)
        yield self.flush()

        # Q2. execute the query and get back a future
        self.query_result_future = self.executor.submit(
            dbsearch.sqlite_kdtree_conesearch,
            self.basedir,
            center_ra,
            center_decl,
            radius_arcmin,
            getcolumns=getcolumns,
            lcclist=lcclist,
            extraconditions=extraconditions
        )

        try:

            # here, we'll yield with_timeout
            # give 5 seconds to the query
            self.query_result = yield gen.with_timeout(
                timedelta(seconds=5.0),
                self.query_result_future
            )

            # A2. we have the query result, send back a query completed message
            if self.query_result is not None:

                collections = self.query_result['databases']
                nrows = sum(self.query_result[x]['nmatches']
                            for x in collections)

                if nrows > 0:

                    retdict = {
                        "message":("query finished OK. "
                                   "objects matched: %s, "
                                   "building dataset..." % nrows),
                        "status":"running",
                        "result":{
                            "setid":self.setid,
                            "nobjects":nrows
                        },
                        "time":'%sZ' % datetime.utcnow().isoformat()
                    }
                    retdict = '%s\n' % json.dumps(retdict)
                    self.write(retdict)
                    yield self.flush()

                    # Q3. make the dataset pickle and finish dataset row in the
                    # DB
                    dspkl_setid = yield self.executor.submit(
                        datasets.sqlite_new_dataset,
                        self.basedir,
                        self.setid,
                        self.creationdt,
                        self.query_result,
                        ispublic=self.result_ispublic
                    )

                    # A3. we have the dataset pickle generated, send back an
                    # update
                    retdict = {
                        "message":("dataset pickle generation complete. "
                                   "collecting light curves into ZIP file..."),
                        "status":"running",
                        "result":{
                            "setid":dspkl_setid,
                        },
                        "time":'%sZ' % datetime.utcnow().isoformat()
                    }
                    retdict = '%s\n' % json.dumps(retdict)
                    self.write(retdict)
                    yield self.flush()

                    #
                    # here, we'll once again do a yield with_timeout because LC
                    # zipping can take some time
                    #

                    self.lczip_future = self.executor.submit(
                        datasets.sqlite_make_dataset_lczip,
                        self.basedir,
                        dspkl_setid,
                        override_lcdir=self.uselcdir  # useful when testing LCC
                                                      # server
                    )

                    try:

                        # we'll give zipping 10 seconds
                        lczip = yield gen.with_timeout(
                            timedelta(seconds=10.0),
                            self.lczip_future,
                        )

                        # A4. we're done with collecting light curves
                        retdict = {
                            "message":("dataset LC ZIP complete. "
                                       "generating dataset CSV..."),
                            "status":"running",
                            "result":{
                                "setid":dspkl_setid,
                                "lczip":'/p%s' % os.path.basename(lczip)
                            },
                            "time":'%sZ' % datetime.utcnow().isoformat()
                        }
                        retdict = '%s\n' % json.dumps(retdict)
                        self.write(retdict)
                        yield self.flush()


                        # Q5. load the dataset to make sure it looks OK and
                        # automatically generate the CSV for it
                        setdict = yield self.executor.submit(
                            datasets.sqlite_get_dataset,
                            self.basedir,
                            dspkl_setid,
                        )

                        # A5. finish request by sending back the dataset URL
                        dataset_url = "%s://%s/set/%s" % (
                            self.request.protocol,
                            self.req_hostname,
                            dspkl_setid
                        )
                        retdict = {
                            "message":("dataset now ready: %s" % dataset_url),
                            "status":"ok",
                            "result":{
                                "setid":dspkl_setid,
                                "seturl":dataset_url,
                                "created":setdict['created_on'],
                                "updated":setdict['last_updated'],
                                "backend_function":setdict['searchtype'],
                                "backend_parsedargs":setdict['searchargs'],
                                "nobjects":setdict['nobjects']
                            },
                            "time":'%sZ' % datetime.utcnow().isoformat()
                        }
                        retdict = '%s\n' % json.dumps(retdict)
                        self.write(retdict)
                        yield self.flush()

                        self.finish()

                    except gen.TimeoutError:

                        dataset_url = "%s://%s/set/%s" % (
                            self.request.protocol,
                            self.req_hostname,
                            self.setid
                        )

                        LOGGER.warning('query for setid: %s went to '
                                       'background while zipping light curves' %
                                       self.setid)

                        retdict = {
                            "message":(
                                "query sent to background after 15 seconds. "
                                "query is complete, "
                                "but light curves of matching objects "
                                "are still being zipped. "
                                "check %s for results later" %
                                dataset_url
                            ),
                            "status":"background",
                            "result":{
                                "setid":self.setid,
                                "seturl":dataset_url
                            },
                            "time":'%sZ' % datetime.utcnow().isoformat()
                        }
                        self.write(retdict)
                        self.finish()

                        # here, we'll yield to the uncancelled lczip_future
                        lczip = yield self.lczip_future

                        # finalize the dataset
                        setdict = yield self.executor.submit(
                            datasets.sqlite_get_dataset,
                            self.basedir,
                            dspkl_setid,
                        )

                        LOGGER.info('background LC zip for setid: %s finished' %
                                    self.setid)

                        #
                        # this is the end
                        #

                # if we didn't find anything, return immediately
                else:

                    retdict = {
                        "status":"failed",
                        "result":{
                            "setid":self.setid,
                            "nobjects":0
                        },
                        "message":"query failed, no matching objects found",
                        "time":'%sZ' % datetime.utcnow().isoformat()
                    }
                    self.write(retdict)
                    self.finish()

            else:

                retdict = {
                    "message":"query failed, no matching objects found",
                    "status":"failed",
                    "result":{
                        "setid":self.setid,
                        "nobjects":0
                    },
                    "time":'%sZ' % datetime.utcnow().isoformat()
                }
                self.write(retdict)
                self.finish()


        # if we timeout, then initiate background processing
        except gen.TimeoutError as e:

            LOGGER.warning('search for setid: %s took too long, '
                           'moving query to background' % self.setid)


            dataset_url = "%s://%s/set/%s" % (
                self.request.protocol,
                self.req_hostname,
                self.setid
            )
            retdict = {
                "message":("query sent to background after 5 seconds. "
                           "query is still running, "
                           "check %s for results later" % dataset_url),
                "status":"background",
                "result":{
                    "setid":self.setid,
                    "seturl":dataset_url
                },
                "time":'%sZ' % datetime.utcnow().isoformat()
            }
            self.write(retdict)
            self.finish()


            # here, we'll yield to the uncancelled query_result_future
            self.query_result = yield self.query_result_future

            # everything else proceeds as planned

            if self.query_result is not None:

                # Q3. make the dataset pickle and finish dataset row in the
                # DB
                dspkl_setid = yield self.executor.submit(
                    datasets.sqlite_new_dataset,
                    self.basedir,
                    self.setid,
                    self.creationdt,
                    self.query_result,
                    ispublic=self.result_ispublic
                )

                # Q4. collect light curve ZIP files
                lczip = yield self.executor.submit(
                    datasets.sqlite_make_dataset_lczip,
                    self.basedir,
                    dspkl_setid,
                    override_lcdir=self.uselcdir  # useful when testing LCC
                    # server
                )

                # Q5. load the dataset to make sure it looks OK and
                # automatically generate the CSV for it
                setdict = yield self.executor.submit(
                    datasets.sqlite_get_dataset,
                    self.basedir,
                    dspkl_setid,
                )

                collections = self.query_result['databases']
                nrows = sum(self.query_result[x]['nmatches']
                            for x in collections)
                LOGGER.info('background query for setid: %s finished, '
                            'objects found: %s ' % (self.setid, nrows))

            else:

                LOGGER.warning('background query for setid: %s finished, '
                               'no objects were found')


            #
            # now, we're actually done
            #



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
                   basedir,
                   uselcdir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir
        self.uselcdir = uselcdir



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
                   basedir,
                   uselcdir):
        '''
        handles initial setup.

        '''

        self.currentdir = currentdir
        self.templatepath = templatepath
        self.assetpath = assetpath
        self.docspath = docspath
        self.executor = executor
        self.basedir = basedir
        self.uselcdir = uselcdir



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
