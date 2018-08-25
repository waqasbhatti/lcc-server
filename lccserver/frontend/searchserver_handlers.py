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

from tornado.escape import xhtml_escape, squeeze, utf8
from tornado import gen

# for signing/verifying tokens
import itsdangerous

###################
## LOCAL IMPORTS ##
###################

from ..backend import dbsearch
dbsearch.set_logger_parent(__name__)
from ..backend import datasets
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
    r'^(\w+)\s(\d{1,3}\.{0,1}\d*)\s([+\-]?\d{1,2}\.{0,1}\d*)$'
)
COORD_HMSMULTI_REGEX = re.compile(
    r'^(\w+)\s(\d{1,2}[: ]\d{2}[: ]\d{2}\.{0,1}\d*)\s'
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

    if hmscoordtry:

        try:

            objectid, ra, dec = hmscoordtry.groups()
            objectid, ra, dec = (
                xhtml_escape(objectid), xhtml_escape(ra), xhtml_escape(dec)
            )
            ra_tuple, dec_tuple = hms_str_to_tuple(ra), dms_str_to_tuple(dec)

            # get rid of quotes and semicolons in objectid
            objectid = objectid.replace('&','').replace(';','')
            objectid = objectid.replace('#','').replace("'",'')
            objectid = objectid.replace('&#39;','')

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
                dec_decimal = dms_to_decimal(dec_sign,
                                             dec_deg,
                                             dec_min,
                                             dec_sec)

                paramsok = True
                objid, radeg, decldeg = objectid, ra_decimal, dec_decimal

            else:

                paramsok = False
                objid, radeg, decldeg = None, None, None

        except Exception as e:
            LOGGER.error('could not parse object line: %s' % objectline)
            paramsok = False
            objid, radeg, decldeg = None, None, None


    elif degcoordtry:

        try:

            objectid, ra, dec = degcoordtry.groups()
            objectid, ra, dec = (
                xhtml_escape(objectid), xhtml_escape(ra), xhtml_escape(dec)
            )

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

    else:

        paramsok = False
        objid, radeg, decldeg = None, None, None

    return paramsok, objid, radeg, decldeg



def parse_xmatch_input(inputtext, matchradtext,
                       maxradius=30.0,
                       maxlines=5001,
                       maxlinelen=280):
    '''
    This tries to parse xmatch input.

    '''

    itext = inputtext

    # parse the xmatchradius text
    try:
        matchrad = float(xhtml_escape(matchradtext))
        if 0 < matchrad < maxradius:
            xmatch_distarcsec = matchrad
        else:
            xmatch_distarcsec = 3.0
    except Exception as e:
        xmatch_distarcsec = 3.0

    # FIXME: does this look OK? there seems to be extra backslashes in req args
    # for some reason. seems to work though
    itextlines = itext.split('\n')

    if len(itextlines) > maxlines:

        LOGGER.error('too many lines to parse')
        return None

    # here, we'll truncate each line to maxlength
    itextlines = [x[:maxlinelen] for x in itextlines if not x.startswith('#')]
    parsed_lines = [parse_objectlist_item(x) for x in itextlines]
    oklines = [x for x in parsed_lines if all(x)]

    if 0 < len(oklines) < maxlines:

        objectid = [x[1] for x in oklines]
        ra = [x[2] for x in oklines]
        decl = [x[3] for x in oklines]

        xmatchdict = {
            'data':{'objectid':objectid,
                    'ra':ra,
                    'decl':decl},
            'columns':['objectid','ra','decl'],
            'types':['str','float','float'],
            'colobjectid':'objectid',
            'colra':'ra',
            'coldec':'decl'
        }

        return xmatchdict, xmatch_distarcsec

    else:

        LOGGER.error('could not parse input xmatch spec')
        return None, None


def parse_conditions(conditions, maxlength=1000):
    '''This parses conditions provided in the query args.

    '''
    conditions = conditions[:maxlength]

    try:
        conditions = xhtml_escape(squeeze(conditions))

        # return the "'" character that got escaped
        conditions = conditions.replace('&#39;',"'")

        # replace the operators with their SQL equivalents
        farr = conditions.split(' ')
        farr = ['>' if x == 'gt' else x for x in farr]
        farr = ['<' if x == 'lt' else x for x in farr]
        farr = ['>=' if x == 'ge' else x for x in farr]
        farr = ['<=' if x == 'le' else x for x in farr]
        farr = ['=' if x == 'eq' else x for x in farr]
        farr = ['!=' if x == 'ne' else x for x in farr]
        farr = ['like' if x == 'ct' else x for x in farr]

        LOGGER.info(farr)

        # deal with like operator
        # FIXME: this is ugly :(
        for i, x in enumerate(farr):
            if x == 'like':
                LOGGER.info(farr[i+1])
                farrnext = farr[i+1]
                farrnext_left = farrnext.index("'")
                farrnext_right = farrnext.rindex("'")
                farrnext = [a for a in farrnext]
                farrnext.insert(farrnext_left+1,'%')
                farrnext.insert(farrnext_right+1,'%')
                farr[i+1] = ''.join(farrnext)

        conditions = ' '.join(farr)
        LOGGER.info('conditions = %s' % conditions)
        return conditions

    except Exception as e:
        LOGGER.exception('could not parse the filter conditions')
        return None


###################################################
## QUERY HANDLER MIXIN FOR RUNNING IN BACKGROUND ##
###################################################

class BackgroundQueryMixin(object):
    """
    This handles background queries automatically.

    """

    @gen.coroutine
    def background_query(self,
                         query_function,
                         query_args,
                         query_kwargs,
                         query_spec):

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
                "api_service":query_spec['name'],
                "api_args":query_spec['args'],
            },
            "time":'%sZ' % datetime.utcnow().isoformat()
        }
        retdict = '%s\n' % json.dumps(retdict)
        self.set_header('Content-Type','application/json; charset=UTF-8')
        self.write(retdict)
        yield self.flush()

        # Q2. execute the query and get back a future
        self.query_result_future = self.executor.submit(
            query_function,
            *query_args,
            **query_kwargs
        )

        try:

            # here, we'll yield with_timeout
            # give 15 seconds to the query
            self.query_result = yield gen.with_timeout(
                timedelta(seconds=15.0),
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

                    # only collect the LCs into a pickle if the user requested
                    # less than 20000 light curves. generating bigger ones is
                    # something we'll handle later
                    if nrows > 20000:

                        # early-collect the dataset to generate its CSV
                        setdict = yield self.executor.submit(
                            datasets.sqlite_get_dataset,
                            self.basedir,
                            dspkl_setid,
                            forcecomplete=True
                        )

                        dataset_url = "%s://%s/set/%s" % (
                            self.request.protocol,
                            self.req_hostname,
                            dspkl_setid
                        )

                        retdict = {
                            "message":(
                                "Dataset pickle generation complete. "
                                "There are more than 20,000 light curves "
                                "to collect so we won't generate a ZIP file. "
                                "See %s for dataset object lists and a "
                                "CSV when the query completes."
                            ) % dataset_url,
                            "status":"background",
                            "result":{
                                "setid":dspkl_setid,
                                "seturl":dataset_url,
                            },
                            "time":'%sZ' % datetime.utcnow().isoformat()
                        }
                        retdict = '%s\n' % json.dumps(retdict)
                        self.write(retdict)

                        yield self.flush()
                        raise tornado.web.Finish()

                    #
                    # otherwise, we're continuing...
                    #

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

                        # we'll give zipping 15 seconds
                        lczip = yield gen.with_timeout(
                            timedelta(seconds=15.0),
                            self.lczip_future,
                        )

                        # A4. we're done with collecting light curves
                        retdict = {
                            "message":("dataset LC ZIP complete. "
                                       "generating dataset CSV..."),
                            "status":"running",
                            "result":{
                                "setid":dspkl_setid,
                                "lczip":'/p/%s' % os.path.basename(lczip)
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
                                "query sent to background after 30 seconds. "
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
                        "message":("Query <code>%s</code> failed. "
                                   "No matching objects were found" %
                                   self.setid),
                        "time":'%sZ' % datetime.utcnow().isoformat()
                    }
                    self.write(retdict)
                    self.finish()

            else:

                retdict = {
                    "message":("Query <code>%s</code> failed. "
                               "No matching objects were found" %
                               self.setid),
                    "status":"failed",
                    "result":{
                        "setid":self.setid,
                        "nobjects":0
                    },
                    "time":'%sZ' % datetime.utcnow().isoformat()
                }
                self.write(retdict)
                self.finish()


        #
        # if the query itself times out, then continue in the background
        #
        except gen.TimeoutError as e:

            LOGGER.warning('search for setid: %s took too long, '
                           'moving query to background' % self.setid)


            dataset_url = "%s://%s/set/%s" % (
                self.request.protocol,
                self.req_hostname,
                self.setid
            )
            retdict = {
                "message":("query sent to background after 15 seconds. "
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

                collections = self.query_result['databases']
                nrows = sum(self.query_result[x]['nmatches']
                            for x in collections)
                LOGGER.info('background query for setid: %s finished, '
                            'objects found: %s ' % (self.setid, nrows))

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

                # only collect the LCs into a pickle if the user requested
                # less than 20000 light curves. generating bigger ones is
                # something we'll handle later
                if nrows > 20000:

                    # early-collect the dataset to generate its CSV
                    setdict = yield self.executor.submit(
                        datasets.sqlite_get_dataset,
                        self.basedir,
                        dspkl_setid,
                        forcecomplete=True
                    )

                    LOGGER.warning(
                        '> 20k LCs requested for zipping in the '
                        'background, will not do so, forcing set: %s to '
                        '"complete" status' % dspkl_setid
                    )

                # if there are less than 20k rows, we will generate an LC zip
                else:

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

                    LOGGER.warning('background LC zip for '
                                   'background query: %s completed OK' %
                                   dspkl_setid)

            else:

                LOGGER.warning('background query for setid: %s finished, '
                               'no objects were found')

            #
            # now we're actually done
            #



###########################
## COLUMN SEARCH HANDLER ##
###########################

class ColumnSearchHandler(tornado.web.RequestHandler,
                          BackgroundQueryMixin):
    '''
    This handles the column search API.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   uselcdir,
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
        self.uselcdir = uselcdir
        self.signer = signer
        self.fernet = fernet



    def write_error(self, status_code, **kwargs):
        '''This overrides the usual write_error function so we can return JSON.

        Mostly useful to not make the frontend UI not hang indefinitely in case
        of a 500 from the backend.

        '''

        if 'exc_info' in kwargs:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists. "
                    "The exception raised was: %s - '%s'" %
                    (kwargs['exc_info'][0], kwargs['exc_info'][1])
                )
            }

        else:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists."
                )
            }

        self.write(retdict)



    @gen.coroutine
    def get(self):
        '''This runs the query.

        URL: /api/columnsearch?<params>

        required params
        ---------------

        extraconditions

        result_ispublic: either 1 or 0

        sort col and sort order

        optional params
        ---------------

        collections

        columns


        '''
        LOGGER.info('request arguments: %r' % self.request.arguments)

        try:

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host


            # REQUIRED: conditions
            conditions = self.get_argument('filters', default=None)

            # yield when parsing the conditions because they might be huge
            if conditions:
                conditions = yield self.executor.submit(
                    parse_conditions,
                    conditions
                )

            # get the other arguments for the server

            # REQUIRED: sort column
            sortcol = xhtml_escape(
                self.get_argument('sortcol',
                                  default='sdssr')
            ).strip()
            sortorder = xhtml_escape(self.get_argument('sortorder')).strip()

            if sortorder not in ('asc','desc'):
                sortorder = 'asc'

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

            #
            # now we've collected all the parameters for
            # sqlite_column_search
            #

        # if something goes wrong parsing the args, bail out immediately
        except Exception as e:

            LOGGER.exception(
                'one or more of the required args are missing or invalid.'
            )
            retdict = {
                "status":"failed",
                "result":None,
                "message":("columnsearch: one or more of the "
                           "required args are missing or invalid.")
            }
            self.set_status(400)
            self.write(retdict)

            # we call this to end the request here (since self.finish() doesn't
            # actually stop executing statements)
            raise tornado.web.Finish()

        LOGGER.info('********* PARSED ARGS *********')
        LOGGER.info('conditions = %s' % conditions)
        LOGGER.info('sortcol = %s' % sortcol)
        LOGGER.info('sortorder = %s' % sortorder)
        LOGGER.info('getcolumns = %s' % getcolumns)
        LOGGER.info('lcclist = %s' % lcclist)

        # send the query to the background worker
        yield self.background_query(
            dbsearch.sqlite_column_search,
            (self.basedir,),
            {"conditions":conditions,
             "sortby":"%s %s" % (sortcol, sortorder),
             "getcolumns":getcolumns,
             "lcclist":lcclist},
            {"name":"columnsearch",
             "args":{"conditions":conditions,
                     "sortcol":sortcol,
                     "sortorder":sortorder,
                     "result_ispublic":self.result_ispublic,
                     "collections":lcclist,
                     "getcolumns":getcolumns}}
        )


#########################
## CONE SEARCH HANDLER ##
#########################

class ConeSearchHandler(tornado.web.RequestHandler,
                        BackgroundQueryMixin):
    '''
    This handles the cone search API.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   uselcdir,
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
        self.uselcdir = uselcdir
        self.signer = signer
        self.fernet = fernet



    def write_error(self, status_code, **kwargs):
        '''This overrides the usual write_error function so we can return JSON.

        Mostly useful to not make the frontend UI not hang indefinitely in case
        of a 500 from the backend.

        '''

        if 'exc_info' in kwargs:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists. "
                    "The exception raised was: %s - '%s'" %
                    (kwargs['exc_info'][0], kwargs['exc_info'][1])
                )
            }

        else:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists."
                )
            }

        self.write(retdict)



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

        collections

        columns

        extraconditions
        '''
        LOGGER.info('request arguments: %r' % self.request.arguments)

        try:

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host


            # REQUIRED: coords
            coordstr = xhtml_escape(self.get_argument('coords'))

            # make sure to truncate to avoid weirdos. clearly, if 280
            # characters is good enough for Twitter, it's good for us too
            coordstr = coordstr[:280]
            coordstr = coordstr.replace('\n','')

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
            extraconditions = self.get_argument('filters', default=None)

            # yield when parsing the conditions because they might be huge
            if extraconditions:
                extraconditions = yield self.executor.submit(
                    parse_conditions,
                    extraconditions
                )

            #
            # now we've collected all the parameters for
            # sqlite_kdtree_conesearch
            #

        # if something goes wrong parsing the args, bail out immediately
        except Exception as e:

            LOGGER.exception(
                'one or more of the required args are missing or invalid'
            )
            retdict = {
                "status":"failed",
                "result":None,
                "message":("conesearch: one or more of the "
                           "required args are missing or invalid.")
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

        # send the query to the background worker
        yield self.background_query(
            dbsearch.sqlite_kdtree_conesearch,
            (self.basedir,
             center_ra,
             center_decl,
             radius_arcmin),
            {"extraconditions":extraconditions,
             "getcolumns":getcolumns,
             "lcclist":lcclist},
            {"name":"conesearch",
             "args":{"coords":coordstr,
                     "extraconditions":extraconditions,
                     "result_ispublic":self.result_ispublic,
                     "collections":lcclist,
                     "getcolumns":getcolumns}}
        )


#############################
## FULLTEXT SEARCH HANDLER ##
#############################

class FTSearchHandler(tornado.web.RequestHandler,
                      BackgroundQueryMixin):
    '''
    This handles the FTS API.

    '''

    def initialize(self,
                   currentdir,
                   apiversion,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   uselcdir,
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
        self.uselcdir = uselcdir
        self.signer = signer
        self.fernet = fernet



    def write_error(self, status_code, **kwargs):
        '''This overrides the usual write_error function so we can return JSON.

        Mostly useful to not make the frontend UI not hang indefinitely in case
        of a 500 from the backend.

        '''

        if 'exc_info' in kwargs:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists. "
                    "The exception raised was: %s - '%s'" %
                    (kwargs['exc_info'][0], kwargs['exc_info'][1])
                )
            }

        else:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists."
                )
            }

        self.write(retdict)



    @gen.coroutine
    def get(self):
        '''This runs the query.

        URL: /api/ftsquery?<params>

        required params
        ---------------

        ftstext: the coord string containing <ra> <dec> <searchradius>
                in either sexagesimal or decimal format

        result_ispublic: either 1 or 0

        optional params
        ---------------

        collections

        columns

        extraconditions


        '''
        LOGGER.info('request arguments: %r' % self.request.arguments)

        try:

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host


            # REQUIRED: ftstext
            ftstext = xhtml_escape(self.get_argument('ftstext'))
            ftstext = ftstext.replace('\n','')

            # make sure the length matches what we want
            # this should handle attacks like '-' to get our entire DB
            if len(ftstext) < 8:
                raise Exception("query string is too short: %s < 8" %
                                len(ftstext))
            elif len(ftstext) > 1024:
                raise Exception("query string is too long: %s > 1024" %
                                len(ftstext))

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
            extraconditions = self.get_argument('filters', default=None)

            # yield when parsing the conditions because they might be huge
            if extraconditions:
                extraconditions = yield self.executor.submit(
                    parse_conditions,
                    extraconditions
                )

            #
            # now we've collected all the parameters for
            # sqlite_fulltext_search
            #

        # if something goes wrong parsing the args, bail out immediately
        except Exception as e:

            LOGGER.exception(
                'one or more of the required args are missing or invalid'
            )
            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "ftsearch: one or more of the "
                    "required args are missing or invalid. "
                    "The query string should be at least 8 characters "
                    "and no more than 1024 characters long. "
                    "Try using fts_indexed_column:\"query\" for "
                    "short descriptors "
                    "(e.g. color_classes:\"RRab\" instead of just \"RRab\")."
                )
            }
            self.write(retdict)

            # we call this to end the request here (since self.finish() doesn't
            # actually stop executing statements)
            raise tornado.web.Finish()

        LOGGER.info('********* PARSED ARGS *********')
        LOGGER.info('ftstext = %s' % ftstext)
        LOGGER.info('getcolumns = %s' % getcolumns)
        LOGGER.info('lcclist = %s' % lcclist)
        LOGGER.info('extraconditions = %s' % extraconditions)

        # send the query to the background worker
        yield self.background_query(
            dbsearch.sqlite_fulltext_search,
            (self.basedir,
             ftstext),
            {"extraconditions":extraconditions,
             "getcolumns":getcolumns,
             "lcclist":lcclist},
            {"name":"ftsquery",
             "args":{"ftstext":ftstext,
                     "extraconditions":extraconditions,
                     "collections":lcclist,
                     "result_ispublic":self.result_ispublic,
                     "getcolumns":getcolumns}}
        )



###########################
## XMATCH SEARCH HANDLER ##
###########################

from tornado.web import _time_independent_equals

class XMatchHandler(tornado.web.RequestHandler,
                    BackgroundQueryMixin):
    '''
    This handles the xmatch search API.

    '''
    def initialize(self,
                   apiversion,
                   currentdir,
                   templatepath,
                   assetpath,
                   executor,
                   basedir,
                   uselcdir,
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
        self.uselcdir = uselcdir
        self.signer = signer
        self.fernet = fernet



    def write_error(self, status_code, **kwargs):
        '''This overrides the usual write_error function so we can return JSON.

        Mostly useful to not make the frontend UI not hang indefinitely in case
        of a 500 from the backend.

        '''

        if 'exc_info' in kwargs:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists. "
                    "The exception raised was: %s - '%s'" %
                    (kwargs['exc_info'][0], kwargs['exc_info'][1])
                )
            }

        else:

            retdict = {
                "status":"failed",
                "result":None,
                "message":(
                    "Encountered an unrecoverable exception "
                    "while processing your query, which has been cancelled. "
                    "Please let the admin of this LCC server "
                    "instance know if this persists."
                )
            }

        self.write(retdict)



    def check_apikey(self):
        '''
        This checks the API key.

        '''
        try:

            authorization = self.request.headers.get('Authorization')

            if authorization:

                key = authorization.split()[1].strip()
                uns = self.signer.loads(key, max_age=86400.0)

                # match the remote IP and API version
                keyok = ((self.request.remote_ip == uns['ip']) and
                         (self.apiversion == uns['ver']))

            else:

                LOGGER.error('no Authorization header key found')
                retdict = {
                    'status':'failed',
                    'message':('No credentials provided or '
                               'they could not be parsed safely'),
                    'result':None
                }

                self.set_status(401)
                return retdict


            if not keyok:

                if 'X-Real-Host' in self.request.headers:
                    self.req_hostname = self.request.headers['X-Real-Host']
                else:
                    self.req_hostname = self.request.host

                newkey_url = "%s://%s/api/key" % (
                    self.request.protocol,
                    self.req_hostname,
                )

                LOGGER.error('API key is valid, but IP or API version mismatch')
                retdict = {
                    'status':'failed',
                    'message':('API key invalid for current LCC API '
                               'version: %s or your IP address has changed. '
                               'Get an up-to-date key from %s' %
                               (self.apiversion, newkey_url)),
                    'result':None
                }

                self.set_status(401)
                return retdict

            # SUCCESS HERE ONLY
            else:

                LOGGER.warning('successful API key auth: %r' % uns)

                retdict = {
                    'status':'ok',
                    'message':('API key verified successfully. Expires: %s' %
                               uns['expiry']),
                    'result':{'expiry':uns['expiry']},
                }

                return retdict

        except itsdangerous.SignatureExpired:

            LOGGER.error('API key "%s" from %s has expired' %
                         (key, self.request.remote_ip))

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host

            newkey_url = "%s://%s/api/key" % (
                self.request.protocol,
                self.req_hostname,
            )

            retdict = {
                'status':'failed',
                'message':('API key has expired. '
                           'Get a new one from %s' % newkey_url),
                'result':None
            }

            self.set_status(401)
            return retdict

        except itsdangerous.BadSignature:

            LOGGER.error('API key "%s" from %s did not pass verification' %
                         (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':'API key could not be verified or has expired.',
                'result':None
            }

            self.set_status(401)
            return retdict

        except Exception as e:

            LOGGER.exception('API key "%s" from %s did not pass verification' %
                             (key, self.request.remote_ip))

            retdict = {
                'status':'failed',
                'message':('API key was not provided, '
                           'could not be verified, '
                           'or has expired.'),
                'result':None
            }

            self.set_status(401)
            return retdict



    def tornado_check_xsrf_cookie(self):
        '''This is the original Tornado XSRF token checker.

        From: http://www.tornadoweb.org
              /en/stable/_modules/tornado/web.html
              #RequestHandler.check_xsrf_cookie

        Modified a bit to not immediately raise 403s since we want to return
        JSON all the time.

        '''

        token = (self.get_argument("_xsrf", None) or
                 self.request.headers.get("X-Xsrftoken") or
                 self.request.headers.get("X-Csrftoken"))

        if not token:

            retdict = {
                'status':'failed',
                'message':("'_xsrf' argument missing from POST'"),
                'result':None
            }

            self.set_status(401)
            return retdict

        _, token, _ = self._decode_xsrf_token(token)
        _, expected_token, _ = self._get_raw_xsrf_token()

        if not token:

            retdict = {
                'status':'failed',
                'message':("'_xsrf' argument missing from POST"),
                'result':None
            }

            self.set_status(401)
            return retdict


        if not _time_independent_equals(utf8(token),
                                        utf8(expected_token)):

            retdict = {
                'status':'failed',
                'message':("XSRF cookie does not match POST argument"),
                'result':None
            }

            self.set_status(401)
            return retdict



    def check_xsrf_cookie(self):
        '''This overrides the usual Tornado XSRF checker.

        We use this because we want the same endpoint to support POSTs from an
        API or from the browser.

        '''

        xsrf_auth = (self.get_argument("_xsrf", None) or
                     self.request.headers.get("X-Xsrftoken") or
                     self.request.headers.get("X-Csrftoken"))

        if xsrf_auth:
            LOGGER.info('using tornado XSRF auth...')
            self.keycheck = self.tornado_check_xsrf_cookie()
        else:
            LOGGER.info('using API Authorization header auth...')
            self.keycheck = self.check_apikey()



    @gen.coroutine
    def post(self):

        '''This runs the query.

        '''

        # at this point, we should have checked the API token or XSRF header and
        # can proceed to dealing with the request itself

        # if we get here and the status is set to 401/403, the request is bad
        if self.get_status() == 401 or self.get_status() == 403:
            self.write(self.keycheck)
            raise tornado.web.Finish()

        # debugging
        LOGGER.info('request arguments: %r' % self.request.body_arguments)

        # REQUIRED: xmatch specifications and xmatch distance
        xmq = self.get_body_argument('xmq')
        xmd = self.get_body_argument('xmd')

        parsed_xmq, parsed_xmd = yield self.executor.submit(
            parse_xmatch_input,
            xmq, xmd,
        )

        # return early if we can't parse the input data
        if not parsed_xmq or not parsed_xmd:

            msg = ("Could not parse the xmatch input or "
                   " the match radius is invalid.")

            retdict = {'status':'failed',
                       'message':msg,
                       'result':None}
            self.write(retdict)
            raise tornado.web.Finish()

        # if we parsed the input OK, proceed
        try:

            if 'X-Real-Host' in self.request.headers:
                self.req_hostname = self.request.headers['X-Real-Host']
            else:
                self.req_hostname = self.request.host

            # OPTIONAL: extraconditions
            extraconditions = self.get_body_argument('filters', default=None)

            # yield when parsing the conditions because they might be huge
            if extraconditions:
                extraconditions = yield self.executor.submit(
                    parse_conditions,
                    extraconditions
                )

            #
            # get the other arguments for the server
            #

            # OPTIONAL: result_ispublic
            self.result_ispublic = (
                True if int(xhtml_escape(
                    self.get_body_argument('result_ispublic'))
                )
                else False
            )

            # OPTIONAL: columns
            getcolumns = self.get_body_arguments('columns[]')

            if getcolumns is not None:
                getcolumns = list(set([xhtml_escape(x) for x in getcolumns]))
            else:
                getcolumns = None

            # OPTIONAL: collections
            lcclist = self.get_body_arguments('collections[]')

            if lcclist is not None:

                lcclist = list(set([xhtml_escape(x) for x in lcclist]))
                if 'all' in lcclist:
                    lcclist.remove('all')
                if len(lcclist) == 0:
                    lcclist = None

            else:
                lcclist = None


            #
            # now we've collected all the parameters for
            # sqlite_xmatch_search
            #

        # if something goes wrong parsing the args, bail out immediately
        except Exception as e:

            LOGGER.exception(
                'one or more of the required args are missing or invalid.'
            )
            retdict = {
                "status":"failed",
                "result":None,
                "message":("xmatch: one or more of the "
                           "required args are missing or invalid.")
            }
            self.set_status(400)
            self.write(retdict)

            # we call this to end the request here (since self.finish() doesn't
            # actually stop executing statements)
            raise tornado.web.Finish()

        LOGGER.info('********* PARSED ARGS *********')
        LOGGER.info('conditions = %s' % extraconditions)
        LOGGER.info('getcolumns = %s' % getcolumns)
        LOGGER.info('lcclist = %s' % lcclist)
        LOGGER.info('xmq = %s' % parsed_xmq)
        LOGGER.info('xmd = %s' % parsed_xmd)

        #
        # we'll use line-delimited JSON to respond
        #

        # send the query to the background worker
        # send the query to the background worker
        yield self.background_query(
            dbsearch.sqlite_xmatch_search,
            (self.basedir,
             parsed_xmq),
            {"xmatch_dist_arcsec":parsed_xmd,
             "xmatch_closest_only":False,
             "extraconditions":extraconditions,
             "getcolumns":getcolumns,
             "lcclist":lcclist},
            {"name":"xmatch",
             "args":{"xmq":xmq,
                     "xmd":xmd,
                     "extraconditions":extraconditions,
                     "result_ispublic":self.result_ispublic,
                     "collections":lcclist,
                     "getcolumns":getcolumns}}
        )
