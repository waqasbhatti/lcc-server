#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''collections.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - Sep 2018
License: MIT - see the LICENSE file for the full text.

This contains functions that get info about LC collections, including footprints
and overlapping calculations.

'''

#############
## LOGGING ##
#############

import logging
from lccserver import log_sub, log_fmt, log_date_fmt

DEBUG = False
if DEBUG:
    level = logging.DEBUG
else:
    level = logging.INFO
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=level,
    style=log_sub,
    format=log_fmt,
    datefmt=log_date_fmt,
)

LOGDEBUG = LOGGER.debug
LOGINFO = LOGGER.info
LOGWARNING = LOGGER.warning
LOGERROR = LOGGER.error
LOGEXCEPTION = LOGGER.exception

#############
## IMPORTS ##
#############

import numpy as np

# scipy.spatial and other stuff for hulls
import math
from scipy.spatial import ConvexHull, Delaunay
from shapely.ops import cascaded_union, polygonize
import shapely.geometry as geometry


from .dbsearch import sqlite_column_search


#################
## ALPHA SHAPE ##
#################

# generating a concave hull (or "alpha shape") of RADEC coverage, using the
# Delaunay triangulation and removing triangles with too large area originally
# from: http://blog.thehumangeo.com/2014/05/12/drawing-boundaries-in-python/
def alpha_shape(points, alpha):
    """Compute the alpha shape (concave hull) of a set of points.

    https://en.wikipedia.org/wiki/Alpha_shape

    @param points: Iterable container of points.
    @param alpha: alpha value to influence the
        gooeyness of the border. Smaller numbers
        don't fall inward as much as larger numbers.
        Too large, and you lose everything!

    The returned things are:

    a shapely.Polygon object, a list of edge points

    To get a list of points making up the Polygon object, do:

    >>> extcoords = np.array(concave_hull.exterior.coords)

    """
    if len(points) < 4:
        # When you have a triangle, there is no sense
        # in computing an alpha shape.
        return geometry.MultiPoint(list(points)).convex_hull

    def add_edge(edges, edge_points, coords, i, j):
        """
        Add a line between the i-th and j-th points,
        if not in the list already
        """
        if (i, j) in edges or (j, i) in edges:
            # already added
            return

        edges.add( (i, j) )
        edge_points.append(coords[ [i, j] ])


    tri = Delaunay(points)
    edges = set()
    edge_points = []
    # loop over triangles:
    # ia, ib, ic = indices of corner points of the
    # triangle
    for ia, ib, ic in tri.simplices:
        pa = points[ia]
        pb = points[ib]
        pc = points[ic]
        # Lengths of sides of triangle
        a = math.sqrt((pa[0]-pb[0])**2 + (pa[1]-pb[1])**2)
        b = math.sqrt((pb[0]-pc[0])**2 + (pb[1]-pc[1])**2)
        c = math.sqrt((pc[0]-pa[0])**2 + (pc[1]-pa[1])**2)
        # Semiperimeter of triangle
        s = (a + b + c)/2.0
        # Area of triangle by Heron's formula
        area = math.sqrt(s*(s-a)*(s-b)*(s-c))
        circum_r = a*b*c/(4.0*area)
        # Here's the radius filter.
        # print circum_r
        if circum_r < 1.0/alpha:
            add_edge(edges, edge_points, points, ia, ib)
            add_edge(edges, edge_points, points, ib, ic)
            add_edge(edges, edge_points, points, ic, ia)
    m = geometry.MultiLineString(edge_points)
    triangles = list(polygonize(m))
    return cascaded_union(triangles), edge_points


############################################
## CONVEX HULL AND ALPHA SHAPE GENERATION ##
############################################

def collection_convex_hull(basedir,
                           collection,
                           conditions=None):
    '''This gets the convex hull for an LC collection.

    conditions is a filter string to be passed into the
    dbsearch.sqlite_column_search function.

    '''


def collection_alpha_shape(basedir,
                           collection,
                           alpha=0.7,
                           conditions=None):
    '''This gets the alpha shape (concave hull) for an LC collection.

    conditions is a filter string to be passed into the
    dbsearch.sqlite_column_search function.

    '''


#############################################
## UPDATING COLLECTION DBs WITH FOOTPRINTS ##
#############################################

def get_collection_footprint(basedir,
                             collection,
                             hull,
                             boundary_points):
    '''This takes the output from the two functions above and saves
    to a table in the collection's catalog-objectinfo.sqlite file.

    This will allow us to put the shapely objects saved as pickles into BLOBs in
    the SQLite databases. We can then get them for each collection when
    LCC-Server starts up, store them in memory, and run fast overlap
    calculations to discard collections that don't contain any objects matching
    a spatial query (cone-search or xmatch).

    '''




####################################
## COLLECTION OVERVIEW SVG MAKING ##
####################################

def collection_overview_svg(basedir, outfile):
    '''This generates a coverage map SVG for all of the collections in basedir.

    Writes to outfile.

    Gets the hulls from the collection catalog-objectinfo.sqlite DBs, so make
    sure you run get_collection_footprint for each collection in basedir.

    '''
