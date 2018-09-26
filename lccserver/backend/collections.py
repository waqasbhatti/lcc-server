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

import pickle
import os.path
import math

import numpy as np
from scipy.spatial import ConvexHull, Delaunay

try:
    from astropy.coordinates import SkyCoord

    from shapely.ops import cascaded_union, polygonize
    import shapely.geometry as geometry

    import matplotlib
    import matplotlib.patheffects as path_effects
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt


except ImportError:
    raise ImportError(
        "The following packages must be installed (via pip) "
        "to use this module:"
        "matplotlib>=2.0, shapely>=1.6, and astropy>=3.0"
    )


from .dbsearch import sqlite_column_search
from .datasets import results_limit_rows, results_random_sample


#################
## ALPHA SHAPE ##
#################

# generating a concave hull (or "alpha shape") of RADEC coverage, using the
# Delaunay triangulation and removing triangles with too large area .
#
# originally from:
# http://blog.thehumangeo.com/2014/05/12/drawing-boundaries-in-python/
#
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
                           randomsample=None,
                           limit=None,
                           conditions='(ndet > 49)',
                           hull_buffer=0.5):
    '''This gets the convex hull for an LC collection.

    conditions is a filter string to be passed into the
    dbsearch.sqlite_column_search function.

    '''

    # get the ra/dec
    res = sqlite_column_search(basedir,
                               getcolumns=['ra','decl'],
                               conditions=conditions,
                               lcclist=[collection])

    if res and len(res[collection]['result']) > 0:

        rows = res[collection]['result']

        if randomsample is not None:
            rows = results_random_sample(rows, sample_count=randomsample)

        if limit is not None:
            rows = results_limit_rows(rows,
                                      rowlimit=limit,
                                      incoming_userid=1,
                                      incoming_role='superuser')

        ra = np.array([x['ra'] for x in rows])
        decl = np.array([x['decl'] for x in rows])
        points = np.column_stack((ra, decl))

        hull = ConvexHull(points)

        hull_ras = []
        hull_decls = []

        for simplex in hull.simplices:

            hull_ras.append(points[simplex,0])
            hull_decls.append(points[simplex,1])

        # now generate a shapely convex_hull object that we can pickle
        shapely_points = geometry.MultiPoint(list(points))
        shapely_convex_hull = shapely_points.convex_hull
        if hull_buffer is not None:
            shapely_convex_hull = shapely_convex_hull.buffer(hull_buffer)

        return (
            shapely_convex_hull,
            np.array(shapely_convex_hull.exterior.coords)
        )

    else:

        LOGERROR('no objects found in collection: %s with conditions: %s' %
                 (collection, conditions))


def collection_alpha_shape(basedir,
                           collection,
                           alpha=0.7,
                           randomsample=None,
                           limit=None,
                           conditions='(ndet > 49)',
                           hull_buffer=0.5):
    '''This gets the alpha shape (concave hull) for an LC collection.

    conditions is a filter string to be passed into the
    dbsearch.sqlite_column_search function.

    '''
    # get the ra/dec
    res = sqlite_column_search(basedir,
                               getcolumns=['ra','decl'],
                               conditions=conditions,
                               lcclist=[collection])

    if res and len(res[collection]['result']) > 0:

        rows = res[collection]['result']

        if randomsample is not None:
            rows = results_random_sample(rows, sample_count=randomsample)

        if limit is not None:
            rows = results_limit_rows(rows,
                                      rowlimit=limit,
                                      incoming_userid=1,
                                      incoming_role='superuser')

        ra = np.array([x['ra'] for x in rows])
        decl = np.array([x['decl'] for x in rows])
        points = np.column_stack((ra, decl))

        shapely_concave_hull, edge_points = alpha_shape(points,
                                                        alpha=alpha)
        if hull_buffer is not None:
            shapely_concave_hull = shapely_concave_hull.buffer(hull_buffer)

        # get the coordinates of the hull
        try:

            hull_coords = np.array(shapely_concave_hull.exterior.coords)

        except Exception as e:

            LOGWARNING('this concave hull may have multiple '
                       'unconnected sections, the alpha parameter '
                       'might be too high. returning a shapely.MultiPolygon '
                       'object and list of edge coords')
            hull_coords = []
            for geom in shapely_concave_hull:
                hull_coords.append(np.array(geom.exterior.coords))

        return shapely_concave_hull, hull_coords



####################################
## COLLECTION FOOTPRINT FUNCTIONS ##
####################################

def generate_collection_footprint(
        basedir,
        collection,
        alpha=0.7,
        randomsample=None,
        limit=None,
        conditions='(ndet > 49)',
        hull_buffer=0.5,
):
    '''This generates the convex and concave hulls for a collection.

    Saves them to a collection-footprint.pkl pickle in the collection's
    directory.

    '''

    convex_hull, convex_boundary_points = collection_convex_hull(
        basedir,
        collection,
        randomsample=randomsample,
        limit=limit,
        conditions=conditions,
        hull_buffer=hull_buffer,
    )
    concave_hull, concave_boundary_points = collection_alpha_shape(
        basedir,
        collection,
        alpha=alpha,
        randomsample=randomsample,
        limit=limit,
        conditions=conditions,
        hull_buffer=hull_buffer,
    )

    footprint = {
        'collection': collection,
        'args':{
            'alpha':alpha,
            'randomsample':randomsample,
            'limit':limit,
            'conditions':conditions,
            'hull_buffer':hull_buffer
        },
        'convex_hull': convex_hull,
        'convex_hull_boundary': convex_boundary_points,
        'concave_hull': concave_hull,
        'concave_hull_boundary': concave_boundary_points,
    }

    outpickle = os.path.join(basedir,
                             collection.replace('_','-'),
                             'catalog-footprint.pkl')

    with open(outpickle, 'wb') as outfd:
        pickle.dump(footprint, outfd, pickle.HIGHEST_PROTOCOL)

    return outpickle



#####################################
## COLLECTION OVERVIEW PLOT MAKING ##
#####################################

def collection_overview_plot(collection_dirlist,
                             outfile,
                             use_hull='concave',
                             use_projection='mollweide',
                             show_galactic_plane=True,
                             show_ecliptic_plane=True,
                             dpi=100):
    '''This generates a coverage map plot for all of the collections in
    collection_dirlist.

    Writes to outfile. This should probably go into the basedir docs/static
    directory.

    Gets the hulls from the catalog-footprint.pkl files in each collection's
    directory.

    '''

    # label sizes
    matplotlib.rcParams['xtick.labelsize'] = 16.0
    matplotlib.rcParams['ytick.labelsize'] = 16.0

    # fonts for the entire thing
    matplotlib.rcParams['font.size'] = 16

    # lines
    matplotlib.rcParams['lines.linewidth'] = 2.0

    # axes
    matplotlib.rcParams['axes.linewidth'] = 2.0
    matplotlib.rcParams['axes.labelsize'] = 14.0

    # xtick setup
    matplotlib.rcParams['xtick.major.size'] = 10.0
    matplotlib.rcParams['xtick.minor.size'] = 5.0
    matplotlib.rcParams['xtick.major.width'] = 1.0
    matplotlib.rcParams['xtick.minor.width'] = 1.0
    matplotlib.rcParams['xtick.major.pad'] = 8.0

    # ytick setup
    matplotlib.rcParams['ytick.major.size'] = 10.0
    matplotlib.rcParams['ytick.minor.size'] = 5.0
    matplotlib.rcParams['ytick.major.width'] = 1.0
    matplotlib.rcParams['ytick.minor.width'] = 1.0
    matplotlib.rcParams['ytick.major.pad'] = 8.0

    fig = plt.figure(figsize=(15,12))
    ax = fig.add_subplot(111, projection=use_projection)
    ax.set_facecolor('#e2e3e5')

    if show_galactic_plane:

        LOGINFO('plotting the Galactic plane')

        galactic_plane = [
            SkyCoord(x,0.0,frame='galactic',unit='deg').icrs
            for x in np.arange(0,360.0,0.25)
        ]
        galactic_plane_ra = np.array([x.ra.value for x in galactic_plane])
        galactic_plane_decl = np.array([x.dec.value for x in galactic_plane])
        galra = galactic_plane_ra[::]
        galdec = galactic_plane_decl[::]
        galra[galra > 180.0] = galra[galra > 180.0] - 360.0

        ax.scatter(np.radians(galra),
                   np.radians(galdec),
                   s=25,
                   color='#ffc107',
                   marker='o',
                   zorder=-99,
                   label='Galactic plane')

    if show_ecliptic_plane:

        LOGINFO('plotting the ecliptic plane')

        # ecliptic plane
        ecliptic_equator = [
            SkyCoord(x,0.0,frame='geocentrictrueecliptic',unit='deg').icrs
            for x in np.arange(0,360.0,0.25)
        ]

        ecliptic_equator_ra = np.array(
            [x.ra.value for x in ecliptic_equator]
        )
        ecliptic_equator_decl = np.array(
            [x.dec.value for x in ecliptic_equator]
        )

        eclra = ecliptic_equator_ra[::]
        ecldec = ecliptic_equator_decl[::]
        eclra[eclra > 180.0] = eclra[eclra > 180.0] - 360.0
        ax.scatter(np.radians(eclra),
                   np.radians(ecldec),
                   s=25,
                   color='#6c757d',
                   marker='o',
                   zorder=-80,
                   label='Ecliptic plane')


    #
    # now, we'll go through each collection
    #

    #
    # set up a color cycler to change colors between collections
    #
    from cycler import cycler
    plt.rcParams['axes.prop_cycle'] = cycler(
        'color',
        [plt.get_cmap('summer')(1.0 * i/len(collection_dirlist))
         for i in range(len(collection_dirlist))]
    )

    collection_labels = {}

    for cdir in collection_dirlist:

        LOGINFO('plotting footprint for collection: %s' % cdir.replace('-','_'))

        footprint_pkl = os.path.join(cdir, 'catalog-footprint.pkl')
        with open(footprint_pkl,'rb') as infd:
            footprint = pickle.load(infd)

        hull_boundary = footprint['%s_hull_boundary' % use_hull]
        hull = footprint['%s_hull' % use_hull]

        if isinstance(hull_boundary, np.ndarray):

            covras = hull_boundary[:,0]
            covdecls = hull_boundary[:,1]
            # wrap the RAs
            covras[covras > 180.0] = covras[covras > 180.0] - 360.0
            ax.fill(
                np.radians(covras),
                np.radians(covdecls),
                linewidth=0.0,
            )
            collection_label = ax.text(np.radians(np.mean(covras)),
                                       np.radians(np.mean(covdecls)),
                                       footprint['collection'],
                                       fontsize=11,
                                       ha='center',
                                       va='center',
                                       zorder=100)
            collection_labels[footprint['collection']] = {
                'label':collection_label,
                'collection_dir':os.path.abspath(cdir)
            }

        elif isinstance(hull_boundary, list):

            LOGWARNING('this collection is not contiguous')

            part_center_ras, part_center_decls, part_areas = [], [], []

            for part, bound in zip(hull, hull_boundary):

                covras = bound[:,0]
                covdecls = bound[:,1]
                # wrap the RAs
                covras[covras > 180.0] = covras[covras > 180.0] - 360.0
                ax.fill(
                    np.radians(covras),
                    np.radians(covdecls),
                    linewidth=0.0,
                )

                part_center_ras.append(np.mean(covras))
                part_center_decls.append(np.mean(covdecls))
                part_areas.append(part.area)

            # since the collection is not contiguous, we'll move its label from
            # the center of the collection to a weighted center calculated by
            # weighting the area of the separate parts
            collection_label_ra = np.average(part_center_ras,
                                             weights=part_areas)
            collection_label_decl = np.average(part_center_decls,
                                               weights=part_areas)

            collection_label = ax.text(
                np.radians(collection_label_ra),
                np.radians(collection_label_decl),
                footprint['collection'],
                fontsize=12,
                ha='center',
                va='center',
                zorder=100,
                color='#b8daff',
            )
            # add an outline to the label so it's visible against any background
            # https://matplotlib.org/users/patheffects_guide.html
            collection_label.set_path_effects(
                [path_effects.Stroke(linewidth=2, foreground='black'),
                 path_effects.Normal()]
            )
            collection_labels[footprint['collection']] = {
                'label':collection_label,
                'collection_dir':os.path.abspath(cdir)
            }

    # make the grid and the ticks
    ax.grid()
    xt = [np.radians(x) for x in
          [-150.0,-120.0,-90.0,-60.0,-30.0,0.0,30,60,90,120,150.0]]
    xtl = ['$14^{\mathrm{h}}$','$16^{\mathrm{h}}$',
           '$18^{\mathrm{h}}$','$20^{\mathrm{h}}$',
           '$22^{\mathrm{h}}$', '$0^{\mathrm{h}}$',
           '$2^{\mathrm{h}}$','$4^{\mathrm{h}}$',
           '$6^{\mathrm{h}}$','$8^{\mathrm{h}}$',
           '$10^{\mathrm{h}}$']
    ax.set_xticks(xt)
    ax.set_xticklabels(xtl)

    # make the axis labels
    ax.set_xlabel('right ascension [hr]')
    ax.set_ylabel('declination [deg]')
    ax.set_title('Available Light Curve Collections')

    ax.legend(loc='lower center', fontsize=12,
              numpoints=1,scatterpoints=1,markerscale=3.0,
              ncol=1,frameon=False)

    # save the plot to the designated file
    plt.savefig(outfile,
                bbox_inches='tight',
                dpi=dpi,
                transparent=False)

    # get the image coordinate extents of all the collection label bounding
    # boxes so we can put image maps on them.
    #
    # the format is: [[left, bottom],[right, top]]
    #
    # for PNGs on the web, we'll have to invert this because they measure from
    # the top of the image
    for key in collection_labels:

        collection_labels[key]['bbox'] = (
            np.array(collection_labels[key]['label'].get_window_extent())
        )

        # put the collection labels back into the footprint pickles so we can
        # look them up easily.
        footprint_pkl = os.path.join(collection_labels[key]['collection_dir'],
                                     'catalog-footprint.pkl')

        with open(footprint_pkl,'rb') as infd:
            footprint = pickle.load(infd)

        footprint['footprint_map_labelcoords'] = collection_labels[key]['bbox']
        with open(footprint_pkl,'wb') as outfd:
            pickle.dump(footprint, outfd, pickle.HIGHEST_PROTOCOL)
        LOGINFO('wrote collection label coords back to %s' % footprint_pkl)


    plt.close('all')
    return outfile, collection_labels
