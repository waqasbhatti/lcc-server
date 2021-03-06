#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''abcat_columns.py - Waqas Bhatti (wbhatti@astro.princeton.edu) - May 2018
License: MIT - see the LICENSE file for the full text.

'''


COLUMN_INFO = {
    'gaia_absmag':{
        'title':'M<sub>G</sub> [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'absolute GAIA magnitude',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.beyond1std':{
        'title':'f<sub>&gt; 1.0-&sigma;</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.3f',
        'description':('fraction of measurements beyond 1-stdev '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.comments':{
        'title':'comments ({magcol})',
        'dtype':'U600',
        'format':'%s',
        'description':("comments on the object and its time-series "
                       "for mag col: {magcol}"),
        'index':False,
        'ftsindex':True,
    },
    '{magcol}.eta_normal':{
        'title':'&eta; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('eta variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.kurtosis':{
        'title':'kurtosis ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'kurtosis of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.linear_fit_slope':{
        'title':'m<sub>linfit</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('slope of a linear fit to the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.mad':{
        'title':'MAD ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('median absolute deviation of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.mag_iqr':{
        'title':'IQR ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('interquartile range of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.magnitude_ratio':{
        'title':'&Delta; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('(max mag - med mag)/(max mag - min mag) '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.median':{
        'title':'median ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'median of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.ndet':{
        'title':'nobs ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':('number of non-nan time-series observations '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.objectisvar':{
        'title':'variability flag ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':("for mag col: {magcol}, 0 = unreviewed, 1 = variable, "
                       "2 = not variable, 3 = can't tell"),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.skew':{
        'title':'skew ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':'skew of the time-series for mag col: {magcol}',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stdev':{
        'title':'&sigma; ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('standard deviation of the time-series '
                       'for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stetsonj':{
        'title':'J<sub>stetson</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson J variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.stetsonk':{
        'title':'K<sub>stetson</sub> ({magcol})',
        'dtype':'f8',
        'format':'%.5f',
        'description':('Stetson K variability index of the '
                       'time-series for mag col: {magcol}'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.varepoch':{
        'title':'epoch ({magcol}) [JD]',
        'dtype':'f8',
        'format':'%.6f',
        'description':('for mag col: {magcol}, JD epoch of minimum '
                       'light if periodic variable'),
        'index':False,
        'ftsindex':False,
    },
    '{magcol}.varisperiodic':{
        'title':'periodicity flag ({magcol})',
        'dtype':'i8',
        'format':'%i',
        'description':('for mag col: {magcol}, 0 = undetermined, 1 = periodic, '
                       '2 = not periodic, 3 = quasi-periodic'),
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.varperiod':{
        'title':'period ({magcol}) [days]',
        'dtype':'f8',
        'format':'%.6f',
        'description':'for mag col: {magcol}, period of variability in days',
        'index':True,
        'ftsindex':False,
    },
    '{magcol}.vartags':{
        'title':'variable tags ({magcol})',
        'dtype':'U600',
        'format':'%s',
        'description':'for mag col: {magcol}, variability tags for this object',
        'index':False,
        'ftsindex':True,
    },
    'bmag':{
        'title':'<em>B</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog magnitude in B filter',
        'index':True,
        'ftsindex':False,
    },
    'color_classes':{
        'title':'color classes',
        'dtype':'U600',
        'format':'%s',
        'description':'stellar classification using SDSS/SEGUE color cuts',
        'index':False,
        'ftsindex':True,
    },
    'decl':{
        'title':'&delta;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'declination of the object [J2000 decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_bmag':{
        'title':'A<sub>B</sub> [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in B band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_hmag':{
        'title':'A<sub>H</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS H band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_imag':{
        'title':'A<sub>I</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in I band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_jmag':{
        'title':'A<sub>J</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS J band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_kmag':{
        'title':'A<sub>Ks</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in 2MASS Ks band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_rmag':{
        'title':'A<sub>R</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in R band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssg':{
        'title':'A<sub>g</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS g band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssi':{
        'title':'A<sub>i</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS i band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssr':{
        'title':'A<sub>r</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS r band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssu':{
        'title':'A<sub>u</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS u band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_sdssz':{
        'title':'A<sub>z</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in SDSS z band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'extinction_vmag':{
        'title':'A<sub>V</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'extinction in V band [mag]',
        'index':True,
        'ftsindex':False,
    },
    'gaia_id':{
        'title':'GAIA DR2 ID',
        'dtype':'U40',
        'format':'%s',
        'description':'cross-matched GAIA DR2 source ID of the object',
        'index':True,
        'ftsindex':True,
    },
    'gaia_parallax':{
        'title':'&omega; [mas]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'GAIA DR2 parallax of the object in milliarcsec',
        'index':True,
        'ftsindex':False,
    },
    'gaia_parallax_err':{
        'title':'&sigma;<sub>&omega;</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':('error in the GAIA DR2 parallax of '
                       'the object in milliarcsec'),
        'index':True,
        'ftsindex':False,
    },
    'gaia_status':{
        'title':'GAIA status',
        'dtype':'U50',
        'format':'%s',
        'description':'GAIA cross-match status',
        'index':False,
        'ftsindex':False,
    },
    'gaiamag':{
        'title':'G [mag]',
        'dtype':'f8',
        'format':'%.3f',
        'description':'GAIA DR2 magnitude',
        'index':True,
        'ftsindex':False,
    },
    'gb':{
        'title':'b',
        'dtype':'f8',
        'format':'%.5f',
        'description':'galactic latitude [decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'gl':{
        'title':'l',
        'dtype':'f8',
        'format':'%.5f',
        'description':'galactic longitude [decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'hmag':{
        'title':'<em>H</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS H band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'imag':{
        'title':'<em>I</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog I band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'jmag':{
        'title':'<em>J</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS J band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'kmag':{
        'title':'<em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog 2MASS Ks band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'lcfname':{
        'title':'LC filename',
        'dtype':'U600',
        'format':'%s',
        'description':'light curve filename',
        'index':False,
        'ftsindex':False,
    },
    'ndet':{
        'title':'ndet',
        'dtype':'i8',
        'format':'%i',
        'description':'number of observations',
        'index':True,
        'ftsindex':False,
    },
    'objectid':{
        'title':'object ID',
        'dtype':'U600',
        'format':'%s',
        'description':'object ID',
        'index':True,
        'ftsindex':True,
    },
    'objecttags':{
        'title':'object tags',
        'dtype':'U600',
        'format':'%s',
        'description':'object type tags',
        'index':False,
        'ftsindex':True,
    },
    'pmdecl':{
        'title':'pmDEC [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in declination [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'pmra':{
        'title':'pmRA [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'proper motion in right ascension [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'propermotion':{
        'title':'PM [mas/yr]',
        'dtype':'f8',
        'format':'%.5f',
        'description':'total proper motion [mas/yr]',
        'index':True,
        'ftsindex':False,
    },
    'ra':{
        'title':'&alpha;',
        'dtype':'f8',
        'format':'%.5f',
        'description':'right ascension of the object [J2000 decimal degrees]',
        'index':True,
        'ftsindex':False,
    },
    'rmag':{
        'title':'<em>R</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog R band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'rpmj':{
        'title':'RPM<sub>J</sub>',
        'dtype':'f8',
        'format':'%.5f',
        'description':'reduced proper motion of the object using 2MASS J mag',
        'index':True,
        'ftsindex':False,
    },
    'sdssg':{
        'title':'<em>g</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS g band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssi':{
        'title':'<em>i</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS i band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssr':{
        'title':'<em>r</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS r band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssu':{
        'title':'<em>u</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS u band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'sdssz':{
        'title':'<em>z</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog SDSS z band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'simbad_best_mainid':{
        'title':'external object ID',
        'dtype':'U50',
        'format':'%s',
        'description':'main SIMBAD ID for this object',
        'index':True,
        'ftsindex':True,
    },
    'simbad_best_objtype':{
        'title':'type flag',
        'dtype':'U20',
        'format':'%s',
        'description':'SIMBAD object type flag for this object',
        'index':False,
        'ftsindex':True,
    },
    'simbad_best_allids':{
        'title':'other IDs',
        'dtype':'U600',
        'format':'%s',
        'description':'other object IDs from SIMBAD',
        'index':False,
        'ftsindex':True,
    },
    'simbad_best_distarcsec':{
        'title':'SIMBAD dist',
        'dtype':'f8',
        'format':'%.5f',
        'description':'distance in arcseconds from closest SIMBAD cross-match',
        'index':True,
        'ftsindex':False,
    },
    'twomassid':{
        'title':'2MASS ID',
        'dtype':'U60',
        'format':'%s',
        'description':'2MASS ID for this object',
        'index':True,
        'ftsindex':True,
    },
    'vmag':{
        'title':'<em>V</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog V band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'ticid':{
        'title':'TIC ID',
        'dtype':'U20',
        'format':'%s',
        'description':'TESS input catalog ID',
        'index':True,
        'ftsindex':True,
    },
    'tic_version':{
        'title':'TIC version',
        'dtype':'U20',
        'format':'%s',
        'description':'TESS input catalog version',
        'index':True,
        'ftsindex':False,
    },
    'tessmag':{
        'title':'<em>T</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'catalog TESS magnitude',
        'index':True,
        'ftsindex':False,
    },
}


# this contains columns that are composed of binary math operators joining two
# columns. intended for stuff like sdssr - jmag, jmag - kmag, sdssr -
# extinction_sdssr, etc.

# FIXME: we're actually relying on the dict keys to be in order here (which is
# true only on Py3.6+) because we generate the dereddened mag columns before we
# get to the dereddened color columns. figure out some way of getting around
# this (but maybe we shouldn't care because we require Python 3.6+ anyway)
COMPOSITE_COLUMN_INFO = {
    #
    # dereddened mags
    #
    'dered_bmag':{
        'from':['-','bmag','extinction_bmag'],
        'title':'<em>V<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened catalog B band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_vmag':{
        'from':['-','vmag','extinction_vmag'],
        'title':'<em>V<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened catalog V band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_rmag':{
        'from':['-','rmag','extinction_rmag'],
        'title':'<em>R<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened catalog R band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_imag':{
        'from':['-','imag','extinction_imag'],
        'title':'<em>I<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened catalog V band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_jmag':{
        'from':['-','jmag','extinction_jmag'],
        'title':'<em>J<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened 2MASS catalog J band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_hmag':{
        'from':['-','hmag','extinction_hmag'],
        'title':'<em>H<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened 2MASS catalog H band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_kmag':{
        'from':['-','kmag','extinction_kmag'],
        'title':'<em>K<sub>s,0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened 2MASS catalog K<sub>s</sub> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssu':{
        'from':['-','sdssu','extinction_sdssu'],
        'title':'<em>u<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened SDSS catalog <em>u</em> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssg':{
        'from':['-','sdssg','extinction_sdssg'],
        'title':'<em>g<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened SDSS catalog <em>g</em> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssr':{
        'from':['-','sdssr','extinction_sdssr'],
        'title':'<em>r<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened SDSS catalog <em>r</em> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssi':{
        'from':['-','sdssi','extinction_sdssi'],
        'title':'<em>i<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened SDSS catalog <em>i</em> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssz':{
        'from':['-','sdssz','extinction_sdssz'],
        'title':'<em>z<sub>0</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'dereddened SDSS catalog <em>z</em> band magnitude',
        'index':True,
        'ftsindex':False,
    },
    #
    # colors
    #
    'color_jmag_kmag':{
        'from':['-','jmag','kmag'],
        'title':'<em>J</em> - <em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>J</em> - <em>K<sub>s</sub></em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_jmag_hmag':{
        'from':['-','jmag','hmag'],
        'title':'<em>J</em> - <em>H</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>J</em> - <em>H</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_hmag_kmag':{
        'from':['-','hmag','kmag'],
        'title':'<em>H</em> - <em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>H</em> - <em>K<sub>s</sub></em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssr_jmag':{
        'from':['-','sdssr','jmag'],
        'title':'<em>r</em> - <em>J</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>r</em> - <em>J</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssi_jmag':{
        'from':['-','sdssi','jmag'],
        'title':'<em>i</em> - <em>J</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>i</em> - <em>J</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssg_kmag':{
        'from':['-','sdssg','kmag'],
        'title':'<em>g</em> - <em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>g</em> - <em>K<sub>s</sub></em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_bmag_vmag':{
        'from':['-','bmag','vmag'],
        'title':'<em>B</em> - <em>V</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>B</em> - <em>V</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_vmag_rmag':{
        'from':['-','vmag','rmag'],
        'title':'<em>V</em> - <em>R</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>V</em> - <em>R</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_vmag_kmag':{
        'from':['-','vmag','kmag'],
        'title':'<em>V</em> - <em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>V</em> - <em>K<sub>s</sub></em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssu_sdssg':{
        'from':['-','sdssu','sdssg'],
        'title':'<em>u</em> - <em>g</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>u</em> - <em>g</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssg_sdssr':{
        'from':['-','sdssg','sdssr'],
        'title':'<em>g</em> - <em>r</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>g</em> - <em>r</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssr_sdssi':{
        'from':['-','sdssr','sdssi'],
        'title':'<em>r</em> - <em>i</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>r</em> - <em>i</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssi_sdssz':{
        'from':['-','sdssi','sdssz'],
        'title':'<em>i</em> - <em>z</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>i</em> - <em>z</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_sdssg_sdssi':{
        'from':['-','sdssg','sdssi'],
        'title':'<em>g</em> - <em>i</em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>g</em> - <em>i</em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'color_gaiamag_kmag':{
        'from':['-','gaiamag','kmag'],
        'title':'<em>G</em> - <em>K<sub>s</sub></em>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>G</em> - <em>K<sub>s</sub></em> color [mag]',
        'index':True,
        'ftsindex':False,
    },
    #
    # dereddened colors
    #
    'dered_jmag_kmag':{
        'from':['-','dered_jmag','dered_kmag'],
        'title':'(<em>J</em> - <em>K</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>J</em> - <em>K</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_jmag_hmag':{
        'from':['-','dered_jmag','dered_hmag'],
        'title':'(<em>J</em> - <em>H</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>J</em> - <em>H</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_hmag_kmag':{
        'from':['-','dered_hmag','dered_kmag'],
        'title':'(<em>H</em> - <em>K<sub>s</sub></em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':('<em>H</em> - <em>K<sub>s</sub></em> '
                       'dereddened color [mag]'),
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssr_jmag':{
        'from':['-','dered_sdssr','dered_jmag'],
        'title':'(<em>r</em> - <em>J</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>r</em> - <em>J</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssi_jmag':{
        'from':['-','dered_sdssi','dered_jmag'],
        'title':'(<em>i</em> - <em>J</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>i</em> - <em>J</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssg_kmag':{
        'from':['-','dered_sdssg','dered_kmag'],
        'title':'(<em>g</em> - <em>K</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':('<em>g</em> - <em>K<sub>s</sub></em> '
                       'dereddened color [mag]'),
        'index':True,
        'ftsindex':False,
    },
    'dered_bmag_vmag':{
        'from':['-','dered_bmag','dered_vmag'],
        'title':'(<em>B</em> - <em>V</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>B</em> - <em>V</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_vmag_rmag':{
        'from':['-','dered_vmag','dered_rmag'],
        'title':'(<em>V</em> - <em>R</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>V</em> - <em>R</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_vmag_kmag':{
        'from':['-','dered_vmag','dered_kmag'],
        'title':'(<em>V</em> - <em>K<sub>s</sub></em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':('<em>V</em> - <em>K<sub>s</sub></em> '
                       'dereddened color [mag]'),
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssu_sdssg':{
        'from':['-','dered_sdssu','dered_sdssg'],
        'title':'(<em>u</em> - <em>g</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>u</em> - <em>g</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssg_sdssr':{
        'from':['-','dered_sdssg','dered_sdssr'],
        'title':'(<em>g</em> - <em>r</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>g</em> - <em>r</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssr_sdssi':{
        'from':['-','dered_sdssr','dered_sdssi'],
        'title':'(<em>r</em> - <em>i</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>r</em> - <em>i</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssi_sdssz':{
        'from':['-','dered_sdssi','dered_sdssz'],
        'title':'(<em>i</em> - <em>z</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>i</em> - <em>z</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
    'dered_sdssg_sdssi':{
        'from':['-','dered_sdssg','dered_sdssi'],
        'title':'(<em>g</em> - <em>i</em>)<sub>0</sub>',
        'dtype':'f8',
        'format':'%.3f',
        'description':'<em>g</em> - <em>i</em> dereddened color [mag]',
        'index':True,
        'ftsindex':False,
    },
}
