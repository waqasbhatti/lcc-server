# LCC-Server: A light curve collection server framework

This is a Python framework to serve collections of light curves. The code here
forms the basis for the [HAT data server](https://data.hatsurveys.org).

At the moment, it includes the following functionality:

- collection of light curves from various projects into a single format

- HTTP API for searching over multiple light curve collections by:
  - filtering on any light curve column and object properties, e.g. objectid,
    mag, variability type, periods, etc.
  - cone-search near coordinates
  - cross-matching to uploaded object list with objectid, ra, decl
  - full-text search on object names, descriptions, etc.

- HTTP API for generating datasets from search results asychronously, caching
  results from searches, and generating output zip bundles containing search
  results and all matching light curves

- HTTP API for detailed information per object
