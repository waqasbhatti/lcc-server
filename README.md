# LCC-Server: A light curve collection server framework

This is a Python framework to serve collections of light curves. It includes the
following functionality that we think is a minimum requirement for any light
curve collection service:

- collection of light curves into a single format

- HTTP API for searching over a light curve collection by:
  - filtering on any light curve column and object properties, e.g. objectid,
    mag, variability type, periods, etc.
  - cone-search over coordinates
  - cross-matching to uploaded object list with objectid, ra, decl

- HTTP API for generating datasets from search results asychronously, caching
  results from searches, and generating output zip bundles containing search
  results and all matching light curves

- HTTP API for accessing various data releases and versions of the light curves

- HTTP API for generating light curves on demand in several formats from the
  collected light curves

## Short term TODO

- HTTP API for plotting unphased, phased light curves on demand with applied
  filters on columns, etc.

- HTTP API for generating light curve collection footprint given a survey
  mosaic; generated datasets can then be footprint aware

- HTTP API for generating stamps from a footprint mosaic for each object if one
  is provided, and from DSS by default.

- access control to all data based on users and groups, HTTP API access via key,
  user and group definition, etc.

This framework forms the basis for the [HAT data
server](https://data.hatsurveys.org). It can run with SQLite3 and PostgreSQL and
requires Python 3.6+. Even on SQLite3, it should be able to scale to millions of
objects because mostly everything is asynchronous and much of the
number-crunching takes place in RAM using numpy.

## Daydreaming about future stuff

Future functionality will include:

- federation APIs so multiple lcc-servers can appear in a single portal. this
  will involve metadata tagging for bandpass, sky footprint, time coverage,
  etc., sharing data in a global backing database so if nodes go offline, they
  can recover from other nodes

- public classification interfaces for periodic variable classification, a rich
  exploration interface built on web-GL

- extension to transient time-domain surveys

- streaming data ingest and alert system for transients and other high cadence
  phenomena

- collaboration tools, including object comments across federated datasets,
  activity streams, and streaming status updates for objects

- serving of calibrated FITS image stamps per object per epoch of any
  time-series, so people can run photometry on their own

- adding in VO TAP query services

- adding in automatic parallelization using cloud services
