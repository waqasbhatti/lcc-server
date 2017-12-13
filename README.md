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

- HTTP API for generating light curves on demand in several formats from the
  collected light curves

- HTTP API for generating light curve collection footprint given a survey
  mosaic; generated datasets can then be footprint aware

- access control to all data based on users and groups, HTTP API access via key,
  user and group definition, etc.

This framework forms the basis for the [HAT data
server](https://data.hatsurveys.org). It is meant to run with PostgreSQL and
Python 3 and should be able to scale to millions of objects.


## Notes

- add a conesearch server based on kdtree and pickles. this will run in
  memory. use the zones thingie to split by declination and query region
  expanding like in that Tamas Budavari paper. this way, we'll only store easy
  stuff in the SQL server, so we can use sqlite if needed.

- add asynchronous result stuff based on HTTP 303 See other like Gaia ADQL. now
  that we know how to run stuff in tornado asynchronously using the
  ProcessPoolExecutors, we probably don't need any stupid message queue nonsense

- add support for search backends: (1) pickles only + SQlite or (2) pickles and
  Postgres
