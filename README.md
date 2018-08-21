# LCC-Server: A light curve collection server framework

This is a Python framework to serve collections of light curves. The code here
forms the basis for the [HAT data server](https://data.hatsurveys.org).

At the moment, it includes the following functionality:

- collection of light curves from various projects into a single output format

- HTTP API for searching over multiple light curve collections by:
  - filtering on database columns of object properties, e.g. objectid,
    mag, variability type, variability indices, etc.
  - cone-search near specified coordinates
  - cross-matching to uploaded object list with objectid, ra, decl
  - full-text search on object names, descriptions, etc.

- HTTP API for generating datasets from search results asychronously, caching
  results from searches, and generating output zip bundles containing search
  results and all matching light curves

- HTTP API for detailed information per object, including light curve plots,
  external catalog info, and period-finding results plus phased LCs if available

## Installation

This package is available on PyPI:

```bash
$ pip install numpy  # to set up Fortran bindings for dependencies
$ pip install lccserver
```

To install the latest version from Github:

```bash
$ git clone https://github.com/waqasbhatti/lcc-server
$ cd lcc-server
$ pip install -e .
```

Some post-installation setup is required to begin serving light curves. In
particular, you will need to set up a base directory where LCC-Server can work
from and various sub-directories. Notes on this TBD...

## License

LCC-Server is provided under the MIT License. See the LICENSE file for the full
text.
