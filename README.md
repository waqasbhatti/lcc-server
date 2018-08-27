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

[![LCC server search interface](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/search-th.png)](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/search-montage.png)

- HTTP API for generating datasets from search results asychronously, caching
  results from searches, and generating output zip bundles containing search
  results and all matching light curves

[![LCC server results display](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/results-th.png)](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/results-montage.png)

- HTTP API for detailed information per object, including light curve plots,
  external catalog info, and period-finding results plus phased LCs if available

[![LCC server object info](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/objectinfo-th.png)](https://raw.githubusercontent.com/waqasbhatti/lcc-server/master/docs/objectinfo-montage.png)


## Installation

**NOTE:** Python >= 3.6 is required. Use of a virtualenv is recommended;
something like this will work well:

```bash
$ python3 -m venv lcc
$ source lcc/bin/activate
```

This package is [available on PyPI](https://pypi.org/project/lccserver). Install
it with the virtualenv activated:

```bash
$ pip install numpy  # to set up Fortran bindings for dependencies
$ pip install lccserver  # add --pre to install unstable versions
```

To install the latest version from Github:

```bash
$ git clone https://github.com/waqasbhatti/lcc-server
$ cd lcc-server
$ pip install -e .
```

If you're on Linux or MacOS, you can install the
[uvloop](https://github.com/MagicStack/uvloop) package to optionally speed up
some of the eventloop bits:

```bash
$ pip install uvloop
```

## Using the server

Some post-installation setup is required to begin serving light curves. In
particular, you will need to set up a base directory where LCC-Server can work
from and various sub-directories.

To make this process easier, there's an interactive CLI available when you
install LCC-Server. This will be in your `$PATH` as [`lcc-server`](https://github.com/waqasbhatti/lcc-server/blob/master/lccserver/cli.py).

A Jupyter notebook walkthough using this CLI to stand up an LCC-Server instance,
with example light curves, can be found in the **astrobase-notebooks** repo:
[lcc-server-setup.ipynb](https://github.com/waqasbhatti/astrobase-notebooks/blob/master/lcc-server-setup.ipynb)
([Jupyter nbviewer link](https://nbviewer.jupyter.org/github/waqasbhatti/astrobase-notebooks/blob/master/lcc-server-setup.ipynb)).

- Documentation for how to use the server for searching LC collections is hosted
at the HAT data server instance: https://data.hatsurveys.org/docs.
- The HTTP API is documented at: https://data.hatsurveys.org/docs/api.
- A standalone Python module API client is available in the astrobase
  repository: [lccs.py](https://github.com/waqasbhatti/astrobase/blob/master/astrobase/services/lccs.py).

These docs are automatically generated from the
[server-docs](https://github.com/waqasbhatti/lcc-server/tree/master/lccserver/server-docs) directory in the git repository.


## License

LCC-Server is provided under the MIT License. See the LICENSE file for the full
text.
