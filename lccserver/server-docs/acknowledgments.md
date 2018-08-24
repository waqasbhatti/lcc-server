## Code

The source code for the Light Curve Collection Server (LCC Server) framework is
available at [Github](https://github.com/waqasbhatti/lcc-server) and is provided
under the [MIT
License](https://github.com/waqasbhatti/lcc-server/blob/master/LICENSE).

## Data

This work has made use of data from the European Space Agency (ESA) mission
[Gaia](https://www.cosmos.esa.int/gaia), processed by the Gaia Data Processing
and Analysis Consortium
([DPAC](https://www.cosmos.esa.int/web/gaia/dpac/consortium)). Funding for the
DPAC has been provided by national institutions, in particular the institutions
participating in the Gaia Multilateral Agreement.

This work has made use of the NASA/IPAC Infrared Science Archive [DUST
service](http://irsa.ipac.caltech.edu/applications/DUST/), which is operated by
the Jet Propulsion Laboratory, California Institute of Technology, under
contract with the National Aeronautics and Space Administration.

This work has made use of the [SkyView
service](https://skyview.gsfc.nasa.gov/current/cgi/titlepage.pl) provided by the
NASA Goddard Space Flight Center. In particular, finder charts for objects are
generated from the [Digitized Sky
Surveys](http://archive.stsci.edu/dss/acknowledging.html), originally made
available by the [Mikulski Archive for Space Telescopes at
STScI](http://archive.stsci.edu/).

This work has made use of the [SIMBAD
database](http://simbad.u-strasbg.fr/simbad), operated at CDS, Strasbourg,
France. See [2000, A&AS, 143, 9 , "The SIMBAD astronomical database", Wenger et
al](http://adsabs.harvard.edu/abs/2000A%26AS..143....9W).

## Software

The server backend is written in [Python](https://www.python.org) and uses the
following packages available on [PyPi](https://pypi.org):

- [ipython](http://ipython.org/)
- [numpy](http://www.numpy.org/)
- [scipy](http://www.scipy.org)
- [tornado](http://www.tornadoweb.org/en/stable/)
- [requests](http://docs.python-requests.org/en/master/)
- [tqdm](https://tqdm.github.io/)
- [markdown](https://python-markdown.github.io/)
- [pygments](http://pygments.org/)
- [itsdangerous](https://www.palletsprojects.com/p/itsdangerous/)
- [cryptography](https://cryptography.io/en/latest/)
- [astrobase](https://github.com/waqasbhatti/astrobase)

The databases used for the server include:

- [SQLite](https://www.sqlite.org)
- [PostgreSQL](https://www.postgresql.org)

The [Bootstrap CSS and HTML framework](https://getbootstrap.com/) underpins much
of the frontend web design. The following JS packages are used for the server
frontend:

- [jQuery](https://jquery.com)
- [oboe.js](http://oboejs.com/)
- [moment.js](http://momentjs.com/)

The icons used are from the Google [Material Design
Icons](https://material.io/tools/icons/?style=baseline) project.
