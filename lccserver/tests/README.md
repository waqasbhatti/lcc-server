This directory contains tests for the LCC-Server.

# SQLite dependency

Most of these tests (and the LCC-Server itself) rely on the fact that the system
SQLite library is new enough to contain the `fts5` full-text search module. For
some older Enterprise Linux systems, this isn't the case. To get the LCC-Server
and these tests running on these systems, you'll have to install a newer version
of the [SQLite amalgamation](https://sqlite.org/download.html). I recommend
downloading the tarball with autoconf so it's easy to install; e.g. for SQLite
3.27.2, use this file:
[sqlite-autoconf-3270200.tar.gz](https://sqlite.org/2019/sqlite-autoconf-3270200.tar.gz).

To install at the default location `/usr/local/lib`:

```bash
$ tar xvf sqlite-autoconf-3270200.tar.gz
$ ./configure
$ make
$ make install
```

Then, override the default location that Python uses for its SQLite library
using `LD_LIBRARY_PATH`:

```bash
$ export LD_LIBRARY_PATH='/usr/local/lib'

# create a virtualenv using Python 3
# here I've installed Python 3.7 to /opt/python37
$ /opt/python37/bin/python3 -m venv env

# activate the virtualenv, launch Python, and check if we've got a newer SQLite
$ source env/bin/activate
(env) $ python3
Python 3.7.0 (default, Jun 28 2018, 15:17:26)
[GCC 4.8.5 20150623 (Red Hat 4.8.5-28)] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import sqlite3
>>> sqlite3.sqlite_version
'3.27.2'
```

You can then run the tests using this virtualenv.
