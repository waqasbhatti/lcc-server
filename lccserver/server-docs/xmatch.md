This page describes how to use the cross-matching function of the LCC server to
upload a list of objects and coordinates, get matches to them within a specified
distance, and optionally filter the results by database column values.

[TOC]

## The web interface

<figure class="figure">
  <img src="/server-static/lcc-server-search-xmatch.png"
       class="figure-img img-fluid"
       alt="The cross-match query form">
  <figcaption class="figure-caption text-center">
    The cross-match query form
  </figcaption>
</figure>


### Input

### Executing the query

### Examples


## The API

The column search query function accepts HTTP requests to its endpoint:

```
POST {{ server_url }}/api/xmatch
```

### Parameters

The search query arguments are encoded as URL query string parameters as
described in the table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`xmq`              | **yes**  |         | A string containing the object names, right ascensions, and declinations of the objects to cross-match to the collections on the LCC server. The object name, RA, Dec rows should be separated by line-feed characters: `\n`. Object coordinates can be in decimal or sexagesimal format. Object names should not have spaces, because these are used to separate object names, RA, and declination for each object row.
`xmd`              | **yes**  | 3.0     | The maximum distance in arcseconds to use for cross-matching. If multiple objects match to an input object within the match radius, duplicate rows for that object will be returned in the output containing all database matches sorted in increasing order of distance.
`result_ispublic`  | **no**   | `1`     | `1` means the resulting dataset will be public and visible on the [Recent Datasets](/datasets) page. `0` means the resulting dataset will only be accessible to people who know its URL.
`collections`      | **no**   | `null`  | Collections to search in. Specify this multiple times to indicate multiple collections to search. If this is null, all collections will be searched.
`columns`          | **no**   | `null`  | Columns to retrieve. Columns used for filtering and sorting are returned automatically so there's no need to specify them here. The database object names, right ascensions, and declinations are returned automatically as well.
`filters`          | **no**   | `null`  | Filters to apply to the objects found. This is a string in SQL format specifying the columns and operators to use to filter the results.


### Results


### API key required


### Examples

#### command-line with `httpie`

[HTTPie](https://httpie.org) is a friendlier alternative to the venerable `cURL`
program. See its [docs](https://httpie.org/doc#installation) for how to install
it. The examples below use HTTPie to demonstrate the LCC server API, but one can
use any other comparable program if desired.


#### Python with `requests`

The Python [requests](http://docs.python-requests.org/en/master/) library makes
HTTP requests from Python code a relatively simple task. To install it, use
`pip` or `conda`. The examples below show how to talk to the LCC server API
using this package.
