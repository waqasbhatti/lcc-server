This page describes how to use the column search query function of the LCC
server to build a database query that matches specified column conditions and
can return results sorted by a column in ascending or descending order.

[TOC]

## The web interface

<figure class="figure">
  <img src="/server-static/lcc-server-search-columnsearch.png"
       class="figure-img img-fluid"
       alt="The column search query form">
  <figcaption class="figure-caption text-center">
    The column search query form
  </figcaption>
</figure>


### Input

### Executing the query

An example for a query that will return more than 20,000 objects. The LCC server
will return immediately after the query finishes running and will not attempt to
generate a ZIP file collecting all of the light curves files for the matching
objects.

```
{"message": "query in run-queue. executing with set ID: FOkEGSTvMmw...", "status": "queued", "result": {"setid": "FOkEGSTvMmw", "api_service": "columnsearch", "api_args": {"conditions": "(sdssr < 12.0)", "sortcol": "sdssr", "sortorder": "asc", "result_ispublic": true, "collections": null, "getcolumns": ["sdssr"]}}, "time": "2018-08-09T01:17:52.904297Z"}
{"message": "query finished OK. objects matched: 50011, building dataset...", "status": "running", "result": {"setid": "FOkEGSTvMmw", "nobjects": 50011}, "time": "2018-08-09T01:17:53.654130Z"}
{"message": "Dataset pickle generation complete. There are more than 20,000 light curves to collect so we won't generate a ZIP file. See http://localhost:12500/set/FOkEGSTvMmw for dataset object lists and a CSV when the query completes.", "status": "background", "result": {"setid": "FOkEGSTvMmw", "seturl": "http://localhost:12500/set/FOkEGSTvMmw"}, "time": "2018-08-09T01:17:55.349453Z"}
```


### Examples


## The API

The column search query function accepts HTTP requests to its endpoint:

```
GET {{ server_url }}/api/columnsearch
```


### Parameters

The search query arguments are encoded as URL query string parameters as
described in the table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`filters`          | **yes**  |         | Filters to apply to the objects found. This is a string in SQL format specifying the columns and operators to use to filter the results.
`sortcol`          | **yes**  | `sdssr` | The column to sort the results by. This is set to `sdssr` by default if this column is present in the collections being searched. If `sdssr` is not present, `sortcol` will be set to `objectid` by default if not specified otherwise.
`sortorder`        | **yes**  | `asc`   | The sort order to return the results in. This is set to ascending values by default: `asc`. The only other option is descending sort order: `desc`.
`result_ispublic`  | **no**   | `1`     | `1` means the resulting dataset will be public and visible on the [Recent Datasets](/datasets) page. `0` means the resulting dataset will only be accessible to people who know its URL.
`collections`      | **no**   | `null`  | Collections to search in. Specify this multiple times to indicate multiple collections to search. If this is null, all collections will be searched.
`columns`          | **no**   | `null`  | Columns to retrieve. Columns used for filtering and sorting are returned automatically so there's no need to specify them here. The database object names, right ascensions, and declinations are returned automatically as well.


### Results


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
