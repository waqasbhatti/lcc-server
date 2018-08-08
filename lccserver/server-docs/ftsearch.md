This page describes how to use the full-text-search query function of the LCC
server to search for a collection of objects that match a specified text string,
optionally filtering the matching objects by database column values.

[TOC]

## The web interface

<figure class="figure">
  <img src="/server-static/lcc-server-search-fts.png"
       class="figure-img img-fluid"
       alt="The full-text query form">
  <figcaption class="figure-caption text-center">
    The full-text query form
  </figcaption>
</figure>

### Input

### Executing the query

### Examples


## The API

The cone search query function accepts HTTP requests to its endpoint:

```
GET {{ server_url }}/api/ftsquery
```

### Parameters

The search query arguments are encoded as URL query string parameters as
described in the table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`ftstext`          | **yes**  |         | A string to match against the full-text indexed columns in the specified collections. For an exact match against object names, use `"` characters to quote the required string.
`result_ispublic`  | **no**   | `1`     | `1` means the resulting dataset will be public and visible on the [Recent Datasets](/datasets) page. `0` means the resulting dataset will only be accessible to people who know its URL.
`collections`      | **no**   | `null`  | Collections to search in. Specify this multiple times to indicate multiple collections to search. If this is null, all collections will be searched.
`columns`          | **no**   | `null`  | Columns to retrieve. Columns used for filtering and sorting are returned automatically so there's no need to specify them here. The database object names, right ascensions, and declinations are returned automatically as well.
`filters`          | **no**   | `null`  | Filters to apply to the objects found. This is a string in SQL format specifying the columns and operators to use to filter the results.


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
