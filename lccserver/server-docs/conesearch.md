This page describes how to use the cone search query function of the LCC server
to search for a collection of objects in a specified sky region, and optionally
filtering the matching objects by various database columns.

[TOC]

## The web interface

<figure class="figure">
  <img src="/server-static/lcc-server-search-conesearch.png"
       class="figure-img img-fluid"
       alt="The cone-search query form">
  <figcaption class="figure-caption text-center">
    The cone-search query form
  </figcaption>
</figure>


### Input

### Executing the query

### Examples


## The API

The cone search query function accepts HTTP requests to its endpoint:

```
GET {{ server_url }}/api/conesearch
```

### Parameters

The search query arguments are encoded as URL query string parameters as
described in the table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`coords`           | **yes**  |         | The center coordinates and search radius as specified previously. These can be in either decimal or sexagesimal format. The search radius is in arcminutes and is optional.
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
