This page describes how to use the column search query function of the LCC
server to build a database query that matches specified column conditions and
can return results sorted by a column in ascending or descending order.

[TOC]

## The web interface

### Input

### Executing the query

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
`filters`          | **yes**  |         |
`sortcol`          | **yes**  |         |
`sortorder`        | **yes**  |         |
`result_ispublic`  | **no**   | `1`     |
`collections`      | **no**   | `null`  |
`columns`          | **no**   | `null`  |


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
