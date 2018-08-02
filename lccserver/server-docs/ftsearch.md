This page describes how to use the full-text-search query function of the LCC
server to search for a collection of objects that match a specified text string,
optionally filtering the matching objects by database column values.

[TOC]

## The web interface

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
`ftstext`          | **yes**  |         |
`result_ispublic`  | **no**   | `1`     |
`collections`      | **no**   | `null`  |
`columns`          | **no**   | `null`  |
`filters`          | **no**   | `null`  |

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
