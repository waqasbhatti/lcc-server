This page describes how to use the cross-matching function of the LCC server to
upload a list of objects and coordinates, get matches to them within a specified
distance, and optionally filter the results by database column values.

[TOC]

## The web interface

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
`xmq`              | **yes**  |         |
`xmd`              | **yes**  |         |
`result_ispublic`  | **no**   | `1`     |
`collections`      | **no**   | `null`  |
`columns`          | **no**   | `null`  |
`filters`          | **no**   | `null`  |


### Results


### API key required

The `xmatch` API requires a token provided as part of the HTTP header. To get an
API key token valid for 24 hours, perform an HTTP `GET` request to:

```
{{ server_url }}/api/key
```

This will return a JSON formatted object with the key to use, e.g.:

```
{
    "message": "key expires: 2018-08-03T16:06:36.684957Z",
    "result": {
        "key": "eyJpcCI6IjEyNy4wLjAuMSIsInZlciI6MSwidG9rZW4iOiJteE55dVhOUkZicmFoQSIsImV4cGlyeSI6IjIwMTgtMDgtMDNUMTY6MDY6MzYuNjg0OTU3WiJ9.DkS9jA.DXngAj-NToG-9qdGbP2QcoMQzFw"
    },
    "status": "ok"
}
```

Use the value of the `result.key` item in the HTTP headers of any subsequent
`POST` requests to the `xmatch` API endpoint. This is of the form:

```
Authorization: Bearer [API key token]
```

See the examples below for how to put the key into HTTP request headers.

You can check if your API key is still valid by performing an HTTP `GET` request
to:

```
{{ server_url }}/api/auth?key=[API key token]

```

If your key passes verification, then it's good to use:

```
{
    "message": "API key verified successfully. Expires: 2018-08-03T16:06:36.684957Z",
    "result": {
        "expiry": "2018-08-03T16:06:36.684957Z"
    },
    "status": "ok"
}
```

If it fails:

```
{
    "message": "API key could not be verified or has expired.",
    "result": null,
    "status": "failed"
}
```

Then you can simply request a new key and continue with your previous requests
to the `xmatch` API endpoints.


### Examples

#### command-line with `httpie`

`httpie` is a friendlier alternative to the venerable `cURL` program. To install
it, see: [https://httpie.org](https://httpie.org). The examples below use
`httpie` to demonstrate how to use the LCC server API, but one can use any other
comparable program if desired.


#### Python with `requests`

The Python [requests](http://docs.python-requests.org/en/master/) library makes
HTTP requests from Python code a relatively simple task. To install it, use
`pip` or `conda`. The examples below show how to talk to the LCC server API
using this module.
