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
