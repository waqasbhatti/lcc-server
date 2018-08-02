This page describes how to use the cone search query function of the LCC server
to search for a collection of objects in a specified sky region, and optionally
filtering the matching objects by various database columns.

[TOC]

## The web interface

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
`coords`           | **yes**  |         |
`result_ispublic`  | **no**   | `1`     |
`collections`      | **no**   | `null`  |
`columns`          | **no**   | `null`  |
`filters`          | **no**   | `null`  |

### Results


### Examples
