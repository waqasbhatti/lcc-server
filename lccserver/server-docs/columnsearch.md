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
