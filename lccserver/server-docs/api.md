This page describes how to interact with the LCC server using its HTTP
API. These API endpoints are available for script or command-line use. For
details on the parameters needed for each search service API, visit its linked
documentation page.

[TOC]

## Search query services

Service | Method and URL | Parameters | API key | Response
------- | --- | ---------- | ---------------- | ----------
`conesearch` | `GET {{ server_url }}/api/conesearch` | [docs](/docs/conesearch#the-api) | **no** | streaming<br>ND-JSON
`ftsquery` | `GET {{ server_url }}/api/ftsquery` | [docs](/docs/ftsearch#the-api) | **no** | streaming<br>ND-JSON
`columnsearch` | `GET {{ server_url }}/api/columnsearch` | [docs](/docs/columnsearch#the-api) | **no** | streaming<br>ND-JSON
`xmatch` | `POST {{ server_url }}/api/xmatch` | [docs](/docs/xmatch#the-api) | **[yes](#api-keys)** | streaming<br>ND-JSON

## Streaming search query responses

Query results are returned with `Content-Type: application/json`. The LCC server is an asychronous service, with queries running in the foreground for up to 30 seconds, and then relegated to a background queue after that. The JSON returned is in [newline-delimited format](https://github.com/ndjson/ndjson-spec), with each line descibing the current state of the query. The final line represents the disposition of the query. You may want to use a streaming JSON parser if you want to react to the query stages in real-time: the LCC server itself uses [oboe.js](https://github.com/jimhigson/oboe.js) to handle this process on the frontend; see also [ijson](https://github.com/isagalaev/ijson) for a Python package. Most command-line applications like HTTPie (with the `--stream` flag) and cURL (with the `-N` flag) can handle this as well. You can also simply wait for up to 30 seconds to get the whole stream at once, and parse it later as needed.

An example of results from a query that finishes within 30 seconds:

```json
{"message": "query in run-queue. executing with set ID: N1tZWPOZyxk...", "status": "queued", "result": {"setid": "N1tZWPOZyxk", "api_service": "conesearch", "api_args": {"coords": "290.0 45.0 60.0", "extraconditions": "(sdssr < 13.0) and (propermotion > 50.0)", "result_ispublic": true, "collections": ["hatnet_keplerfield"], "getcolumns": ["propermotion", "sdssr", "sdssg", "sdssi"]}}, "time": "2018-08-09T01:04:00.878206Z"}
{"message": "query finished OK. objects matched: 11, building dataset...", "status": "running", "result": {"setid": "N1tZWPOZyxk", "nobjects": 11}, "time": "2018-08-09T01:04:01.199606Z"}
{"message": "dataset pickle generation complete. collecting light curves into ZIP file...", "status": "running", "result": {"setid": "N1tZWPOZyxk"}, "time": "2018-08-09T01:04:01.206888Z"}
{"message": "dataset LC ZIP complete. generating dataset CSV...", "status": "running", "result": {"setid": "N1tZWPOZyxk", "lczip": "/p/lightcurves-N1tZWPOZyxk.zip"}, "time": "2018-08-09T01:04:01.213817Z"}
{"message": "dataset now ready: {{ server_url }}/set/N1tZWPOZyxk", "status": "ok", "result": {"setid": "N1tZWPOZyxk", "seturl": "{{ server_url }}/set/N1tZWPOZyxk", "created": "2018-08-09T01:04:00.874976", "updated": "2018-08-09T01:04:01.211014", "backend_function": "sqlite_kdtree_conesearch", "backend_parsedargs": {"center_ra": 290.0, "center_decl": 45.0, "radius_arcmin": 60.0, "getcolumns": ["propermotion", "sdssr", "sdssg", "sdssi", "db_oid", "kdtree_oid", "db_ra", "db_decl", "kdtree_ra", "kdtree_decl", "db_lcfname", "dist_arcsec"], "extraconditions": "(sdssr < 13.0) and (propermotion > 50.0)", "lcclist": ["hatnet_keplerfield"]}, "nobjects": 11}, "time": "2018-08-09T01:04:01.219230Z"}
```

An example for a longer running query that takes more than 30 seconds:

```json
{"message": "query in run-queue. executing with set ID: iRM6Vy7hP08...", "status": "queued", "result": {"setid": "iRM6Vy7hP08", "api_service": "conesearch", "api_args": {"coords": "295.35 -25.38 60.0", "extraconditions": "", "result_ispublic": true, "collections": null, "getcolumns": []}}, "time": "2018-08-09T01:21:28.595478Z"}
{"message": "query finished OK. objects matched: 7960, building dataset...", "status": "running", "result": {"setid": "iRM6Vy7hP08", "nobjects": 7960}, "time": "2018-08-09T01:21:29.449012Z"}
{"message": "dataset pickle generation complete. collecting light curves into ZIP file...", "status": "running", "result": {"setid": "iRM6Vy7hP08"}, "time": "2018-08-09T01:21:29.879868Z"}
{"message": "query sent to background after 30 seconds. query is complete, but light curves of matching objects are still being zipped. check {{ server_url }}/set/iRM6Vy7hP08 for results later", "status": "background", "result": {"setid": "iRM6Vy7hP08", "seturl": "{{ server_url }}/set/iRM6Vy7hP08"}, "time": "2018-08-09T01:21:44.893590Z"}
```

An example of a query that could not be parsed:

```json
{"status": "failed", "result": null, "message": "could not parse the input coords string"}
{"status": "failed", "result": null, "message": "one or more of the required args are missing or invalid"}
```

An example of a query that did not match any items:

```json
{"message": "query in run-queue. executing with set ID: M1aMW0PUkIw...", "status": "queued", "result": {"setid": "M1aMW0PUkIw", "api_service": "conesearch", "api_args": {"coords": "290.0 -45.0 10.0", "extraconditions": "", "result_ispublic": true, "collections": null, "getcolumns": []}}, "time": "2018-08-09T01:29:46.694673Z"}
{"status": "failed", "result": {"setid": "M1aMW0PUkIw", "nobjects": 0}, "message": "Query <code>M1aMW0PUkIw<\/code> failed. No matching objects were found", "time": "2018-08-09T01:29:46.863618Z"}
```

## Collections, datasets, and object information

Service | Method and URL | Parameters | API key | Response
------- | --- | ---------- | ---------------- | ----------
`collection-list` | `GET {{ server_url }}/api/collections` | [docs](#collection-list-api) | **no** | JSON
`dataset-list` | `GET {{ server_url }}/api/datasets` | [docs](#dataset-list-api) | **no** | JSON
`dataset` | `GET {{ server_url }}/set/[setid]` | [docs](#dataset-api) | **no** | JSON
`objectinfo` | `GET {{ server_url }}/object` | [docs](#object-information-api) | **no** | JSON

### Collection list API

### Dataset list API

### Dataset API

### Object information API


## API keys

Some APIs require a token provided as part of the HTTP header. To get an
API key token valid for 24 hours, perform an HTTP `GET` request to:

```
{{ server_url }}/api/key
```

This will return a JSON formatted object with the key to use, e.g.:

```json
{
    "message": "key expires: 2018-08-03T16:06:36.684957Z",
    "result": {
        "key": "eyJpcCI6IjEyNy4wLjAuMSIsInZlciI6MSwidG9rZW4iOiJteE55dVhOUkZicmFoQSIsImV4cGlyeSI6IjIwMTgtMDgtMDNUMTY6MDY6MzYuNjg0OTU3WiJ9.DkS9jA.DXngAj-NToG-9qdGbP2QcoMQzFw"
    },
    "status": "ok"
}
```

Use the value of the `result.key` item in the HTTP headers of any subsequent requests to the search API endpoint which requires an API key. This is of the form:

```
Authorization: Bearer [API key token]
```

You can check if your API key is still valid by performing an HTTP `GET` request
to:

```
{{ server_url }}/api/auth?key=[API key token]

```

If your key passes verification, then it's good to use:

```json
{
    "message": "API key verified successfully. Expires: 2018-08-03T16:06:36.684957Z",
    "result": {
        "expiry": "2018-08-03T16:06:36.684957Z"
    },
    "status": "ok"
}
```

If it fails:

```json
{
    "message": "API key could not be verified or has expired.",
    "result": null,
    "status": "failed"
}
```

Then you can simply request a new key and continue with your previous requests
to the API key-secured API endpoints.
