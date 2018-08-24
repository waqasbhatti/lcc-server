This page describes how to interact with the LCC server using its HTTP
API. These API endpoints are available for script or command-line use. For
details on the parameters needed for each search service API, visit its linked
documentation page.

[TOC]

## API Python client

A client for the API is implemented as simple functions in a single-file Python module: [lccs.py](https://github.com/waqasbhatti/astrobase/blob/master/astrobase/services/lccs.py). This doesn't depend on anything other than the Python standard library, so can be dropped in anywhere it's needed.


## Search query services

Service | Method and URL | Parameters | API key | Response
------- | --- | ---------- | ---------------- | ----------
`conesearch` | `GET {{ server_url }}/api/conesearch` | [docs](/docs/conesearch#the-api) | **no** | streaming<br>ND-JSON
`ftsquery` | `GET {{ server_url }}/api/ftsquery` | [docs](/docs/ftsearch#the-api) | **no** | streaming<br>ND-JSON
`columnsearch` | `GET {{ server_url }}/api/columnsearch` | [docs](/docs/columnsearch#the-api) | **no** | streaming<br>ND-JSON
`xmatch` | `POST {{ server_url }}/api/xmatch` | [docs](/docs/xmatch#the-api) | **[yes](#api-keys)** | streaming<br>ND-JSON

## Streaming search query responses

Query results are returned with `Content-Type: application/json`. The LCC server is an asynchronous service, with queries running in the foreground for up to 30 seconds, and then relegated to a background queue after that. The JSON returned is in [newline-delimited format](https://github.com/ndjson/ndjson-spec), with each line describing the current state of the query. The final line represents the disposition of the query. You may want to use a streaming JSON parser if you want to react to the query stages in real-time: the LCC server itself uses [oboe.js](https://github.com/jimhigson/oboe.js) to handle this process on the frontend; see also [ijson](https://github.com/isagalaev/ijson) for a Python package. Most command-line applications like HTTPie (with the `--stream` flag) and cURL (with the `-N` flag) can handle this as well. You can also simply wait for up to 30 seconds to get the whole stream at once, and parse it later as needed.

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

An example of a query that will return more than 20,000 objects. The LCC server
will return immediately after the query finishes running and will not attempt to
generate a ZIP file collecting all of the light curve files for the matching
objects.

```json
{"message": "query in run-queue. executing with set ID: FOkEGSTvMmw...", "status": "queued", "result": {"setid": "FOkEGSTvMmw", "api_service": "columnsearch", "api_args": {"conditions": "(sdssr < 12.0)", "sortcol": "sdssr", "sortorder": "asc", "result_ispublic": true, "collections": null, "getcolumns": ["sdssr"]}}, "time": "2018-08-09T01:17:52.904297Z"}
{"message": "query finished OK. objects matched: 50011, building dataset...", "status": "running", "result": {"setid": "FOkEGSTvMmw", "nobjects": 50011}, "time": "2018-08-09T01:17:53.654130Z"}
{"message": "Dataset pickle generation complete. There are more than 20,000 light curves to collect so we won't generate a ZIP file. See http://localhost:12500/set/FOkEGSTvMmw for dataset object lists and a CSV when the query completes.", "status": "background", "result": {"setid": "FOkEGSTvMmw", "seturl": "{{ server_url }}/set/FOkEGSTvMmw"}, "time": "2018-08-09T01:17:55.349453Z"}
```


## Collections, datasets, and object information

Service | Method and URL | Parameters | API key | Response
------- | --- | ---------- | ---------------- | ----------
`collection-list` | `GET {{ server_url }}/api/collections` | [docs](#collection-list-api) | **no** | JSON
`dataset-list` | `GET {{ server_url }}/api/datasets` | [docs](#dataset-list-api) | **no** | JSON
`dataset` | `GET {{ server_url }}/set/[setid]` | [docs](#dataset-api) | **no** | JSON
`objectinfo` | `GET {{ server_url }}/object` | [docs](#object-information-api) | **no** | JSON

### Collection list API

This returns a list of current light curve collections held by the LCC server
and returns JSON. No arguments are allowed to this API service. It can be
accessed using a `GET` request to:

```
{{ server_url }}/api/collections
```

The returned JSON's `result` key contains the following items, each of which
provide details on the available light curve collections:

Key | Contents
--- | --------
`available_columns` | a list of columns that are available across all collections
`available_fts_columns` | a list of full-text-search indexed columns available across all collections
`available_index_columns` | a list of indexed columns available across all collections
`collections` | an object that contains lists of keys detailing collection information

The `collections` key contains the following useful items:

Key | Contents
--- | --------
`name` | a list of names for each light curve collection
`nobjects` | a list of counts of objects in each light curve collection
`collection_id` | a list of collection short name, for each collection, used for forming download paths for light curves
`columnjson` | a list of JSON objects containing each collection's column names, titles, descriptions, string formats, and information on their indexed status
`columnlist` | a list of comma-separated string lists of all column names per collection
`ftsindexedcols` | a list of comma-separated strings containing the columns that have been indexed for fast text search for each collection
`indexedcols` | a list of comma-separated strings containing the columns that have been indexed for each collection
`datarelease` | a list of the data release numbers for each collection
`db_collection_id` | a list of the actual database collection IDs used for each collection
`description` | a list of descriptions of the light curve collection's contents
`lcformatdesc` | a list of links to light curve format description JSONs for the original format of each collection's light curve files (this is used by LCC server to convert original format LCs to common LCC CSV LCs)
`maxra` | a list of the maximum right ascension in decimal degrees for each collection' objects
`maxdecl` | a list of the maximum declination in decimal degrees for each collection' objects
`minra` | a list of the minimum right ascension in decimal degrees for each collection' objects
`mindecl` | a list of the minimum declination in decimal degrees for each collection' objects


### Dataset list API

The dataset list API is used to get lists of public datasets available on the LCC server. The service can be accessed via a `GET` request to:

```
{{ server_url }}/api/datasets
```

and takes a single parameter:

Parameter | Required | Default | Description
--------- | -------- | ------- | -----------
`nsets` | **no** | `25` | The number of recent datasets to return. This is capped at 1000.

This returns a JSON. The `result` key contains a list of objects detailing each dataset sorted in time order with the most recently generated public datasets at the top of the list. Each dataset object contains the following useful items:

Key | Value
--- | -----
`setid` | the set ID of the dataset, use this to visit the dataset page at `{{ server_url }}/set/[setid]`
`created_on` | an [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time formatted string in UTC, indicating when the dataset was created
`last_updated` | an [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time formatted string in UTC, indicating when the dataset was last updated
`dataset_csv` | a link to download the dataset's data table CSV
`dataset_fpath` | a link to download the dataset's Python pickle file
`lczip_fpath` | a link to download the light curve ZIP file containing light curves for all objects that make up the dataset
`nobjects` | the number of objects in the dataset
`queried_collections` | the database collection IDs of all light curve collection's that were searched
`query_type` | the type of query that was run to generate this dataset
`query_params` | the parsed input arguments passed to the backend search function of the search service that generated this dataset


### Dataset API

The dataset API can be used to fetch a dataset in JSON form. It is available for any dataset page generated by the results of search queries. To view any dataset in JSON form, use:

```
{{ server_url }}/set/[setid]?json=1
```

This will return a JSON containing the dataset header as well as the data table in raw format. To get the data table in formatted string format, add `&strformat=1` to the URL. The returned JSON contains the following useful elements.

Key | Value
--- | -----
`collections` | a list of all DB collection IDs searched to generate this dataset
`columns` | a list of all columns present in the dataset data table
`coldesc` | a JSON object listing each column's title, string format, description, and Numpy dtype
`created` | an [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time formatted string in UTC, indicating when the dataset was created
`updated` | an [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) time formatted string in UTC, indicating when the dataset was last updated
`nobjects` | the number of objects in the dataset
`rowstatus` | a message indicating if all rows of the data table are returned in the JSON; datasets with more than 3000 objects in them will have only 3000 rows returned, use the dataset CSV for the entire data table
`dataset_csv` | a link for the data table's CSV file; add `{{ server_url }}` to the front of this to generate a full URL
`dataset_pickle` | a link for the dataset's pickle file; add `{{ server_url }}` to the front of this to generate a full URL
`lczip` | a link for the dataset's light curve ZIP file; add `{{ server_url }}` to the front of this to generate a full URL. **NOTE:** If there are more than 20,000 objects in the dataset, the LC ZIP file will not be created.
`searchtype` | the search service used to generate this dataset
`searchargs` | the parsed input arguments used by the backend search function
`status` | if the dataset is ready, this will be `complete`. if the search or light curve ZIP operation for the dataset is still running, the status will be `in progress`.


### Object information API

This service is used to get detailed information including finding charts, comments, object type and variability tags, and period-search results (if available) for any object made available publicly in the LCC server's collection databases. A `GET` request can be made to the following URL.

```
{{ server_url }}/api/object
```

The following parameters are all required:

Parameter | Required | Description
--------- | -------- | -----------
`objectid` | **yes** | the database object ID of the object. **NOTE:** this is returned as the column `db_oid` in any search query result.
`collection` | **yes** | the database collection ID of the object. **NOTE:** this is returned as the column `collection` in any search query result.

This returns a JSON containing all available object information in the `result` key. Some important items include:

Key | Value
--- | -----
`objectinfo` | all object magnitude, color, GAIA cross-match, and object type information available for this object
`objectcomments` | comments on the object's variability if available
`varinfo` | variability comments, variability features, type tags, period and epoch information if available
`neighbors` | information on the neighboring objects of this object in its parent light curve collection
`xmatch` | information on any cross-matches to external catalogs (e.g. KIC, EPIC, TIC, APOGEE, etc.)
`finderchart` | a base-64 encoded PNG image of the object's DSS2 RED finder chart. To convert this to an actual PNG, try [this snippet of Python code](https://github.com/waqasbhatti/astrobase/blob/a05940886c729036d1471af5e4a5ff120e3e23eb/astrobase/checkplot.py#L1339).
`magseries` | a base-64 encoded PNG image of the object's light curve. To convert this to an actual PNG, try [this snippet of Python code](https://github.com/waqasbhatti/astrobase/blob/a05940886c729036d1471af5e4a5ff120e3e23eb/astrobase/checkplot.py#L1339).
`pfmethods` | a list of period-finding methods applied to the object if any. If this list is present, use the keys in it to get to the actual period-finding results for each method. These will contain base-64 encoded PNGs of the periodogram and phased light curves using the best three peaks in the periodogram, as well as period and epoch information.


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
        "expires": "2018-08-03T16:06:36.684957Z"
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
