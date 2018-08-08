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

The cone-search service is used to query the LCC server collection databases for
objects that fall within a specified radius of the provided central
coordinates. The service requires a central coordinate specification string,
along with an optional search radius in arcminutes, using any of the formats
below.

```
HH:MM:SS[.ssss] [+-]DD:MM:SS.ssss [DD.dddd]
HH MM SS[.ssss] [+-]DD MM SS.ssss [DD.dddd]
DDD[.dddd] [+-]DDD[.ddd] [DD.ddd]
```

The maximum search radius is 60 arcminutes or 1 degree.

The cone-search service also takes several optional inputs. These include:

1. **Collection to search in:** specified using the select control in the
   top-left portion of the cone-search tab. Multiple collections can be selected
   by holding either the <kbd>Ctrl</kbd> or the <kbd>Cmd</kbd> buttons when
   clicking on the options in the control. By default, the service will search
   all collections available on the LCC server until it finds a match. Results
   will always be returned with the name of the collection any matches were
   found in.

2. **Columns to retrieve:** specified using the select control in the
   bottom-left portion of the cone-search tab. This select control will update
   automatically to present only the columns available in *all* of the
   collections that are selected to search in. The service will return the
   `objectid`, `ra`, and `decl` columns for any matches by default, so these
   don't need to be selected. Specify any additional columns to retrieve by
   selecting them in this control, holding either the <kbd>Ctrl</kbd> or the
   <kbd>Cmd</kbd> when clicking on the options to select multiple columns.

3. **Filters on database columns:** specified using the controls just under the
   main coordinates input box. First choose a column to filter on using the left
   select control, then choose the operator to use from the center select
   control, and finally type the value to filter on in the input box on the
   right. Hit <kbd>Enter</kbd> or click on the **+** button to add the filter to
   the list of active filters. After the first filter is added, you can add an
   arbitrary number of additional filters, specifying the logical method to use
   (`AND`/`OR`) to chain them. This effectively builds up a `WHERE` SQL clause
   that will be applied to the objects that match the initial coordinate search
   specification.

Execute the cone-search query by clicking on the **Search** button or just by
hitting <kbd>Enter</kbd> once you're done typing into the main coordinate input
box. The query will enter the run-queue and begin executing as soon as
possible. If it does not finish within 30 seconds, either because the query
itself took too long or if the light curve collection for the matching objects
took too long, it will be sent to a background queue. You will be informed of
the URL where the results of the query will appear as a *dataset*. From the
dataset page associated with this query, you can view and download the table (as
CSV) generated based on the columns specified and download light curves
(individually or in a collected ZIP file) of all matching objects.

### Examples

**Example 1**: Search for any objects within 15 arcminutes of the coordinates
(&alpha;, &delta;) = (290.0,45.0):

<figure class="figure">
  <img src="/server-static/lcc-server-conesearch-example1.png"
       class="figure-img img-fluid"
       alt="Cone search query example 1 input">
  <figcaption class="figure-caption text-center">
    Cone search query example 1 input
  </figcaption>
</figure>

**Example 2**: Search for objects within 60 arciminutes of the coordinates
(&alpha;, &delta;) = (290.0,45.0), but restrict the query to just the
`hatnet_keplerfield` collection, return the columns: `sdssg`, `sdssr`, `sdssi`,
`propermotion` and return only objects that match the condition: `(sdssr < 13.0)
and (propermotion > 50.0)`:

<figure class="figure">
  <img src="/server-static/lcc-server-conesearch-example2.png"
       class="figure-img img-fluid"
       alt="Cone search query example 2 input">
  <figcaption class="figure-caption text-center">
    Cone search query example 2 input
  </figcaption>
</figure>


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
