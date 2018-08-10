This page describes how to use the cone search query service of the LCC server
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

The cone-search query service requires a central coordinate specification
string, along with an optional search radius in arcminutes, using any of the
formats below.

```
HH:MM:SS[.ssss] [+-]DD:MM:SS.ssss [DD.dddd]
HH MM SS[.ssss] [+-]DD MM SS.ssss [DD.dddd]
DDD[.dddd] [+-]DDD[.ddd] [DD.ddd]
```

The maximum search radius is 60 arcminutes or 1 degree.

The cone-search service also takes several optional inputs. These include:

1. **Collection to search in:** specified using the select control in the
   top-left portion of the cone-search tab. Multiple collections can be selected
   by holding <kbd>Ctrl</kbd> (Linux/Windows) or <kbd>Cmd</kbd> (MacOS) when
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
   selecting them in this control, holding <kbd>Ctrl</kbd> (Linux/Windows) or
   <kbd>Cmd</kbd> (MacOS) when clicking on the options to select multiple
   columns.

3. **Filters on database columns:** specified using the controls just under the
   main coordinates input box. First, choose a column to filter on using the
   left select control, then choose the operator to use from the middle select
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
took too long, it will be sent to a background queue. In either case, you will
be informed of the permanent URL where the results of the query will appear as a
*dataset*. From the dataset page associated with this query, you can view and
download a data table CSV file generated based on the columns specified. You may
also download light curves (individually or in a collected ZIP file) of all
matching objects.

### Examples

**Example 1:** Search for any objects within 15 arcminutes of the coordinates
(&alpha;, &delta;) = (290.0,45.0):

<figure class="figure">
  <img src="/server-static/lcc-server-conesearch-example1.png"
       class="figure-img img-fluid"
       alt="Cone search query example 1 input">
  <figcaption class="figure-caption text-center">
    Cone search query example 1 input
  </figcaption>
</figure>

**Example 2:** Search for objects within 60 arcminutes of the coordinates
(&alpha;, &delta;) = (290.0,45.0), but restrict the query to just the
`hatnet_keplerfield` collection, return the columns: `sdssg`, `sdssr`, `sdssi`,
`propermotion`, and return only objects that match the condition: `(sdssr < 13.0)
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

The cone search query service accepts HTTP requests to its endpoint:

```
GET {{ server_url }}/api/conesearch
```

See the general [API page](/docs/api) for how to handle the responses from the query service.

### Parameters

The search query arguments are encoded as URL query string parameters as
described in the table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`coords`           | **yes**  |         | The center coordinates and search radius as specified previously. These can be in either decimal or sexagesimal format. The search radius is in arcminutes and is optional.
`result_ispublic`  | **no**   | `1`     | `1` means the resulting dataset will be public and visible on the [Recent Datasets](/datasets) page. `0` means the resulting dataset will only be accessible to people who know its URL.
`collections[]`      | **no**   | `null`  | Collections to search in. Specify this multiple times to indicate multiple collections to search. If this is null, all collections will be searched.
`columns[]`          | **no**   | `null`  | Columns to retrieve. Columns used for filtering and sorting are returned automatically so there's no need to specify them here. The database object names, right ascensions, and declinations are returned automatically as well.
`filters`          | **no**   | `null`  | Filters to apply to the objects found. This is a string in SQL format specifying the columns and operators to use to filter the results. You will have to use special codes for mathematical operators since non-text symbols are automatically stripped from the query input:<br>&lt; &rarr; `lt`<br> &gt; &rarr; `gt`<br> &le; &rarr; `le`<br> &ge; &rarr; `ge`<br> = &rarr; `eq`<br> &ne; &rarr; `ne`<br> contains &rarr; `ct`


### Examples

Run the query from Example 1 above, using [HTTPie](https://httpie.org)[^1]:

```
$ http --stream GET {{ server_url }}/api/conesearch coords=='290.0 45.0 15.0' result_ispublic=='1'
```

Run the query from Example 2 above, using the Python
[Requests](http://docs.python-requests.org/en/master/)[^2] package:

```python
import requests, json

# build up the params
params = {'coords':'290.0 45.0 60.0',
          'collections[]':['hatnet_keplerfield'],
          'columns[]':['sdssg','sdssr','sdssi','propermotion'],
          'filters':'(sdssr lt 13.0) and (propermotion gt 50.0)',
          'result_ispublic':1}

# this is the URL to hit
url = '{{ server_url }}/api/conesearch'

# fire the request
resp = requests.get(url, params)
print(resp.status_code)

# parse the line-delimited JSON
textlines = resp.text.split('\n')[:-1]  # the last line is empty so remove it
jsonlines = [json.loads(x) for x in textlines]

# get the status and URL of the dataset created as a result of our query
# the last item in the list of JSON strings tells us what happened
query_status = jsonlines[-1]['status']
dataset_seturl = jsonlines[-1]['result']['seturl'] if query_status == 'ok' else None

if dataset_seturl:

    # can now get the dataset as JSON if needed
    resp = requests.get(dataset_seturl,{'json':1,'strformat':1})
    print(resp.status_code)

    # this is now a Python dict
    dataset = resp.json()

    # these are links to the dataset products - add {{ server_url }} to the front
    # of these to make a full URL that you can simply wget or use requests again.
    print(dataset['dataset_csv'])
    print(dataset['lczip'])

    # these are the columns of the dataset table
    print(dataset['columns'])

    # these are the rows of the dataset table (up to 3000 - the CSV contains everything)
    print(dataset['rows'])
```

[^1]: HTTPie is a friendlier alternative to the venerable `cURL`
program. See its [docs](https://httpie.org/doc#installation) for how to install
it.
[^2]: The Requests package makes HTTP requests from Python code a relatively simple
task. To install it, use `pip` or `conda`.
