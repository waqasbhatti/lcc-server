This page describes how to use the cross-matching search service of the LCC
server to upload a list of objects and coordinates, get matches to them within a
specified distance, and optionally filter the results by database column values.

[TOC]

## The web interface

<figure class="figure">
  <img src="/server-static/lcc-server-search-xmatch.png"
       class="figure-img img-fluid"
       alt="The cross-match query form">
  <figcaption class="figure-caption text-center">
    The cross-match query form
  </figcaption>
</figure>


### Input

### Examples


## The API

The cross-match service accepts HTTP requests to its endpoint:

```
POST {{ server_url }}/api/xmatch
```

### Parameters

The search query arguments are encoded as form parameters as described in the
table below.

Parameter          | Required | Default | Description
------------------ | -------- | ------- | -----------
`xmq`              | **yes**  |         | A string containing the object names, right ascensions, and declinations of the objects to cross-match to the collections on the LCC server. The object name, RA, Dec rows should be separated by line-feed characters: `\n`. Object coordinates can be in decimal or sexagesimal format. Object names should not have spaces, because these are used to separate object names, RA, and declination for each object row.
`xmd`              | **yes**  | 3.0     | The maximum distance in arcseconds to use for cross-matching. If multiple objects match to an input object within the match radius, duplicate rows for that object will be returned in the output containing all database matches sorted in increasing order of distance.
`result_ispublic`  | **no**   | `1`     | `1` means the resulting dataset will be public and visible on the [Recent Datasets](/datasets) page. `0` means the resulting dataset will only be accessible to people who know its URL.
`collections`      | **no**   | `null`  | Collections to search in. Specify this multiple times to indicate multiple collections to search. If this is null, all collections will be searched.
`columns`          | **no**   | `null`  | Columns to retrieve. Columns used for filtering and sorting are returned automatically so there's no need to specify them here. The database object names, right ascensions, and declinations are returned automatically as well.
`filters`          | **no**   | `null`  | Filters to apply to the objects found. This is a string in SQL format specifying the columns and operators to use to filter the results. You will have to use special codes for mathematical operators since non-text symbols are automatically stripped from the query input:<br>&lt; &rarr; `lt`<br> &gt; &rarr; `gt`<br> &le; &rarr; `le`<br> &ge; &rarr; `ge`<br> = &rarr; `eq`<br> &ne; &rarr; `ne`<br> contains &rarr; `ct`


### Examples

Let's run the query from the example above, using [HTTPie](https://httpie.org)[^1]. First, let's make a file of object names and coordinate rows. Let's call this `xmatch-upload.txt`:
```
# example object and coordinate list
# objectid ra dec
aaa 289.99698 44.99839
bbb 293.358 -23.206
ccc 294.197 +23.181
ddd 19 25 27.9129 +42 47 03.693
eee 19:25:27 -42:47:03.21
# .
# .
# .
# etc. lines starting with '#' will be ignored
# (max 5000 objects)
```

Next, get an API key if we don't have one:
```
$ http GET {{ server_url }}/api/key
```

Finally, call the actual service endpoint and load in the xmatch object rows from the file we created:
```
$ http --stream -f POST {{ server_url }}/api/xmatch Authorization:'Bearer [apikey]' xmq=@xmatch-upload.txt xmd='3.0' result_ispublic=1
```

The same workflow is perhaps more straightforward in Python with the [Requests](http://docs.python-requests.org/en/master/)[^2] package:

```python
import requests, json

# get the API key
apikey_info = requests.get('{{ server_url }}/api/key')
apikey = apikey_info.json()['result']['key']

# read in our object name and coordinates file
with open('xmatch-upload.txt','r') as infd:
    upload = infd.read()

# build up the params
params = {'xmq':upload,
          'xmd':3.0,
          'result_ispublic':1}

# this is the URL to hit
url = '{{ server_url }}/api/xmatch'

# fire the request
resp = requests.post(url, params, headers={'Authorization':'Bearer %s' % apikey})
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
