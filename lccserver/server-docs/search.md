This page describes how to use the search functions available on the LCC-Server.

[TOC]

## Light curve collections

The LCC-Server operates on collections of light curves. To see an overview of
the collections available, click on the **Collections** tab:

<figure class="figure">
  <img src="/server-static/lcc-server-search-collections.png"
       class="figure-img img-fluid"
       alt="The LCC server collections overview">
  <figcaption class="figure-caption text-center">
    The LCC server collections overview
  </figcaption>
</figure>

Clicking on a collection's name in the overview map will bring up its entry in
the collection information list on the right side of the page. Clicking on any
of the headers in this entry will expand the selected entry section. The
**Search this collection** links will bring you to the appropriate search
service page tab with this collection loaded and ready to go. The **Explore this
collection** links will launch searches that find objects in the collection
based on their variability and other criteria.

## Searching for objects

Clicking on any of the search service tabs will bring you to the search
interface. Each of the services have their own detailed documentation laying out
their input parameters:

- [cone search around specified coordinates](/docs/conesearch)
- [full-text search for object names and descriptions](/docs/ftsearch)
- [search based on column filters and sort conditions](/docs/columnsearch)
- [upload objects to match against available collections](/docs/xmatch)

An example using the **conesearch** service is shown below:

<figure class="figure">
  <img src="/server-static/lcc-server-search-overview.png"
       class="figure-img img-fluid"
       alt="The LCC server search interface">
  <figcaption class="figure-caption text-center">
    The LCC server search interface
  </figcaption>
</figure>

All search services follow the same basic scheme:

1. Choose light curve collections to search in. This step is optional; the LCC
   server will search all collections by default. To see the available
   collections and descriptions of their database columns, click on the
   <strong>Collections</strong> tab.
2. Choose columns to retrieve. Object names, RA, Dec, and light curve download
   links for all matched objects will be returned by default so there is no need
   to specify them here.
3. Add database column filters on the matched objects. This step is
   optional.
4. Choose the visibility of the [search result](#search-results). This step is
   optional.
5. Type in the query coordinates or text as appropriate.

Hitting the search button will start the query immediately. All queries run
asynchronously and you will be notified of the progress of and results from your
query as it progresses through the execution stages. Clicking on the
<strong>Queries</strong> tab will show the status of your query:

<figure class="figure">
  <img src="/server-static/lcc-server-search-query-status.png"
       class="figure-img img-fluid"
       alt="The query status tab">
  <figcaption class="figure-caption text-center">
    The query status tab
  </figcaption>
</figure>

## Search results

All queries produce [datasets](/docs/datasets) for their search
results. By default, all datasets are **unlisted**, do not show up in the
<strong>Datasets</strong> tab for any user other than yourself, but are
accessible by other people if they know the dataset URL.

<figure class="figure">
  <img src="/server-static/lcc-server-search-recent-datasets.png"
       class="figure-img img-fluid" alt="The recent datasets tab">
  <figcaption class="figure-caption text-center">
    The recent datasets tab
  </figcaption>
</figure>

Other possible dataset visibility states are **private** and **public**. Setting
the dataset visibility to **private** ensures that you alone can access the
dataset and its contents. Other users will not be able to access the dataset URL
or download the dataset products. Setting the dataset visibility to **public**
will allow the dataset to show up in the list of public datasets on the
**Datasets** tab and on the [datasets page]({{ server_url }}/datasets).
