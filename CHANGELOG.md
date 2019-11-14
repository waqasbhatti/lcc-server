# v0.2.7

## Fixes

- Unpinned required scipy version.
- Fixed messaging for case when there aren't 100 recent datasets to show.

# v0.2.6

## Fixes

- Now correctly handles a failed sign-up request if the entered password doesn't
  meet the required conditions on complexity.

# v0.2.5

## New stuff

- The foreground query timeout, LC zip process timeout, the maximum number of
  dataset rows allowed to generate an LC zip for, and the number of rows per
  page for a dataset are all now configurable via the `site-info.json`
  file. [Here is the updated
  base](https://github.com/waqasbhatti/lcc-server/blob/5081181d8a26ae96e3133f068b9298be158ad19f/lccserver/cli.py#L207-L221)
  of the `site-info.json` file that is copied over when the `lcc-server` CLI is
  used to `init` a project base directory. Add the new keys to your own
  `site-info.json` file to activate these configurable parameters.

## Changes

- Authnzerver: user login now requires `email_verified` AND `is_active` status
  as opposed to just `is_active` status previously.
- Frontend: Some fixes to the messaging when an asychronous query is still
  running when the timeout expires.

## Fixes

- Fixed several test failures.
- Authnzerver: correctly delete all user cookies and all associated session
  entries when a user is deleted.
- Admin: additional guards against escalation of privilege when editing user
  info.

# v0.2.4

## Fixes

- Fixed a possible directory traversal when looking up docs pages.


# v0.2.3

## Fixes

- Remove some unused functions from `authnzerver.actions.user`.
- Frontend: typo fix: `noreferer` -> `noreferrer` for generated `_target=blank`
  links.


# v0.2.2

## Fixes

- Add in missing LC format JSONs to the package manifest.


# v0.2.1

This is a major new release. The format of various databases used by LCC-Server
has changed, so it's not backward compatible with collections being served by
the v0.1 series of releases. Use the `lcc-server` CLI tool to re-import your
light curves, checkplots, and period-finding results. The format of the output
LCC-CSV light curve files produced has **not** changed, so if you have these
lying around, you do not need to regenerate them.

## New stuff

- New user account functionality, allowing users to have private or unlisted
  search result datasets, per-user access privileges and rate-limits, and the
  ability to be emailed when long-running queries finish. API keys can be
  generated for each user on their user home page, allowing access to objects
  and datasets marked as private. This functionality requires a working email
  server. If one is not provided, user sign-ins and sign-ups will be disabled.
- All search services can now accept optional limits on the rows returned, can
  randomly sample search result rows, and can sort result rows by any database
  column.
- Imported light curve collections now add to a footprint map that is displayed
  on the new **Collections** tab of the interface. The list of LC collections on
  this tab now includes links to directly search each collection, a summary of
  each LC collection's items, and links to explore the collection using some
  basic search queries.
- The full-text search service can now look up objects in SIMBAD by name using
  the SESAME resolver. This also enables looking up objects in LC collections
  that are associated with star clusters, nebulae, or other extended sources.
- The dataset browser is now paginated for large datasets. Per-object
  information pop-ups now include variability index information and GAIA
  neighbor information.
- Datasets can be edited by logged-in users. Their name, dataset URL,
  description, and citations can be changed. For staff and superuser-level
  users, dataset ownership can be changed.
- LCC-Server users can be managed by superuser-level user accounts. These users
  can also edit LCC-Server site settings and email server settings.
- Logged-in LCC-Server users can launch SIMBAD queries for individual objects
  when browsing them in the dataset view if these results of these queries
  weren't in the original checkplots ingested by the LCC-Server.

## Changes

- Dataset access now uses visibilities: **private**, **unlisted**, **public**,
  instead of just **private** and **public** in v0.1.
- All search service endpoints now only accept POST requests. An API key is thus
  needed if you want to call these non-interactively. Per-user API keys are
  required used to access datasets and objects that have been marked as
  **private**.
- New API options have been added for most search query services to enable the
  new result random-sample, sorting, and limiting functionality.
- Light curve ZIP files and dataset CSVs that are associated with **private**
  datasets will no longer be visible or accessible to users other than the one
  that generated them (via the interface or a user-tied API key).
- All requests are now rate-limited based on user privilege level. All search
  query services now check user access and privilege level and will not return
  result rows that are marked as **private** or go over the user's row limit.
- The `lcc-server` CLI can now run period-finding on an imported LC collection
  in addition to checkplot making. Choosing this option will incorporate phased
  light curves into the per-object information displayed by the LCC-Server.
- The `lcc-server` CLI will ask for administrative user credentials when it is
  used to set up a new `basedir`. This will generate the first superuser level
  user account that can be used to manage server settings, users, and edit
  datasets.

## Fixes

Many bug fixes all over the place. See the commit log for details.


# v0.1.4

## New stuff

- paths in `lcformat-description.json` can now point to the current collection
directory and the user's home directory by using the `{{collection_dir}}` and
`{{home_dir}}` shortcuts.

- added example systemd unit files.

- `dbsearch`: can now redact search arguments from search results.

## Fixes

- `searchserver_handlers`: better messaging if a cone search doesn't return
  anything in any LC collection.

- templates: remove broken page load autofocus.

- `indexserver`: fix typo in parsing `cpaddr` command line argument.


# v0.1.3

## New stuff

- added `edit-collection` lcc-server CLI command.

## Fixes

- fixed typo: `abs_gaiamag` should have been `gaia_absmag`. These will now be
  extracted correctly from the checkplot pickles if present.


# v0.1.2

## Fixes

- cli, backend: fixed operations that were actually broken, but were working
  because we weren't testing them right.

## Changes:

- docs: removed useless markdown version of Jupyter notebook walk-through.


# v0.1.1

## Fixes

- README: use absolute links since we now use README.md at PyPi as well.


# v0.1.0

- Initial release.
