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
