## Notes on checkplotserver integration

- in lcc-server: add a kwarg to enable this mode: --editable-objects=1
- in lcc-server: eventually add permissions to this once we have the user-auth
  backend up and running

- in checkplotserver: implement a special handler that loads a single checkplot
  provided as an HTTP GET request arg into the usual checkplotserver view
  (instead of returning JSON like --standalone=1)
- in checkplotserver: implement another special handler that can load lists of
  checkplots provided in an HTTP POST request into the usual checkplotserver
  view (to edit all objects in a dataset for example)
- in checkplotserver: we'll need to finish support for saving checkplots after
  running period-finders, etc. from the frontend

This will let us use lcc-server as a frontend to a full variability and
period-finding pipeline, starting just from the lcc-server CLI (which can now
make checkplot pickles if users ask for it).
