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
make checkplot pickles if users ask for it):

- if multiple objects are sent to the cpserver, we'll have a modal pop-up asking
  if we can sort the objects in order of decreasing variability index (either
  stetsonj or 1.0/eta_normal) before proceeding to the checkplotserver view
- this way, we can try the following neat trick:
  - once the user has classified enough objects (let's say 10% of the objects in
    the view), we can ask if they want to use their labels as input to
    astrobase's RF classifier.
  - this will run astrobase.varclass.period_features to generate periodic light
    curve features.
  - this will then run the classifier for the rest of the objects in the view
    using a specified test-train split
