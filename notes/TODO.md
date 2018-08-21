## Short term TODO

- HTTP API for plotting unphased, phased light curves on demand with applied
  filters on columns, etc.

- HTTP API for generating light curve collection footprint given a survey
  mosaic; generated datasets can then be footprint aware

- HTTP API for generating stamps from a footprint mosaic for each object if one
  is provided, and from DSS by default.

- access control to all data based on users and groups, HTTP API access via key,
  user and group definition, etc.

## Daydreaming about future stuff

Future functionality will include:

- federation APIs so multiple lcc-servers can appear in a single portal. this
  will involve metadata tagging for bandpass, sky footprint, time coverage,
  etc., sharing data in a global backing database so if nodes go offline, they
  can recover from other nodes

- public classification interfaces for periodic variable classification, a rich
  exploration interface built on web-GL

- extension to transient time-domain surveys

- streaming data ingest and alert system for transients and other high cadence
  phenomena

- collaboration tools, including object comments across federated datasets,
  activity streams, and streaming status updates for objects

- serving of calibrated FITS image stamps per object per epoch of any
  time-series, so people can run photometry on their own

- adding in VO TAP query services

- adding in automatic parallelization using cloud services
