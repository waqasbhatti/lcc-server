This is for the NSF CSSI program:
https://www.nsf.gov/pubs/2018/nsf18531/nsf18531.htm

Deadline: April 18, 2018
CSSIQueries@nsf.gov
Nigel A. Sharp, Program Director, MPS/AST, telephone: (703) 292-4905

# NSF INFO

Proposers are asked to identify whether their proposal is an "Elements" or
"Framework" proposal in the proposal title. Proposers are also asked to identify
whether the proposal is a “Data” proposal or a “Software” proposal within the
title, based on whether the proposed cyberinfrastructure will primarily support
data-driven research or software-driven research. OAC recognizes that proposal
submissions will span a continuum of cyberinfrastructure
possibilities. Therefore, researchers with questions as to how their proposal
should be classified should not hesitate to contact the cognizant program
officers listed in this solicitation.

## Competitiveness

A competitive proposal will:

Identify science and engineering challenges where the proposed
cyberinfrastructure enables fundamental new science and engineering advances,
and describe how the proposed project fosters partnerships and community
development that will have a significant impact on science and engineering
research;

Indicate how the proposed cyberinfrastructure builds capability, capacity and
cohesiveness of a national CI ecosystem; and

Provide a compelling discussion of the cyberinfrastructure’s potential use by a
wider audience and its contribution to a national cyberinfrastructure.

## Award Info

The Division of Astronomical Sciences (AST) is interested in proposals to
support the development and dissemination of sustainable software and tools for
data handling and computational activities that enable progress on key questions
in astronomy and astrophysics.

Elements awards shall not exceed a total of $600,000 and 3 years duration.

# Proposal Elements

## Overview (1-page)

The overview includes a summary description of the project, the innovative
infrastructure being proposed, its research and education goals, and the
community (communities) that will be impacted. The statement on intellectual
merit should describe the potential of the proposed infrastructure to advance
knowledge. The statement on broader impacts should describe the potential of the
proposed activity to benefit society and contribute to the achievement of
specific, desired societal outcomes. The Project Summary should be written in
the third person, informative to other persons working in the same or related
fields, and, insofar as possible, understandable to a scientifically- or
technically-literate lay reader. It should not be an abstract of the proposal.

## Project Description (15-pages)

The Project Description should define an agenda that will lead to sustainable
software and data cyberinfrastructure capable of enabling transformative,
robust, and reliable science and engineering. In addition to the guidance
specified in the PAPPG, the Project Description should explicitly address the
following items:

*Science-driven:* How will the project outcomes fill well-recognized science and
engineering needs of the research community, and advance research capability
within a significant area or areas of science and engineering? What are the
broader impacts of the project, such as benefits to science and engineering
communities beyond initial targets, under-represented communities, and education
and workforce development? The Project Description should provide a compelling
discussion of the potential to benefit its intended as well as broader
communities.

*Innovation:* What innovative and transformational capabilities will the project
bring to its target communities, and how will the project integrate innovation
and discovery into the project activities, such as through empirical research
embedded as an integral component of the project activities? Such research might
encompass reproducibility, provenance, effectiveness, usability, and adoption of
the components, adaptability to new technologies and to changing requirements,
and the development of lifecycle processes used in the project.

*Close collaborations among stakeholders:* How will the project activities
engage CI experts, specialists, and scientists and engineers working in concert
with the relevant domain scientists and engineers who are users of CI?

*Building on existing, recognized capabilities:* How will the project activities
build on and leverage existing NSF and national cyberinfrastructure investments,
as appropriate?

*Project plans, and system and process architecture:* The Project Description
should include high-quality management plans. The proposal should include user
interactions and a community-driven approach, and provide a timeline including a
proof-of-concept demonstration of the key components. The proposal must include
a list of tangible metrics to be used to measure the success of the project
activities, and describe how progress will be measured along the way. If the
outcome of the project is software or data cyberinfrastructure, the architecture
of the CI and the engineering process to be used for the design, development,
documentation, testing, validation, and release of the software, its deployment
and associated outreach to the end user community, and an acceptance and
evaluation plan that involves end users, all must be sufficiently described. The
description of the CI architecture and processes should explain how security,
trustworthiness, provenance, reproducibility, and usability will be addressed by
the project and integrated into the proposed system and the engineering process,
and how adaptability to new technologies and changing requirements will be
addressed by the project and built into the proposed system, as appropriate.

*Sustained and sustainable impacts:* The Project Description should address how
the project outcomes and its activities will have long-term impacts, and how
these will be sustained beyond the lifetime of the award, as appropriate. If the
outcome of the project is software or data cyberinfrastructure, the proposal
should identify what license will be used for the released CI, and why this
license has been chosen. PIs who have been previously funded under previous CI
awards should show quantifiable evidence of the use, impact and sustainability
of the previously funded work (and include a citation to the published CI in
their biographical sketches as one of their relevant products, if appropriate).

*Broader impacts:* Broader impacts may be accomplished through the research
itself, through the activities that are directly related to specific research
projects, or through activities that are supported by, but are complementary to,
the project. NSF values the advancement of scientific knowledge and activities
that contribute to achievement of societally relevant outcomes. Such outcomes
include, but are not limited to: full participation of women, persons with
disabilities, and underrepresented minorities in science, technology,
engineering, and mathematics (STEM); improved STEM education and educator
development at any level; increased public scientific literacy and public
engagement with science and technology; improved well-being of individuals in
society; development of a diverse, globally competitive STEM workforce;
increased partnerships between academia, industry, and others; improved national
security; increased economic competitiveness of the United States; and enhanced
infrastructure for research and education.

# IDEAS

## LCC-Server: A light curve collection server framework

- astrobase evolution into a collaborative variable star classification
  framework that allows data downloads, etc. -> connections to citizen science.

This is a Python framework to serve collections of light curves. It includes the
following functionality that we think is a minimum requirement for any light
curve collection service:

- collection of light curves into a single format

- HTTP API for searching over a light curve collection by:
  - filtering on any light curve column and object properties, e.g. objectid,
    mag, variability type, periods, etc.
  - cone-search over coordinates
  - cross-matching to uploaded object list with objectid, ra, decl

- HTTP API for generating datasets from search results asychronously, caching
  results from searches, and generating output zip bundles containing search
  results and all matching light curves

- HTTP API for accessing various data releases and versions of the light curves

- HTTP API for generating light curves on demand in several formats from the
  collected light curves

- HTTP API for plotting unphased, phased light curves on demand with applied
  filters on columns, etc.

- HTTP API for generating light curve collection footprint given a survey
  mosaic; generated datasets can then be footprint aware

- HTTP API for generating stamps from a footprint mosaic for each object if one
  is provided, and from DSS by default.

- access control to all data based on users and groups, HTTP API access via key,
  user and group definition, etc.

This framework forms the basis for the [HAT data
server](https://data.hatsurveys.org). It is meant to run with PostgreSQL and
Python 3 and should be able to scale to millions of objects.


### Notes

- add a conesearch server based on kdtree and pickles. this will run in
  memory. use the zones thingie to split by declination and query region
  expanding like in that Tamas Budavari paper. this way, we'll only store easy
  stuff in the SQL server, so we can use sqlite if needed.

- add asynchronous result stuff based on HTTP 303 See other like Gaia ADQL. now
  that we know how to run stuff in tornado asynchronously using the
  ProcessPoolExecutors, we probably don't need any stupid message queue nonsense

- add support for search backends: (1) pickles only + SQlite or (2) pickles and
  Postgres


### Stuff that will go in a proposal

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


Significance:

- enable publication of previously unearthed light curves

- share independent reductions of the same dataset; important for TESS with
  reductions via aperture phot vs image sub phot, etc.
