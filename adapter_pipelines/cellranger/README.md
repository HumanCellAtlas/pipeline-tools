# Overview

This directory contains an adapter pipeline used by the Secondary Analysis Service to run the CellRanger count pipeline

# Files

* adapter.wdl
The adapter pipeline, which parses a bundle manifest from the Data Storage Service, runs the CellRanger count analysis pipeline, then runs the submission pipeline to submit the results to the Ingest Service.

* adapter_example_static.json
Example inputs to use when running this pipeline that stay the same for every run.

* options.json
Options file to use when running workflow.
