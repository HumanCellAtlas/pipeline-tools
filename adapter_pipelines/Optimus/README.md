# Overview

This directory contains an adapter pipeline used by the Secondary Analysis Service to run the Optimus pipeline

# Files

* adapter.wdl
The adapter pipeline, which parses a bundle manifest from the Data Storage Service, runs the Optimus analysis pipeline, then runs the submission pipeline to submit the results to the Ingest Service.

* adapter_example_bundle_specific.json and adapter_example_static.json
Example inputs to use when running these pipelines, split into two files, one for inputs that vary from bundle to bundle, and the other for inputs that stay the same for every run.

* options.json
Options file to use when running workflow.
