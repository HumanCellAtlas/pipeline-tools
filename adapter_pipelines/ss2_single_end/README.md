# Overview

This directory contains an adapter pipeline used by the Secondary Analysis Service to run the SmartSeq2SingleSample pipeline on single-end data.

# Files

* adapter.wdl
The adapter pipeline, which parses a bundle manifest from the Data Storage Service, runs the Smart-seq2 analysis pipeline, then runs the submission pipeline to submit the results to the Ingest Service.

* adapter_example_static.json and adapter_example_bundle_specific.json
Example inputs to use when running these pipelines, split into two files, one for inputs that vary from bundle to bundle, and the other for inputs that stay the same for every run.

* adapter_example_static_demo.json
A smaller set of static (reference) inputs that run faster and can be used for demonstration and testing purposes.

* dependencies.json
The locations of the wdls imported by the SmartSeq2Singlesample workflow

* options.json
Options file to use when running workflow.
