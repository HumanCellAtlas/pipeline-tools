This directory contains pipelines used by the Secondary Analysis Service to interface with the Human Cell Atlas Data Storage Service and Ingestion Service.

# submit.wdl

This pipeline is used to create analysis bundles. It talks to the Ingestion Service, which in turn creates bundles in the Data Storage Service.

# submit_example.json

Example inputs for running submit.wdl.

# format_map_example.json

An example format map for use with submit.wdl, which requires a format map as an input.

# 10x

Directory containing DCP adapter for 10x pipeline.

# smart_seq2

Directory containing DCP adapter for Smart-seq2 pipeline.
