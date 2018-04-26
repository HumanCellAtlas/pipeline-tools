Overview
========
This package provides utilities for retrieving files from the data storage service for the Human Cell Atlas, and for
submitting an analysis bundle to the Human Cell Atlas Data Coordination Platform.

It also contains functions for interacting with Google Cloud Storage.

The steps in the submission process are as follows:

* Create analysis.json
* Create submission envelope and upload metadata
* Get URN needed to stage files
* Stage files by using the `hca-cli <https://github.com/HumanCellAtlas/dcp-cli>`_
* Confirm submission


Installing
==========

Install it like this::

    pip install git+git://github.com/HumanCellAtlas/pipeline-tools.git

You can use the cloud storage functions like this::

    import pipeline_tools
    from pipeline_tools import gcs_utils
    bucket, object = gcs_utils.parse_bucket_blob_from_gs_link('gs://my-bucket/path/to/object')

The rest of the package consists of scripts that are meant to be invoked from the command line as described below.


Running unit tests
==================

To run unit tests, first create a virtual environment with the requirements::

    virtualenv test-env
    source test-env/bin/activate
    pip install -r requirements.txt -r test-requirements.txt

Then, run unit tests from the root of the pipeline-tools repo like this::

    bash test.sh

To run schema integration tests, do::

    export TEST_SUITE="latest_schema"
    bash test.sh

create_analysis_json.py
=======================
Creates analysis.json file, which is following the v4 metadata schema.

Invoke it like this::

    create-analysis-json \
      --analysis_id ${workflow_id} \
      --metadata_json ${metadata_json} \
      --input_bundles ${bundle_uuid} \
      --reference_bundle ${reference_bundle} \
      --run_type ${run_type} \
      --method ${method} \
      --schema_version ${schema_version} \
      --inputs_file ${write_objects(inputs)} \
      --outputs_file ${write_lines(outputs)} \
      --format_map ${format_map}

All arguments are required.

create_envelope.py
==================
Creates submission envelope and uploads metadata.

Invoke it like this::

    create-envelope \  
      --submit_url ${submit_url} \
      --analysis_json_path analysis.json

Both arguments are required.

get_staging_urn.py
==================
Obtains URN needed for staging files. Queries ingest API until URN is available.
The URN (Uniform Resource Name) is a long string that looks like this:
hca:sta:aws:staging:{short hash}:{long hash}

It gets decoded by the `hca-cli <https://github.com/HumanCellAtlas/dcp-cli>`_ to extract the staging location and credentials
needed to stage files.

Invoke it like this::

    get-staging-urn \
      --envelope_url ${submission_url} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds} > submission_urn.txt

envelope_url is required

confirm_submission.py
=====================
Confirms submission. This causes the ingest service to finalize the submission and create a bundle in the storage service.

Waits until submission status is "Valid", since submission cannot be confirmed until then.

Invoke it like this::

    confirm-submission \
      --envelope_url ${submission_url} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds}

envelope_url is required
