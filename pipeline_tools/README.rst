Overview
========

.. image:: https://travis-ci.org/HumanCellAtlas/pipeline-tools.svg?branch=master
    :target: https://travis-ci.org/HumanCellAtlas/pipeline-tools

.. image:: https://readthedocs.org/projects/pipeline-tools/badge/?version=latest
    :target: http://pipeline-tools.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. note::
    This tool is still under active development, so there could be significant changes to its API.

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

Install it like this:

.. code::

    pip install git+git://github.com/HumanCellAtlas/pipeline-tools.git

You can use the cloud storage functions like this:

.. code:: python

    import pipeline_tools
    from pipeline_tools import gcs_utils
    bucket, object = gcs_utils.parse_bucket_blob_from_gs_link('gs://my-bucket/path/to/object')

The rest of the package consists of scripts that are meant to be invoked from the command line as described below.

Usage
=====
get_analysis_workflow_metadata.py
------------------------
Utility function fetches required information for creating submission to Ingest service, such as the Cromwell workflow
metadata, the UUID of the analysis workflow, and the version of the given pipeline.

Invoke it like this:

.. code::

    get-analysis-workflow-metadata \
      --analysis_output_path ${analysis_output_path} \
      --cromwell_url ${cromwell_url} \
      --use_caas ${use_caas} \
      --caas_key_file ${caas_key_file}

All arguments are required, except the `--caas_key_file`, which is set to `None` by default.

create_analysis_metadata.py
---------------------------
Creates both analysis_protocol.json and analysis_process.json files, which are following different versions of the
HCA metadata schema. For a full list of the HCA metadata schemas, check `here <https://schema.humancellatlas.org/type>`_.

Invoke it like this:

.. code::

    create-analysis-metadata \
      --analysis_id ${workflow_id} \
      --metadata_json ${metadata_json} \
      --input_bundles ${input_bundle_uuid} \
      --reference_bundle ${reference_bundle} \
      --run_type ${run_type} \
      --method ${method} \
      --schema_url ${schema_url} \
      --analysis_process_schema_version ${analysis_process_schema_version} \
      --analysis_protocol_schema_version ${analysis_protocol_schema_version} \
      --pipeline_version ${pipeline_version} \
      --analysis_file_version ${analysis_file_version} \
      --inputs_file ${write_objects(inputs)} \
      --outputs_file ${write_lines(outputs)} \
      --format_map ${format_map}

All arguments are required.

create_envelope.py
------------------
Creates submission envelope and uploads metadata files.

Invoke it like this:

.. code::

    create-envelope \
      --submit_url ${submit_url} \
      --analysis_process_path analysis_process.json \
      --analysis_protocol_path analysis_protocol.json \
      --schema_url ${schema_url} \
      --analysis_file_version ${analysis_file_version}

All arguments are required.

get_staging_urn.py
------------------
Obtains URN needed for staging files. Queries ingest API until URN is available.
The URN (Uniform Resource Name) is a long string that looks like this:
hca:sta:aws:staging:{short hash}:{long hash}

It gets decoded by the `hca-cli <https://github.com/HumanCellAtlas/dcp-cli>`_ to extract the staging location and credentials
needed to stage files.

Invoke it like this:

.. code::

    get-staging-urn \
      --envelope_url ${submission_url} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds} > submission_urn.txt

envelope_url is required

get_files_to_upload.py
----------------------
Gets a list of files to be uploaded(staged) by the HCA-CLI, writes the list to disk.

Invoke it like this:

.. code::

    get-files-to-upload \
        --files ${sep=' ' files} \
        --uploaded_files $uploaded_files

Both arguments are required.

confirm_submission.py
---------------------
Confirms submission. This causes the ingest service to finalize the submission and create a bundle in the storage service.

Waits until submission status is "Valid", since submission cannot be confirmed until then.

Invoke it like this:

.. code::

    confirm-submission \
      --envelope_url ${submission_url} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds}

envelope_url is required


Testing
=======

Running unit tests
------------------

To run unit tests, first create a virtual environment with the requirements:

.. code::

    virtualenv test-env
    source test-env/bin/activate
    pip install -r requirements.txt -r test-requirements.txt

Then, run unit tests from the root of the pipeline-tools repo like this:

.. code::

    bash test.sh

To run schema integration tests, do:

.. code::

    export TEST_SUITE="latest_schema"
    bash test.sh
