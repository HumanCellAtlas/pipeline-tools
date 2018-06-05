# Get Cromwell metadata for the workflow that produced the given output
task get_metadata {
  String analysis_output_path
  String runtime_environment
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean use_caas
  Boolean record_http

  command <<<
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"
    touch request_000.txt && touch response_000.txt

    # Force the binary layer of the stdout and stderr streams (which is available as their buffer attribute)
    # to be unbuffered. This is the same as "-u", more info: https://docs.python.org/3/using/cmdline.html#cmdoption-u
    export PYTHONUNBUFFERED=TRUE

    get-analysis-metadata \
      --analysis_output_path ${analysis_output_path} \
      --runtime_environment ${runtime_environment} \
      --use_caas ${use_caas}
  >>>
  runtime {
    docker: "gcr.io/broad-dsde-mint-${runtime_environment}/cromwell-metadata:v0.19.0"
  }
  output {
    File metadata = "metadata.json"
    String workflow_id = read_string("workflow_id.txt")
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
  }
}

# Create the submission object in ingest
task create_submission {
  String workflow_id
  File metadata_json
  String input_bundle_uuid
  String reference_bundle
  String run_type
  String method
  String schema_version
  Array[Object] inputs
  Array[String] outputs
  File format_map
  String submit_url
  Float? retry_multiplier
  Int? retry_max_interval
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean record_http

  command <<<
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"
    touch request_000.txt && touch response_000.txt

    # Force the binary layer of the stdout and stderr streams (which is available as their buffer attribute)
    # to be unbuffered. This is the same as "-u", more info: https://docs.python.org/3/using/cmdline.html#cmdoption-u
    export PYTHONUNBUFFERED=TRUE

    # First, create the analysis.json
    # Note that create-analysis-json can take a comma-separated list of bundles,
    # but current workflows only take a single input bundle
    create-analysis-json \
      --analysis_id ${workflow_id} \
      --metadata_json ${metadata_json} \
      --input_bundles ${input_bundle_uuid} \
      --reference_bundle ${reference_bundle} \
      --run_type ${run_type} \
      --method ${method} \
      --schema_version ${schema_version} \
      --inputs_file ${write_objects(inputs)} \
      --outputs_file ${write_lines(outputs)} \
      --format_map ${format_map}

    # Now create the submission object
    create-envelope \
      --submit_url ${submit_url} \
      --analysis_json_path analysis.json \
      --schema_version ${schema_version}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:se-test-v6-changes"
  }
  output {
    File analysis_json = "analysis.json"
    String submission_url = read_string("submission_url.txt")
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
  }
}

# Stage files for the submission
task stage_files {
  String submission_url
  Array[File] files
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  # These left and right brace definitions are a workaround so Cromwell won't
  # interpret the bash array reference below as a WDL variable.
  String lb = "{"
  String rb = "}"
  Boolean record_http

  command <<<
    set -e
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"
    touch request_000.txt && touch response_000.txt

    # Force the binary layer of the stdout and stderr streams (which is available as their buffer attribute)
    # to be unbuffered. This is the same as "-u", more info: https://docs.python.org/3/using/cmdline.html#cmdoption-u
    export PYTHONUNBUFFERED=TRUE

    # Get the urn needed for staging files
    staging_urn=$(get-staging-urn --envelope_url ${submission_url})

    # Select staging area
    echo "hca upload select $staging_urn"
    hca upload select $staging_urn

    # Stage the files
    files=( ${sep=' ' files} )
    for f in "$${lb}files[@]${rb}"
    do
      echo "hca upload file $f"
      hca upload file $f
    done
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:se-test-v6-changes"
  }
  output {
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
    # Make this task output a placeholder Bool here for confirm_submission so that they can be executed in order
    Boolean ready_to_be_confirmed = true
  }
}

# Confirm the submission
task confirm_submission {
  # Make this task ask for a placeholder Bool here for so that stage_files task and this can be executed in order
  Boolean ready_to_be_confirmed
  String submission_url
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean record_http

  command <<<
    set -e
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"
    touch request_000.txt && touch response_000.txt

    # Force the binary layer of the stdout and stderr streams (which is available as their buffer attribute)
    # to be unbuffered. This is the same as "-u", more info: https://docs.python.org/3/using/cmdline.html#cmdoption-u
    export PYTHONUNBUFFERED=TRUE

    # Confirm the submission
    confirm-submission --envelope_url ${submission_url}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:se-test-v6-changes"
  }

  output {
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
  }
}

workflow submit {
  Array[Object] inputs
  Array[File] outputs
  File format_map
  String submit_url
  String input_bundle_uuid
  String reference_bundle
  String run_type
  String schema_version
  String method
  String runtime_environment
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean use_caas
  # By default, don't record http requests
  Boolean record_http = false

  call get_metadata {
    input:
      analysis_output_path = outputs[0],
      runtime_environment = runtime_environment,
      use_caas=use_caas,
      record_http = record_http,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval
  }

  call create_submission {
    input:
      reference_bundle = reference_bundle,
      run_type = run_type,
      schema_version = schema_version,
      method = method,
      submit_url = submit_url,
      inputs = inputs,
      outputs = outputs,
      format_map = format_map,
      metadata_json = get_metadata.metadata,
      input_bundle_uuid = input_bundle_uuid,
      workflow_id = get_metadata.workflow_id,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http
  }

  call stage_files {
    input:
      submission_url = create_submission.submission_url,
      files = outputs,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http
  }

  call confirm_submission {
    input:
      ready_to_be_confirmed = stage_files.ready_to_be_confirmed,
      submission_url = create_submission.submission_url,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http
  }

  output {
    File analysis_json = create_submission.analysis_json
    Array[File] create_envelope_requests = create_submission.http_requests
    Array[File] create_envelope_responses = create_submission.http_responses
    Array[File] stage_and_confirm_requests = stage_files.http_requests
    Array[File] stage_and_confirm_responses = stage_files.http_responses
    Array[File] confirm_submission_requests = confirm_submission.http_requests
    Array[File] confirm_submission_responses = confirm_submission.http_responses
  }
}
