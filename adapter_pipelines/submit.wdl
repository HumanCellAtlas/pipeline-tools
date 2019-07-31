# Get Cromwell metadata for the workflow that produced the given output
task get_metadata {
  String analysis_output_path
  String runtime_environment
  String cromwell_url
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean record_http
  String pipeline_tools_version

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

    get-analysis-workflow-metadata \
      --analysis_output_path ${analysis_output_path} \
      --cromwell_url ${cromwell_url}
  >>>
  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
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
  String pipeline_version
  File metadata_json
  String input_bundle_uuid
  String reference_bundle
  String run_type
  String method
  String schema_url
  String analysis_process_schema_version
  String analysis_protocol_schema_version
  String analysis_file_version
  Array[Object] inputs
  Array[String] outputs
  File format_map
  String submit_url
  Float? retry_multiplier
  Int? retry_max_interval
  Int? retry_timeout
  Int? individual_request_timeout
  Boolean record_http
  String pipeline_tools_version
  Boolean add_md5s
  String runtime_environment
  File service_account_key_path

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

    # First, create both analysis_process.json and analysis_protocol.json
    # Note that create-analysis-metadata can take a comma-separated list of bundles,
    # but current workflows only take a single input bundle
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
      --format_map ${format_map} \
      --add_md5s ${add_md5s}

    # Now build the submission object
    create-envelope \
      --submit_url ${submit_url} \
      --analysis_process_path analysis_process.json \
      --analysis_protocol_path analysis_protocol.json \
      --outputs_file_path outputs.json \
      --schema_url ${schema_url} \
      --analysis_file_version ${analysis_file_version} \
      --runtime_environment ${runtime_environment} \
      --service_account_key_path ${service_account_key_path}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
  }
  output {
    File analysis_process = "analysis_process.json"
    File analysis_protocol = "analysis_protocol.json"
    File outputs_files = "outputs.json"
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
  String pipeline_tools_version
  Int disk_space

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
    get-upload-urn -envelope_url ${submission_url} -output upload_urn.txt
    upload_urn=$(cat upload_urn.txt)

    # Select upload area
    echo "hca upload select $upload_urn"
    hca upload select $upload_urn

    # Check if any files were already uploaded to the staging area
    uploaded_files=$(hca upload list)

    if [ -z $uploaded_files ]; then
      unstaged_files=(${sep=' ' files})
    else
      echo "Checking which files to upload to the staging area..."
      get-files-to-upload \
        --files ${sep=' ' files} \
        --uploaded_files $uploaded_files
      unstaged_files=($(cat files.txt))
    fi
    # Stage the files
    for f in "$${lb}unstaged_files[@]${rb}"
    do
      echo "hca upload files $f"
      hca upload files $f
    done
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
    disks: "local-disk ${disk_space} HDD"
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
  String pipeline_tools_version
  String runtime_environment
  File service_account_key_path

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
    confirm-submission --envelope_url ${submission_url} \
      --runtime_environment ${runtime_environment} \
      --service_account_key_path ${service_account_key_path}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
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
  String schema_url
  String cromwell_url
  String analysis_process_schema_version
  String analysis_protocol_schema_version
  String analysis_file_version
  String method
  String runtime_environment
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  # By default, don't record http requests
  Boolean record_http = false
  String pipeline_tools_version
  Boolean add_md5s
  # Version of the pipeline, should be included in the pipeline file
  String pipeline_version
  # Disk space to allocate for stage_files task
  Int disk_space
  # Service account key for generating JWT for submission to Ingest
  File service_account_key_path = "gs://broad-dsde-mint-${runtime_environment}-credentials/caas_key.json"

  call get_metadata {
    input:
      analysis_output_path = outputs[0],
      runtime_environment = runtime_environment,
      cromwell_url = cromwell_url,
      record_http = record_http,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      pipeline_tools_version = pipeline_tools_version
  }

  call create_submission {
    input:
      reference_bundle = reference_bundle,
      run_type = run_type,
      schema_url = schema_url,
      analysis_process_schema_version = analysis_process_schema_version,
      analysis_protocol_schema_version = analysis_protocol_schema_version,
      analysis_file_version = analysis_file_version,
      method = method,
      submit_url = submit_url,
      inputs = inputs,
      outputs = outputs,
      format_map = format_map,
      metadata_json = get_metadata.metadata,
      input_bundle_uuid = input_bundle_uuid,
      workflow_id = get_metadata.workflow_id,
      pipeline_version = pipeline_version,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version,
      add_md5s = add_md5s,
      runtime_environment = runtime_environment,
      service_account_key_path = service_account_key_path
  }

  call stage_files {
    input:
      submission_url = create_submission.submission_url,
      files = outputs,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version,
      disk_space = disk_space
  }

  call confirm_submission {
    input:
      ready_to_be_confirmed = stage_files.ready_to_be_confirmed,
      submission_url = create_submission.submission_url,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version,
      runtime_environment = runtime_environment,
      service_account_key_path = service_account_key_path
  }

  output {
    File analysis_process = create_submission.analysis_process
    File analysis_protocol = create_submission.analysis_protocol
    Array[File] create_envelope_requests = create_submission.http_requests
    Array[File] create_envelope_responses = create_submission.http_responses
    Array[File] stage_and_confirm_requests = stage_files.http_requests
    Array[File] stage_and_confirm_responses = stage_files.http_responses
    Array[File] confirm_submission_requests = confirm_submission.http_requests
    Array[File] confirm_submission_responses = confirm_submission.http_responses
  }
}
