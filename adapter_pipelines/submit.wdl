# Get Cromwell metadata for the workflow that produced the given output
task get_metadata {
  String analysis_output_path
  String runtime_environment
  Boolean use_caas

  command <<<
    get-analysis-metadata \
      --analysis_output_path ${analysis_output_path} \
      --runtime_environment ${runtime_environment} \
      --use_caas ${use_caas}
  >>>
  runtime {
    docker: "gcr.io/broad-dsde-mint-${runtime_environment}/cromwell-metadata:0.1.5"
  }
  output {
    File metadata = "metadata.json"
    String workflow_id = read_string("workflow_id.txt")
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
  Int retry_seconds
  Int timeout_seconds

  command <<<
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
      --schema_version ${schema_version} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:v0.17.0"
  }
  output {
    File analysis_json = "analysis.json"
    String submission_url = read_string("submission_url.txt")
  }
}

# Stage files, then confirm submission
task stage_and_confirm {
  String submission_url
  Array[File] files
  Int retry_seconds
  Int timeout_seconds
  # These left and right brace definitions are a workaround so Cromwell won't
  # interpret the bash array reference below as a WDL variable.
  String lb = "{"
  String rb = "}"

  command <<<
    set -e

    # Get the urn needed for staging files
    staging_urn=$(get-staging-urn \
        --envelope_url ${submission_url} \
        --retry_seconds ${retry_seconds} \
        --timeout_seconds ${timeout_seconds})

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

    # Confirm the submission
    confirm-submission \
      --envelope_url ${submission_url} \
      --retry_seconds ${retry_seconds} \
      --timeout_seconds ${timeout_seconds}
  >>>

  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:v0.17.0"
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
  Int retry_seconds
  Int timeout_seconds
  Boolean use_caas

  call get_metadata {
    input:
      analysis_output_path = outputs[0],
      runtime_environment = runtime_environment,
      use_caas=use_caas
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
      retry_seconds = retry_seconds,
      timeout_seconds = timeout_seconds
  }

  call stage_and_confirm {
    input:
      submission_url = create_submission.submission_url,
      files = outputs,
      retry_seconds = retry_seconds,
      timeout_seconds = timeout_seconds
  }

  output {
    File analysis_json = create_submission.analysis_json
  }
}
