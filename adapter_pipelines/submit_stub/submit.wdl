task submit_stub {
  command <<<
    echo "This is a stub submission task for testing purposes only."
  >>>
  runtime {
    # Use the slim version of the docker image here for efficiency
    docker: "python:3.6-slim"
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
  File service_account_key_path = "gs://broad-dsde-mint-${runtime_environment}-credentials/caas_key.json"

  call submit_stub

  output {}
}
