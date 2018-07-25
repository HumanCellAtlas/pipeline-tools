task submit_stub {
  command <<<
    echo "This is a stub submission task for testing purposes only."
  >>>
  runtime {
    # Use the slim version of the docker image here for efficiency
    docker: "python:2.7-slim"
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
  String schema_version
  String analysis_file_version
  String method
  String runtime_environment
  Boolean use_caas
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  # By default, don't record http requests
  Boolean record_http = false
  String pipeline_tools_version

  call submit_stub

  output {}
}
