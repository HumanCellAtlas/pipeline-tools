task submit_stub {
  command <<<
    echo "This is a stub submission task for testing purposes only."
  >>>
  runtime {
    docker: "python:2.7"
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
  Boolean use_caas
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  # By default, don't record http requests
  Boolean record_http = false

  call submit_stub

  output {}
}
