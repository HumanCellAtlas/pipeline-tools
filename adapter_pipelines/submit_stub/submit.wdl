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
  Int retry_seconds
  Int timeout_seconds

  call submit_stub

  output {}
}
