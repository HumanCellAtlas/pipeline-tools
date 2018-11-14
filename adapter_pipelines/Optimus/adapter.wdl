import "Optimus.wdl" as Optimus
import "submit.wdl" as submit_wdl


task GetInputs {
  String bundle_uuid
  String bundle_version
  String dss_url
  Int? retry_timeout
  Float? retry_multiplier
  Int? retry_max_interval
  Int? individual_request_timeout
  Boolean record_http
  String pipeline_tools_version

  command <<<
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"

    # Force the binary layer of the stdout and stderr streams to be unbuffered.
    python -u <<CODE
    from pipeline_tools import input_utils

    input_utils.create_optimus_input_tsv(
                    "${bundle_uuid}",
                    "${bundle_version}",
                    "${dss_url}")

    CODE
  >>>
  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
  }
  output {
    String sample_id = read_string("sample_id.txt")
    Array[File] r1_fastq = read_lines("r1.txt")
    Array[File] r2_fastq = read_lines("r2.txt")
    Array[File] i1_fastq = read_lines("i1.txt")
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
  }
}

task inputs_for_submit {
    Array[File] r1_fastq
    Array[File] r2_fastq
    Array[File] i1_fastq
    Array[Object] other_inputs
    String pipeline_tools_version

    command <<<
      # Force the binary layer of the stdout and stderr streams to be unbuffered.
      python -u <<CODE

      inputs = []

      print('fastq_files')
      inputs.append({"name": "r1_fastq", "value": "${sep=', ' r1_fastq}"})
      inputs.append({"name": "r2_fastq", "value": "${sep=', ' r2_fastq}"})
      inputs.append({"name": "i1_fastq", "value": "${sep=', ' i1_fastq}"})

      print('other inputs')
      with open('${write_objects(other_inputs)}') as f:
          keys = f.readline().strip().split('\t')
          for line in f:
              values = line.strip().split('\t')
              input = {}
              for i, key in enumerate(keys):
                  input[key] = values[i]
              print(input)
              inputs.append(input)

      print('write inputs.tsv')
      with open('inputs.tsv', 'w') as f:
          f.write('name\tvalue\n')
          for input in inputs:
              print(input)
              f.write('{0}\t{1}\n'.format(input['name'], input['value']))
      print('finished')
      CODE
    >>>

    runtime {
      docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
    }

    output {
      File inputs = "inputs.tsv"
    }
}

workflow AdapterOptimus {
  String bundle_uuid
  String bundle_version

  File whitelist  # 10x genomics cell barcode whitelist for 10x V2
  File tar_star_reference  # star reference
  File annotations_gtf  # gtf containing annotations for gene tagging
  File ref_genome_fasta  # genome fasta file
  String fastq_suffix = ".gz"  # add this suffix to fastq files for picard

  # Submission
  File format_map
  String dss_url
  String submit_url
  String method
  String schema_url
  String cromwell_url
  String analysis_process_schema_version
  String analysis_protocol_schema_version
  String analysis_file_version
  String run_type
  Int? retry_max_interval
  Float? retry_multiplier
  Int? retry_timeout
  Int? individual_request_timeout
  String reference_bundle
  Boolean use_caas

  # Set runtime environment such as "dev" or "staging" or "prod" so submit task could choose proper docker image to use
  String runtime_environment
  # By default, don't record http requests, unless we override in inputs json
  Boolean record_http = false
  Int max_cromwell_retries = 0
  Boolean add_md5s = false

  String pipeline_tools_version = "v0.42.1"

  call GetInputs as prep {
    input:
      bundle_uuid = bundle_uuid,
      bundle_version = bundle_version,
      dss_url = dss_url,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version
  }

  call Optimus.Optimus as analysis {
    input:
      r1_fastq = prep.r1_fastq,
      r2_fastq = prep.r2_fastq,
      i1_fastq = prep.i1_fastq,
      sample_id = prep.sample_id,
      whitelist = whitelist,
      tar_star_reference = tar_star_reference,
      annotations_gtf = annotations_gtf,
      ref_genome_fasta = ref_genome_fasta,
      fastq_suffix = fastq_suffix
  }

  call inputs_for_submit {
    input:
      r1_fastq = prep.r1_fastq,
      r2_fastq = prep.r2_fastq,
      i1_fastq = prep.i1_fastq,
      other_inputs = [
        {
          "name": "whitelist",
          "value": whitelist
        },
        {
          "name": "sample_id",
          "value": prep.sample_id
        },
        {
          "name": "tar_star_reference",
          "value": tar_star_reference
        },
        {
          "name": "annotations_gtf",
          "value": annotations_gtf
        },
        {
          "name": "ref_genome_fasta",
          "value": ref_genome_fasta
        }
      ],
      pipeline_tools_version = pipeline_tools_version
  }

  Array[Object] inputs = read_objects(inputs_for_submit.inputs)

  call submit_wdl.submit {
    input:
      inputs = inputs,
      outputs = [
        analysis.bam,
        analysis.matrix,
        analysis.matrix_row_index,
        analysis.matrix_col_index,
        analysis.cell_metrics,
        analysis.gene_metrics
      ],
      format_map = format_map,
      submit_url = submit_url,
      cromwell_url = cromwell_url,
      input_bundle_uuid = bundle_uuid,
      reference_bundle = reference_bundle,
      run_type = run_type,
      schema_url = schema_url,
      analysis_process_schema_version = analysis_process_schema_version,
      analysis_protocol_schema_version = analysis_protocol_schema_version,
      analysis_file_version = analysis_file_version,
      method = method,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      runtime_environment = runtime_environment,
      use_caas = use_caas,
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version,
      add_md5s = add_md5s,
      pipeline_version = analysis.pipeline_version
  }
}
