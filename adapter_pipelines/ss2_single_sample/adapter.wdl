import "SmartSeq2SingleSample.wdl" as ss2
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
    from pipeline_tools.pipelines.smartseq2 import smartseq2

    smartseq2.create_ss2_input_tsv(
                  "${bundle_uuid}",
                  "${bundle_version}",
                  "${dss_url}")

    CODE
  >>>
  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
  }
  output {
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
    Object inputs = read_object("inputs.tsv")
  }
}

workflow AdapterSmartSeq2SingleCell{
  # variable parameters provided by the notification sent by the data storage service
  String bundle_uuid
  String bundle_version

  # submission parameters
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

  # Set runtime environment such as "dev" or "staging" or "prod" so submit task could choose proper docker image to use
  String runtime_environment
  # By default, don't record http requests, unless we override in inputs json
  Boolean record_http = false
  Boolean add_md5s = false

  String pipeline_tools_version = "v0.56.6"

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

  call ss2.SmartSeq2SingleCell as analysis {
    input:
      genome_ref_fasta = prep.inputs.genome_ref_fasta,
      rrna_intervals = prep.inputs.rrna_intervals,
      gene_ref_flat = prep.inputs.gene_ref_flat,
      hisat2_ref_index = prep.inputs.hisat2_ref_index,
      hisat2_ref_trans_index = prep.inputs.hisat2_ref_trans_index,
      rsem_ref_index = prep.inputs.rsem_ref_index,
      hisat2_ref_name = prep.inputs.hisat2_ref_name,
      hisat2_ref_trans_name = prep.inputs.hisat2_ref_trans_name,
      stranded = prep.inputs.stranded,
      sample_name = prep.inputs.sample_id,
      output_name = prep.inputs.sample_id,
      fastq1 = prep.inputs.fastq_1,
      fastq2 = prep.inputs.fastq_2
  }

  call submit_wdl.submit {
    input:
      inputs = [
        {
          "name": "fastq1",
          "value": prep.inputs.fastq_1
        },
        {
          "name": "fastq2",
          "value": prep.inputs.fastq_2
        },
        {
          "name": "sample_name",
          "value": prep.inputs.sample_id
        },
        {
          "name": "output_name",
          "value": prep.inputs.sample_id
        },
        {
          "name": "genome_ref_fasta",
          "value": prep.inputs.genome_ref_fasta
        },
        {
          "name": "rrna_intervals",
          "value": prep.inputs.rrna_intervals
        },
        {
          "name": "gene_ref_flat",
          "value": prep.inputs.gene_ref_flat
        },
        {
          "name": "hisat2_ref_index",
          "value": prep.inputs.hisat2_ref_index
        },
        {
          "name": "hisat2_ref_trans_name",
          "value": prep.inputs.hisat2_ref_trans_name
        },
        {
          "name": "rsem_ref_index",
          "value": prep.inputs.rsem_ref_index
        },
        {
          "name": "hisat2_ref_name",
          "value": prep.inputs.hisat2_ref_name
        },
         {
          "name": "hisat2_ref_trans_name",
          "value": prep.inputs.hisat2_ref_trans_name
        },
        {
          "name": "stranded",
          "value": prep.inputs.stranded
        }
      ],
      outputs = flatten(
        select_all(
          [[analysis.aligned_bam,
            analysis.bam_index,
            analysis.insert_size_metrics,
            analysis.quality_distribution_metrics,
            analysis.quality_by_cycle_metrics,
            analysis.bait_bias_summary_metrics,
            analysis.rna_metrics,
            analysis.aligned_transcriptome_bam,
            analysis.rsem_gene_results,
            analysis.rsem_isoform_results
           ], analysis.group_results, analysis.zarr_output_files]
        )
      ),
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
      record_http = record_http,
      pipeline_tools_version = pipeline_tools_version,
      add_md5s = add_md5s,
      pipeline_version = analysis.pipeline_version,
      # The bam files are by far the largest outputs. The extra 5 GB should easily cover everything else.
      disk_space = ceil(size(analysis.aligned_bam, "GB") + size(analysis.aligned_transcriptome_bam, "GB") + 5)
  }
}
