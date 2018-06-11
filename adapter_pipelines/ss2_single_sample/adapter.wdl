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

  command <<<
    export RECORD_HTTP_REQUESTS="${record_http}"
    export RETRY_TIMEOUT="${retry_timeout}"
    export RETRY_MULTIPLIER="${retry_multiplier}"
    export RETRY_MAX_INTERVAL="${retry_max_interval}"
    export INDIVIDUAL_REQUEST_TIMEOUT="${individual_request_timeout}"

    # Force the binary layer of the stdout and stderr streams to be unbuffered.
    python -u <<CODE
    from pipeline_tools import input_utils

    input_utils.create_ss2_input_tsv(
                    "${bundle_uuid}",
                    "${bundle_version}",
                    "${dss_url}")

    CODE
  >>>
  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:v0.20.0"
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

  # fixed parameters
  File gtf_file
  File genome_ref_fasta
  File rrna_intervals
  File gene_ref_flat
  File hisat2_ref_index
  File hisat2_ref_trans_index
  File rsem_ref_index
  File hisat2_ref_name
  File hisat2_ref_trans_name
  String stranded

  # submission parameters
  File format_map
  String dss_url
  String submit_url
  String method
  String schema_version
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

  call GetInputs as prep {
    input:
      bundle_uuid = bundle_uuid,
      bundle_version = bundle_version,
      dss_url = dss_url,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      record_http = record_http
  }

  call ss2.SmartSeq2SingleCell as analysis {
    input:
      gtf_file = gtf_file,
      genome_ref_fasta = genome_ref_fasta,
      rrna_intervals = rrna_intervals,
      gene_ref_flat = gene_ref_flat,
      hisat2_ref_index = hisat2_ref_index,
      hisat2_ref_trans_index = hisat2_ref_trans_index,
      rsem_ref_index = rsem_ref_index,
      hisat2_ref_name = hisat2_ref_name,
      hisat2_ref_trans_name = hisat2_ref_trans_name,
      stranded = stranded,
      sample_name = prep.inputs.sample_id,
      output_name = prep.inputs.sample_id,
      fastq1 = prep.inputs.fastq_1,
      fastq2 = prep.inputs.fastq_2,
      max_retries = max_cromwell_retries
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
          "name": "gtf_file",
          "value": gtf_file
        },
        {
          "name": "genome_ref_fasta",
          "value": genome_ref_fasta
        },
        {
          "name": "rrna_intervals",
          "value": rrna_intervals
        },
        {
          "name": "gene_ref_flat",
          "value": gene_ref_flat
        },
        {
          "name": "hisat2_ref_index",
          "value": hisat2_ref_index
        },
        {
          "name": "hisat2_ref_trans_name",
          "value": hisat2_ref_trans_name
        },
        {
          "name": "rsem_ref_index",
          "value": rsem_ref_index
        },
        {
          "name": "hisat2_ref_name",
          "value": hisat2_ref_name
        },
         {
          "name": "hisat2_ref_trans_name",
          "value": hisat2_ref_trans_name
        },
        {
          "name": "stranded",
          "value": stranded
        }
      ],
      outputs = [
        analysis.aligned_bam,
        analysis.alignment_summary_metrics,
        analysis.bait_bias_detail_metrics,
        analysis.bait_bias_summary_metrics,
        analysis.base_call_dist_metrics,
        analysis.base_call_pdf,
        analysis.dedup_metrics,
        analysis.error_summary_metrics,
        analysis.gc_bias_detail_metrics,
        analysis.gc_bias_dist_pdf,
        analysis.gc_bias_summary_metrics,
        analysis.insert_size_hist,
        analysis.insert_size_metrics,
        analysis.hisat2_log_file,
        analysis.hisat2_met_file,
        analysis.pre_adapter_details_metrics,
        analysis.quality_by_cycle_metrics,
        analysis.quality_by_cycle_pdf,
        analysis.quality_distribution_dist_pdf,
        analysis.quality_distribution_metrics,
        analysis.rna_coverage,
        analysis.rna_metrics,
        analysis.aligned_transcriptome_bam,
        analysis.hisat2_transcriptome_log_file,
        analysis.hisat2_transcriptome_met_file,
        analysis.rsem_cnt_log,
        analysis.rsem_gene_results,
        analysis.rsem_isoform_results,
        analysis.rsem_model_log,
        analysis.rsem_theta_log,
        analysis.rsem_time_log
      ],
      format_map = format_map,
      submit_url = submit_url,
      input_bundle_uuid = bundle_uuid,
      reference_bundle = reference_bundle,
      run_type = run_type,
      schema_version = schema_version,
      method = method,
      retry_multiplier = retry_multiplier,
      retry_max_interval = retry_max_interval,
      retry_timeout = retry_timeout,
      individual_request_timeout = individual_request_timeout,
      runtime_environment = runtime_environment,
      use_caas = use_caas,
      record_http = record_http
  }
}
