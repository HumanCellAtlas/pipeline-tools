import "cellranger.wdl" as CellRanger
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
    from pipeline_tools.pipelines.cellranger import cellranger

    cellranger.create_cellranger_input_tsv(
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
    String reference_name = read_string("reference_name.txt")
    File transcriptome_tar_gz = read_string("transcriptome_tar_gz.txt")
    Int expect_cells = read_string("expect_cells.txt")
    Array[File] fastqs = read_lines("fastqs.txt")
    Array[String] fastq_names = read_lines("fastq_names.txt")
    Array[File] http_requests = glob("request_*.txt")
    Array[File] http_responses = glob("response_*.txt")
  }
}

task RenameFiles {
    Array[File] file_paths
    Array[String] new_file_names
    String pipeline_tools_version

    command <<<
      python -u <<CODE
      import subprocess

      files=["${sep='","' file_paths}"]
      file_names=["${sep='","' new_file_names}"]

      for idx, f in enumerate(files):
          subprocess.check_output(['mv', f, file_names[idx]])

      CODE
    >>>
    runtime {
      docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
      disks: "local-disk 100 HDD"
    }
    output {
      Array[File] outputs = new_file_names
    }
}

task InputsForSubmit {
    Array[String] fastqs
    Array[Object] other_inputs
    Int? expect_cells
    String pipeline_tools_version

    command <<<
      # Force the binary layer of the stdout and stderr streams to be unbuffered.
      python -u <<CODE

      inputs = []

      print('fastq_files')
      inputs.append({"name": "fastqs", "value": "${sep=', ' fastqs}"})

      print('other inputs')
      with open('${write_objects(other_inputs)}') as f:
          keys = f.readline().strip().split('\t')
          for line in f:
              values = line.strip().split('\t')
              input_map = {}
              for i, key in enumerate(keys):
                  input_map[key] = values[i]
              print(input_map)
              inputs.append(input_map)

      print('expect cells')
      if "${expect_cells}":
          inputs.append({"name": "expect_cells", "value": "${expect_cells}"})

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

workflow Adapter10xCount {
  String bundle_uuid
  String bundle_version

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

  # Set runtime environment such as "dev" or "staging" or "prod" so submit task could choose proper docker image to use
  String runtime_environment
  # By default, don't record http requests, unless we override in inputs json
  Boolean record_http = false
  Boolean add_md5s = false

  String pipeline_tools_version = "v0.56.6"

  call GetInputs {
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

  # Rename the fastq files to the format required by CellRanger:
  #  {sample_id}_S1_L001_R1_001.fastq.gz'
  call RenameFiles as rename_fastqs {
    input:
      file_paths = GetInputs.fastqs,
      new_file_names = GetInputs.fastq_names,
      pipeline_tools_version = pipeline_tools_version
  }

  # CellRanger gets the paths to the fastq directories from the array of fastqs,
  # so the order of those files does not matter
  call CellRanger.CellRanger as analysis {
    input:
      sample_id = GetInputs.sample_id,
      fastqs = rename_fastqs.outputs,
      reference_name = GetInputs.reference_name,
      transcriptome_tar_gz = GetInputs.transcriptome_tar_gz,
      expect_cells = GetInputs.expect_cells
  }

  call InputsForSubmit {
    input:
      fastqs = GetInputs.fastqs,
      other_inputs = [
        {
          "name": "sample_id",
          "value": GetInputs.sample_id
        },
        {
          "name": "reference_name",
          "value": GetInputs.reference_name
        },
        {
          "name": "transcriptome_tar_gz",
          "value": GetInputs.transcriptome_tar_gz
        }
      ],
      expect_cells = GetInputs.expect_cells,
      pipeline_tools_version = pipeline_tools_version
  }

  # Rename analysis files so that all the file names are unique. For example, rename
  # "${sample_id}/outs/raw_gene_bc_matrices/${reference}/barcodes.tsv" to "raw_barcodes.tsv" so that
  # it does not overwrite "${sample_id}/outs/filtered_gene_bc_matrices/${reference}/barcodes.tsv"
  # when uploading files
  call RenameFiles as output_files {
    input:
      file_paths = [analysis.raw_barcodes, analysis.raw_genes, analysis.raw_matrix],
      new_file_names = ["raw_barcodes.tsv", "raw_genes.tsv", "raw_matrix.mtx"],
      pipeline_tools_version = pipeline_tools_version
  }

  Array[Object] inputs = read_objects(InputsForSubmit.inputs)

  call submit_wdl.submit {
    input:
      inputs = inputs,
      outputs = flatten([[
        analysis.qc,
        analysis.sorted_bam,
        analysis.sorted_bam_index,
        analysis.barcodes,
        analysis.genes,
        analysis.matrix,
        analysis.filtered_gene_h5,
        analysis.raw_gene_h5,
        analysis.mol_info_h5,
        analysis.web_summary
      ], output_files.outputs]),
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
      # The sorted bam is the largest output. Other outputs will increase space by ~50%.
      # Factor of 2 and addition of 50 GB gives some buffer.
      disk_space = ceil(size(analysis.sorted_bam, "GB") * 2 + 50)
  }
}
