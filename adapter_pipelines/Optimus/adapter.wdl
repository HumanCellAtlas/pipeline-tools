import "Optimus.wdl" as Optimus
import "submit.wdl" as submit_wdl


task GetInputs {
  String bundle_uuid
  String bundle_version
  String dss_url
  Int retry_seconds
  Int timeout_seconds
  String pipeline_tools_version

  command <<<
    # Force the binary layer of the stdout and stderr streams to be unbuffered.
    python -u <<CODE
    from pipeline_tools import input_utils

    input_utils.create_optimus_input_tsv(
                    "${bundle_manifest}",
                    "${bundle_version}",
                    "${dss_url}",
                    ${retry_seconds},
                    ${timeout_seconds})

    CODE
  >>>
  runtime {
    docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
  }
  output {
    String sample_id = read_string("inputs.tsv")
    Array[Array[File]] fastq_inputs = read_tsv("fastq_inputs.tsv")
  }
}

task inputs_for_submit {
    Array[Array[File]] fastq_inputs
    Array[Object] other_inputs
    String pipeline_tools_version

    command <<<
      # Force the binary layer of the stdout and stderr streams to be unbuffered.
      python -u <<CODE
      inputs = []
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

      print('fastq_files')
      fastq_files = '${sep=", " fastq_inputs}'.replace('/cromwell_root/', 'gs://')
      inputs.append({"name": "fastq_files", "value": fastq_files})

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

task outputs_for_submit {
    Array[Array[File]] tag_gene_exon_log
    Array[Array[File]] umi_metrics
    Array[Array[File]] duplicate_metrics
    Array[File] other_outputs
    String pipeline_tools_version

    command <<<
      # Force the binary layer of the stdout and stderr streams to be unbuffered.
      python -u <<CODE
      print('other outputs')

      outputs = ['${sep="', '" other_outputs}']

      tag_gene_exon_log = '${sep=', ' tag_gene_exon_log}'.replace('[', '').replace(']', '').replace('"', '').split(', ')
      outputs.extend(tag_gene_exon_log)

      umi_metrics = '${sep=', ' umi_metrics}'.replace('[', '').replace(']', '').replace('"', '').split(', ')
      outputs.extend(umi_metrics)

      duplicate_metrics = '${sep=', ' duplicate_metrics}'.replace('[', '').replace(']', '').replace('"', '').split(', ')
      outputs.extend(duplicate_metrics)

      print('write outputs.tsv')
      with open('outputs.tsv', 'w') as f:
          for output in outputs:
              output_path = output.replace('/cromwell_root/', 'gs://')
              f.write('{0}\n'.format(output_path))
      print('finished')
      CODE
    >>>

    runtime {
      docker: "quay.io/humancellatlas/secondary-analysis-pipeline-tools:" + pipeline_tools_version
    }

    output {
      Array[File] outputs = read_lines("outputs.tsv")
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
  String reference_bundle
  String method
  String schema_url
  String schema_version
  String analysis_file_version
  String run_type
  Int retry_seconds
  Int timeout_seconds
  Boolean use_caas

  # Set runtime environment such as "dev" or "staging" or "prod" so submit task could choose proper docker image to use
  String runtime_environment
  String pipeline_tools_version = "v0.23.0"

  call GetInputs as prep {
    input:
      bundle_uuid = bundle_uuid,
      bundle_version = bundle_version,
      dss_url = dss_url,
      retry_seconds = retry_seconds,
      timeout_seconds = timeout_seconds,
      pipeline_tools_version = pipeline_tools_version
  }

  call Optimus.Optimus as analysis {
    input:
      fastq_inputs = prep.fastq_inputs,
      sample_id = prep.sample_id,
      whitelist = whitelist,
      tar_star_reference = tar_star_reference,
      annotations_gtf = annotations_gtf,
      ref_genome_fasta = ref_genome_fasta,
      fastq_suffix = fastq_suffix
  }

  call inputs_for_submit {
    input:
      fastq_inputs = prep.fastq_inputs,
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

  call outputs_for_submit {
    input:
      tag_gene_exon_log = analysis.tag_gene_exon_log,
      umi_metrics = analysis.umi_metrics,
      duplicate_metrics = analysis.duplicate_metrics,
      other_outputs = [
        analysis.bam,
        analysis.matrix,
        analysis.matrix_summary,
        analysis.picard_metrics
      ],
      pipeline_tools_version = pipeline_tools_version
  }

  Array[Object] inputs = read_objects(inputs_for_submit.inputs)

  call submit_wdl.submit {
    input:
      inputs = inputs,
      outputs = outputs_for_submit.outputs,
      format_map = format_map,
      submit_url = submit_url,
      input_bundle_uuid = bundle_uuid,
      reference_bundle = reference_bundle,
      run_type = run_type,
      schema_url = schema_url,
      schema_version = schema_version,
      analysis_file_version = analysis_file_version,
      method = method,
      retry_seconds = retry_seconds,
      timeout_seconds = timeout_seconds,
      runtime_environment = runtime_environment,
      use_caas = use_caas,
      pipeline_tools_version = pipeline_tools_version
  }
}
