import "Optimus.wdl" as Optimus
import "submit.wdl" as submit_wdl


task GetInputs {
  String bundle_uuid
  String bundle_version
  String dss_url
  Int retry_seconds
  Int timeout_seconds

  command <<<
    python <<CODE
    from pipeline_tools import utils

    # Get bundle manifest
    uuid = '${bundle_uuid}'
    version = '${bundle_version}'
    dss_url = '${dss_url}'
    retry_seconds = ${retry_seconds}
    timeout_seconds = ${timeout_seconds}
    print('Getting bundle manifest for id {0}, version {1}'.format(uuid, version))
    manifest_files = utils.get_manifest_files(uuid, version, dss_url, timeout_seconds, retry_seconds)

    print('Downloading assay.json')
    assay_json_uuid = manifest_files['name_to_meta']['assay.json']['uuid']
    assay_json = utils.get_file_by_uuid(assay_json_uuid, dss_url)

    # Parse inputs from assay_json and write to fastq_inputs
    print('Writing fastq inputs to fastq_inputs.tsv')
    lanes = assay_json['seq']['lanes']
    r1 = [manifest_files['name_to_meta'][lane['r1']]['url'] for lane in lanes]
    r2 = [manifest_files['name_to_meta'][lane['r2']]['url'] for lane in lanes]
    i1 = [manifest_files['name_to_meta'][lane['i1']]['url'] for lane in lanes]
    fastq_inputs = [list(i) for i in zip(r1, r2, i1)]

    with open('fastq_inputs.tsv', 'w') as f:
        for line in fastq_inputs:
            f.write('\t'.join(line) +'\n')

    print('Writing sample ID to inputs.tsv')
    sample_id = assay_json['sample_id']
    with open('inputs.tsv', 'w') as f:
        f.write('sample_id\n')
        f.write('{0}\n'.format(sample_id))
    print('Wrote input map')
    CODE
  >>>
  runtime {
    docker: "humancellatlas/pipeline-tools:0.1.4"
  }
  output {
    Object inputs = read_object("inputs.tsv")
    Array[Array[File]] fastq_inputs = read_tsv("fastq_inputs.tsv")
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
  String schema_version
  String run_type
  Int retry_seconds
  Int timeout_seconds

  # Set runtime environment such as "dev" or "staging" or "prod" so submit task could choose proper docker image to use
  String runtime_environment

  call GetInputs as prep {
    input:
      bundle_uuid = bundle_uuid,
      bundle_version = bundle_version,
      dss_url = dss_url,
      retry_seconds = retry_seconds,
      timeout_seconds = timeout_seconds
  }

  call Optimus.Optimus as analysis {
    input:
      fastq_inputs = prep.fastq_inputs,
      sample_id = prep.inputs.sample_id,
      whitelist = whitelist,
      tar_star_reference = tar_star_reference,
      annotations_gtf = annotations_gtf,
      ref_genome_fasta = ref_genome_fasta,
      fastq_suffix = fastq_suffix

  # call submit here.
  }
}