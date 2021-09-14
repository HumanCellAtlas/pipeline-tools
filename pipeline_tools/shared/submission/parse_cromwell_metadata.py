import argparse
import json
from pipeline_tools.shared.submission import format_map


def parse_optimus_metadata(metadata_json):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)
    ref_fasta_path = metadata['inputs']['ref_genome_fasta']
    pipeline_version = metadata['calls']['Optimus.OptimusLoomGeneration'][0]['inputs']['pipeline_version']
    return ref_fasta_path, pipeline_version


def parse_SS2_metadata(metadata_json):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)

    # find reference fasta path in metadata.json
    ref_fasta_path = metadata['inputs']['genome_ref_fasta']

    # find pipeline version in metadata.json
    # project level run should be the version of the MultiSampleSmartSeq2 pipeline
    multi_sample_pipeline_version = metadata['calls']['MultiSampleSmartSeq2.AggregateLoom'][0]['inputs']['pipeline_version']

    # intermediate level run should be the version of the SmartSeq2SingleCell pipeline
    # version number stored in metadata.json, so the prefix needs to be added
    single_sample_pipeline_version = "SmartSeq2SingleSample_v" + metadata['calls'][format_map.get_call_type(metadata)][0]['outputs']['pipeline_version_out']

    return ref_fasta_path, multi_sample_pipeline_version, single_sample_pipeline_version


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--cromwell-metadata-json',
                        dest='cromwell_metadata',
                        required=True,
                        help='path to cromwell metadata json')

    parser.add_argument('--pipeline-type',
                        dest='pipeline_type',
                        required=True,
                        help='Optimus or SS2')

    args = parser.parse_args()

    pipeline_type = args.pipeline_type
    metadata = args.cromwell_metadata

    if pipeline_type.lower() == "optimus":
        ref_fasta_path, pipeline_version = parse_optimus_metadata(metadata)
    elif pipeline_type.lower() == "ss2":
        ref_fasta_path, pipeline_version, single_sample_pipeline_version = parse_SS2_metadata(metadata)
    else:
        raise RuntimeError('pipeline-type must be Optimus or SS2')

    with open('pipeline_version.txt', 'w') as f:
        f.write(pipeline_version)

    with open('single_sample_pipeline_version.txt', 'w') as f:
        # only write to file if single sample pipeline version is stored
        if ("single_sample_pipeline_version" in locals()):
            f.write(single_sample_pipeline_version)

    with open('ref_fasta.txt', 'w') as f:
        f.write(ref_fasta_path)


if __name__ == '__main__':
    main()
