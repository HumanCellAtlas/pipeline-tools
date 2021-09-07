import argparse
import json
from distutils.util import strtobool


def parse_optimus_metadata(metadata_json):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)
    ref_fasta_path = metadata['inputs']['ref_genome_fasta']
    pipeline_version = metadata['calls']['Optimus.OptimusLoomGeneration'][0]['inputs']['pipeline_version']
    return ref_fasta_path, pipeline_version


def parse_SS2_metadata(metadata_json, project_level):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)

    # find reference fasta path in metadata.json
    ref_fasta_path = metadata['inputs']['genome_ref_fasta']

    # find pipeline version in metadata.json
    if project_level:
        # project level run should be the version of the MultiSampleSmartSeq2 pipeline
        pipeline_version = metadata['calls']['MultiSampleSmartSeq2.AggregateLoom'][0]['inputs']['pipeline_version']
    else:
        # intermediate level run should be the version of the SmartSeq2SingleCell pipeline
        # version number stored in metadata.json, so the prefix needs to be added
        pipeline_version = "SmartSeq2SingleSample_v" + metadata['calls']['MultiSampleSmartSeq2.sc_pe'][0]['outputs']['pipeline_version_out']

    return ref_fasta_path, pipeline_version


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

    parser.add_argument("--project-level",
                        required=False,
                        type=lambda x: bool(strtobool(x)),
                        help="Boolean representing project level vs intermediate level")

    args = parser.parse_args()

    pipeline_type = args.pipeline_type
    metadata = args.cromwell_metadata
    project_level = args.project_level

    if pipeline_type == "Optimus":
        ref_fasta_path, pipeline_version = parse_optimus_metadata(metadata)
    elif pipeline_type == "SS2":
        ref_fasta_path, pipeline_version = parse_SS2_metadata(metadata, project_level)
    else:
        raise RuntimeError('pipeline-type must be Optimus or SS2')

    with open('pipeline_version.txt', 'w') as f:
        f.write(pipeline_version)

    with open('ref_fasta.txt', 'w') as f:
        f.write(ref_fasta_path)


if __name__ == '__main__':
    main()
