import argparse
import json



def parse_optimus_metadata(metadata_json):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)
    ref_fasta_path = metadata['inputs']['ref_genome_fasta']
    pipeline_version = metadata['calls']['Optimus.OptimusLoomGeneration'][0]['inputs']['pipeline_version']
    return ref_fasta_path, pipeline_version

def parse_SS2_metadata(metadata_json):
    with open(metadata_json, 'r') as f:
        metadata = json.load(f)
    ref_fasta_path = "SS2 still needs to be parsed"
    pipeline_version = "SS2 still needs to be parsed"
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

    args = parser.parse_args()

    pipeline_type = args.pipeline_type
    metadata = args.cromwell_metadata

    if pipeline_type == "Optimus":
        ref_fasta_path, pipeline_version = parse_optimus_metadata(metadata)
    elif pipeline_type == "SS2":
        ref_fasta_path, pipeline_version = parse_SS2_metadata(metadata)
    else:
        raise RuntimeError('pipeline-type must be Optimus or SS2')

    with open('pipeline_version.txt', 'w') as f:
        f.write(pipeline_version)

    with open('ref_fasta.txt', 'w') as f:
        f.write(ref_fasta_path)



if __name__ == '__main__':
    main()