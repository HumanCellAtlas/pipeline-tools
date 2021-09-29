#!/usr/bin/env python3

import subprocess
import argparse
import json


def main():
    description = """Creates json files needed for HCA DCP2 MVP"""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--analysis_files_metadata_jsons',
                        dest='analysis_files_metadata_jsons',
                        required=True,
                        help="Paths to analysis files metadata jsons")
    parser.add_argument('--analysis_process_jsons',
                        dest='analysis_process_jsons',
                        required=True,
                        help="Paths to analysis process jsons")
    parser.add_argument('--analysis_protocol_jsons',
                        dest='analysis_protocol_jsons',
                        help="Paths to analysis protocol jsons")
    parser.add_argument('--analysis_files_descriptors_jsons',
                        dest='analysis_files_descriptors_jsons',
                        required=True,
                        help="Paths to analysis files descriptors jsons")
    parser.add_argument('--links_jsons',
                        dest='links_jsons',
                        required=True,
                        help="Paths to links jsons")
    parser.add_argument('--reference_metadata_jsons',
                        dest='reference_metadata_jsons',
                        required=True,
                        help="Paths to reference metadata jsons")
    parser.add_argument('--reference_file_descriptor_jsons',
                        dest='reference_file_descriptor_jsons',
                        required=True,
                        help="Paths to reference file descriptor jsons")
    parser.add_argument('--data_files',
                        dest='data_files',
                        required=True,
                        help="Paths to bam and loom data_files")
    parser.add_argument('--staging-bucket',
                        dest='staging_bucket',
                        help="Path to staging bucket")

    args = parser.parse_args()

    analysis_files_metadata_jsons = args.analysis_files_metadata_jsons
    analysis_process_jsons = args.analysis_process_jsons
    analysis_protocol_jsons = args.analysis_protocol_jsons
    analysis_files_descriptors_jsons = args.analysis_files_descriptors_jsons
    links_jsons = args.links_jsons
    data_files = args.data_files
    reference_metadata_jsons = args.reference_metadata_jsons
    reference_file_descriptor_jsons = args.reference_file_descriptor_jsons
    staging_bucket = args.staging_bucket

    # File paths are written to files then load into json lists
    # Some SS2 runs have thousands of files and can not all be passed as arguments

    with open(analysis_files_metadata_jsons) as f:
        analysis_files_metadata_jsons_list = json.load(f)
        for file in analysis_files_metadata_jsons_list:
            subprocess.run('gsutil cp {0} {1}metadata/analysis_file/'.format(file, staging_bucket),
                           shell=True)

    with open(analysis_process_jsons) as f:
        analysis_process_jsons_list = json.load(f)
        for file in analysis_process_jsons_list:
            subprocess.run('gsutil cp {0} {1}metadata/analysis_process/'.format(file, staging_bucket),
                           shell=True)

    with open(analysis_protocol_jsons) as f:
        analysis_protocol_jsons_list = json.load(f)
        for file in analysis_protocol_jsons_list:
            subprocess.run('gsutil cp {0} {1}metadata/analysis_protocol/'.format(file, staging_bucket),
                           shell=True)

    with open(analysis_files_descriptors_jsons) as f:
        analysis_files_descriptors_jsons_list = json.load(f)
        for file in analysis_files_descriptors_jsons_list:
            subprocess.run('gsutil cp {0} {1}descriptors/analysis_file/'.format(file, staging_bucket),
                           shell=True)

    with open(links_jsons) as f:
        links_jsons_list = json.load(f)
        for file in links_jsons_list:
            subprocess.run('gsutil cp {0} {1}links/'.format(file, staging_bucket),
                           shell=True)

    with open(data_files) as f:
        data_files_list = json.load(f)
        for file in data_files_list:
            subprocess.run('gsutil cp {0} {1}data/'.format(file, staging_bucket),
                           shell=True)

    with open(reference_metadata_jsons) as f:
        reference_metadata_jsons_list = json.load(f)
        for file in reference_metadata_jsons_list:
            subprocess.run('gsutil cp {0} {1}metadata/reference_file/'.format(file, staging_bucket),
                           shell=True)

    with open(reference_file_descriptor_jsons) as f:
        reference_file_descriptor_jsons_list = json.load(f)
        for file in reference_file_descriptor_jsons_list:
            subprocess.run('gsutil cp {0} {1}descriptors/reference_file/'.format(file, staging_bucket),
                           shell=True)


if __name__ == '__main__':
    main()
