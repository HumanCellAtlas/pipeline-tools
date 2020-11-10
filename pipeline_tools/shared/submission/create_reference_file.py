#!/usr/bin/env python
import argparse
import json
import uuid

from pipeline_tools.shared.submission.format_map import (
    EXTENSION_TO_FORMAT,
    NAMESPACE,
    convert_datetime,
)
from pipeline_tools.shared.submission.create_analysis_metadata import get_file_format


def build_reference_file(
    file_path,
    sha256,
    raw_schema_url,
    reference_file_schema_version,
    version,
    reference_type,
    ncbi_taxon_id,
    genus_species,
    assembly_type,
    reference_version,
):
    """Create the submission envelope in Ingest service.

    Args:
        file_path (str): Path to the reference file.
        sha256 (str): sha256 hash value of the reference file.
        reference_type (str): type of the reference file (e.g. )
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        reference_file_schema_version (str): Version of the metadata schema that the file_descriptor.json conforms to.
    """

    SCHEMA_TYPE = 'file'
    entity_id = get_uuid5(sha256)
    formatted_version = convert_datetime(version)

    reference_file = {
        'describedBy': get_reference_file_described_by(
            schema_url=raw_schema_url, schema_version=reference_file_schema_version
        ),
        'schema_version': reference_file_schema_version,
        'schema_type': SCHEMA_TYPE,
        'file_core': get_file_core(file_path),
        'provenance': {'document_id': entity_id, 'submission_date': formatted_version},
        'ncbi_taxon_id': int(ncbi_taxon_id),
        'genus_species': {'text': genus_species},
        'assembly_type': assembly_type,
        'reference_type': reference_type,
        'reference_version': reference_version,
    }

    # Write the reference_file metadata
    with open(f'{entity_id}_{formatted_version}.json', 'w') as f:
        json.dump(reference_file, f, indent=2, sort_keys=True)

    with open('reference_uuid.txt', 'w') as f:
        f.write(entity_id)


def get_file_core(file_path):
    file_core = {
        'file_name': file_path.split('/')[-1],
        'format': get_file_format(file_path, EXTENSION_TO_FORMAT),
    }

    return file_core


def get_uuid5(sha256):
    return str(uuid.uuid5(NAMESPACE, sha256))


def get_reference_file_described_by(schema_url, schema_version):
    return f'{schema_url.strip("/")}/type/file/{schema_version}/reference_file'


def get_relative_file_location(file_url):
    """The object name of the data file relative to the staging area's `data/` directory"""
    return file_url.rsplit('/data/')[-1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--file_path', required=True, help='Path to the reference file.'
    )
    parser.add_argument(
        '--sha256', required=True, help='The sha256 hash value of the reference file.'
    )
    parser.add_argument(
        '--schema_url', required=True, help='URL for retrieving HCA metadata schemas.'
    )
    parser.add_argument(
        '--reference_file_schema_version',
        required=True,
        help='The metadata schema version that the links files conform to.',
    )
    parser.add_argument(
        '--ncbi_taxon_id',
        required=True,
        help='The NCBI taxonomy id associated with the reference',
    )
    parser.add_argument(
        '--workspace_version', required=True, help='The workspace version value'
    )
    parser.add_argument(
        '--reference_type',
        required=True,
        help='The type of the reference_file',
        choices=[
            "genome sequence",
            "transcriptome sequence",
            "annotation reference",
            "transcriptome index",
            "genome sequence index",
        ],
    )
    parser.add_argument(
        '--assembly_type',
        required=True,
        help='The genome assembly type',
        choices=["primary assembly", "complete assembly", "patch assembly"],
    )
    parser.add_argument('--genus_species', required=True, help='The genus species')
    parser.add_argument(
        '--reference_version',
        required=True,
        help='The genome version of the reference file',
    )
    args = parser.parse_args()

    schema_url = args.schema_url.strip('/')

    build_reference_file(
        sha256=args.sha256,
        file_path=args.file_path,
        raw_schema_url=schema_url,
        reference_file_schema_version=args.reference_file_schema_version,
        ncbi_taxon_id=args.ncbi_taxon_id,
        version=args.workspace_version,
        reference_type=args.reference_type,
        genus_species=args.genus_species,
        assembly_type=args.assembly_type,
        reference_version=args.reference_version,
    )


if __name__ == '__main__':
    main()
