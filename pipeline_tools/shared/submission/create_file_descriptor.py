#!/usr/bin/env python
import argparse
import json

# import uuid


def build_file_descriptor(
    analysis_protocol_path,
    analysis_process_path,
    outputs_file_path,
    raw_schema_url,
    file_descriptor_schema_version,
):
    """Create the submission envelope in Ingest service.

    Args:
        analysis_protocol_path (str): Path to the analysis_protocol json file.
        analysis_process_path (str): Path to the analysis_process json file.
        outputs_file_path (str): Path to the outputs json file.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        file_descriptor_schema_version (str): Version of the metadata schema that the file_descriptor.json conforms to.
    """

    SCHEMA_TYPE = 'file_descriptor'

    file_descriptor = {
        'describedBy': get_file_descriptor_described_by(
            schema_url=raw_schema_url, schema_version=file_descriptor_schema_version
        ),
        'schema_version': file_descriptor_schema_version,
        'schema_type': SCHEMA_TYPE,
        'content_type': None,
        'size': None,
        'sha256': None,
        'crc32c': None,
        'file_id': None,
        'file_version': None,
        'file_name': None,
    }

    return file_descriptor


def get_file_descriptor_described_by(schema_url, schema_version):
    return f'{schema_url}/system/{schema_version}/file_descriptor'


def get_file_location(file_url):
    return file_url.rsplit('/data/')[-1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--analysis_process_path',
        required=True,
        help='Path to the analysis_process.json file.',
    )
    parser.add_argument(
        '--outputs_file_path', required=True, help='Path to the outputs.json file.'
    )
    parser.add_argument(
        '--analysis_protocol_path',
        required=True,
        help='Path to the analysis_protocol.json file.',
    )
    parser.add_argument(
        '--schema_url', required=True, help='URL for retrieving HCA metadata schemas.'
    )
    parser.add_argument(
        '--links_schema_version',
        required=True,
        help='The metadata schema version that the links files conform to.',
    )
    args = parser.parse_args()

    schema_url = args.schema_url.strip('/')

    links = build_file_descriptor(
        analysis_protocol_path=args.analysis_protocol_path,
        analysis_process_path=args.analysis_process_path,
        outputs_file_path=args.outputs_file_path,
        raw_schema_url=schema_url,
        file_descriptor_schema_version=args.links_schema_version,
    )

    # Write links to file
    print('Writing links.json to disk...')
    print(links)
    print("NOW")
    with open('links.json', 'w') as f:
        json.dump(links, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
