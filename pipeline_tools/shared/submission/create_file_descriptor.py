#!/usr/bin/env python
# import argparse
import datetime
import json
import re
import uuid


def build_file_descriptor(
    file_path,
    size,
    sha256,
    crc32c,
    raw_schema_url,
    file_descriptor_schema_version,
    file_info,
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
    entity_id = get_uuid5(sha256)
    file_version = get_datetime_from_file_info(file_info)

    file_descriptor = {
        'describedBy': get_file_descriptor_described_by(
            schema_url=raw_schema_url, schema_version=file_descriptor_schema_version
        ),
        'schema_version': file_descriptor_schema_version,
        'schema_type': SCHEMA_TYPE,
        'content_type': 'application/unknown',  # TODO: Make this actually work
        'size': int(size),
        'sha256': sha256,
        'crc32c': crc32c,
        'file_id': entity_id,
        'file_version': file_version,
        'file_name': get_relative_file_location(file_path),
    }

    with open(f'{entity_id}_{file_version}.json', 'w') as f:
        json.dump(file_descriptor, f, indent=2, sort_keys=True)


def get_datetime_from_file_info(file_info):
    """Retrieve the datetime from the file info string and convert into mandated
    format. Add '.000000' for microseconds"""

    # TODO: Get rid of the need for this workaround (fix wdl)

    regex = r'([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)'
    try:
        original_datetime = re.search(regex, file_info).group(0)
        formatted_datetime = original_datetime.replace('Z', '.000000Z')
    except AttributeError:
        original_datetime = datetime.datetime.utcnow().isoformat()
        formatted_datetime = original_datetime + 'Z'
    return formatted_datetime


def get_uuid5(sha256):
    NAMESPACE = uuid.UUID('c6591d1d-27bc-4c94-bd54-1b51f8a2456c')
    return str(uuid.uuid5(NAMESPACE, sha256))


def get_file_descriptor_described_by(schema_url, schema_version):
    return f'{schema_url.strip("/")}/system/{schema_version}/file_descriptor'


def get_relative_file_location(file_url):
    """The object name of the data file relative to the staging area's `data/` directory"""
    return file_url.rsplit('/data/')[-1]


# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         '--analysis_process_path',
#         required=True,
#         help='Path to the analysis_process.json file.',
#     )
#     parser.add_argument(
#         '--outputs_file_path', required=True, help='Path to the outputs.json file.'
#     )
#     parser.add_argument(
#         '--analysis_protocol_path',
#         required=True,
#         help='Path to the analysis_protocol.json file.',
#     )
#     parser.add_argument(
#         '--schema_url', required=True, help='URL for retrieving HCA metadata schemas.'
#     )
#     parser.add_argument(
#         '--links_schema_version',
#         required=True,
#         help='The metadata schema version that the links files conform to.',
#     )
#     args = parser.parse_args()
#
#     schema_url = args.schema_url.strip('/')
#
#     links = build_file_descriptor(
#         analysis_protocol_path=args.analysis_protocol_path,
#         analysis_process_path=args.analysis_process_path,
#         outputs_file_path=args.outputs_file_path,
#         raw_schema_url=schema_url,
#         file_descriptor_schema_version=args.links_schema_version,
#     )
#
#     # Write links to file
#     print('Writing links.json to disk...')
#     print(links)
#     print("NOW")
#     with open('links.json', 'w') as f:
#         json.dump(links, f, indent=2, sort_keys=True)
#
#
# if __name__ == '__main__':
#     main()
