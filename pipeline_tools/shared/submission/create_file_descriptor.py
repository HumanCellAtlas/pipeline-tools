#!/usr/bin/env python
import argparse
import json
import mimetypes
import os

from pipeline_tools.shared.submission.format_map import (
    get_uuid5,
    convert_datetime,
    MIME_FORMATS,
)

from file_descriptors.descriptor import Descriptor


[mimetypes.add_type(entry[0], entry[1]) for entry in MIME_FORMATS]


def build_file_descriptor(
    input_uuid,
    file_path,
    size,
    sha256,
    crc32c,
    creation_time,
    raw_schema_url,
    file_descriptor_schema_version,
):
    """Create the submission envelope in Ingest service.
    Args:
        file_path (str): Path to the described file.
        size (str): Size of the described file in bytes.
        input_uuid (str): UUID of the input file in the HCA Data Browser.
        sha256 (str): sha256 hash value of the described file.
        crc32c (str): crc32c hash value of the described file.
        creation_time (str): Timestamp of the creation time of the described file.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        file_descriptor_schema_version (str): Version of the metadata schema that the file_descriptor.json conforms to.
    """

    SCHEMA_TYPE = 'file_descriptor'
    relative_location = get_relative_file_location(file_path)
    file_version = convert_datetime(creation_time)
    file_extension = os.path.splitext(file_path)[1]

    file_id = get_uuid5(get_uuid5(f"{str(input_uuid)}{file_extension}"))

    file_descriptor = {
        'describedBy': get_file_descriptor_described_by(
            schema_url=raw_schema_url, schema_version=file_descriptor_schema_version
        ),
        'schema_version': file_descriptor_schema_version,
        'schema_type': SCHEMA_TYPE,
        'content_type': mimetypes.guess_type(file_path)[0] or 'application/unknown',
        'size': int(size),
        'sha256': sha256,
        'crc32c': crc32c,
        'file_id': file_id,
        'file_version': file_version,
        'file_name': relative_location,
    }

    return file_descriptor


def get_file_descriptor_described_by(schema_url, schema_version):
    return f'{schema_url.strip("/")}/system/{schema_version}/file_descriptor'


def get_relative_file_location(file_url):
    """The object name of the data file relative to the staging area's `data/` directory"""
    return file_url.rsplit('/')[-1]


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--size', required=True, help='Size of the file in bytes.')
    # parser.add_argument('--sha256', required=True, help='sha256 of the file.')
    # parser.add_argument('--crc32c', required=True, help='crc32c of the file.')
    # parser.add_argument(
    #     '--input_uuid', required=True, help='Input file UUID from the HCA Data Browser.'
    # )
    # parser.add_argument(
    #     '--file_path', required=True, help='Path to the loom/bam file to describe.'
    # )
    # parser.add_argument(
    #     '--pipeline-type', required=True, help='Type of pipeline(SS2 or Optimus)'
    # )
    # parser.add_argument(
    #     '--creation_time',
    #     required=True,
    #     help='Time of file creation, as reported by "gsutil ls -l"',
    # )
    # 
    # args = parser.parse_args()
# 
    # #schema_url = args.schema_url.strip('/')
# 
    # descriptor_entity_id = get_uuid5(
    #     f"{str(args.input_uuid)}{os.path.splitext(args.file_path)[1]}"
    # )
    #descriptor = build_file_descriptor(
    #    input_uuid=args.input_uuid,
    #    file_path=args.file_path,
    #    size=args.size,
    #    sha256=args.sha256,
    #    crc32c=args.crc32c,
    #    creation_time=args.creation_time,
    #    raw_schema_url=schema_url,
    #    file_descriptor_schema_version=args.file_descriptor_schema_version,
    #)

    #print(descriptor_entity_id)
#
    #file_version = descriptor['file_version']

    # Write descriptor to file
    #with open(f'{descriptor_entity_id}_{file_version}.json', 'w') as f:
    #    json.dump(descriptor, f, indent=2, sort_keys=True)

    foo = Descriptor(0,0,0,0,0,0)
    foo.dude()
    print(MIME_FORMATS)


if __name__ == '__main__':
    main()
