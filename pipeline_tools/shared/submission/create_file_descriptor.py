#!/usr/bin/env python
import argparse
import json
import mimetypes
import os

from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map


class Descriptor():
    """Descriptor class implements the creation of a json file descriptor for Optimus and SS2 pipeline outputs

    HCA system consumes json files that describe the .bam/.loom/.fa outputs of Optimus and SS2 pipelines

    The json files have the following form:

        {
            "describedBy": "https://schema.humancellatlas.org/system/2.0.0/file_descriptor",
            "schema_type": "file_descriptor",
            "content_type": "application/vnd.loom",
            "size": 21806469
            "sha256": "c12d50051a5b8820124f596529c6cbdc0280b71883acbde08e30311cdb30edfa",
            "crc32c": "e598a0f6",
            "file_id": "317a2bfc-ea58-50ae-b64e-4c58d0c01a74",
            "file_name": "heart_1k_test_v2_S1_L001.loom",
            "file_version": "2021-07-08T17:22:45.000000Z",
        }

    See https://schema.humancellatlas.org/system/2.0.0/file_descriptor for full spec
    """

    # Add additional custom mimetypes
    [mimetypes.add_type(entry[0], entry[1]) for entry in format_map.MIME_FORMATS]

    # All descriptors will share these schema attributes
    describedBy = SCHEMAS["FILE_DESCRIPTOR"]["describedBy"]
    schema_type = SCHEMAS["FILE_DESCRIPTOR"]["schema_type"]
    schema_version = SCHEMAS["FILE_DESCRIPTOR"]["schema_version"]

    def __init__(
        self,
        size,
        sha256,
        crc32c,
        input_uuid,
        file_path,
        pipeline_type,
        creation_time,
            workspace_version):

        # Grab timestamp that adheres to schema
        file_version = format_map.convert_datetime(creation_time)

        # Grab the mimetype of the file thats been submitted
        content_type = mimetypes.guess_type(file_path)[0] or 'application/unknown'

        # Get the type of file currently being processed
        entity_type = format_map.get_entity_type(file_path)

        # Grab the extension of the file thats been submitted
        file_extension = os.path.splitext(file_path)[1]

        # Grab the raw name of the file thats been submitted
        file_name = file_path.rsplit("/")[-1]

        # Generate unique file UUID5 by hashing twice
        # This is deterministic and should always produce the same output given the same input
        #
        # file_entity_id is the ID for the bam/loom/bai/fa being processed
        #
        # file_id is the ID for the descriptor file being created
        file_entity_id = format_map.get_uuid5(f"{input_uuid}{entity_type}{file_extension}")
        file_id = format_map.get_uuid5(file_entity_id)

        self.size = int(size)
        self.crc32c = crc32c
        self.sha256 = sha256
        self.file_id = file_id
        self.file_path = file_path
        self.file_name = file_name
        self.input_uuid = input_uuid
        self.entity_type = entity_type
        self.file_version = file_version
        self.content_type = content_type
        self.creation_time = creation_time
        self.pipeline_type = pipeline_type
        self.file_extension = file_extension
        self.file_entity_id = file_entity_id
        self.workspace_version = workspace_version

    def __descriptor__(self):
        return {
            "describedBy" : self.describedBy,
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version,
            "content_type" : self.content_type,
            "size" : self.size,
            "sha256" : self.sha256,
            "crc32c" : self.crc32c,
            "file_id" : self.file_id,
            "file_version" : self.file_version,
            "file_name" : self.file_name
        }

    def get_json(self):
        """Returns the json to be stored in the /descriptors/*/ bucket
            * = analysis_file or reference_file
        """
        return self.__descriptor__()

    @property
    def uuid(self):
        return self.input_uuid

    @property
    def extension(self):
        return self.file_extension

    @property
    def work_version(self):
        return self.workspace_version

    @property
    def entity(self):
        return self.entity_type

    @property
    def entity_id(self):
        return self.file_entity_id


# Entry point for unit tests
def test_build_file_descriptor(
    size,
    sha256,
    crc32c,
    input_uuid,
    file_path,
    pipeline_type,
    creation_time,
        workspace_version):

    test_file_descriptor = Descriptor(
        size,
        sha256,
        crc32c,
        input_uuid,
        file_path,
        pipeline_type,
        creation_time,
        workspace_version)

    return test_file_descriptor.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", required=True, type=int, help="size of the file in Mb")
    parser.add_argument('--sha256', required=True, help='sha256 of the file.')
    parser.add_argument('--crc32c', required=True, help='crc32c of the file.')
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline (SS2 or Optimus)')
    parser.add_argument('--file_path', required=True, help='Path to the loom/bam file to describe.')
    parser.add_argument('--input_uuid', required=True, help='Input file UUID from the HCA Data Browser.')
    parser.add_argument('--creation_time', required=True, help='Time of file creation, as reported by "gsutil ls -l"',)
    parser.add_argument('--workspace_version', required=True, help='Workspace version value i.e. timestamp for workspace')

    args = parser.parse_args()

    # Create file descriptor object
    file_descriptor = Descriptor(
        args.size,
        args.sha256,
        args.crc32c,
        args.input_uuid,
        args.file_path,
        args.pipeline_type,
        args.creation_time,
        args.workspace_version)

    # Get the JSON content to be written
    descriptor_json = file_descriptor.get_json()

    # Generate filename based on UUID and version
    descriptor_json_filename = f"{file_descriptor.entity_id}_{file_descriptor.work_version}.json"

    print(f"Writing {file_descriptor.extension} descriptor file to disk...")
    with open(descriptor_json_filename, 'w') as f:
        json.dump(descriptor_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
