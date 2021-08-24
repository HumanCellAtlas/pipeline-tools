#!/usr/bin/env python
import argparse
import json
import os

from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map


class ReferenceFile():
    """ReferenceFile class implements the creation of a json file that contains metadata for a given reference file

    The json file created should have the following form:

        {
            "assembly_type": "primary assembly",
            "describedBy": "https://schema.humancellatlas.org/type/file/3.2.0/reference_file",
            "file_core": {
                "file_name": "GRCm38.primary_assembly.genome.fa",
                "format": "fasta"
            },
            "genus_species": {
                "text": "Mus musculus"
            },
            "ncbi_taxon_id": 10090,
            "provenance": {
                "document_id": "c11000b1-2e69-532b-8c72-03dd4c9617d5",
                "submission_date": "2021-05-24T12:00:00.000000Z"
            },
            "reference_type": "genome sequence",
            "reference_version": "GencodeM21",
            "schema_type": "file",
            "schema_version": "3.2.0"
        }

    See https://schema.humancellatlas.org/type/file/3.2.0/reference_file for full spec
    """

    # All reference files will share these schema attributes
    describedBy = SCHEMAS["METADATA_REFERENCE"]["describedBy"]
    schema_type = SCHEMAS["METADATA_REFERENCE"]["schema_type"]
    schema_version = SCHEMAS["METADATA_REFERENCE"]["schema_version"]

    def __init__(
        self,
        file_path,
        input_uuid,
        genus_species,
        assembly_type,
        pipeline_type,
        ncbi_taxon_id,
        reference_type,
        workspace_version,
            reference_version):

        # Grab the extension of the reference file
        file_extension = os.path.splitext(file_path)[1]

        # Generate unique ID for file name where output will be saved
        file_id = format_map.get_uuid5(f"{input_uuid}reference_file{file_extension}")

        # Get the raw reference file name
        file_name = file_path.split("/")[-1]

        # Get the format of the file
        file_format = format_map.get_file_format(file_path)

        self.file_path = file_path
        self.file_id = file_id
        self.file_name = file_name
        self.input_uuid = input_uuid
        self.file_format = file_format
        self.genus_species = genus_species
        self.pipeline_type = pipeline_type
        self.ncbi_taxon_id = ncbi_taxon_id
        self.assembly_type = assembly_type
        self.file_extension = file_extension
        self.reference_type = reference_type
        self.workspace_version = workspace_version
        self.reference_version = reference_version

    def __reference_file__(self):
        return {
            "assembly_type" : self.assembly_type,
            "describedBy" : self.describedBy,
            "file_core" : {
                "file_name" : self.file_name,
                "format" : self.file_format
            },
            "genus_species" : {
                "text" : self.genus_species
            },
            "ncbi_taxon_id" : int(self.ncbi_taxon_id),
            "provenance" : {
                "document_id" : self.file_id,
                "submission_date" : self.workspace_version
            },
            "reference_type" : self.reference_type,
            "reference_version" : self.reference_version,
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version
        }

    def get_json(self):
        """Returns the json to be stored in /metadata/reference_file/ bucket"""
        return self.__reference_file__()

    @property
    def work_version(self):
        return self.workspace_version

    @property
    def id(self):
        return self.file_id


# Entry point for unit tests
def test_create_reference_file(
    file_path,
    input_uuid,
    genus_species,
    assembly_type,
    pipeline_type,
    ncbi_taxon_id,
    reference_type,
    workspace_version,
        reference_version):

    test_reference_file = ReferenceFile(
        file_path,
        input_uuid,
        genus_species,
        assembly_type,
        pipeline_type,
        ncbi_taxon_id,
        reference_type,
        workspace_version,
        reference_version)

    return test_reference_file.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--genus_species', required=True, help='The genus species')
    parser.add_argument('--file_path', required=True, help='Path to the reference file.')
    parser.add_argument('--workspace_version', required=True, help='Timestamp used to version the workspace',)
    parser.add_argument('--input_uuid', required=True, help='Input file UUID from the HCA Data Browser',)
    parser.add_argument('--reference_version', required=True, help='The genome version of the reference file',)
    parser.add_argument('--ncbi_taxon_id', required=True, help='The NCBI taxonomy id associated with the reference',)
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline (SS2 or Optimus)')
    parser.add_argument('--assembly_type', required=True, help='The genome assembly type',
                        choices=["primary assembly", "complete assembly", "patch assembly"],
                        )
    parser.add_argument('--reference_type', required=True, help='The type of the reference_file',
                        choices=["genome sequence", "transcriptome sequence", "annotation reference", "transcriptome index", "genome sequence index", ],
                        )

    args = parser.parse_args()

    # Create reference file object
    reference_file = ReferenceFile(
        args.file_path,
        args.input_uuid,
        args.genus_species,
        args.assembly_type,
        args.pipeline_type,
        args.ncbi_taxon_id,
        args.reference_type,
        args.workspace_version,
        args.reference_version)

    # Get the json content to be written
    reference_json = reference_file.get_json()

    # Create filename based on file id and version
    reference_json_filename = f'{reference_file.id}_{reference_file.work_version}.json'

    print("Writing reference file metadata to disk...")
    # Write the reference_file metadata
    with open(f"{reference_json_filename}", 'w') as f:
        json.dump(reference_json, f, indent=2, sort_keys=True)

    # Write the reference_file id to a file
    with open('reference_uuid.txt', 'w') as f:
        f.write(reference_file.id)


if __name__ == '__main__':
    main()
