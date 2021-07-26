#!/usr/bin/env python
import argparse
import json
import os
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map


class AnalysisFile():
    """AnalysisFile class implements the creation of a  json analysis file for Optimus and SS2 pipeline outputs


    HCA system consumes json files that describe the .bam/.loom outputs of Optimus and SS2 pipelines.


    The json files have the following form:


        {
        "describedBy": "https://schema.humancellatlas.org/type/file/6.2.0/analysis_file",
        "file_core": {
            "content_description": [],
            "file_name": "heart_1k_test_v2_S1_L001.bam",
            "format": "bam"
        },
        "provenance": {
            "document_id": "d649938b-be5f-58cf-be04-0c1e6381eb9e",
            "submission_date": "2021-05-24T12:00:00.000000Z"
        },
        "schema_type": "file"
        }


    See https://schema.humancellatlas.org/type/file/6.2.0/analysis_file for full spec
    """

    DCP2_MATRIX_CONTENT_DESCRIPTION = {
        "text": "DCP/2-generated matrix",
        "ontology": "data:3917",
        "ontology_label": "Count Matrix",
    }

    # All analysis files will share these attributes
    describedBy = SCHEMAS["ANALYSIS_FILE"]["describedBy"]
    schema_type = SCHEMAS["ANALYSIS_FILE"]["schema_type"]
    schema_version = SCHEMAS["ANALYSIS_FILE"]["schema_version"]

    def __init__(
        self,
        input_uuid,
        outputs_file,
        pipeline_type,
        file_path,
            creation_time):

        # Get the file version and file extension from params
        file_extension = os.path.splitext(file_path)[1]
        file_version = format_map.convert_datetime(creation_time)
        outputs = format_map.get_outputs(outputs_file)

        self.input_uuid = input_uuid
        self.file_extension = file_extension
        self.file_version = file_version
        self.pipeline_type = pipeline_type
        self.analysis_outputs = [
            {
                'provenance': {
                    'document_id': format_map.get_uuid5(
                        f"{str(self.input_uuid)}{os.path.splitext(output['file_path'])[1]}"
                    ),
                    'submission_date': format_map.convert_datetime(output['timestamp']),
                },
                'file_core': {
                    'file_name': output['file_path'].split('/')[-1],
                    'format': format_map.get_file_format(output['file_path']),
                    'content_description': [self.DCP2_MATRIX_CONTENT_DESCRIPTION]
                    if output['file_path'].endswith(".loom")
                    else [],
                },
            }
            for output in outputs
        ]
        return self.analysis_outputs

    def __analysis_file__(self):
        return {
            'describedBy': self.describedBy,
            'file_core': self.analysis_outputs['file_core'],
            'provenance': self.analysis_outputs['provenance'],
            'schema_type': self.schema_type
        }

    def get_json(self):
        return self.__analysis_file__()

    @property
    def uuid(self):
        return self.input_uuid
        
    @property
    def extension(self):
        return self.file_extension

    @property
    def version(self):
        return self.file_version


# Entry point for unit tests
def test_build_analysis_file(
    input_uuid,
    outputs_file,
    pipeline_type,
    file_path,
        creation_time):

    test_analysis_file = AnalysisFile(
        input_uuid,
        outputs_file,
        pipeline_type,
        file_path,
        creation_time
    )
    return test_analysis_file.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline(SS2 or Optimus)')
    parser.add_argument('--input_uuid', required=True, help='Input file UUID from the HCA Data Browser')
    parser.add_argument(
        '--outputs_file',
        required=True,
        help='Path to tsv file containing info about outputs.'
    )
    parser.add_argument('--file_path', required=True, help='Path to the loom/bam file to describe.')
    parser.add_argument(
        '--creation_time',
        required=True,
        help='Time of file creation, as reported by "gsutil ls -l"',
    )

    args = parser.parse_args()

    analysis_file = AnalysisFile(
        args.input_uuid,
        args.outputs_file,
        args.pipeline_type,
        args.file_path,
        args.creation_time
    )

    # Get the JSON content to be written
    analysis_file_json = analysis_file.get_json()

    # Generate unique analysis file UUID based on input file's UUID and extension
    analysis_file_json_id = format_map.get_uuid5(
        f"{analysis_file.input_uuid}{analysis_file.extension}")

    # Generate filename based on UUID and version
    analysis_file_json_filename = f"{analysis_file_json_id}_{analysis_file.version}.json"

    # Write to file
    with open(analysis_file_json_filename, 'w') as f:
        json.dump(analysis_file_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
