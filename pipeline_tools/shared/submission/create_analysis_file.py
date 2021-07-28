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
        metadata_json,
        pipeline_type,
            workspace_version):

        metadata_json = format_map.get_outputs(metadata_json)
        outputs = metadata_json['outputs']
        timestamp = format_map.convert_datetime(metadata_json['start'])

        self.input_uuid = input_uuid
        self.pipeline_type = pipeline_type
        self.workspace_version = workspace_version
        for output in outputs:
            if '.loom' in output:
                self.loom_output = {
                    'provenance': {
                        'document_id': format_map.get_uuid5(
                            f"{str(self.input_uuid)}{os.path.splitext(outputs[output])[1]}"
                        ),
                        'submission_date': timestamp
                    },
                    'file_core': {
                        'file_name': outputs[output].split('/')[-1],
                        'format': format_map.get_file_format(outputs[output]),
                        'content_description': []
                    }
                }
            elif '.bam' in output:
                self.bam_output = {
                    'provenance': {
                        'document_id': format_map.get_uuid5(
                            f"{str(self.input_uuid)}{os.path.splitext(outputs[output])[1]}"
                        ),
                        'submission_date': timestamp
                    },
                    'file_core': {
                        'file_name': outputs[output].split('/')[-1],
                        'format': format_map.get_file_format(outputs[output]),
                        'content_description': [self.DCP2_MATRIX_CONTENT_DESCRIPTION]
                    }
                }

    def __analysis_file__(self, file_type):
        if 'bam' == file_type:
            return {
                'describedBy': self.describedBy,
                'file_core': self.bam_output['file_core'],
                'provenance': self.bam_output['provenance'],
                'schema_type': self.schema_type
            }
        elif 'loom' == file_type:
            return {
                'describedBy': self.describedBy,
                'file_core': self.loom_output['file_core'],
                'provenance': self.loom_output['provenance'],
                'schema_type': self.schema_type
            }
        else:
            return {}

    def get_json(self, file_type):
        return self.__analysis_file__(file_type)

    def get_outputs_json(self):
        return [self.get_json('bam'), self.get_json('loom')]

    @property
    def uuid(self):
        return self.input_uuid


# Entry point for unit tests
def test_build_analysis_file(
    input_uuid,
    metadata_json,
    pipeline_type,
        workspace_version):

    test_analysis_file = AnalysisFile(
        input_uuid,
        metadata_json,
        pipeline_type,
        workspace_version
    )
    return test_analysis_file.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline(SS2 or Optimus)')
    parser.add_argument('--input_uuid', required=True, help='Input file UUID from the HCA Data Browser')
    parser.add_argument(
        '--metadata_json',
        required=True,
        help='Path to json file containing metadata.'
    )
    parser.add_argument('--workspace_version', required=True, help='Workspace version value i.e. timestamp for workspace')

    args = parser.parse_args()

    analysis_file = AnalysisFile(
        args.input_uuid,
        args.metadata_json,
        args.pipeline_type,
        args.workspace_version
    )

    # Get the JSON content to be written
    analysis_file_json = analysis_file.get_outputs_json()

    # Write outputs to file
    print('Writing outputs.json to disk...')
    with open('outputs.json', 'w') as f:
        json.dump(analysis_file_json, f, indent=2, sort_keys=True)

    print('Writing analysis_file output(s) json to disk...')
    if not os.path.exists("analysis_files"):
        os.mkdir("analysis_files")

    for output in analysis_file_json:
        entity_id = output['provenance']['document_id']
        version = output['provenance']['submission_date']
        with open(f'analysis_files/{entity_id}_{version}.json', 'w') as f:
            json.dump(output, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
