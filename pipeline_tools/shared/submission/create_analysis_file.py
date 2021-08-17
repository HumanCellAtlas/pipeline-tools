#!/usr/bin/env python
import os
import json
import argparse
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

    # Content description for bam files
    DCP2_MATRIX_CONTENT_DESCRIPTION = SCHEMAS["ANALYSIS_FILE"]["loom_content_description"]

    # All analysis files will share these attributes
    describedBy = SCHEMAS["ANALYSIS_FILE"]["describedBy"]
    schema_type = SCHEMAS["ANALYSIS_FILE"]["schema_type"]
    schema_version = SCHEMAS["ANALYSIS_FILE"]["schema_version"]

    def __init__(
        self,
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
            project_level=False):

        self.input_file = input_file
        self.input_uuid = input_uuid
        self.project_level = project_level
        self.pipeline_type = pipeline_type
        self.workspace_version = workspace_version
        self.outputs = self.__pipeline_outputs__()

        # Get content based on file type (loom or bam)
        self.__get_content_by_type__()

    # create analysis file based on file type
    def __analysis_file__(self, file_type):
        if "bam" == file_type:
            return {
                "describedBy": self.describedBy,
                "file_core": self.bam_output["file_core"],
                "provenance": self.bam_output["provenance"],
                "schema_type": self.schema_type
            }
        elif "loom" == file_type:
            return {
                "describedBy": self.describedBy,
                "file_core": self.loom_output["file_core"],
                "provenance": self.loom_output["provenance"],
                "schema_type": self.schema_type
            }
        else:
            return {}

    # Get analysis file json based on file type
    def get_json(self, file_type):
        return self.__analysis_file__(file_type)

    def get_outputs_json(self):
        """Get the outputs.json array based on the project level. If project level then only return loom

        Returns:
            outputs(array): array of /metadata/analysis_file/*
        """

        if self.project_level:
            return [self.get_json("loom")]
        return [self.get_json("loom"), self.get_json("bam")]

    # Get file details by file name
    def __get_file_save_id__(self, file_name):

        file_name = file_name.rsplit("/")[-1]
        entity_type = format_map.get_entity_type(file_name)
        file_extension = format_map.get_file_format(file_name)

        # Generate unique file UUID5 by hashing
        # This is deterministic and should always produce the same output given the same input
        # file_save_id is used to save the analysis file - {file_save_id}_{workspace_verison}.json
        self.file_save_id = format_map.get_uuid5(f"{self.input_uuid}{entity_type}{file_extension}")

        return self.file_save_id

    def __get_content_by_type__(self):
        """Get JSON info for bam and loom analysis files and save"""
        outputs = self.outputs
        for output in self.outputs:
            if ".loom" in output:
                # Generate loom output
                self.loom_output = {
                    "provenance": {
                        "document_id": self.__get_file_save_id__(outputs[output]),
                        "submission_date": self.workspace_version,
                        "submitter_id": "e67aaabe-93ea-564a-aa66-31bc0857b707" if self.project_level else ""
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": [self.DCP2_MATRIX_CONTENT_DESCRIPTION]
                    }
                }
            elif ".bam" in output:
                # Generate bam output
                self.bam_output = {
                    "provenance": {
                        "document_id": self.__get_file_save_id__(outputs[output]),
                        "submission_date": self.workspace_version
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": []
                    }
                }

    def __pipeline_outputs__(self):
        """Return dict of the outputs that were produced by the pipeline (single loom for project, metadata.json for intermediate)

        Returns:
            outputs(dict): output produced by the pipeline run
        """

        if self.project_level:
            return {"project_level.loom" : self.input_file}

        # If intermediate then get the bam/loom outputs from metadata.json
        metadata_json = format_map.get_workflow_metadata(self.input_file)
        return metadata_json["outputs"]

    @property
    def uuid(self):
        return self.input_uuid

    @property
    def work_version(self):
        return self.workspace_version


# Entry point for unit tests
def test_build_analysis_file(
    input_uuid,
    input_file,
    pipeline_type,
    workspace_version,
        project_level=False):

    test_analysis_file = AnalysisFile(
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
        project_level
    )
    return test_analysis_file.get_outputs_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline(SS2 or Optimus)")
    parser.add_argument("--input_uuid", required=True, help="Input file UUID from the HCA Data Browser")
    parser.add_argument("--workspace_version", required=True, help="Workspace version value i.e. timestamp for workspace")
    parser.add_argument("--project_level", type=bool, default=False, required=False, help="Boolean representing project level vs intermediate level")
    parser.add_argument("--input_file", required=True, help="Path to metadata.json for intermediate level, path to merged loom file for project level")

    args = parser.parse_args()

    analysis_file = AnalysisFile(
        args.input_uuid,
        args.input_file,
        args.pipeline_type,
        args.workspace_version,
        args.project_level
    )

    # Write analysis file for each file type
    if not os.path.exists("analysis_files"):
        os.mkdir("analysis_files")

    print("Writing outputs.json to disk...")
    analysis_file_json = analysis_file.get_outputs_json()
    with open("outputs.json", "w") as f:
        json.dump(analysis_file_json, f, indent=2, sort_keys=True)

    print("Writing analysis_file output(s) to disk...")
    for output in analysis_file_json:
        file_save_id = output["provenance"]["document_id"]
        with open(f"analysis_files/{file_save_id}_{analysis_file.work_version}.json", "w") as f:
            json.dump(output, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
