#!/usr/bin/env python
import os
import json
import argparse
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map
from pipeline_tools.shared.exceptions import UnsupportedPipelineType
from distutils.util import strtobool


class AnalysisFile():
    """AnalysisFile class implements the creation of a  json analysis file for Optimus and SS2 pipeline outputs


    HCA system consumes json files that describe the .bam/.loom outputs of Optimus and SS2 pipelines.


    The json files have the following form:


        {
        "describedBy": "https://schema.humancellatlas.org/type/file/6.3.0/analysis_file",
        "file_core": {
            "content_description": [],
            "file_name": "heart_1k_test_v2_S1_L001.bam",
            "file_source" : "DCP/2 Analysis
            "format": "bam"
        },
        "provenance": {
            "document_id": "d649938b-be5f-58cf-be04-0c1e6381eb9e",
            "submission_date": "2021-05-24T12:00:00.000000Z"
        },
        "schema_type": "file"
        }


    See https://schema.humancellatlas.org/type/file/6.3.0/analysis_file for full spec
    """

    # Content description for analysis files
    LOOM_CONTENT_DESCRIPTION = SCHEMAS["ANALYSIS_FILE"]["loom_content_description"]
    BAM_CONTENT_DESCRIPTION = SCHEMAS["ANALYSIS_FILE"]["bam_content_description"]
    BAI_CONTENT_DESCRIPTION = SCHEMAS["ANALYSIS_FILE"]["bai_content_description"]

    # All analysis files will share these attributes
    describedBy = SCHEMAS["ANALYSIS_FILE"]["describedBy"]
    schema_type = SCHEMAS["ANALYSIS_FILE"]["schema_type"]
    schema_version = SCHEMAS["ANALYSIS_FILE"]["schema_version"]
    file_source = SCHEMAS["ANALYSIS_FILE"]["file_source"]

    def __init__(
        self,
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
        project_level=False,
        ss2_bam_file="",
            ss2_bai_file=""):

        self.input_file = input_file
        self.input_uuid = input_uuid
        self.project_level = project_level
        self.pipeline_type = pipeline_type
        self.ss2_bam_file = ss2_bam_file
        self.ss2_bai_file = ss2_bai_file
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
                "schema_type": self.schema_type,
                "schema_version": self.schema_version
            }
        elif "loom" == file_type:
            return {
                "describedBy": self.describedBy,
                "file_core": self.loom_output["file_core"],
                "provenance": self.loom_output["provenance"],
                "schema_type": self.schema_type,
                "schema_version": self.schema_version
            }
        elif "bai" == file_type:
            return {
                "describedBy": self.describedBy,
                "file_core": self.bai_output["file_core"],
                "provenance": self.bai_output["provenance"],
                "schema_type": self.schema_type,
                "schema_version": self.schema_version
            }
        else:
            return {}

    # Get analysis file json based on file type
    def get_json(self, file_type):
        return self.__analysis_file__(file_type)

    def get_outputs_json(self):
        """Get the outputs.json array based on the project level and pipeline type. If project level then only return loom

        Returns:
            outputs(array): array of /metadata/analysis_file/*
        """

        if self.project_level:
            return [self.get_json("loom")]

        pipeline_type = self.pipeline_type.lower()

        if pipeline_type == "optimus":
            return [self.get_json("loom"), self.get_json("bam")]
        elif pipeline_type == "ss2":
            return [self.get_json("bam"), self.get_json("bai")]
        else:
            raise UnsupportedPipelineType("Pipeline must be optimus or ss2")
            return {}

    # Get file details by file name
    def __get_file_save_id__(self, file_name):

        entity_type = format_map.get_entity_type(file_name)
        file_extension = os.path.splitext(file_name)[1]

        # Generate unique file UUID5 by hashing
        # This is deterministic and should always produce the same output given the same input
        # file_save_id is used to save the analysis file - {file_save_id}_{workspace_verison}.json
        self.file_save_id = format_map.get_file_entity_id(self.input_uuid, entity_type, file_extension)

        return self.file_save_id

    def __get_content_by_type__(self):
        """Get JSON info for bam and loom analysis files and save"""
        outputs = self.outputs
        for output in self.outputs:
            if outputs[output].endswith(".loom"):
                # Generate loom output
                self.loom_output = {
                    "provenance": {
                        "document_id": self.__get_file_save_id__(outputs[output]),
                        "submission_date": self.workspace_version,
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "file_source": self.file_source,
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": [self.LOOM_CONTENT_DESCRIPTION]
                    }
                }
                if self.project_level:
                    self.loom_output["provenance"]["submitter_id"] = "e67aaabe-93ea-564a-aa66-31bc0857b707"
            elif outputs[output].endswith(".bam"):
                # Generate bam output
                self.bam_output = {
                    "provenance": {
                        "document_id": self.__get_file_save_id__(outputs[output]),
                        "submission_date": self.workspace_version
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "file_source": self.file_source,
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": [self.BAM_CONTENT_DESCRIPTION]
                    }
                }
            elif outputs[output].endswith(".bai"):
                # Generate bai output
                self.bai_output = {
                    "provenance": {
                        "document_id": self.__get_file_save_id__(outputs[output]),
                        "submission_date": self.workspace_version
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "file_source": self.file_source,
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": [self.BAI_CONTENT_DESCRIPTION]
                    }
                }

    def __pipeline_outputs__(self):
        """Return dict of the outputs that were produced by the pipeline (single loom for project, metadata.json for intermediate)

        If pipeline is ss2 then we use localized intermediate level bai and bam file

        Returns:
            outputs(dict): output produced by the pipeline run
        """

        if self.project_level:
            print("Using project-level outputs....")
            return {"project_level.loom" : self.input_file}

        # If pipeline type is optimus then we can can get the intermediate outputs from metadata.json
        if self.pipeline_type.lower() == "optimus":
            # If intermediate then get the bam/loom outputs from metadata.json
            metadata_json = format_map.get_workflow_metadata(self.input_file)
            return metadata_json["outputs"]

        # If pipeline type is ss2 then create 'outputs' by adding the localized file to an object
        elif self.pipeline_type.lower() == "ss2":
            return {"ss2_intermediate.bai": self.ss2_bai_file, "ss2_intermediate.bam": self.ss2_bam_file}

        raise UnsupportedPipelineType("Pipeline must be optimus or ss2")

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
    parser.add_argument("--project_level", required=True, type=lambda x: bool(strtobool(x)), help="Boolean representing project level vs intermediate level")
    parser.add_argument("--input_file", required=False, help="Path to metadata.json for intermediate level, path to merged loom file for project level")
    parser.add_argument("--ss2_bam_file", required=False, help="Localized path to intermediate ss2 bam file")
    parser.add_argument("--ss2_bai_file", required=False, help="Localized path to intermediate ss2 bai file")

    args = parser.parse_args()

    analysis_file = AnalysisFile(
        args.input_uuid,
        args.input_file,
        args.pipeline_type,
        args.workspace_version,
        args.project_level,
        args.ss2_bam_file,
        args.ss2_bai_file
    )

    # Write analysis file for each file type
    print("Writing outputs.json to disk...")
    analysis_file_json = analysis_file.get_outputs_json()
    with open("outputs.json", "w") as f:
        json.dump(analysis_file_json, f, indent=2, sort_keys=True)

    print("Writing analysis_file output(s) to disk...")
    for output in analysis_file_json:
        file_save_id = output["provenance"]["document_id"]
        with open(f"{file_save_id}_{analysis_file.work_version}.json", "w") as f:
            json.dump(output, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
