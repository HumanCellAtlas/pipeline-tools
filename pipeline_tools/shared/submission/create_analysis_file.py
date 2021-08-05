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

        self.input_uuid = input_uuid
        self.project_level = project_level
        self.pipeline_type = pipeline_type
        self.workspace_version = workspace_version

        if self.project_level:
            outputs = [input_file]
            self.input_uuid = format_map.get_uuid5(self.input_uuid)
        else:
            # use metadata.json file to retrieve outputs
            metadata_json = format_map.get_workflow_metadata(input_file)
            outputs = metadata_json["outputs"]

        # get content for each file type
        self.__get_outputs_by_type(outputs)

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

    # Get output json for both types of analysis file
    def get_outputs_json(self):
        return [self.get_json("bam"), self.get_json("loom")]

    # Get file details by file name
    def get_file_details(self, file_name):
        # Get the type of file currently being processed
        entity_type = format_map.get_entity_type(file_name)

        # Grab the extension of the file thats been submitted
        if self.project_level: 
            file_extension = format_map.get_file_format(file_name)
        else:
            file_extension = os.path.splitext(file_name)[1]

        # Grab the raw name of the file thats been submitted
        file_name = file_name.rsplit("/")[-1]

        # Generate unique file UUID5 by hashing twice
        # This is deterministic and should always produce the same output given the same input
        # file_save_id is used to save the analysis file - {file_save_id}_{workspace_verison}.json
        file_save_id = format_map.get_uuid5(f"{self.input_uuid}{entity_type}{file_extension}")

        return {
            "uuid": self.input_uuid,
            "entity_type": entity_type,
            "file_extension": file_extension,
            "file_name": file_name,
            "file_save_id": file_save_id
        }

    # generate content for each file type
    def __get_outputs_by_type(self, outputs):
        for output in outputs:
            if ".loom" in output:
                if self.project_level:
                    # Generate loom output
                    loom_file_details = self.get_file_details(output)
                    self.loom_output = {
                        "provenance": {
                            "document_id": loom_file_details["file_save_id"],
                            "submission_date": self.workspace_version,
                            "submitter_id": "e67aaabe-93ea-564a-aa66-31bc0857b707"
                        },
                        "file_core": {
                            "file_name": loom_file_details["file_name"],
                            "format": format_map.get_file_format(output),
                            "content_description": [self.DCP2_MATRIX_CONTENT_DESCRIPTION]
                        }
                    }
                else:
                    # Generate loom output
                    loom_file_details = self.get_file_details(outputs[output])
                    self.loom_output = {
                        "provenance": {
                            "document_id": loom_file_details["file_save_id"],
                            "submission_date": self.workspace_version
                        },
                        "file_core": {
                            "file_name": outputs[output].split("/")[-1],
                            "format": format_map.get_file_format(outputs[output]),
                            "content_description": [self.DCP2_MATRIX_CONTENT_DESCRIPTION]
                        }
                    }

            elif ".bam" in output:
                # Generate bam output
                bam_file_details = self.get_file_details(outputs[output])
                self.bam_output = {
                    "provenance": {
                        "document_id": bam_file_details["file_save_id"],
                        "submission_date": self.workspace_version
                    },
                    "file_core": {
                        "file_name": outputs[output].split("/")[-1],
                        "format": format_map.get_file_format(outputs[output]),
                        "content_description": []
                    }
                }

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
    return test_analysis_file.get_json("loom")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline(SS2 or Optimus)")
    parser.add_argument("--input_uuid", required=True, help="Input file UUID from the HCA Data Browser")
    parser.add_argument("--workspace_version", required=True, help="Workspace version value i.e. timestamp for workspace")
    parser.add_argument("--project_level", type=bool, required=False, help="Boolean representing project level vs intermediate level")
    parser.add_argument(
        "--input_file",
        required=True,
        help="Path to json file containing metadata for intermediate level, path to intermediate bam analysis file for project level"
    )

    args = parser.parse_args()

    analysis_file = AnalysisFile(
        args.input_uuid,
        args.input_file,
        args.pipeline_type,
        args.workspace_version,
        args.project_level
    )

    # Write analysis file for each file type
    print("Writing analysis_file output(s) json to disk...")
    if not os.path.exists("analysis_files"):
        os.mkdir("analysis_files")

    if args.project_level:
        # Get the project level JSON content to be written
        analysis_file_json = analysis_file.get_json("loom")
        file_save_id = analysis_file_json["provenance"]["document_id"]
        with open(f"analysis_files/{file_save_id}_{analysis_file.work_version}.json", "w") as f:
            json.dump(analysis_file_json, f, indent=2, sort_keys=True)
    else:
        # Get the intermediate level JSON content to be written
        analysis_file_json = analysis_file.get_outputs_json()

        # Write outputs to file
        print("Writing outputs.json to disk...")
        with open("outputs.json", "w") as f:
            json.dump(analysis_file_json, f, indent=2, sort_keys=True)

        for output in analysis_file_json:
            file_save_id = output["provenance"]["document_id"]
            with open(f"analysis_files/{file_save_id}_{analysis_file.work_version}.json", "w") as f:
                json.dump(output, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
