#!/usr/bin/env python
import argparse
import json
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map
from distutils.util import strtobool


class AnalysisProtocol():
    """AnalysisProtocol class implements the creation of a json analysis protocol for Optimus and SS2 pipeline outputs


    HCA system consumes json files that describe the optimus analysis outputs of Optimus and SS2 pipelines.


    The json files have the following form:


        {
        "computational_method": "https://dockstore.org/workflows/github.com/broadinstitute/warp/Optimus:Optimus_v4.2.3",
        "describedBy": "https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol",
        "protocol_core": {
            "protocol_id": "optimus_v4.2.3"
        },
        "provenance": {
            "document_id": "f2cdb4e5-b439-5cdf-ac41-161ff39d5790",
            "submission_date": "2021-05-24T12:00:00.000000Z",
        },
        "schema_type": "protocol",
        "type": {
            "text": "analysis_protocol"
        }
        }


    See https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol for full spec
    """

    # All analysis files will share these attributes
    describedBy = SCHEMAS["ANALYSIS_PROTOCOL"]["describedBy"]
    schema_type = SCHEMAS["ANALYSIS_PROTOCOL"]["schema_type"]
    schema_version = SCHEMAS["ANALYSIS_PROTOCOL"]["schema_version"]

    def __init__(
        self,
        input_uuid,
        pipeline_type,
        pipeline_version,
        workspace_version,
            project_level=False):

        if project_level:
            self.type = {
                "text": "analysis; merge matrices"
            }
        else:
            self.type = {
                "text": "analysis_protocol"
            }

        self.input_uuid = input_uuid
        self.pipeline_type = pipeline_type
        self.work_version = workspace_version
        self.pipeline_version = pipeline_version
        self.computational_method = f"https://dockstore.org/workflows/github.com/broadinstitute/warp/{self.pipeline_type}:{self.pipeline_version}"
        self.protocol_core = {
            "protocol_id": self.pipeline_version
        }

    def __analysis_protocol__(self):
        return {
            "computational_method": self.computational_method,
            "describedBy": self.describedBy,
            "protocol_core": self.protocol_core,
            "provenance": self.__get_provenance(),
            "schema_type": self.schema_type,
            "schema_version" : self.schema_version,
            "type": self.type
        }

    def get_json(self):
        return self.__analysis_protocol__()

    def __get_hash_input(self):
        """Hashing analysis protocol object without provenance to be used in get_provenance"""

        return {
            "computational_method": self.computational_method,
            "describedBy": self.describedBy,
            "protocol_core": self.protocol_core,
            "schema_type": self.schema_type,
            "type": self.type
        }

    def __get_provenance(self):
        """Using hashed object from get_hash_input to generate entity_id and complete provenance"""

        string_to_hash = json.dumps(self.__get_hash_input())
        entity_id = format_map.get_uuid5(string_to_hash)

        return {
            "document_id": entity_id,
            "submission_date": self.workspace_version,
        }

    @property
    def entity_id(self):
        provenance = self.__get_provenance()
        entity_id = provenance["document_id"]
        return entity_id

    @property
    def workspace_version(self):
        return self.work_version


# Entry point for unit tests
def test_build_analysis_protocol(
    input_uuid,
    pipeline_type,
    pipeline_version,
    workspace_version,
        project_level=False):

    test_analysis_protocol = AnalysisProtocol(
        input_uuid,
        pipeline_type,
        pipeline_version,
        workspace_version,
        project_level
    )
    return test_analysis_protocol.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_uuid", required=True, help="Input file UUID from the HCA Data Browser")
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline (SS2, Optimus or OptimusPostProcessing)")
    parser.add_argument("--workspace_version", required=True, help="Workspace version value i.e. timestamp for workspace")
    parser.add_argument("--project_level", required=True, type=lambda x: bool(strtobool(x)), help="Boolean representing project level vs intermediate level")
    parser.add_argument("--pipeline_version", required=True, help="The version of the pipeline, currently provided by the label of the adapter workflow around the analysis workflow.")

    args = parser.parse_args()

    analysis_protocol = AnalysisProtocol(
        args.input_uuid,
        args.pipeline_type,
        args.pipeline_version,
        args.workspace_version,
        args.project_level
    )

    # Get the JSON content to be written
    analysis_protocol_json = analysis_protocol.get_json()

    # Determine file name
    analysis_protocol_filename = f"{analysis_protocol.entity_id}_{analysis_protocol.workspace_version}.json"

    # Write analysis_protocol to file
    print("Writing analysis_protocol.json to disk...")
    with open(f"{analysis_protocol_filename}", "w") as f:
        json.dump(analysis_protocol_json, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
