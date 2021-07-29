#!/usr/bin/env python
import argparse
import json
import os
import uuid
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map


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
            "update_date": "2021-05-24T12:00:00.000000Z"
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
    type = {
        "text": "analysis_protocol"
    }

    def __init__(
        self,
        input_uuid,
        method,
        pipeline_version,
        pipeline_type,
            workspace_version):

        self.input_uuid = input_uuid
        self.computational_method = method
        self.protocol_core = {
            "protocol_id": pipeline_version
        }
        self.pipeline_type = pipeline_type
        self.provenance = {
            "submission_date": workspace_version,
            "update_date": workspace_version
        }

        string_to_hash = json.dumps(self.get_json())
        entity_id = str(uuid.uuid5(format_map.NAMESPACE, string_to_hash)).lower()

        self.provenance['document_id'] = entity_id

    def __analysis_protocol__(self):
        return {
            "computational_method": self.computational_method,
            "describedBy": self.describedBy,
            "protocol_core": self.protocol_core,
            "provenance": self.provenance,
            "schema_type": self.schema_type,
            "type": self.type
        }

    def get_json(self):
        return self.__analysis_protocol__()

    @property
    def uuid(self):
        return self.input_uuid


# Entry point for unit tests
def test_build_analysis_protocol(
    input_uuid,
    method,
    pipeline_version,
    pipeline_type,
        workspace_version):

    test_analysis_protocol = AnalysisProtocol(
        input_uuid,
        method,
        pipeline_version,
        pipeline_type,
        workspace_version
    )
    return test_analysis_protocol.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_uuid', required=True, help='Input file UUID from the HCA Data Browser')
    parser.add_argument(
        '--method',
        required=True,
        help='Supposed to be method store url, for now can be url for wdl in skylab, or the name of'
        ' the analysis workflow.',
    )
    parser.add_argument(
        '--pipeline_version',
        required=True,
        help='The version of the pipeline, currently provided by the label of the adapter workflow'
        ' around the analysis workflow.',
    )
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline(SS2 or Optimus)')
    parser.add_argument('--workspace_version', required=True, help='Workspace version value i.e. timestamp for workspace')

    args = parser.parse_args()

    analysis_protocol = AnalysisProtocol(
        args.input_uuid,
        args.method,
        args.pipeline_version,
        args.pipeline_type,
        args.workspace_version
    )

    # Get the JSON content to be written
    analysis_protocol_json = analysis_protocol.get_json()

    # Generate filename based on UUID and version
    analysis_protocol_filename = f"{analysis_protocol_json['provenance']['document_id']}_{analysis_protocol_json['provenance']['submission_date']}.json"

    # Write analysis_protocol to file
    print('Writing analysis_protocol.json to disk...')
    if not os.path.exists("analysis_protocol"):
        os.mkdir("analysis_protocol")

    with open(f'analysis_protocol/{analysis_protocol_filename}', 'w') as f:
        json.dump(analysis_protocol_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
