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
    
    def __init__(self, file_path, creation_time, method, pipeline_version, provenance_version):

        # Get the file version and file extension from params
        file_extension = os.path.splitext(file_path)[1]
        file_version = format_map.convert_datetime(creation_time)

        self.file_extension = file_extension
        self.file_version = file_version
        self.computational_method = method
        self.protocol_core = {
            "protocol_id": pipeline_version  # might need to do something to this input...
        }
        string_to_hash = json.dumps(self, sort_keys=True)
        entity_id = str(uuid.uuid5(format_map.NAMESPACE, string_to_hash)).lower()
        self.provenance = {
            "document_id": entity_id,
            "submission_date": provenance_version,
            "update_date": provenance_version
        }

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
        
    @property
    def extension(self):
        return self.file_extension

    @property
    def version(self):
        return self.file_version


def main():
    parser = argparse.ArgumentParser()
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
    parser.add_argument(
        '--version',
        required=True,
        help='A version (or timestamp) attribute shared across all workflows'
        'within an individual workspace.',
    )
    parser.add_argument('--file_path', required=True, help='Path to the loom/bam file to describe.')
    parser.add_argument(
        '--creation_time',
        required=True,
        help='Time of file creation, as reported by "gsutil ls -l"',
    )

    args = argparse.parse_args()

    analysis_protocol = AnalysisProtocol(
        args.method,
        args.pipeline_version,
        args.version
    )

    # Get the JSON content to be written
    analysis_protocol_json = analysis_protocol.get_json()

    # Generate unique analysis protocol UUID based on input file's UUID and extension
    analysis_protocol_id = format_map.get_uuid5(
        f"{analysis_protocol.input_uuid}{analysis_protocol.extension}")

    # Generate filename based on UUID and version
    analysis_protocol_filename = f"{analysis_protocol_json}_{analysis_protocol.version}.json"

    # Write to file
    with open(analysis_protocol_filename, 'w') as f:
        json.dump(analysis_protocol_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()