#!/usr/bin/env python
import argparse
import json
import os
import arrow
from pipeline_tools.shared.submission.create_analysis_protocol import AnalysisProtocol
import uuid
from csv import DictReader
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map

class AnalysisProcess():
    """AnalysisProcess class implements the creation of a  json analysis process for Optimus and SS2 pipeline outputs


    HCA system consumes json files that describe the optimus analysis outputs of Optimus and SS2 pipelines.


    The json files have the following form:


    {
    "analysis_run_type": "run",
    "describedBy": "https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process",
    "inputs": [
        {
        "parameter_name": "r1_fastq",
        "parameter_value": "['gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/heart_1k_test_v2_S1_L001_R1_001.fastq.gz']"
        }
    ],
    "process_core": {
        "process_id": "151fe264-c670-4c77-a47c-530ff6b3127b"
    },
    "provenance": {
        "document_id": "151fe264-c670-4c77-a47c-530ff6b3127b",
        "submission_date": "2021-05-24T12:00:00.000000Z"
    },
    "reference_files": [
        "c11000b1-2e69-532b-8c72-03dd4c9617d5"
    ],
    "schema_type": "process",
    "tasks": [
        {
        "cpus": 1,
        "disk_size": "local-disk 1 HDD",
        "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
        "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CalculateCellMetrics/shard-0/stderr",
        "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CalculateCellMetrics/shard-0/stdout",
        "memory": "43.9453125 GB",
        "start_time": "2021-07-08T17:08:08.740Z",
        "stop_time": "2021-07-08T17:11:46.643Z",
        "task_name": "CalculateCellMetrics",
        "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
        }
    ],
    "timestamp_start_utc": "2021-07-08T16:08:06.619Z",
    "timestamp_stop_utc": "2021-07-08T17:23:57.332Z",
    "type": {
        "text": "analysis"
    }


    See https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process for full spec
    """

    # All analysis files will share these attributes
    describedBy = SCHEMAS["ANALYSIS_PROCESS"]["describedBy"]
    schema_type = SCHEMAS["ANALYSIS_PROCESS"]["schema_type"]
    schema_version = SCHEMAS["ANALYSIS_PROCESS"]["schema_version"]
    type = {
        "text": "analysis_protocol"
    }

    def __init__(
            self,
            input_uuid,
            file_path,
            creation_time,
            run_type,
            inputs,
            pipeline_version,
            version,
            references,
            metadata_json,
            pipeline_type):

        # Get the file version and file extension from params
        file_extension = os.path.splitext(file_path)[1]
        file_version = format_map.convert_datetime(creation_time)

        workflow_metadata = format_map.get_workflow_metadata(metadata_json)
        workflow_tasks = format_map.get_workflow_tasks(workflow_metadata)
        timestamp_start_utc = format_map.format_timestamp(workflow_metadata.get('start'))
        timestamp_stop_utc = format_map.format_timestamp(workflow_metadata.get('end'))

        string_to_hash = json.dumps(self, sort_keys=True)
        entity_id = str(uuid.uuid5(format_map.NAMESPACE, string_to_hash)).lower()

        provenance = {
            "document_id": entity_id,
            "submission_date": version,
            "update_date": version
        }
        process_core = {"process_id": pipeline_version}
        type = {
            "text": run_type
        }

        self.input_uuid = input_uuid
        self.file_extension = file_extension
        self.file_version = file_version
        self.analysis_run_type = run_type
        self.inputs = inputs
        self.process_core = process_core
        self.provenance = provenance
        self.reference_files = references
        self.tasks = workflow_tasks
        self.timestamp_start_utc = timestamp_start_utc
        self.timestamp_stop_utc = timestamp_stop_utc
        self.type = type
        self.pipeline_type = pipeline_type

    def __analysis_process__(self):
        return {
            "analysis_run_type" : self.analysis_run_type,
            "describedBy" : self.describedBy,
            "inputs" : self.inputs,
            "process_core" : self.process_core,
            "provenance" : self.provenance,
            "reference_files" : self.reference_files,
            "schema_type" : self.schema_type,
            "tasks" : self.tasks,
            "timestamp_start_utc" : self.timestamp_start_utc,
            "timestamp_stop_utc" : self.timestamp_stop_utc,
            "type": self.type
        }

    def get_json(self):
        return self.__analysis_process__()

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
def test_build_analysis_process(
    input_uuid,
    file_path,
    creation_time,
    run_type,
    inputs,
    pipeline_version,
    version,
    references,
    metadata_json,
        pipeline_type):

    test_analysis_process = AnalysisProcess(
        input_uuid,
        file_path,
        creation_time,
        run_type,
        inputs,
        pipeline_version,
        version,
        references,
        metadata_json,
        pipeline_type
    )
    return test_analysis_process.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--run_type',
        required=True,
        help='Should always be "run" for now, may be "copy-forward" in some cases in future',
    )
    parser.add_argument('--file_path', required=True, help='Path to the loom/bam file to describe.')
    parser.add_argument(
        '--creation_time',
        required=True,
        help='Time of file creation, as reported by "gsutil ls -l"',
    )
    parser.add_argument(
        '--inputs_file',
        required=True,
        help='Path to tsv file containing info about inputs.',
    )
    parser.add_argument(
        '--references',
        help='List of UUIDs for the reference genome',
        required=True,
        nargs='+',
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
    parser.add_argument(
        '--metadata_json',
        required=True,
        help='Path to the JSON obtained from calling Cromwell /metadata for analysis workflow UUID.',
    )
    parser.add_argument('--pipeline_type', required=True, help='Type of pipeline(SS2 or Optimus)')
    parser.add_argument(
        '--fastq1_input_files_tsv', help='',
        required=False
    )
    parser.add_argument(
        '--fastq2_input_files_tsv', help='',
        required=False
    )
    parser.add_argument(
        '--input_ids_tsv', help='',
        required=False
    )

    args = argparse.parse_args()

    # Get metadata for inputs and outputs
    inputs = format_map.get_inputs(args.inputs_file)
    if args.input_ids_tsv is not None and args.fastq1_input_files_tsv is not None:
        inputs = format_map.get_inputs_ss2(inputs, args.input_ids_tsv, args.fastq1_input_files_tsv, args.fastq2_input_files_tsv)

    analysis_process = AnalysisProcess(
        args.input_uuid,
        args.inputs,
        args.references,
        args.pipeline_version,
        args.version,
        args.metadata_json,
        args.run_type,
        args.file_path,
        args.creation_time,
        args.pipeline_type
    )

    # Get the JSON content to be written
    analysis_process_json = analysis_process.get_json()

    # Generate unique analysis process UUID based on input file's UUID and extension
    analysis_process_id = format_map.get_uuid5(
        f"{analysis_process.input_uuid}{analysis_process.extension}")

    # Generate filename based on UUID and version
    analysis_process_filename = f"{analysis_process_json}_{analysis_process.version}.json"

    # Write to file
    with open(analysis_process_filename, 'w') as f:
        json.dump(analysis_process_json, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
