#!/usr/bin/env python
import argparse
import json
import os
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

    # Input fields to retrieve from metadata.json
    input_fields = SCHEMAS["ANALYSIS_PROCESS"]["input_fields"]

    def __init__(
        self,
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
        references=[],
        project_level=False,
            loom_timestamp=""):

        if project_level:
            self.type = {
                "text": "analysis; merge matrices"
            }
            self.tasks = []
            self.inputs = []
            self.reference_files = []

            # Retrieve process id from input_file
            process_id = format_map.get_analysis_workflow_id(str(input_file))

            # Retrieve timestamps from loom_timestamp
            timestamp_start_utc = loom_timestamp
            timestamp_stop_utc = loom_timestamp
        else:
            # Retrieve inputs and tasks from metatdata_json
            workflow_metadata = format_map.get_workflow_metadata(input_file)
            all_inputs = workflow_metadata["inputs"]
            process_inputs = format_map.get_workflow_inputs(all_inputs, self.input_fields)
            process_id = workflow_metadata["id"]
            workflow_tasks = format_map.get_workflow_tasks(workflow_metadata)

            # Retrieve timestamps from workflow_metadata
            timestamp_start_utc = format_map.format_timestamp(workflow_metadata.get("start"))
            timestamp_stop_utc = format_map.format_timestamp(workflow_metadata.get("end"))

            self.type = {
                "text": "analysis"
            }
            self.tasks = workflow_tasks
            self.inputs = process_inputs
            self.reference_files = references

        # Determine analysis_run_type from file path
        if "cacheCopy" in str(input_file):
            self.analysis_run_type = "copy-forward"
        else:
            self.analysis_run_type = "run"

        provenance = {
            "document_id": process_id,
            "submission_date": workspace_version  # TODO: check this too
        }
        process_core = {
            "process_id": process_id
        }

        self.input_uuid = input_uuid
        self.workspace_version = workspace_version
        self.process_core = process_core
        self.provenance = provenance
        self.timestamp_start_utc = timestamp_start_utc
        self.timestamp_stop_utc = timestamp_stop_utc
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
    def process_id(self):
        return self.provenance["document_id"]

    @property
    def uuid(self):
        return self.input_uuid

    @property
    def version(self):
        return self.workspace_version


# Entry point for unit tests
def test_build_analysis_process(
    input_uuid,
    input_file,
    pipeline_type,
    workspace_version,
    references=[],
    project_level=False,
        loom_timestamp=""):

    test_analysis_process = AnalysisProcess(
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
        references,
        project_level,
        loom_timestamp
    )
    return test_analysis_process.get_json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_uuid", required=True, help="Input file UUID from the HCA Data Browser")
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline(SS2 or Optimus)")
    parser.add_argument("--workspace_version", required=True, help="Workspace version value i.e. timestamp for workspace")
    parser.add_argument("--loom_timestamp", required=False, help="The timestamp for the stratified project matrix loom file")
    parser.add_argument("--project_level", type=bool, required=False, help="Boolean representing project level vs intermediate level")
    parser.add_argument(
        "--references",
        help="List of UUIDs for the reference genome",
        required=False,
        nargs="+",
    )
    parser.add_argument(
        "--input_file",
        required=True,
        help="Path to the JSON obtained from calling Cromwell /metadata for analysis workflow UUID.",
    )

    args = parser.parse_args()

    analysis_process = AnalysisProcess(
        args.input_uuid,
        args.input_file,
        args.pipeline_type,
        args.workspace_version,
        args.references,
        args.project_level,
        args.loom_timestamp
    )

    # Get the JSON content to be written
    analysis_process_json = analysis_process.get_json()

    # Determine file name
    analysis_process_filename = (
        f"{analysis_process.process_id}"
        f"_{analysis_process.workspace_version}"
        f".json"
    )

    # Write analysis_process to file
    print('Writing analysis_process.json to disk...')
    if not os.path.exists("analysis_process"):
        os.mkdir("analysis_process")

    with open(f'analysis_process/{analysis_process_filename}', 'w') as f:
        json.dump(analysis_process_json, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
