#!/usr/bin/env python
import argparse
import json
import os
import re
from pipeline_tools.shared.schema_utils import SCHEMAS
from pipeline_tools.shared.submission import format_map
from pipeline_tools.shared.exceptions import UnsupportedPipelineType
from distutils.util import strtobool


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
    optimus_input_fields = SCHEMAS["ANALYSIS_PROCESS"]["optimus_input_fields"]
    ss2_input_fields = SCHEMAS["ANALYSIS_PROCESS"]["ss2_input_fields"]

    def __init__(
        self,
        input_uuid,
        input_file,
        pipeline_type,
        workspace_version,
        references=[],
        project_level=False,
        loom_timestamp="",
            ss2_index=0):

        self.ss2_index = ss2_index
        self.input_uuid = input_uuid
        self.input_file = input_file
        self.reference_files = references
        self.pipeline_type = pipeline_type
        self.project_level = project_level
        self.loom_timestamp = loom_timestamp
        self.workspace_version = workspace_version

    def __analysis_process__(self):
        return {
            "analysis_run_type" : self.__run_type__(),
            "describedBy" : self.describedBy,
            "inputs" : self.__inputs__(),
            "process_core" : self.__process_core__(),
            "provenance" : self.__provenance__(),
            "reference_files" : self.__references_files__(),
            "schema_type" : self.schema_type,
            "schema_version" : self.schema_version,
            "tasks" : self.__tasks__(),
            "timestamp_start_utc" : self.__timestamp__()[0],
            "timestamp_stop_utc" : self.__timestamp__()[1],
            "type": self.__type__()
        }

    def get_json(self):
        return self.__analysis_process__()

    def __type__(self):
        """Return type of process being completed based on project or intermediate"""

        if self.project_level:
            return {"text": "analysis; merge matrices"}

        return {"text": "analysis"}

    def __tasks__(self):
        """Return the tasks of the pipeline run based on project or intermediate"""

        if self.project_level or self.pipeline_type.lower() == "ss2":
            return []

        workflow_metadata = self.__metadata__()
        return format_map.get_workflow_tasks(workflow_metadata)

    def __references_files__(self):
        """Return the hashed representation of the reference fasta file path"""

        if self.project_level:
            return []

        return [format_map.get_file_entity_id(r,
                format_map.get_entity_type(r),
                os.path.splitext(r)[1])
                for r in self.reference_files]

    def __inputs__(self):
        """Return the inputs to the pipeline based on project or intermediate"""

        if self.project_level:
            return []

        workflow_metadata = self.__metadata__()
        input_fields = self.optimus_input_fields if self.pipeline_type.lower() == "optimus" else self.ss2_input_fields

        return format_map.get_workflow_inputs(workflow_metadata["inputs"], input_fields)

    def __metadata__(self):

        metadata = format_map.get_workflow_metadata(self.input_file)

        # Return the unique metadata.json for optimus
        if self.pipeline_type.lower() == "optimus":
            return metadata

        # SS2 only has one metadata.json, if intermediate run then return the subworkflow task
        # If project level run then return AggregateLoom metadata
        if self.pipeline_type.lower() == "ss2":
            if not self.project_level:
                return metadata["calls"][format_map.get_call_type(metadata)][self.ss2_index]
            return metadata["calls"]["MultiSampleSmartSeq2.AggregateLoom"][0]

        raise UnsupportedPipelineType("Pipeline must be optimus or ss2")

    def __process_id__(self):

        workflow_metadata = self.__metadata__()
        uuid_regex = r"([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})"

        if self.pipeline_type.lower() == "optimus":
            return workflow_metadata["id"]

        # Grab the process id for the subworkflow for intermediate level ss2
        # Otherwise get the process ID for the Aggregate loom tasks
        if self.pipeline_type.lower() == "ss2":
            if not self.project_level:
                return workflow_metadata["subWorkflowId"]
            else:
                # Get the ID from the gs:// path
                return re.findall(uuid_regex, workflow_metadata['stdout'])[-1]

        raise UnsupportedPipelineType("Pipeline must be optimus or ss2")

    def __timestamp__(self):

        workflow_metadata = self.__metadata__()

        start, end = format_map.format_timestamp(workflow_metadata["start"]), format_map.format_timestamp(workflow_metadata["end"])
        return [start, end]

    def __provenance__(self):

        return {
            "document_id": self.__process_id__(),
            "submission_date": self.workspace_version
        }

    def __process_core__(self):

        return {
            "process_id": self.__process_id__()
        }

    def __run_type__(self):

        return "copy-forward" if "cacheCopy" in self.input_file else "run"

    @property
    def process_id(self):
        return self.__process_id__()

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
    parser.add_argument("--pipeline_type", required=True, help="Type of pipeline(SS2 or Optimus)")
    parser.add_argument("--workspace_version", required=True, help="Workspace version value i.e. timestamp for workspace")
    parser.add_argument("--loom_timestamp", required=False, help="The timestamp for the stratified project matrix loom file")
    parser.add_argument("--input_uuid", required=True, help="Input file UUID from the HCA Data Browser (project stratum string for project level)")
    parser.add_argument("--input_file", required=True, help="Path to the JSON obtained from calling Cromwell /metadata for analysis workflow UUID.")
    parser.add_argument("--references", required=False, nargs="+", help="File path for the reference genome fasta")
    parser.add_argument("--project_level", required=True, type=lambda x: bool(strtobool(x)), help="Boolean representing project level vs intermediate level")
    parser.add_argument("--ss2_index", required=False, type=int, help="The index of the ss2 scatter task, need to grab intermediate run data from metadata.json")

    args = parser.parse_args()

    analysis_process = AnalysisProcess(
        args.input_uuid,
        args.input_file,
        args.pipeline_type,
        args.workspace_version,
        args.references,
        args.project_level,
        args.loom_timestamp,
        args.ss2_index
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
    with open(f'{analysis_process_filename}', 'w') as f:
        json.dump(analysis_process_json, f, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
