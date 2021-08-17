import os
import pytest
from pathlib import Path

import pipeline_tools.shared.submission.create_analysis_process as cap


@pytest.fixture(scope="module")
def test_data():
    class Data:
        project_level = True
        pipeline_type = "optimus"
        input_uuid = "heart_1k_test_v2_S1_L001"
        loom_timestamp = "2021-07-26T13:12:24.000000Z"
        workspace_version = "2021-05-24T12:00:00.000000Z"
        references = "c11000b1-2e69-532b-8c72-03dd4c9617d5"
        project_level_pipeline_type = "OptimusPostProcessing"
        project_level_input_uuid = "1fd499c5-f397-4bff-9af0-eb42c37d5fbe"
        input_file = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/updated-data/staging/metadata/metadata.json'
        project_level_input_file = "/cromwell_root/fc-c307d7b3-8386-40a1-b32c-73b9e16e0103/pipeline_to28bf5862-3220-4133-92ce-c829f9dcd708/TestHcaAdapter/c4b9a3c3-861e-4e3f-b393-da1b24354ee1/call-target_OptimusPostProcessing/OptimusPostProcessing/7f6c3249-2d24-407d-966f-411d84fbeba8/call-MergeLooms/hca_adapter_testing.loom"

    return Data


class TestCreateAnalysisProcess(object):
    def test_build_analysis_process(self, test_data):
        analysis_process = cap.test_build_analysis_process(
            input_uuid=test_data.input_uuid,
            input_file=test_data.input_file,
            pipeline_type=test_data.pipeline_type,
            workspace_version=test_data.workspace_version,
            references=test_data.references
        )

        assert analysis_process.get("analysis_run_type") == "run"
        assert analysis_process.get("timestamp_start_utc") == "2021-07-08T16:08:06.619Z"
        assert analysis_process.get("timestamp_stop_utc") == "2021-07-08T17:23:57.332Z"
        assert analysis_process.get("schema_type") == "process"
        assert analysis_process.get("analysis_run_type") == "run"
        assert analysis_process.get("reference_files") == "c11000b1-2e69-532b-8c72-03dd4c9617d5"
        assert (
            analysis_process.get("describedBy")
            == "https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process"
        )
        assert analysis_process.get("type") == {
            "text": "analysis"
        }
        assert analysis_process.get("process_core") == {
            "process_id": "151fe264-c670-4c77-a47c-530ff6b3127b"
        }
        assert analysis_process.get("provenance") == {
            "document_id": "151fe264-c670-4c77-a47c-530ff6b3127b",
            "submission_date": "2021-05-24T12:00:00.000000Z"
        }
        assert analysis_process.get("inputs") == [
            {
                "parameter_name": "r2_fastq",
                "parameter_value": "['gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/heart_1k_test_v2_S1_L001_R2_001.fastq.gz']"
            },
            {
                "parameter_name": "annotations_gtf",
                "parameter_value": "gs://hca-dcp-mint-test-data/yanc-test/gencode.vM21.annotation.gtf.gz"
            },
            {
                "parameter_name": "i1_fastq",
                "parameter_value": "['gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/heart_1k_test_v2_S1_L001_I1_001.fastq.gz']"
            },
            {
                "parameter_name": "chemistry",
                "parameter_value": "tenX_v2"
            },
            {
                "parameter_name": "tar_star_reference",
                "parameter_value": "gs://hca-dcp-mint-test-data/20190507-PipelinesSurge/mouse_reference/star_primary_gencode_mouse_vM21.tar"
            },
            {
                "parameter_name": "whitelist",
                "parameter_value": "gs://hca-dcp-sc-pipelines-test-data/whitelists/737K-august-2016.txt"
            },
            {
                "parameter_name": "ref_genome_fasta",
                "parameter_value": "gs://hca-dcp-mint-test-data/yanc-test/GRCm38.primary_assembly.genome.fa"
            },
            {
                "parameter_name": "r1_fastq",
                "parameter_value": "['gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/heart_1k_test_v2_S1_L001_R1_001.fastq.gz']"
            },
            {
                "parameter_name": "input_id",
                "parameter_value": "heart_1k_test_v2_S1_L001"
            }
        ]
        assert analysis_process.get("tasks") == [
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
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 1 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CalculateGeneMetrics/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CalculateGeneMetrics/shard-0/stdout",
                "memory": "29.296875 GB",
                "start_time": "2021-07-08T17:08:42.399Z",
                "stop_time": "2021-07-08T17:11:01.648Z",
                "task_name": "CalculateGeneMetrics",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 2,
                "disk_size": "local-disk 1 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CellSortBam/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CellSortBam/shard-0/stdout",
                "memory": "97.65625 GB",
                "start_time": "2021-07-08T17:05:32.680Z",
                "stop_time": "2021-07-08T17:08:07.644Z",
                "task_name": "CellSortBam",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 51 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-umitools:0.0.1",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CorrectUMItools/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CorrectUMItools/shard-0/stdout",
                "memory": "15.625 GB",
                "start_time": "2021-07-08T17:02:11.739Z",
                "stop_time": "2021-07-08T17:05:31.646Z",
                "task_name": "CorrectUMItools",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 14 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CreateSparseCountMatrix/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-CreateSparseCountMatrix/shard-0/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T17:08:24.039Z",
                "stop_time": "2021-07-08T17:12:46.644Z",
                "task_name": "CreateSparseCountMatrix",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 16,
                "disk_size": "local-disk 501 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.12",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-FastqProcessing/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-FastqProcessing/stdout",
                "memory": "39.0625 GB",
                "start_time": "2021-07-08T16:08:09.160Z",
                "stop_time": "2021-07-08T16:12:40.645Z",
                "task_name": "FastqProcessing",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 2,
                "disk_size": "local-disk 1 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-GeneSortBam/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-GeneSortBam/shard-0/stdout",
                "memory": "97.65625 GB",
                "start_time": "2021-07-08T17:05:32.680Z",
                "stop_time": "2021-07-08T17:08:40.644Z",
                "task_name": "GeneSortBam",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 20 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeCellMetrics/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeCellMetrics/stdout",
                "memory": "3.759765625 GB",
                "start_time": "2021-07-08T17:11:49.060Z",
                "stop_time": "2021-07-08T17:16:31.643Z",
                "task_name": "MergeCellMetrics",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 20 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeCountFiles/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeCountFiles/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T17:12:49.239Z",
                "stop_time": "2021-07-08T17:16:31.643Z",
                "task_name": "MergeCountFiles",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 20 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-sctools:v0.3.11",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeGeneMetrics/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeGeneMetrics/stdout",
                "memory": "3.759765625 GB",
                "start_time": "2021-07-08T17:11:04.180Z",
                "stop_time": "2021-07-08T17:15:01.646Z",
                "task_name": "MergeGeneMetrics",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 500 HDD",
                "docker_image": "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.3.3-1513176735",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeSorted/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-MergeSorted/stdout",
                "memory": "17.724609375 GB",
                "start_time": "2021-07-08T17:08:34.240Z",
                "stop_time": "2021-07-08T17:12:07.645Z",
                "task_name": "MergeSorted",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 11 HDD",
                "docker_image": "quay.io/humancellatlas/modify-gtf:0.1.0",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-ModifyGtf/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-ModifyGtf/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T16:08:09.160Z",
                "stop_time": "2021-07-08T16:13:31.643Z",
                "task_name": "ModifyGtf",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 4,
                "disk_size": "local-disk 200 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-loom-output:0.0.6-1",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-OptimusLoomGeneration/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-OptimusLoomGeneration/stdout",
                "memory": "18 GB",
                "start_time": "2021-07-08T17:20:18.030Z",
                "stop_time": "2021-07-08T17:23:55.643Z",
                "task_name": "OptimusLoomGeneration",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 201 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-picard:v0.2.2-2.10.10",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreCountSort/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreCountSort/shard-0/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T17:05:32.680Z",
                "stop_time": "2021-07-08T17:08:22.645Z",
                "task_name": "PreCountSort",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 201 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-picard:v0.2.2-2.10.10",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreMergeSort/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreMergeSort/shard-0/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T17:05:32.680Z",
                "stop_time": "2021-07-08T17:08:31.643Z",
                "task_name": "PreMergeSort",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 201 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-picard:v0.2.2-2.10.10",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreUMISort/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-PreUMISort/shard-0/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T16:59:42.819Z",
                "stop_time": "2021-07-08T17:02:10.646Z",
                "task_name": "PreUMISort",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 20 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-dropletutils:0.1.4",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-RunEmptyDrops/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-RunEmptyDrops/stdout",
                "memory": "15.625 GB",
                "start_time": "2021-07-08T17:16:33.630Z",
                "stop_time": "2021-07-08T17:20:16.646Z",
                "task_name": "RunEmptyDrops",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 16,
                "disk_size": "local-disk 63 SSD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-star:v0.2.2-2.5.3a-40ead6e",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-StarAlign/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-StarAlign/shard-0/stdout",
                "memory": "33.30078125 GB",
                "start_time": "2021-07-08T16:12:44.570Z",
                "stop_time": "2021-07-08T16:55:31.644Z",
                "task_name": "StarAlign",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 21 HDD",
                "docker_image": "quay.io/humancellatlas/secondary-analysis-dropseqtools:v0.2.2-1.13",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-TagGenes/shard-0/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-TagGenes/shard-0/stdout",
                "memory": "8.056640625 GB",
                "start_time": "2021-07-08T16:55:32.920Z",
                "stop_time": "2021-07-08T16:59:40.646Z",
                "task_name": "TagGenes",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            },
            {
                "cpus": 1,
                "disk_size": "local-disk 1 HDD",
                "docker_image": "ubuntu:18.04",
                "log_err": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-checkOptimusInput/stderr",
                "log_out": "gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/fa1c9233-78ee-4077-b401-c97685106c64/Optimus/151fe264-c670-4c77-a47c-530ff6b3127b/call-checkOptimusInput/stdout",
                "memory": "1 GB",
                "start_time": "2021-07-08T16:08:09.160Z",
                "stop_time": "2021-07-08T16:08:16.649Z",
                "task_name": "checkOptimusInput",
                "zone": "us-central1-a,us-central1-b,us-central1-c,us-central1-f"
            }
        ]


class TestCreateProjectLevelAnalysisProcess(object):
    def test_build_analysis_process(self, test_data):
        analysis_process = cap.test_build_analysis_process(
            input_uuid=test_data.project_level_input_uuid,
            input_file=test_data.project_level_input_file,
            pipeline_type=test_data.pipeline_type,
            workspace_version=test_data.workspace_version,
            project_level=test_data.project_level,
            loom_timestamp=test_data.loom_timestamp
        )

        assert analysis_process.get("analysis_run_type") == "run"
        assert analysis_process.get("timestamp_start_utc") == "2021-07-26T13:12:24.000000Z"
        assert analysis_process.get("timestamp_stop_utc") == "2021-07-26T13:12:24.000000Z"
        assert analysis_process.get("schema_type") == "process"
        assert analysis_process.get("analysis_run_type") == "run"
        assert analysis_process.get("reference_files") == []
        assert (
            analysis_process.get("describedBy")
            == "https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process"
        )
        assert analysis_process.get("type") == {
            "text": "analysis; merge matrices"
        }
        assert analysis_process.get("process_core") == {
            "process_id": "7f6c3249-2d24-407d-966f-411d84fbeba8"
        }
        assert analysis_process.get("provenance") == {
            "document_id": "7f6c3249-2d24-407d-966f-411d84fbeba8",
            "submission_date": "2021-05-24T12:00:00.000000Z"
        }
        assert analysis_process.get("inputs") == []
        assert analysis_process.get("tasks") == []
