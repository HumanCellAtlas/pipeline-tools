import os
import pytest

import pipeline_tools.shared.submission.create_analysis_protocol as cap
from pathlib import Path


@pytest.fixture(scope="module")
def test_data():
    class Data:
        input_uuid = 0
        pipeline_type = "Optimus"
        project_level_pipeline_type = "OptimusPostProcessing"
        pipeline_version = "optimus_v4.2.3"
        project_level_pipeline_version = "optimus_post_processing_v1.0.0"
        workspace_version = "2021-05-24T12:00:00.000000Z"

    return Data


@pytest.fixture
def data_file():
    def _data_file(file_name):
        return (
            f"{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/{file_name}"
        )

    return _data_file


class TestCreateAnalysisProtocol(object):
    def test_build_analysis_protocol(self, test_data):
        analysis_protocol = cap.test_build_analysis_protocol(
            input_uuid=test_data.input_uuid,
            pipeline_version=test_data.pipeline_version,
            pipeline_type=test_data.pipeline_type,
            workspace_version=test_data.workspace_version
        )

        assert (
            analysis_protocol.get("computational_method")
            == "https://dockstore.org/workflows/github.com/broadinstitute/warp/Optimus:Optimus_v4.2.3"
        )
        assert (
            analysis_protocol.get("describedBy")
            == "https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol"
        )
        assert analysis_protocol.get("schema_type") == "protocol"
        assert analysis_protocol.get("type") == {
            "text": "analysis_protocol"
        }
        assert analysis_protocol.get("protocol_core") == {
            "protocol_id": "optimus_v4.2.3"
        }
        assert analysis_protocol.get("provenance") == {
            "document_id": "f2cdb4e5-b439-5cdf-ac41-161ff39d5790",
            "submission_date": "2021-05-24T12:00:00.000000Z",
            "update_date": "2021-05-24T12:00:00.000000Z"
        }


class TestCreateAnalysisProtocolProjectLevel(object):
    def test_build_analysis_protocol(self, test_data):
        analysis_protocol = cap.test_build_analysis_protocol(
            input_uuid=test_data.input_uuid,
            pipeline_version=test_data.project_level_pipeline_version,
            pipeline_type=test_data.project_level_pipeline_type,
            workspace_version=test_data.workspace_version,
            project_level=True
        )

        assert analysis_protocol.get("computational_method") == "https://dockstore.org/workflows/github.com/broadinstitute/warp/OptimusPostProcessing:Optimus_post_processing_v1.0.0"
        assert (
            analysis_protocol.get("describedBy")
            == "https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol"
        )
        assert analysis_protocol.get("schema_type") == "protocol"
        assert analysis_protocol.get("type") == {
            "text": "analysis; merge matrices"
        }
        assert analysis_protocol.get("protocol_core") == {
            "protocol_id": "optimus_post_processing_v1.0.0"
        }
        # The document_id will not match test data because we are updating computational_method to be
        # a docstore URL, instead of the pipeline version. This causes the hash used for the file name
        # to be different.
        assert analysis_protocol.get("provenance") == {
            "document_id": "1c03b7cd-b34c-532d-a696-1092740797c6",
            "submission_date": "2021-05-24T12:00:00.000000Z",
            "update_date": "2021-05-24T12:00:00.000000Z"
        }
