import os
import pytest

import pipeline_tools.shared.submission.create_analysis_file as caf
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = 'heart_1k_test_v2_S1_L001'
        metadata_json = 'pipeline_tools/tests/data/updated-data/staging/metdata/metadata.json'
        pipeline_type = 'optimus'
        workspace_version = '2021-05-24T12:00:00.000000Z'

    return Data


@pytest.fixture
def data_file():
    def _data_file(file_name):
        return (
            f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/{file_name}'
        )

    return _data_file


class TestCreateAnalysisFile(object):
    def test_build_analysis_file(self, test_data):
        analysis_file = caf.test_build_analysis_file(
            input_uuid=test_data.input_uuid,
            metadata_json=test_data.metadata_json,
            pipeline_type=test_data.pipeline_type,
            workspace_version=test_data.workspace_version
        )

        assert (
            analysis_file.get('describedBy')
            == 'https://schema.humancellatlas.org/type/file/6.2.0/analysis_file'
        )
        assert analysis_file.get('schema_type') == 'file'
        assert analysis_file.get('provenance') == {
            'document_id': '87795ce9-03ce-51f3-b8d8-4ad6f8931fe0',
            'submission_date': '2021-05-24T12:00:00.000000Z'
        }
        assert analysis_file.get('file_core') == {
            'content_description': [
                {
                    "ontology": "data:3917",
                    "ontology_label": "Count Matrix",
                    "text": "DCP/2-generated matrix"
                }
            ],
            'file_name': 'heart_1k_test_v2_S1_L001.loom',
            'format': 'loom'
        }
