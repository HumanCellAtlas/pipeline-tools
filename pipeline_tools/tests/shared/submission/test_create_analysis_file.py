import os
import pytest

import pipeline_tools.shared.submission.create_analysis_file as caf
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = 0
        metadata_json = 'path/to/file'
        pipeline_type = 'optimus'
        workspace_version = '2021-01-13T17:53:12.000000Z'

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
            'document_id': '12345abcde',
            'submission_date': '2021-07-26T14:48:29Z'
        }
        assert analysis_file.get('file_core') == {
            'content_description': [],
            'file_name': '/fake/file/path.fasta',
            'format': 'bam'
        }
        assert analysis_file.get('workspace_version') == '2021-01-13T17:53:12.000000Z'
