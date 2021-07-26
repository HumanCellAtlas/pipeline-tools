import os
import pytest

import pipeline_tools.shared.submission.create_analysis_file as caf
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = 0
        outputs_file = [
            {
                'file_path': 'fake/path/to/file',
                'timestamp': '2021-07-26T14:48:29Z'
            }
        ]
        pipeline_type = 'optimus'
        file_path = '/fake/file/path.fasta'
        creation_time = '2021-07-14T13:00:00Z'

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
            outputs_file=test_data.outputs_file,
            pipeline_type=test_data.pipeline_type,
            file_path=test_data.file_path,
            creation_time=test_data.creation_time
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
