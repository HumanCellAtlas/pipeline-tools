import os
import pytest

import pipeline_tools.shared.submission.create_analysis_process as cap
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = 0
        pipeline_type = 'optimus',
        method = 'foo_method',
        pipeline_version = 'optimus_v4.2.3',
        version = '2021-05-24T12:00:00.000000Z',
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


class TestCreateAnalysisProcess(object):
    def test_build_analysis_process(self, test_data):
        analysis_process = cap.test_build_analysis_process(
            input_uuid=test_data.input_uuid,
            file_path=test_data.file_path,
            creation_time=test_data.creation_time,
            method=test_data.method,
            pipeline_version=test_data.pipeline_version,
            version=test_data.version,
            pipeline_type=test_data.pipeline_type,
        )

        assert (analysis_process.get('analysis_run_type') == 'run')
        assert (
            analysis_process.get('describedBy')
            == 'https://schema.humancellatlas.org/type/process/analysis/12.0.0/analysis_process'
        )
        assert analysis_process.get('schema_type') == 'process'
        assert analysis_process.get('type') == {
            'text': 'analysis'
        }
        assert analysis_process.get('protocol_core') == {
            'protocol_id': '151fe264-c670-4c77-a47c-530ff6b3127b'
        }
        assert analysis_process.get('inputs') == {
            'parameter_name': 'r1_fastq',
            'parameter_value': "['gs://fc-04191d93-a91a-4fb2-adeb-b3f1296ec1c5/heart_1k_test_v2_S1_L001_R1_001.fastq.gz']"
        }
        assert analysis_process.get('provenance') == {
            'document_id': 'optimus_v4.2.3',
            'submission_date': '2021-05-24T12:00:00.000000Z',
            'update_date': '2021-05-24T12:00:00.000000Z'
        }
        assert analysis_process.get('timestamp_start_utc') == '2021-07-08T16:08:06.619Z'
        assert analysis_process.get('timestamp_stop_utc') == '2021-07-08T17:23:57.332Z'
