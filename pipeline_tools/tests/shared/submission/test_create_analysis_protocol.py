import os
import pytest

import pipeline_tools.shared.submission.create_analysis_protocol as cap
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


class TestCreateAnalysisProtocol(object):
    def test_build_analysis_protocol(self, test_data):
        analysis_protocol = cap.test_build_analysis_protocol(
            input_uuid=test_data.input_uuid,
            file_path=test_data.file_path,
            creation_time=test_data.creation_time,
            method=test_data.method,
            pipeline_version=test_data.pipeline_version,
            version=test_data.version,
            pipeline_type=test_data.pipeline_type,
        )

        assert (
            analysis_protocol.get('computational_method')
            == 'https://dockstore.org/workflows/github.com/broadinstitute/warp/Optimus:Optimus_v4.2.3'
        )
        assert (
            analysis_protocol.get('describedBy')
            == 'https://schema.humancellatlas.org/type/protocol/analysis/9.1.0/analysis_protocol'
        )
        assert analysis_protocol.get('schema_type') == 'protocol'
        assert analysis_protocol.get('type') == {
            'text': 'analysis_protocol'
        }
        assert analysis_protocol.get('protocol_core') == {
            'protocol_id': 'optimus_v4.2.3'
        }
        assert analysis_protocol.get('provenance') == {
            'document_id': 'optimus_v4.2.3',
            'submission_date': '2021-05-24T12:00:00.000000Z',
            'update_date': '2021-05-24T12:00:00.000000Z'
        }
