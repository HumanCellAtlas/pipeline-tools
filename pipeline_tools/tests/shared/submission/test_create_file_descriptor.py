import os
import pytest

import pipeline_tools.shared.submission.create_file_descriptor as cfd
from pathlib import Path


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = 0
        file_path = '/fake/file/path.fasta'
        size = 1000
        sha256 = '12998c017066eb0d2a70b94e6ed3192985855ce390f321bbdb832022888bd251'
        crc32c = '0b83b575'
        pipeline_type = 'optimus'
        creation_time = '2021-07-14T16:01:45Z'
        workspace_version = '2021-01-13T17:53:12.000000Z'

    return Data


@pytest.fixture
def data_file():
    def _data_file(file_name):
        return (
            f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/{file_name}'
        )

    return _data_file


class TestCreateDescriptor(object):
    def test_build_file_descriptor(self, test_data):
        file_descriptor = cfd.test_build_file_descriptor(
            size=test_data.size,
            sha256=test_data.sha256,
            crc32c=test_data.crc32c,
            input_uuid=test_data.input_uuid,
            file_path=test_data.file_path,
            pipeline_type=test_data.pipeline_type,
            creation_time=test_data.creation_time,
            workspace_version=test_data.workspace_version
        )

        assert (
            file_descriptor.get('describedBy')
            == 'https://schema.humancellatlas.org/system/2.0.0/file_descriptor'
        )
        assert file_descriptor.get('schema_type') == 'file_descriptor'
        assert file_descriptor.get('content_type') == 'application/octet-stream'
        assert file_descriptor.get('size') == 1000
        assert (
            file_descriptor.get('sha256')
            == '12998c017066eb0d2a70b94e6ed3192985855ce390f321bbdb832022888bd251'
        )
        assert file_descriptor.get('crc32c') == '0b83b575'
        assert file_descriptor.get('file_id') == '2beee6e4-7e7d-52b9-9180-fc052cb7791d'
        assert file_descriptor.get('file_version') == '2021-07-14T16:01:45.000000Z'
        assert file_descriptor.get('file_name') == 'path.fasta'
