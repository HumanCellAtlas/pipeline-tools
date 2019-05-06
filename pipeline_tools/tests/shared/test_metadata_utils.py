import json
import os
import pytest

from humancellatlas.data.metadata.api import Bundle


from pipeline_tools.shared import metadata_utils
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
from pathlib import Path


data_dir = f'{Path(os.path.split(__file__)[0]).absolute().parents[0]}/data/'


@pytest.fixture(scope='module')
def ss2_manifest_json_vx():
    with open('{0}metadata/ss2_vx/manifest.json'.format(data_dir)) as f:
        ss2_manifest_json_vx = json.load(f)
    return ss2_manifest_json_vx


@pytest.fixture(scope='module')
def ss2_metadata_files_vx():
    with open('{0}metadata/ss2_vx/metadata_files.json'.format(data_dir)) as f:
        ss2_metadata_files_vx = json.load(f)
    return ss2_metadata_files_vx


@pytest.fixture(scope='module')
def test_ss2_bundle_uuid_vx(ss2_manifest_json_vx):
    return ss2_manifest_json_vx['bundle']['uuid']


@pytest.fixture(scope='module')
def test_ss2_bundle_version_vx(ss2_manifest_json_vx):
    return ss2_manifest_json_vx['bundle']['version']


@pytest.fixture(scope='module')
def test_ss2_bundle_manifest_vx(ss2_manifest_json_vx):
    return ss2_manifest_json_vx['bundle']['files']


@pytest.fixture(scope='module')
def test_ss2_bundle_vx(
    test_ss2_bundle_uuid_vx,
    test_ss2_bundle_version_vx,
    test_ss2_bundle_manifest_vx,
    ss2_metadata_files_vx,
):
    return Bundle(
        uuid=test_ss2_bundle_uuid_vx,
        version=test_ss2_bundle_version_vx,
        manifest=test_ss2_bundle_manifest_vx,
        metadata_files=ss2_metadata_files_vx,
    )


@pytest.fixture(scope='module')
def tenx_manifest_json_vx():
    with open('{0}metadata/tenx_vx/manifest.json'.format(data_dir)) as f:
        tenx_manifest_json_vx = json.load(f)
    return tenx_manifest_json_vx


@pytest.fixture(scope='module')
def tenx_metadata_files_vx():
    with open('{0}metadata/tenx_vx/metadata_files.json'.format(data_dir)) as f:
        tenx_metadata_files_vx = json.load(f)
    return tenx_metadata_files_vx


@pytest.fixture(scope='module')
def tenx_metadata_files_vx_with_no_expected_cell_count():
    with open(
        '{0}metadata/tenx_vx/metadata_files_with_no_expected_cell_count.json'.format(
            data_dir
        )
    ) as f:
        tenx_metadata_files_vx_with_no_expected_cell_count = json.load(f)
    return tenx_metadata_files_vx_with_no_expected_cell_count


@pytest.fixture(scope='module')
def test_tenx_bundle_uuid_vx(tenx_manifest_json_vx):
    return tenx_manifest_json_vx['bundle']['uuid']


@pytest.fixture(scope='module')
def test_tenx_bundle_version_vx(tenx_manifest_json_vx):
    return tenx_manifest_json_vx['bundle']['version']


@pytest.fixture(scope='module')
def test_tenx_bundle_manifest_vx(tenx_manifest_json_vx):
    return tenx_manifest_json_vx['bundle']['files']


@pytest.fixture(scope='module')
def test_tenx_bundle_vx(
    test_tenx_bundle_uuid_vx,
    test_tenx_bundle_version_vx,
    test_tenx_bundle_manifest_vx,
    tenx_metadata_files_vx,
):
    return Bundle(
        uuid=test_tenx_bundle_uuid_vx,
        version=test_tenx_bundle_version_vx,
        manifest=test_tenx_bundle_manifest_vx,
        metadata_files=tenx_metadata_files_vx,
    )


@pytest.fixture(scope='module')
def test_tenx_bundle_vx_with_no_expected_cell_count(
    test_tenx_bundle_uuid_vx,
    test_tenx_bundle_version_vx,
    test_tenx_bundle_manifest_vx,
    tenx_metadata_files_vx_with_no_expected_cell_count,
):
    return Bundle(
        uuid=test_tenx_bundle_uuid_vx,
        version=test_tenx_bundle_version_vx,
        manifest=test_tenx_bundle_manifest_vx,
        metadata_files=tenx_metadata_files_vx_with_no_expected_cell_count,
    )


class TestMetadataUtils(object):
    def test_get_sample_id(self, test_ss2_bundle_vx):
        sample_id = metadata_utils.get_sample_id(test_ss2_bundle_vx)
        assert sample_id == 'f89a7a2e-a789-495c-bf37-11e82757cc82'

    def test_download_file(self, requests_mock, test_ss2_bundle_manifest_vx):
        manifest_dict = {'project.json': test_ss2_bundle_manifest_vx[0]}
        item = tuple(manifest_dict.items())[
            0
        ]  # to test this func without calling `map`, we must convert typing here
        dss_url = 'https://dss.mock.org/v0'
        file_id = test_ss2_bundle_manifest_vx[0]['uuid']

        expect_result_from_dcp_utils = {'file': 'test', 'id': file_id}
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(
            dss_url=dss_url, file_id=file_id
        )

        def _request_callback(request, context):
            context.status_code = 200
            return expect_result_from_dcp_utils

        requests_mock.get(url, json=_request_callback)

        with HttpRequestsManager():
            file_name, file_response_js = metadata_utils.download_file(
                item=item, dss_url=dss_url, http_requests=HttpRequests()
            )

        assert file_name == 'project.json'
        assert file_response_js['file'] == expect_result_from_dcp_utils['file']
        assert requests_mock.call_count == 1
