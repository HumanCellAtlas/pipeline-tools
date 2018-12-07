import json
import os
import pytest
import unittest.mock as mock
from humancellatlas.data.metadata.api import Bundle
from unittest.mock import patch

from pipeline_tools import input_utils
from pipeline_tools.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


data_dir = os.path.split(__file__)[0] + '/data/'


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
def test_ss2_bundle_vx(test_ss2_bundle_uuid_vx,
                       test_ss2_bundle_version_vx,
                       test_ss2_bundle_manifest_vx,
                       ss2_metadata_files_vx):
    return Bundle(uuid=test_ss2_bundle_uuid_vx,
                  version=test_ss2_bundle_version_vx,
                  manifest=test_ss2_bundle_manifest_vx,
                  metadata_files=ss2_metadata_files_vx)


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
    with open('{0}metadata/tenx_vx/metadata_files_with_no_expected_cell_count.json'.format(data_dir)) as f:
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
def test_tenx_bundle_vx(test_tenx_bundle_uuid_vx, test_tenx_bundle_version_vx, test_tenx_bundle_manifest_vx,
                       tenx_metadata_files_vx):
    return Bundle(uuid=test_tenx_bundle_uuid_vx,
                  version=test_tenx_bundle_version_vx,
                  manifest=test_tenx_bundle_manifest_vx,
                  metadata_files=tenx_metadata_files_vx)


@pytest.fixture(scope='module')
def test_tenx_bundle_vx_with_no_expected_cell_count(test_tenx_bundle_uuid_vx,
                                                    test_tenx_bundle_version_vx,
                                                    test_tenx_bundle_manifest_vx,
                                                    tenx_metadata_files_vx_with_no_expected_cell_count):
    return Bundle(uuid=test_tenx_bundle_uuid_vx,
                  version=test_tenx_bundle_version_vx,
                  manifest=test_tenx_bundle_manifest_vx,
                  metadata_files=tenx_metadata_files_vx_with_no_expected_cell_count)


class TestInputUtils(object):

    def test_get_sample_id(self, test_ss2_bundle_vx):
        sample_id = input_utils.get_sample_id(test_ss2_bundle_vx)
        assert sample_id == 'f89a7a2e-a789-495c-bf37-11e82757cc82'

    def test_get_expected_cell_count(self, test_tenx_bundle_vx):
        total_estimated_cells = input_utils.get_expected_cell_count(test_tenx_bundle_vx)
        assert total_estimated_cells == 10000

    def test_get_expected_cell_count_sets_default_value(self, test_tenx_bundle_vx_with_no_expected_cell_count):
        total_estimated_cells = input_utils.get_expected_cell_count(test_tenx_bundle_vx_with_no_expected_cell_count)
        assert total_estimated_cells == 3000

    def test_get_urls_to_files_for_ss2(self, test_ss2_bundle_vx, test_ss2_bundle_manifest_vx):
        fastq_url1, fastq_url2 = input_utils.get_urls_to_files_for_ss2(test_ss2_bundle_vx)
        assert fastq_url1 == [f['url'] for f in test_ss2_bundle_manifest_vx if f['name'] == 'R1.fastq.gz'][0]
        assert fastq_url2 == [f['url'] for f in test_ss2_bundle_manifest_vx if f['name'] == 'R2.fastq.gz'][0]

    def test_download_file(self, requests_mock, test_ss2_bundle_manifest_vx):
        manifest_dict = {'project.json': test_ss2_bundle_manifest_vx[0]}
        item = tuple(manifest_dict.items())[0]  # to test this func without calling `map`, we must convert typing here
        dss_url = 'https://dss.mock.org/v0'
        file_id = test_ss2_bundle_manifest_vx[0]['uuid']

        expect_result_from_dcp_utils = {'file': 'test', 'id': file_id}
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(
                dss_url=dss_url, file_id=file_id)

        def _request_callback(request, context):
            context.status_code = 200
            return expect_result_from_dcp_utils

        requests_mock.get(url, json=_request_callback)

        with HttpRequestsManager():
            file_name, file_response_js = input_utils.download_file(item=item,
                                                                    dss_url=dss_url,
                                                                    http_requests=HttpRequests())

        assert file_name == 'project.json'
        assert file_response_js['file'] == expect_result_from_dcp_utils['file']
        assert requests_mock.call_count == 1

    def test_create_ss2_input_tsv(self, tmpdir, test_ss2_bundle_uuid_vx, test_ss2_bundle_version_vx):
        def mocked_get_content_for_ss2_input_tsv(bundle_uuid, bundle_version, dss_url, http_requests):
            return 'url1', 'url2', 'fake_id'

        file_path = tmpdir.join('inputs.tsv')
        with patch('pipeline_tools.input_utils._get_content_for_ss2_input_tsv',
                   side_effect=mocked_get_content_for_ss2_input_tsv):
            input_utils.create_ss2_input_tsv(bundle_uuid=test_ss2_bundle_uuid_vx,
                                             bundle_version=test_ss2_bundle_version_vx,
                                             dss_url='foo_url',
                                             input_tsv_name=file_path)
        assert file_path.read() == 'fastq_1\tfastq_2\tsample_id\n{0}\t{1}\t{2}\n'.format('url1', 'url2', 'fake_id')

    @mock.patch('pipeline_tools.input_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.input_utils.get_sample_id')
    def test_get_cellranger_inputs(self, mock_sample_id, mock_bundle, test_tenx_bundle_vx):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_tenx_bundle_vx
        input_utils.get_cellranger_input_files(uuid='bundle_uuid', version='bundle_version', dss_url='foo_url')
        expected_fastqs = [
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R1.fastq.gz',
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R2.fastq.gz',
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/I1.fastq.gz'
        ]
        expected_fastq_names = ['fake_id_S1_L001_R1_001.fastq.gz', 'fake_id_S1_L001_R2_001.fastq.gz', 'fake_id_S1_L001_I1_001.fastq.gz']
        expected_total_estimated_cells = 10000

        with open('sample_id.txt') as f:
            sample_id = f.read().strip()
            assert sample_id == 'fake_id'

        with open('expect_cells.txt') as f:
            total_estimated_cells = f.read().strip()
            assert int(total_estimated_cells) == expected_total_estimated_cells

        with open('fastqs.txt') as f:
            actual_fastqs = f.readlines()
        for idx, url in enumerate(actual_fastqs):
            assert actual_fastqs[idx].strip() == expected_fastqs[idx]

        with open('fastq_names.txt') as f:
            actual_fastq_names = f.readlines()
        for idx, url in enumerate(actual_fastq_names):
            assert actual_fastq_names[idx].strip() == expected_fastq_names[idx]

        os.remove('fastqs.txt')
        os.remove('fastq_names.txt')
        os.remove('sample_id.txt')
        os.remove('expect_cells.txt')


