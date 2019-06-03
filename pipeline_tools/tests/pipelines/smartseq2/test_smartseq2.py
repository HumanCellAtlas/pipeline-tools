import json
import os
import pytest
from humancellatlas.data.metadata.api import Bundle
from unittest.mock import patch


from pipeline_tools.pipelines.smartseq2 import smartseq2

from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
from pathlib import Path


data_dir = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/'


@pytest.fixture(scope='module')
def ss2_tsv_contents():
    with open(f"{data_dir}expected_ss2.tsv") as f:
        tsv_contents = f.read()
    return tsv_contents


@pytest.fixture(scope='module')
def ss2_manifest_json_vx():
    with open(f"{data_dir}metadata/ss2_vx/manifest.json") as f:
        ss2_manifest_json_vx = json.load(f)
    return ss2_manifest_json_vx


@pytest.fixture(scope='module')
def ss2_metadata_files_vx():
    with open(f"{data_dir}metadata/ss2_vx/metadata_files.json") as f:
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


class TestSmartSeq2(object):
    def test_create_ss2_input_tsv(
        self,
        tmpdir,
        test_ss2_bundle_uuid_vx,
        test_ss2_bundle_version_vx,
        ss2_tsv_contents,
    ):
        def mocked_get_content_for_ss2_input_tsv(
            bundle_uuid, bundle_version, dss_url, http_requests
        ):
            return 'url1', 'url2', 'fake_id', 9606

        file_path = tmpdir.join('inputs.tsv')
        with patch(
            'pipeline_tools.pipelines.smartseq2.smartseq2._get_content_for_ss2_input_tsv',
            side_effect=mocked_get_content_for_ss2_input_tsv,
        ), HttpRequestsManager():
            smartseq2.create_ss2_input_tsv(
                bundle_uuid=test_ss2_bundle_uuid_vx,
                bundle_version=test_ss2_bundle_version_vx,
                dss_url='foo_url',
                input_tsv_name=file_path,
            )
        assert file_path.read() == ss2_tsv_contents

    def test_get_urls_to_files_for_ss2(
        self, test_ss2_bundle_vx, test_ss2_bundle_manifest_vx
    ):
        fastq_url1, fastq_url2 = smartseq2.get_urls_to_files_for_ss2(test_ss2_bundle_vx)
        assert (
            fastq_url1
            == [
                f['url']
                for f in test_ss2_bundle_manifest_vx
                if f['name'] == 'R1.fastq.gz'
            ][0]
        )
        assert (
            fastq_url2
            == [
                f['url']
                for f in test_ss2_bundle_manifest_vx
                if f['name'] == 'R2.fastq.gz'
            ][0]
        )
