import json
import os
import pytest
from humancellatlas.data.metadata.api import Bundle, ManifestEntry
from unittest import mock


from pipeline_tools.pipelines.smartseq2 import smartseq2
from pipeline_tools.shared.reference_id import ReferenceId
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


@pytest.fixture(scope='module')
def test_fastq1_manifest_entry(test_ss2_bundle_manifest_vx):
    file_manifest_json = [
        f for f in test_ss2_bundle_manifest_vx if f['name'] == 'R1.fastq.gz'
    ]
    return ManifestEntry.from_json(file_manifest_json[0])


@pytest.fixture(scope='module')
def test_fastq2_manifest_entry(test_ss2_bundle_manifest_vx):
    file_manifest_json = [
        f for f in test_ss2_bundle_manifest_vx if f['name'] == 'R2.fastq.gz'
    ]
    return ManifestEntry.from_json(file_manifest_json[0])


class TestSmartSeq2(object):
    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    def test_create_ss2_input_tsv(
        self,
        mock_sample_id,
        mock_bundle,
        mock_ncbi_taxon_id,
        tmpdir,
        test_ss2_bundle_vx,
        ss2_tsv_contents,
    ):
        file_path = tmpdir.join('inputs.tsv')
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_ss2_bundle_vx
        mock_ncbi_taxon_id.return_value = ReferenceId.Human.value
        with HttpRequestsManager():
            smartseq2.create_ss2_input_tsv(
                bundle_uuid='bundle_id',
                bundle_version='bundle_version',
                dss_url='foo_url',
                input_tsv_name=file_path,
            )
        assert file_path.read() == ss2_tsv_contents

    def test_get_fastq_manifest_entry_for_ss2(
        self, test_ss2_bundle_vx, test_ss2_bundle_manifest_vx
    ):
        fastq1_manifest_entry, fastq2_manifest_entry = smartseq2.get_fastq_manifest_entry_for_ss2(
            test_ss2_bundle_vx
        )
        assert (
            fastq1_manifest_entry.url
            == [
                f['url']
                for f in test_ss2_bundle_manifest_vx
                if f['name'] == 'R1.fastq.gz'
            ][0]
        )
        assert (
            fastq2_manifest_entry.url
            == [
                f['url']
                for f in test_ss2_bundle_manifest_vx
                if f['name'] == 'R2.fastq.gz'
            ][0]
        )

    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    def test_get_ss2_paired_end_inputs_to_hash(
        self,
        mock_sample_id,
        mock_bundle,
        mock_ncbi_taxon_id,
        test_ss2_bundle_vx,
        test_fastq1_manifest_entry,
        test_fastq2_manifest_entry,
    ):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_ss2_bundle_vx
        mock_ncbi_taxon_id.return_value = ReferenceId.Human.value
        with HttpRequestsManager():
            inputs_to_hash = smartseq2.get_ss2_paired_end_inputs_to_hash(
                bundle_uuid='bundle_id',
                bundle_version='bundle_version',
                dss_url='foo_url',
            )
        fastq1_hashes = f'{test_fastq1_manifest_entry.sha1}{test_fastq1_manifest_entry.sha256}{test_fastq1_manifest_entry.s3_etag}{test_fastq1_manifest_entry.crc32c}'
        fastq2_hashes = f'{test_fastq2_manifest_entry.sha1}{test_fastq2_manifest_entry.sha256}{test_fastq2_manifest_entry.s3_etag}{test_fastq2_manifest_entry.crc32c}'
        assert inputs_to_hash == (
            'fake_id',
            ReferenceId.Human.value,
            fastq1_hashes,
            fastq2_hashes,
        )
