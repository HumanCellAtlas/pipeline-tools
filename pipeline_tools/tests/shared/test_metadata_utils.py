import json
import os
import pytest
from humancellatlas.data.metadata.api import Bundle, ManifestEntry
from pipeline_tools.shared import metadata_utils
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
def test_fastq_file_manifest(test_ss2_bundle_manifest_vx):
    file_manifest_json = [
        f for f in test_ss2_bundle_manifest_vx if f['name'] == 'R1.fastq.gz'
    ]
    return ManifestEntry.from_json(file_manifest_json[0])


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

    def test_get_ncbi_taxon_id(self, test_ss2_bundle_vx):
        ncbi_taxon_id = metadata_utils.get_ncbi_taxon_id(test_ss2_bundle_vx)
        assert ncbi_taxon_id == 9606

    def test_get_library_construction_method_ontology(self, test_tenx_bundle_vx):
        library_construction_method_ontology = metadata_utils.get_library_construction_method_ontology(
            test_tenx_bundle_vx
        )
        assert library_construction_method_ontology == "EFO:0009310"

    def test_get_hashes_from_file_manifest(self, test_fastq_file_manifest):
        file_hashes = metadata_utils.get_hashes_from_file_manifest(
            test_fastq_file_manifest
        )
        sha1 = test_fastq_file_manifest.sha1
        sha256 = test_fastq_file_manifest.sha256
        s3_etag = test_fastq_file_manifest.s3_etag
        crc32c = test_fastq_file_manifest.crc32c
        expected_file_hashes = f'{sha1}{sha256}{s3_etag}{crc32c}'
        assert file_hashes == expected_file_hashes
