import json
import os
import pytest
import unittest.mock as mock
from humancellatlas.data.metadata.api import Bundle


from pipeline_tools.pipelines.cellranger import cellranger
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
from pipeline_tools.shared.reference_id import ReferenceId
from pathlib import Path


data_dir = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/'


@pytest.fixture(scope='module')
def tenx_manifest_json_vx():
    with open(f"{data_dir}metadata/tenx_vx/manifest.json") as f:
        tenx_manifest_json_vx = json.load(f)
    return tenx_manifest_json_vx


@pytest.fixture(scope='module')
def tenx_metadata_files_vx():
    with open(f"{data_dir}metadata/tenx_vx/metadata_files.json") as f:
        tenx_metadata_files_vx = json.load(f)
    return tenx_metadata_files_vx


@pytest.fixture(scope='module')
def tenx_metadata_files_vx_with_no_expected_cell_count():
    with open(
        f"{data_dir}metadata/tenx_vx/metadata_files_with_no_expected_cell_count.json"
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


class TestCellRanger(object):
    # mock.patch should be in reverse order relative to the arguments
    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    def test_get_cellranger_inputs(
        self, mock_sample_id, mock_bundle, mock_ncbi_taxon_id, test_tenx_bundle_vx
    ):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_tenx_bundle_vx
        mock_ncbi_taxon_id.return_value = ReferenceId.Human.value
        with HttpRequestsManager():
            cellranger.create_cellranger_input_tsv(
                uuid='bundle_uuid', version='bundle_version', dss_url='foo_url'
            )
        expected_fastqs = [
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R1.fastq.gz',
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R2.fastq.gz',
            'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/I1.fastq.gz',
        ]
        expected_fastq_names = [
            'fake_id_S1_L001_R1_001.fastq.gz',
            'fake_id_S1_L001_R2_001.fastq.gz',
            'fake_id_S1_L001_I1_001.fastq.gz',
        ]
        expected_total_estimated_cells = 10000
        expected_reference_name = 'GRCh38'
        expected_transcriptome_tar_gz = 'gs://hca-dcp-mint-test-data/reference/GRCh38_Gencode/GRCh38_GencodeV27_Primary_CellRanger.tar'

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

        with open('reference_name.txt') as f:
            actual_ref_name = f.read().strip()
            assert actual_ref_name == expected_reference_name

        with open('transcriptome_tar_gz.txt') as f:
            actual_transcriptome_tar_gz = f.read().strip()
            assert actual_transcriptome_tar_gz == expected_transcriptome_tar_gz

        os.remove('fastqs.txt')
        os.remove('fastq_names.txt')
        os.remove('sample_id.txt')
        os.remove('expect_cells.txt')
        os.remove('reference_name.txt')
        os.remove('transcriptome_tar_gz.txt')

    def test_get_expected_cell_count(self, test_tenx_bundle_vx):
        total_estimated_cells = cellranger.get_expected_cell_count(test_tenx_bundle_vx)
        assert total_estimated_cells == 10000

    def test_get_expected_cell_count_sets_default_value(
        self, test_tenx_bundle_vx_with_no_expected_cell_count
    ):
        total_estimated_cells = cellranger.get_expected_cell_count(
            test_tenx_bundle_vx_with_no_expected_cell_count
        )
        assert total_estimated_cells == 3000
