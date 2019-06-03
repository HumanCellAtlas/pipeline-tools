import json
import os

from humancellatlas.data.metadata.api import Bundle


from pipeline_tools.pipelines.optimus import optimus
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
import pytest
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


class TestOptimus(object):
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    def test_get_cellranger_inputs(
        self, mock_sample_id, mock_bundle, test_tenx_bundle_vx, mock_ncbi_taxon_id
    ):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_tenx_bundle_vx
        mock_ncbi_taxon_id.return_value = 9606
        with HttpRequestsManager():
            optimus.create_optimus_input_tsv(
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
        expected_tar_star_reference = 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/buildReference/output_bucket/star_primary_gencode_v27.tar'
        expected_annotations_gtf = 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/gencode.v27.primary_assembly.annotation.gtf.gz'
        expected_ref_genome_fasta = 'gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/optimusGencodeV27/GRCh38.primary_assembly.genome.fa'

        with open('sample_id.txt') as f:
            sample_id = f.read().strip()
            assert sample_id == 'fake_id'

        with open('fastqs.txt') as f:
            actual_fastqs = f.readlines()
        for idx, url in enumerate(actual_fastqs):
            assert actual_fastqs[idx].strip() == expected_fastqs[idx]

        with open('fastq_names.txt') as f:
            actual_fastq_names = f.readlines()
        for idx, url in enumerate(actual_fastq_names):
            assert actual_fastq_names[idx].strip() == expected_fastq_names[idx]

        with open('tar_star_reference.txt') as f:
            actual_tar_star_reference = f.read().strip()
            assert actual_tar_star_reference == expected_tar_star_reference

        with open('annotations_gtf.txt') as f:
            actual_annotations_gtf = f.read().strip()
            assert actual_annotations_gtf == expected_annotations_gtf

        with open('ref_genome_fasta.txt') as f:
            actual_ref_genome_fasta = f.read().strip()
            assert actual_ref_genome_fasta == expected_ref_genome_fasta

        os.remove('fastqs.txt')
        os.remove('fastq_names.txt')
        os.remove('sample_id.txt')
        os.remove('expect_cells.txt')
        os.remove('tar_star_reference.txt')
        os.remove('annotations_gtf.txt')
        os.remove('ref_genome_fasta.txt')
