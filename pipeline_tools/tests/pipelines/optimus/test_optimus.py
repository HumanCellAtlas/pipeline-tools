import json
import os
import pytest
import unittest.mock as mock
from humancellatlas.data.metadata.api import Bundle, ManifestEntry


from pipeline_tools.pipelines.optimus import optimus, chemistry
from pipeline_tools.shared.reference_id import ReferenceId
from pipeline_tools.shared.tenx_utils import UnsupportedTenXChemistryError
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
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
    # mock.patch should be in reverse order relative to the arguments
    @mock.patch(
        'pipeline_tools.shared.metadata_utils.get_library_construction_method_ontology'
    )
    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    def test_get_optimus_inputs(
        self,
        mock_sample_id,
        mock_bundle,
        mock_ncbi_taxon_id,
        mock_lib_construction_method,
        test_tenx_bundle_vx,
    ):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_tenx_bundle_vx
        mock_ncbi_taxon_id.return_value = ReferenceId.Human.value
        mock_lib_construction_method.return_value = (
            optimus.LibraryConstructionMethod.tenX_v2.value
        )
        with HttpRequestsManager():
            optimus.create_optimus_input_tsv(
                uuid='bundle_uuid', version='bundle_version', dss_url='foo_url'
            )
        expected_r1 = 'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R1.fastq.gz'
        expected_r2 = 'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/R2.fastq.gz'
        expected_i1 = 'gs://org-hca-dss-checkout-integration/bundles/3eebea0c-8b80-4007-a860-6802a215276d.2018-10-05T145809.216048Z/I1.fastq.gz'
        expected_tar_star_reference = 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/buildReference/output_bucket/star_primary_gencode_v27.tar'
        expected_annotations_gtf = 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/gencode.v27.primary_assembly.annotation.gtf.gz'
        expected_ref_genome_fasta = 'gs://hca-dcp-analysis-pipelines-reference/alignmentReferences/optimusGencodeV27/GRCh38.primary_assembly.genome.fa'
        expected_chemistry = optimus.Chemistry.tenX_v2.value

        assert_file_contents('sample_id.txt', 'fake_id')
        assert_file_contents('r1.txt', expected_r1)
        assert_file_contents('r2.txt', expected_r2)
        assert_file_contents('i1.txt', expected_i1)
        assert_file_contents('tar_star_reference.txt', expected_tar_star_reference)
        assert_file_contents('annotations_gtf.txt', expected_annotations_gtf)
        assert_file_contents('ref_genome_fasta.txt', expected_ref_genome_fasta)
        assert_file_contents('chemistry.txt', expected_chemistry)

        os.remove('sample_id.txt')
        os.remove('r1.txt')
        os.remove('r2.txt')
        os.remove('i1.txt')
        os.remove('tar_star_reference.txt')
        os.remove('annotations_gtf.txt')
        os.remove('ref_genome_fasta.txt')
        os.remove('chemistry.txt')

    @mock.patch(
        'pipeline_tools.shared.metadata_utils.get_library_construction_method_ontology'
    )
    @mock.patch('pipeline_tools.shared.metadata_utils.get_ncbi_taxon_id')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_bundle_metadata')
    @mock.patch('pipeline_tools.shared.metadata_utils.get_sample_id')
    def test_optimus_inputs_to_hash(
        self,
        mock_sample_id,
        mock_bundle,
        mock_ncbi_taxon_id,
        mock_lib_construction_method,
        test_tenx_bundle_vx,
        test_tenx_bundle_manifest_vx,
    ):
        mock_sample_id.return_value = 'fake_id'
        mock_bundle.return_value = test_tenx_bundle_vx
        mock_ncbi_taxon_id.return_value = ReferenceId.Human.value
        mock_lib_construction_method.return_value = (
            optimus.LibraryConstructionMethod.tenX_v2.value
        )
        with HttpRequestsManager():
            inputs = optimus.get_optimus_inputs_to_hash(
                uuid='bundle_uuid', version='bundle_version', dss_url='foo_url'
            )
        read1_manifest_json = [
            f for f in test_tenx_bundle_manifest_vx if f['name'] == 'R1.fastq.gz'
        ][0]
        read2_manifest_json = [
            f for f in test_tenx_bundle_manifest_vx if f['name'] == 'R2.fastq.gz'
        ][0]
        index1_manifest_json = [
            f for f in test_tenx_bundle_manifest_vx if f['name'] == 'I1.fastq.gz'
        ][0]
        read1_manifest = ManifestEntry.from_json(read1_manifest_json)
        read2_manifest = ManifestEntry.from_json(read2_manifest_json)
        index1_manifest = ManifestEntry.from_json(index1_manifest_json)
        r1_hashes = f'{read1_manifest.sha1}{read1_manifest.sha256}{read1_manifest.s3_etag}{read1_manifest.crc32c}'
        r2_hashes = f'{read2_manifest.sha1}{read2_manifest.sha256}{read2_manifest.s3_etag}{read2_manifest.crc32c}'
        i1_hashes = f'{index1_manifest.sha1}{index1_manifest.sha256}{index1_manifest.s3_etag}{index1_manifest.crc32c}'
        expected_chemistry = optimus.Chemistry.tenX_v2.value

        assert inputs == (
            'fake_id',
            ReferenceId.Human.value,
            f'{r1_hashes}{r2_hashes}{i1_hashes}',
            expected_chemistry,
        )

    def test_get_tenx_chemistry(self):
        ontology_id = "EFO:0009310"
        expected_chemistry = chemistry.Chemistry.tenX_v2.value
        tenx_chemistry = optimus.get_tenx_chemistry(ontology_id)
        assert tenx_chemistry == expected_chemistry

    def test_get_tenx_chemistry_raises_error_for_unsupported_values(self):
        fake_ontology_id = "EFO:0000000"
        with pytest.raises(UnsupportedTenXChemistryError):
            tenx_chemistry = optimus.get_tenx_chemistry(fake_ontology_id)


def assert_file_contents(actual_file, expected_contents):
    with open(actual_file) as f:
        actual = f.read().strip()
        assert actual == expected_contents
