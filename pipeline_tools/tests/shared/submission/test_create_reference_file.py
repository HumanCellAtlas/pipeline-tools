import pytest

import pipeline_tools.shared.submission.create_reference_file as crf


@pytest.fixture(scope='module')
def test_data():
    class Data:
        input_uuid = "GRCm38.primary_assembly.genome.fa"
        file_path = "GRCm38.primary_assembly.genome.fa"
        genus_species = "Mus musculus"
        workspace_version = "2021-05-24T12:00:00Z"
        reference_version = "GencodeM21"
        ncbi_taxon_id = 10090
        pipeline_type = "optimus"
        assembly_type = "primary assembly"
        reference_type = "genome sequence"

    return Data


class TestCreateReferenceFile(object):
    def test_build_reference_file(self, test_data):
        reference_file = crf.test_create_reference_file(
            test_data.file_path,
            test_data.input_uuid,
            test_data.genus_species,
            test_data.assembly_type,
            test_data.pipeline_type,
            test_data.ncbi_taxon_id,
            test_data.reference_type,
            test_data.workspace_version,
            test_data.reference_version)

        assert(reference_file['describedBy'] == "https://schema.humancellatlas.org/type/file/3.2.0/reference_file")
        assert(reference_file['schema_type'] == "file")
        assert(reference_file['schema_version'] == "3.2.0")
        assert(reference_file['reference_type'] == "genome sequence")
        assert(reference_file['reference_version'] == "GencodeM21")
        assert(reference_file['file_core']['file_name'] == "GRCm38.primary_assembly.genome.fa")
        assert(reference_file['file_core']['format'] == "fasta")
        assert(reference_file['provenance']['document_id'] == "d4d4d1d2-0a52-55d1-a0c0-ecfa36398a9b")
        assert(reference_file['provenance']['submission_date'] == "2021-05-24T12:00:00Z")
        assert(reference_file['genus_species']['text'] == "Mus musculus")
        assert(reference_file['ncbi_taxon_id'] == 10090)
