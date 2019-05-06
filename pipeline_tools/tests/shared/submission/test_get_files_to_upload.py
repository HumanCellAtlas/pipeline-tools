import pytest

from pipeline_tools.shared.submission import get_files_to_upload


@pytest.fixture(scope='module')
def test_data():
    class Data:
        files = [
            'gs://foo/call-Star/Aligned.sortedByCoord.out.bam',
            'gs://foo/GSM1957573_rna_metrics',
        ]

    return Data


class TestCheckStagedFiles(object):
    def test_files_to_upload(self, test_data):
        uploaded_files = ['Aligned.sortedByCoord.out.bam']
        result = get_files_to_upload.get_files_to_upload(
            test_data.files, uploaded_files
        )
        assert result == ['gs://foo/GSM1957573_rna_metrics']

    def test_no_files_to_upload(self, test_data):
        uploaded_files = ['Aligned.sortedByCoord.out.bam', 'GSM1957573_rna_metrics']
        result = get_files_to_upload.get_files_to_upload(
            test_data.files, uploaded_files
        )
        assert result == []

    def test_get_file_name_from_path(self):
        file_path = 'gs://foo/call-Star/Aligned.sortedByCoord.out.bam'
        file_name = get_files_to_upload.get_file_name_from_path(file_path)
        assert file_name == 'Aligned.sortedByCoord.out.bam'
