import unittest
from pipeline_tools import get_files_to_upload


class TestCheckStagedFiles(unittest.TestCase):

    def setUp(self):
        self.files = ['gs://foo/call-Star/Aligned.sortedByCoord.out.bam', 'gs://foo/GSM1957573_rna_metrics']

    def test_files_to_upload(self):
        uploaded_files = ['Aligned.sortedByCoord.out.bam']
        result = get_files_to_upload.get_files_to_upload(self.files, uploaded_files)
        self.assertEqual(result, ['gs://foo/GSM1957573_rna_metrics'])

    def test_no_files_to_upload(self):
        uploaded_files = ['Aligned.sortedByCoord.out.bam', 'GSM1957573_rna_metrics']
        result = get_files_to_upload.get_files_to_upload(self.files, uploaded_files)
        self.assertEqual(result, [])

    def test_get_file_name_from_path(self):
        file_path = 'gs://foo/call-Star/Aligned.sortedByCoord.out.bam'
        file_name = get_files_to_upload.get_file_name_from_path(file_path)
        self.assertEqual(file_name, 'Aligned.sortedByCoord.out.bam')
