#!/usr/bin/env python
import io
import unittest
try:
    # if python3
    import unittest.mock as mock
except ImportError:
    # if python2
    import mock

from pipeline_tools import gcs_utils


def _make_credentials():
    import google.auth.credentials
    return mock.Mock(spec=google.auth.credentials.Credentials)


class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.PROJECT = 'PROJECT'
        cls.CREDENTIALS = _make_credentials()
        cls.BUCKET_NAME = 'BUCKET_NAME'
        cls.client = mock.Mock(project=cls.PROJECT, credentials=cls.CREDENTIALS)
        cls.bucket = cls.client.bucket(cls.BUCKET_NAME)
        cls.blob_name = 'test_blob'
        cls.blob = cls.bucket.blob(cls.blob_name)

    def test_get_filename_from_gs_link(self):
        """Test if get_filename_from_gs_link can get correct filename from google cloud storage link.
        """
        link = "gs://test_bucket_name/test_wdl_file.wdl"
        self.assertEqual(gcs_utils.get_filename_from_gs_link(link), 'test_wdl_file.wdl')

    def test_parse_bucket_blob_from_gs_link_one_slash(self):
        """Test if parse_bucket_blob_from_gs_link can correctly parse bucket name and blob name
         from a single slash google cloud storage link.
         """
        link = "gs://test_bucket_name/test_wdl_file.wdl"
        bucket_name, blob_name = gcs_utils.parse_bucket_blob_from_gs_link(link)
        self.assertEqual(bucket_name, 'test_bucket_name')
        self.assertEqual(blob_name, 'test_wdl_file.wdl')

    def test_parse_bucket_blob_from_gs_link_extra_slashes(self):
        """Test if parse_bucket_blob_from_gs_link can correctly parse bucket name and blob name
         from a slash google cloud storage link with extra slashes.
        """
        link = "gs://test_bucket_name/special/test/wdl/file.wdl"
        bucket_name, blob_name = gcs_utils.parse_bucket_blob_from_gs_link(link)
        self.assertEqual(bucket_name, 'test_bucket_name')
        self.assertEqual(blob_name, 'special/test/wdl/file.wdl')

    def test_download_to_bytes_readable(self):
        """Test if download_to_buffer correctly download blob and store it into Bytes Buffer."""
        result = gcs_utils.download_to_buffer(self.bucket.blob(self.blob_name))
        self.assertIsInstance(result, io.BytesIO)

    def test_download_gcs_blob(self):
        """Test if download_gcs_blob can correctly create destination file on the disk."""
        gcs_client = gcs_utils.GoogleCloudStorageClient(key_location="test_key", scopes=['test_scope'])
        gcs_client.storage_client = self.client
        result = gcs_utils.download_gcs_blob(gcs_client, self.BUCKET_NAME, self.blob_name)
        self.assertIsInstance(result, io.BytesIO)

    def test_lazyproperty_initialize_late_for_gcs_client(self):
        """Test if the LazyProperty decorator can work well with GoogleCloudStorageClient class."""
        gcs_client = gcs_utils.GoogleCloudStorageClient(key_location="test_key", scopes=['test_scope'])
        self.assertIsNotNone(gcs_client)
        self.assertEqual(gcs_client.key_location, "test_key")
        self.assertEqual(gcs_client.scopes[0], "test_scope")


if __name__ == '__main__':
    unittest.main()
