import google.auth.credentials
import io
import pytest
import unittest.mock as mock

from pipeline_tools.shared import gcs_utils


@pytest.fixture(scope='module')
def test_data():
    class Data:
        PROJECT = 'PROJECT'
        CREDENTIALS = mock.Mock(spec=google.auth.credentials.Credentials)
        BUCKET_NAME = 'BUCKET_NAME'
        client = mock.Mock(project=PROJECT, credentials=CREDENTIALS)
        bucket = client.bucket(BUCKET_NAME)
        blob_name = 'test_blob'
        blob = bucket.blob(blob_name)

    return Data


class TestGCSUtils(object):
    def test_get_filename_from_gs_link(self):
        """Test if get_filename_from_gs_link can get correct filename from google cloud storage link.
        """
        link = "gs://test_bucket_name/test_wdl_file.wdl"
        assert gcs_utils.get_filename_from_gs_link(link) == 'test_wdl_file.wdl'

    def test_parse_bucket_blob_from_gs_link_one_slash(self):
        """Test if parse_bucket_blob_from_gs_link can correctly parse bucket name and blob name
         from a single slash google cloud storage link.
         """
        link = "gs://test_bucket_name/test_wdl_file.wdl"
        bucket_name, blob_name = gcs_utils.parse_bucket_blob_from_gs_link(link)
        assert bucket_name == 'test_bucket_name'
        assert blob_name == 'test_wdl_file.wdl'

    def test_parse_bucket_blob_from_gs_link_extra_slashes(self):
        """Test if parse_bucket_blob_from_gs_link can correctly parse bucket name and blob name
         from a slash google cloud storage link with extra slashes.
        """
        link = "gs://test_bucket_name/special/test/wdl/file.wdl"
        bucket_name, blob_name = gcs_utils.parse_bucket_blob_from_gs_link(link)
        assert bucket_name == 'test_bucket_name'
        assert blob_name == 'special/test/wdl/file.wdl'

    def test_download_to_bytes_readable(self, test_data):
        """Test if download_to_buffer correctly download blob and store it into Bytes Buffer."""
        result = gcs_utils.download_to_buffer(
            test_data.bucket.blob(test_data.blob_name)
        )
        assert isinstance(result, io.BytesIO)

    def test_download_gcs_blob(self, test_data):
        """Test if download_gcs_blob can correctly create destination file on the disk."""
        gcs_client = gcs_utils.GoogleCloudStorageClient(
            key_location="test_key", scopes=['test_scope']
        )
        gcs_client.storage_client = test_data.client
        result = gcs_utils.download_gcs_blob(
            gcs_client, test_data.BUCKET_NAME, test_data.blob_name
        )
        assert isinstance(result, io.BytesIO)

    def test_lazyproperty_initialize_late_for_gcs_client(self):
        """Test if the LazyProperty decorator can work well with GoogleCloudStorageClient class."""
        gcs_client = gcs_utils.GoogleCloudStorageClient(
            key_location="test_key", scopes=['test_scope']
        )
        assert gcs_client is not None
        assert gcs_client.key_location == "test_key"
        assert gcs_client.scopes[0] == "test_scope"
