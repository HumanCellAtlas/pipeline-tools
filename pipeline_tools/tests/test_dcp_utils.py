import unittest
import requests_mock
import os
import json
import requests
from pipeline_tools import dcp_utils
from tenacity import stop_after_attempt, stop_after_delay


class TestDCPUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(cls.data_file('metadata/v4/ss2_manifest.json')) as f:
            cls.ss2_manifest_json_v4 = json.load(f)
            cls.ss2_manifest_files_v4 = dcp_utils.get_manifest_file_dicts(cls.ss2_manifest_json_v4)
        cls.DSS_URL = "https://dss.mock.org/v0"
        cls.FILE_ID = "test_id"
        cls.BUNDLE_UUID = "test_uuid"
        cls.BUNDLE_VERSION = "test_version"
        cls.AUTH_TOKEN = {
            "access_token": "test_token",
            "expires_in": 86400,
            "token_type": "Bearer"
        }

    @requests_mock.mock()
    def test_get_file_by_uuid(self, mock_request):
        expect_file = {"file": "test", "id": self.FILE_ID}
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(
            dss_url=self.DSS_URL, file_id=self.FILE_ID)

        def _request_callback(request, context):
            context.status_code = 200
            return expect_file

        mock_request.get(url, json=_request_callback)

        json_response = dcp_utils.get_file_by_uuid(self.FILE_ID, self.DSS_URL)

        self.assertEqual(json_response['file'], expect_file['file'])

    @requests_mock.mock()
    def test_get_file_by_uuid_retries_on_error(self, mock_request):
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(dss_url=self.DSS_URL, file_id=self.FILE_ID)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(url, json=_request_callback)
        with self.assertRaises(requests.HTTPError):
            # Make the test complete faster by limiting the number of retries
            response = dcp_utils.get_file_by_uuid.retry_with(stop=stop_after_attempt(3))(self.FILE_ID, self.DSS_URL)
        self.assertNotEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_get_manifest_retries_on_error(self, mock_request):
        url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
            dss_url=self.DSS_URL, bundle_uuid=self.BUNDLE_UUID, bundle_version=self.BUNDLE_VERSION)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(url, json=_request_callback)
        with self.assertRaises(requests.HTTPError):
            # Make the test complete faster by limiting the number of retries
            response = dcp_utils.get_manifest.retry_with(stop=stop_after_attempt(3))(self.BUNDLE_UUID, self.BUNDLE_VERSION, self.DSS_URL)
        self.assertNotEqual(mock_request.call_count, 1)

    def test_get_manifest_file_dicts(self):
        result = dcp_utils.get_manifest_file_dicts(self.ss2_manifest_json_v4)

        name_to_meta = result['name_to_meta']
        url_to_name = result['url_to_name']
        self.assertEqual(len(name_to_meta), 5)
        self.assertEqual(len(url_to_name), 5)
        self.assertEqual(name_to_meta['R2.fastq.gz']['url'], 'gs://org-humancellatlas-dss-staging/blobs/foo.bar')
        self.assertEqual(url_to_name['gs://org-humancellatlas-dss-staging/blobs/foo.bar'], 'R2.fastq.gz')

    def test_get_file_uuid(self):
        uuid = dcp_utils.get_file_uuid(self.ss2_manifest_files_v4, 'assay.json')
        self.assertEqual(uuid, 'e56638c7-f026-42d0-9be8-24b71a7d6e86')

    def test_get_file_url(self):
        url = dcp_utils.get_file_url(self.ss2_manifest_files_v4, 'R2.fastq.gz')
        self.assertEqual(url, 'gs://org-humancellatlas-dss-staging/blobs/foo.bar')

    @requests_mock.mock()
    def test_auth_token(self, mock_request):
        url = "https://test.auth0"

        def _request_callback(request, context):
            context.status_code = 200
            return self.AUTH_TOKEN

        mock_request.post(url, json=_request_callback)

        token = dcp_utils.get_auth_token(url)

        self.assertEqual(token, self.AUTH_TOKEN)

    def test_make_auth_header(self):
        expect_header = {
            "Content-type": "application/json",
            "Authorization": "{token_type} {access_token}".format(token_type=self.AUTH_TOKEN['token_type'],
                                                                  access_token=self.AUTH_TOKEN['access_token'])
        }
        headers = dcp_utils.make_auth_header(self.AUTH_TOKEN)

        self.assertEqual(headers, expect_header)

    @staticmethod
    def data_file(file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name
