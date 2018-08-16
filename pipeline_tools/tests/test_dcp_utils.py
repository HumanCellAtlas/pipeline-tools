import requests
import requests_mock
import unittest

from pipeline_tools import dcp_utils
from pipeline_tools.http_requests import HttpRequests
from .http_requests_manager import HttpRequestsManager


class TestDCPUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DSS_URL = "https://dss.mock.org/v0"
        cls.FILE_ID = "test_id"
        cls.BUNDLE_UUID = "test_uuid"
        cls.BUNDLE_VERSION = "test_version"
        cls.AUTH_TOKEN = {
            "access_token": "test_token",
            "expires_in"  : 86400,
            "token_type"  : "Bearer"
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

        with HttpRequestsManager():
            json_response = dcp_utils.get_file_by_uuid(self.FILE_ID, self.DSS_URL, HttpRequests())

        self.assertEqual(json_response['file'], expect_file['file'])

        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_get_file_by_uuid_retries_on_error(self, mock_request):
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(dss_url=self.DSS_URL, file_id=self.FILE_ID)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(url, json=_request_callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            dcp_utils.get_file_by_uuid(self.FILE_ID, self.DSS_URL, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_get_manifest_retries_on_error(self, mock_request):
        url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
                dss_url=self.DSS_URL, bundle_uuid=self.BUNDLE_UUID, bundle_version=self.BUNDLE_VERSION)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(url, json=_request_callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            dcp_utils.get_manifest(self.BUNDLE_UUID, self.BUNDLE_VERSION, self.DSS_URL, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_get_auth_token(self, mock_request):
        url = "https://test.auth0"

        def _request_callback(request, context):
            context.status_code = 200
            return self.AUTH_TOKEN

        mock_request.post(url, json=_request_callback)

        with HttpRequestsManager():
            token = dcp_utils.get_auth_token(HttpRequests(), url=url)

        self.assertEqual(token, self.AUTH_TOKEN)

    def test_make_auth_header(self):
        expect_header = {
            "Content-type" : "application/json",
            "Authorization": "{token_type} {access_token}".format(token_type=self.AUTH_TOKEN['token_type'],
                                                                  access_token=self.AUTH_TOKEN['access_token'])
        }
        headers = dcp_utils.make_auth_header(self.AUTH_TOKEN)

        self.assertEqual(headers, expect_header)
