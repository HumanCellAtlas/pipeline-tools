import unittest
import requests_mock
from pipeline_tools import dcp_utils


class TestDCPUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DSS_URL = "https://dss.mock.org/v0"
        cls.FILE_ID = "test_id"
        cls.BUNDLE_UUID = "test_uuid"
        cls.BUNDLE_VERSION = "test_version"
        cls.TIMEOUT_SECONDS = 100
        cls.RETRY_SECONDS = 10
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

        self.assertEquals(json_response['file'], expect_file['file'])

    @requests_mock.mock()
    def test_get_manifest_files(self, mock_request):
        expect_manifest = {
            'name_to_meta': {
                'test_name1': {'name': 'test_name1', 'url': 'test_url1'},
                'test_name2': {'name': 'test_name2', 'url': 'test_url2'}
            },
            'url_to_name': {
                'test_url1': 'test_name1',
                'test_url2': 'test_name2'
            }
        }
        url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
            dss_url=self.DSS_URL, bundle_uuid=self.BUNDLE_UUID, bundle_version=self.BUNDLE_VERSION)

        def _request_callback(request, context):
            context.status_code = 200
            return {
                'bundle': {
                    'files': [
                        {'name': 'test_name1', 'url': 'test_url1'},
                        {'name': 'test_name2', 'url': 'test_url2'}
                    ]

                }
            }

        mock_request.get(url, json=_request_callback)

        result = dcp_utils.get_manifest_files(self.BUNDLE_UUID, self.BUNDLE_VERSION, self.DSS_URL,
                                              self.TIMEOUT_SECONDS, self.RETRY_SECONDS)

        self.assertEquals(result, expect_manifest)

    @requests_mock.mock()
    def test_auth_token(self, mock_request):
        url = "https://test.auth0"

        def _request_callback(request, context):
            context.status_code = 200
            return self.AUTH_TOKEN

        mock_request.post(url, json=_request_callback)

        token = dcp_utils.get_auth_token(url)

        self.assertEquals(token, self.AUTH_TOKEN)

    def test_make_auth_header(self):
        expect_header = {
            "content-type": "application/json",
            "authorization": "{token_type} {access_token}".format(token_type=self.AUTH_TOKEN['token_type'],
                                                                  access_token=self.AUTH_TOKEN['access_token'])
        }
        headers = dcp_utils.make_auth_header(self.AUTH_TOKEN)

        self.assertEquals(headers, expect_header)
