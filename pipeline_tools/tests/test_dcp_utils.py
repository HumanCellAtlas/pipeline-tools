import pytest
import requests

from pipeline_tools import dcp_utils
from pipeline_tools.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


@pytest.fixture(scope='module')
def test_data():
    class Data:
        DSS_URL = "https://dss.mock.org/v0"
        FILE_ID = "test_id"
        BUNDLE_UUID = "test_uuid"
        BUNDLE_VERSION = "test_version"
        AUTH_TOKEN = {
            "access_token": "test_token",
            "expires_in"  : 86400,
            "token_type"  : "Bearer"
        }

    return Data


class TestDCPUtils(object):

    def test_get_file_by_uuid(self, requests_mock, test_data):
        expect_file = {"file": "test", "id": test_data.FILE_ID}
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(
                dss_url=test_data.DSS_URL, file_id=test_data.FILE_ID)

        def _request_callback(request, context):
            context.status_code = 200
            return expect_file

        requests_mock.get(url, json=_request_callback)

        with HttpRequestsManager():
            json_response = dcp_utils.get_file_by_uuid(test_data.FILE_ID, test_data.DSS_URL, HttpRequests())

        assert json_response['file'] == expect_file['file']

        assert requests_mock.call_count == 1

    def test_get_file_by_uuid_retries_on_error(self, requests_mock, test_data):
        url = '{dss_url}/files/{file_id}?replica=gcp'.format(dss_url=test_data.DSS_URL, file_id=test_data.FILE_ID)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.get(url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            dcp_utils.get_file_by_uuid(test_data.FILE_ID, test_data.DSS_URL, HttpRequests())
        assert requests_mock.call_count == 3

    def test_get_manifest_retries_on_error(self, requests_mock, test_data):
        url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
                dss_url=test_data.DSS_URL, bundle_uuid=test_data.BUNDLE_UUID, bundle_version=test_data.BUNDLE_VERSION)

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.get(url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            dcp_utils.get_manifest(test_data.BUNDLE_UUID, test_data.BUNDLE_VERSION, test_data.DSS_URL, HttpRequests())
        assert requests_mock.call_count == 3

    def test_get_auth_token(self, requests_mock, test_data):
        url = "https://test.auth0"

        def _request_callback(request, context):
            context.status_code = 200
            return test_data.AUTH_TOKEN

        requests_mock.post(url, json=_request_callback)

        with HttpRequestsManager():
            token = dcp_utils.get_auth_token(HttpRequests(), url=url)

        assert token == test_data.AUTH_TOKEN

    def test_make_auth_header(self, test_data):
        expect_header = {
            "Content-type" : "application/json",
            "Authorization": "{token_type} {access_token}".format(token_type=test_data.AUTH_TOKEN['token_type'],
                                                                  access_token=test_data.AUTH_TOKEN['access_token'])
        }
        headers = dcp_utils.make_auth_header(test_data.AUTH_TOKEN)

        assert headers == expect_header
