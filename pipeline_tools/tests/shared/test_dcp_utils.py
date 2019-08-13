import pytest

from pipeline_tools.shared import dcp_utils
from pipeline_tools.shared.http_requests import HttpRequests
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
            "expires_in": 86400,
            "token_type": "Bearer",
        }

    return Data


class TestDCPUtils(object):
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
            "Content-type": "application/json",
            "Authorization": "{token_type} {access_token}".format(
                token_type=test_data.AUTH_TOKEN['token_type'],
                access_token=test_data.AUTH_TOKEN['access_token'],
            ),
        }
        headers = dcp_utils.make_auth_header(test_data.AUTH_TOKEN)

        assert headers == expect_header
