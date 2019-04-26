import os

from pipeline_tools.shared import http_requests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


class TestHttpRequestsManager(object):
    def test_enter_creates_directory(self):
        with HttpRequestsManager() as temp_dir:
            assert os.path.isdir(temp_dir) is True

    def test_exit_deletes_directory(self):
        with HttpRequestsManager() as temp_dir:
            temp_dir_name = temp_dir
            assert os.path.isdir(temp_dir_name) is True
        assert os.path.isdir(temp_dir) is False

    def test_enter_sets_environment_vars(self):
        with HttpRequestsManager() as temp_dir:
            assert http_requests.HTTP_RECORD_DIR in os.environ
            assert os.environ[http_requests.HTTP_RECORD_DIR] == temp_dir
            assert http_requests.RECORD_HTTP_REQUESTS in os.environ
            assert os.environ[http_requests.RECORD_HTTP_REQUESTS] == 'true'
            assert http_requests.RETRY_MAX_TRIES in os.environ
            assert os.environ[http_requests.RETRY_MAX_TRIES] == '3'
            assert http_requests.RETRY_MAX_INTERVAL in os.environ
            assert os.environ[http_requests.RETRY_MAX_INTERVAL] == '10'
            assert http_requests.RETRY_TIMEOUT in os.environ
            assert os.environ[http_requests.RETRY_TIMEOUT] == '1'
            assert http_requests.RETRY_MULTIPLIER in os.environ
            assert os.environ[http_requests.RETRY_MULTIPLIER] == '0.01'
            assert http_requests.INDIVIDUAL_REQUEST_TIMEOUT in os.environ
            assert os.environ[http_requests.INDIVIDUAL_REQUEST_TIMEOUT] == '1'

    def test_exit_deletes_environment_var(self):
        with HttpRequestsManager() as temp_dir:
            pass
        assert http_requests.HTTP_RECORD_DIR not in os.environ
        assert http_requests.RECORD_HTTP_REQUESTS not in os.environ
        assert http_requests.RETRY_MAX_TRIES not in os.environ
        assert http_requests.RETRY_MAX_INTERVAL not in os.environ
        assert http_requests.RETRY_TIMEOUT not in os.environ
        assert http_requests.RETRY_MULTIPLIER not in os.environ
        assert http_requests.INDIVIDUAL_REQUEST_TIMEOUT not in os.environ
