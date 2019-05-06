import pytest
import requests
from tenacity import RetryError

import pipeline_tools.shared.submission.get_upload_urn as getter
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


@pytest.fixture(scope='module')
def test_data():
    class Data:
        envelope_url = 'https://api.ingest.integration.data.humancellatlas.org/submissionEnvelopes/abcde'
        envelope_json = {
            'stagingDetails': {'stagingAreaLocation': {'value': 'test_urn'}}
        }

    return Data


class TestGetStagingUrn(object):
    def test_get_upload_urn_empty_js(self):
        js = {}
        assert getter.get_upload_urn(js) is None

    def test_get_upload_urn_null_details(self):
        js = {'stagingDetails': None}
        assert getter.get_upload_urn(js) is None

    def test_get_upload_urn_null_location(self):
        js = {'stagingDetails': {'stagingAreaLocation': None}}
        assert getter.get_upload_urn(js) is None

    def test_get_upload_urn_null_value(self):
        js = {'stagingDetails': {'stagingAreaLocation': {'value': None}}}
        assert getter.get_upload_urn(js) is None

    def test_get_upload_urn_valid_value(self, test_data):
        assert getter.get_upload_urn(test_data.envelope_json) == 'test_urn'

    def test_run(self, requests_mock, test_data):
        def _request_callback(request, context):
            context.status_code = 200
            return test_data.envelope_json

        requests_mock.get(test_data.envelope_url, json=_request_callback)
        with HttpRequestsManager():
            response = getter.run(test_data.envelope_url, HttpRequests())
        assert requests_mock.call_count == 1

    def test_run_retry_if_urn_is_none(self, requests_mock, test_data):
        def _request_callback(request, context):
            context.status_code = 200
            return {}

        requests_mock.get(test_data.envelope_url, json=_request_callback)
        with pytest.raises(RetryError), HttpRequestsManager():
            getter.run(test_data.envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_run_retry_if_error(self, requests_mock, test_data):
        def _request_callback(request, context):
            context.status_code = 500
            return {}

        requests_mock.get(test_data.envelope_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            getter.run(test_data.envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_run_retry_if_read_timeout_error_occurs(self, requests_mock, test_data):
        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.get(test_data.envelope_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            getter.run(test_data.envelope_url, HttpRequests())
        assert requests_mock.call_count == 3
