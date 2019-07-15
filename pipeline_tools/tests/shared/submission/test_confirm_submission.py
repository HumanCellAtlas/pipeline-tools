import pytest
import requests
from tenacity import RetryError

import unittest.mock as mock
from pipeline_tools.shared.submission import confirm_submission
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


@pytest.fixture(scope='module')
def test_data():
    class Data:
        headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer auth_token",
        }
        runtime_environment = 'dev'
        service_account_path = 'fake_path/to/service_account_key.json'

    return Data


class TestConfirmSubmission(object):
    def test_wait_for_valid_status_with_valid_envelope(self, requests_mock):
        status = 'Valid'
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': status}

        requests_mock.get(envelope_url, json=_request_callback)

        with HttpRequestsManager():
            result = confirm_submission.wait_for_valid_status(
                envelope_url, HttpRequests()
            )
        assert result.get('submissionState') == status
        assert requests_mock.call_count == 1

    def test_wait_for_valid_status_with_complete_envelope(self, requests_mock):
        status = 'Complete'
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': status}

        requests_mock.get(envelope_url, json=_request_callback)

        with HttpRequestsManager():
            result = confirm_submission.wait_for_valid_status(
                envelope_url, HttpRequests()
            )
        assert result.get('submissionState') == status
        assert requests_mock.call_count == 1

    def test_wait_for_valid_status_with_invalid_envelope(self, requests_mock):
        status = 'Invalid'
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': status}

        requests_mock.get(envelope_url, json=_request_callback)

        with HttpRequestsManager():
            result = confirm_submission.wait_for_valid_status(
                envelope_url, HttpRequests()
            )
        assert result.get('submissionState') == status
        assert requests_mock.call_count == 1

    def test_wait_for_valid_status_retries_if_not_valid(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Validating'}

        requests_mock.get(envelope_url, json=_request_callback)
        with pytest.raises(RetryError), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_wait_for_valid_status_retries_on_error(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.get(envelope_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_wait_for_valid_status_retries_on_read_timeout_error(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.get(envelope_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_confirm_success(self, requests_mock, test_data):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {}

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with HttpRequestsManager(), mock.patch(
            'pipeline_tools.shared.auth_utils.DCPAuthClient.get_auth_header',
            return_value=test_data.headers,
        ):
            confirm_submission.confirm(
                envelope_url,
                HttpRequests(),
                test_data.runtime_environment,
                test_data.service_account_path,
            )
        assert requests_mock.call_count == 1

    def test_confirm_retries_on_error(self, requests_mock, test_data):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with pytest.raises(requests.HTTPError), HttpRequestsManager(), mock.patch(
            'pipeline_tools.shared.auth_utils.DCPAuthClient.get_auth_header',
            return_value=test_data.headers,
        ):
            confirm_submission.confirm(
                envelope_url,
                HttpRequests(),
                test_data.runtime_environment,
                test_data.service_account_path,
            )
        assert requests_mock.call_count == 3

    def test_confirm_retries_on_read_timeout_error(self, requests_mock, test_data):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager(), mock.patch(
            'pipeline_tools.shared.auth_utils.DCPAuthClient.get_auth_header',
            return_value=test_data.headers,
        ):
            confirm_submission.confirm(
                envelope_url,
                HttpRequests(),
                test_data.runtime_environment,
                test_data.service_account_path,
            )
        assert requests_mock.call_count == 3
