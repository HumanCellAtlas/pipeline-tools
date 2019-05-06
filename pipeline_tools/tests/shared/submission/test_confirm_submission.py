import pytest
import requests
from tenacity import RetryError

from pipeline_tools.shared.submission import confirm_submission
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


class TestConfirmSubmission(object):
    def test_wait_for_valid_status_success(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Valid'}

        requests_mock.get(envelope_url, json=_request_callback)

        with HttpRequestsManager():
            result = confirm_submission.wait_for_valid_status(
                envelope_url, HttpRequests()
            )
        assert result is True
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

    def test_confirm_success(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {}

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        assert requests_mock.call_count == 1

    def test_confirm_retries_on_error(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        assert requests_mock.call_count == 3

    def test_confirm_retries_on_read_timeout_error(self, requests_mock):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.put(
            '{}/submissionEvent'.format(envelope_url), json=_request_callback
        )
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        assert requests_mock.call_count == 3
