import unittest
import requests
import requests_mock
from pipeline_tools import confirm_submission
from pipeline_tools.http_requests import HttpRequests
from .http_requests_manager import HttpRequestsManager
from tenacity import RetryError


class TestConfirmSubmission(unittest.TestCase):

    @requests_mock.mock()
    def test_wait_for_valid_status_success(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Valid'}

        mock_request.get(envelope_url, json=_request_callback)

        with HttpRequestsManager():
            result = confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        self.assertEqual(result, True)
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_wait_for_valid_status_retries_if_not_valid(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Validating'}

        mock_request.get(envelope_url, json=_request_callback)
        with self.assertRaises(RetryError), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_wait_for_valid_status_retries_on_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(envelope_url, json=_request_callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_wait_for_valid_status_retries_on_read_timeout_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        mock_request.get(envelope_url, json=_request_callback)
        with self.assertRaises(requests.ReadTimeout), HttpRequestsManager():
            confirm_submission.wait_for_valid_status(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_confirm_success(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 200
            return {}

        mock_request.put('{}/submissionEvent'.format(envelope_url), json=_request_callback)
        with HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_confirm_retries_on_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.put('{}/submissionEvent'.format(envelope_url), json=_request_callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_confirm_retries_on_read_timeout_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        mock_request.put('{}/submissionEvent'.format(envelope_url), json=_request_callback)
        with self.assertRaises(requests.ReadTimeout), HttpRequestsManager():
            confirm_submission.confirm(envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)
