import unittest
import requests
import requests_mock
from pipeline_tools import confirm_submission
from tenacity import stop_after_attempt, RetryError


class TestConfirmSubmission(unittest.TestCase):

    @requests_mock.mock()
    def test_wait_for_valid_status(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Valid'}

        mock_request.get(envelope_url, json=_request_callback)
        status = confirm_submission.wait_for_valid_status(envelope_url)
        self.assertEqual(status, 'Valid')

    @requests_mock.mock()
    def test_wait_for_valid_status_retries_if_not_valid(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 200
            return {'submissionState': 'Validating'}

        mock_request.get(envelope_url, json=_request_callback)
        with self.assertRaises(RetryError):
            # Make the test complete faster by limiting the number of retries
            confirm_submission.wait_for_valid_status.retry_with(stop=stop_after_attempt(3))(envelope_url)
        self.assertNotEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_wait_for_valid_status_retries_on_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(envelope_url, json=_request_callback)
        with self.assertRaises(requests.HTTPError):
            # Make the test complete faster by limiting the number of retries
            confirm_submission.wait_for_valid_status.retry_with(stop=stop_after_attempt(3))(envelope_url)
        self.assertNotEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_confirm_retries_on_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes/abcde'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.put('{}/submissionEvent'.format(envelope_url), json=_request_callback)
        with self.assertRaises(requests.HTTPError):
            # Make the test complete faster by limiting the number of retries
            confirm_submission.confirm.retry_with(stop=stop_after_attempt(3))(envelope_url)
        self.assertNotEqual(mock_request.call_count, 1)
