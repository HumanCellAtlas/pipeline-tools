import unittest
import requests
import requests_mock
import pipeline_tools.get_staging_urn as gsu
from .http_requests_manager import HttpRequestsManager
from pipeline_tools.http_requests import HttpRequests
from tenacity import RetryError


class TestGetStagingUrn(unittest.TestCase):

    def setUp(self):
        self.envelope_url = 'http://api.ingest.integration.data.humancellatlas.org/submissionEnvelopes/abcde'
        self.envelope_json = {
            'stagingDetails': {
                'stagingAreaLocation': {
                    'value': 'test_urn'
                }
            }
        }

    def test_get_staging_urn_empty_js(self):
        js = {}
        self.assertIsNone(gsu.get_staging_urn(js))

    def test_get_staging_urn_null_details(self):
        js = { 
            'stagingDetails': None
        }
        self.assertIsNone(gsu.get_staging_urn(js))

    def test_get_staging_urn_null_location(self):
        js = { 
            'stagingDetails': {
                'stagingAreaLocation': None
            }
        }
        self.assertIsNone(gsu.get_staging_urn(js))

    def test_get_staging_urn_null_value(self):
        js = { 
            'stagingDetails': {
                'stagingAreaLocation': {
                    'value': None
                }
            }
        }
        self.assertIsNone(gsu.get_staging_urn(js))

    def test_get_staging_urn_valid_value(self):
        self.assertEqual(gsu.get_staging_urn(self.envelope_json), 'test_urn')

    @requests_mock.mock()
    def test_run(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 200
            return self.envelope_json
        mock_request.get(self.envelope_url, json=_request_callback)
        with HttpRequestsManager():
            response = gsu.run(self.envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_run_retry_if_urn_is_none(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 200
            return {}
        mock_request.get(self.envelope_url, json=_request_callback)
        with self.assertRaises(RetryError), HttpRequestsManager():
            gsu.run(self.envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_run_retry_if_error(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 500
            return {}
        mock_request.get(self.envelope_url, json=_request_callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            gsu.run(self.envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_run_retry_if_read_timeout_error_occurs(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout
        mock_request.get(self.envelope_url, json=_request_callback)
        with self.assertRaises(requests.ReadTimeout), HttpRequestsManager():
            gsu.run(self.envelope_url, HttpRequests())
        self.assertEqual(mock_request.call_count, 3)


if __name__ == '__main__':
    unittest.main()
