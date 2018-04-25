import unittest
import requests
import requests_mock
import pipeline_tools.get_staging_urn as gsu
from tenacity import stop_after_attempt, stop_after_delay, RetryError


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
        response = gsu.run(self.envelope_url)
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_run_retry_if_urn_is_none(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 200
            return {}
        mock_request.get(self.envelope_url, json=_request_callback)
        with self.assertRaises(RetryError):
            # Make the test complete faster by limiting the number of retries
            response = gsu.run.retry_with(stop=stop_after_attempt(3))(self.envelope_url)
        self.assertNotEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test__run_retry_if_error(self, mock_request):
        def _request_callback(request, context):
            context.status_code = 500
            return {}
        mock_request.get(self.envelope_url, json=_request_callback)
        with self.assertRaises(requests.HTTPError):
            # Make the test complete faster by limiting the number of retries
            response = gsu.run.retry_with(stop=stop_after_attempt(3))(self.envelope_url)
        self.assertNotEqual(mock_request.call_count, 1)


if __name__ == '__main__':
    unittest.main()
