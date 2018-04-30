import unittest
import os
import json
import requests_mock
import pipeline_tools.create_envelope as submit
from tenacity import stop_after_attempt


class TestCreateEnvelope(unittest.TestCase):

    def setUp(self):
        self.headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer auth_token"
        }
        with open(self.data_file('response.json')) as f:
            self.links_json = json.load(f)

        with open(self.data_file('analysis.json')) as f:
            self.analysis_json = json.load(f)

    @requests_mock.mock()
    def test_get_envelope_url(self, mock_request):
        submit_url = "http://api.ingest.dev.data.humancellatlas.org/"

        def _request_callback(request, context):
            context.status_code = 200
            return self.links_json

        mock_request.get(submit_url, json=_request_callback)
        envelope_url = submit.get_envelope_url(submit_url, self.headers)
        expected = "http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes"
        self.assertEqual(envelope_url, expected)
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_get_envelope_url_retries_on_error(self, mock_request):
        submit_url = 'http://api.ingest.dev.data.humancellatlas.org/'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.get(submit_url, json=_request_callback)
        with self.assertRaises(ValueError):
            # Make the test complete faster by limiting the number of retries
            submit.get_envelope_url.retry_with(stop=stop_after_attempt(3))(submit_url, self.headers)
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_create_submission_envelope(self, mock_request):
        pass

    @requests_mock.mock()
    def test_create_submission_envelope_retries_on_error(self, mock_request):
        envelope_url = 'http://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.post(envelope_url, json=_request_callback)
        with self.assertRaises(ValueError):
            # Make the test complete faster by limiting the number of retries
            submit.create_submission_envelope.retry_with(stop=stop_after_attempt(3))(envelope_url, self.headers)
        self.assertEqual(mock_request.call_count, 3)

    def test_create_analysis(self):
        pass

    @requests_mock.mock()
    def test_create_analysis_retries_on_error(self, mock_request):
        analyses_url = 'http://api.ingest.dev.data.humancellatlas.org/abcde/processes'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.post(analyses_url, json=_request_callback)
        with self.assertRaises(ValueError):
            # Make the test complete faster by limiting the number of retries
            submit.create_analysis.retry_with(stop=stop_after_attempt(3))(analyses_url, self.headers, self.analysis_json)
        self.assertEqual(mock_request.call_count, 3)

    def test_add_input_bundles(self):
        pass

    @requests_mock.mock()
    def test_add_input_bundles_retries_on_error(self, mock_request):
        input_bundles_url = 'http://api.ingest.dev.data.humancellatlas.org/processes/abcde/bundleReferences'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.put(input_bundles_url, json=_request_callback)
        with self.assertRaises(ValueError):
            # Make the test complete faster by limiting the number of retries
            submit.add_input_bundles.retry_with(stop=stop_after_attempt(3))(input_bundles_url, self.headers, self.analysis_json)

        self.assertEqual(mock_request.call_count, 3)

    def test_add_file_reference(self):
        pass

    @requests_mock.mock()
    def test_add_file_reference_retries_on_error(self, mock_request):
        file_refs_url = 'http://api.ingest.dev.data.humancellatlas.org/processes/abcde/fileReference'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        mock_request.put(file_refs_url, json=_request_callback)
        file_ref = {
            'fileName': 'aligned_bam',
            'content': {
                'describedBy': 'https://schema.humancellatlas.org/type/file/schema_version/analysis_file',
                'schema_type': 'file',
                'file_core': {
                    'describedBy': 'https://schema.humancellatlas.org/core/file/schema_version/file_core',
                    'file_name': 'test',
                    'file_format': 'bam'
                }
            }
        }
        with self.assertRaises(ValueError):
            # Make the test complete faster by limiting the number of retries
            submit.add_file_reference.retry_with(stop=stop_after_attempt(3))(file_ref, file_refs_url, self.headers)
        self.assertEqual(mock_request.call_count, 3)

    def test_get_entity(self):
        with open(self.data_file('response.json')) as f:
            js = json.load(f)
            entity_url = submit.get_entity_url(js, 'processes')
            self.assertEqual(entity_url, 'http://api.ingest.dev.data.humancellatlas.org/processes')

    def test_get_input_bundle_uuid(self):
        with open(self.data_file('analysis.json')) as f:
            js = json.load(f)
            self.assertEqual(submit.get_input_bundle_uuid(js), '75a7f618-9adc-48af-a249-0010305160f6')

    def test_get_output_files(self):
        schema_version = 'version_232'
        analysis_file_schema_url = 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(schema_version)
        file_core_schema_url = 'https://schema.humancellatlas.org/core/file/{}/file_core'.format(schema_version)

        with open(self.data_file('analysis.json')) as f:
            js = json.load(f)
            outputs = submit.get_output_files(js, schema_version)
            self.assertEqual(len(outputs), 3)
            self.assertEqual(outputs[0]['fileName'], 'aligned_bam')
            self.assertEqual(outputs[0]['content']['schema_type'], 'file')
            self.assertEqual(outputs[0]['content']['describedBy'], analysis_file_schema_url)
            self.assertEqual(outputs[0]['content']['file_core']['describedBy'], file_core_schema_url)
            self.assertEqual(outputs[0]['content']['file_core']['file_name'], 'aligned_bam')
            self.assertEqual(outputs[0]['content']['file_core']['file_format'], 'bam')

    def test_check_status_bad_codes(self):
        with self.assertRaises(ValueError):
            submit.check_status(404, 'foo')
        with self.assertRaises(ValueError):
            submit.check_status(500, 'foo')
        with self.assertRaises(ValueError):
            submit.check_status(301, 'foo')

    def test_check_status_acceptable_codes(self):
        try:
          submit.check_status(200, 'foo')
        except ValueError as e:
            self.fail(str(e))

        try:
            submit.check_status(202, 'foo')
        except ValueError as e:
            self.fail(str(e))

        try:
            submit.check_status(301, 'foo', '3xx')
        except ValueError as e:
            self.fail(str(e))

    def data_file(self, file_name):
        return os.path.split(__file__)[0] + '/data/' + file_name


if __name__ == '__main__':
    unittest.main()
