import json
import os
import pytest
import requests

import pipeline_tools.shared.submission.create_envelope as submit
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager
from pathlib import Path


data_dir = f'{Path(os.path.split(__file__)[0]).absolute().parents[1]}/data/'


@pytest.fixture(scope='module')
def test_data():
    class Data:
        headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer auth_token",
        }
        with open('{0}response.json'.format(data_dir)) as f:
            links_json = json.load(f)

        with open('{0}analysis_process.json'.format(data_dir)) as f:
            analysis_process = json.load(f)

        with open('{0}analysis_protocol.json'.format(data_dir)) as f:
            analysis_protocol = json.load(f)

        with open('{0}add_protocol_response.json'.format(data_dir)) as f:
            add_analysis_protocol_response = json.load(f)

        analysis_protocol_id = analysis_protocol['protocol_core']['protocol_id']
        analysis_process_id = analysis_process['process_core']['process_id']

    return Data


class TestCreateEnvelope(object):
    def test_get_subject_url_for_processes(self, test_data):
        entity_url = submit.get_subject_url(test_data.links_json, 'processes')
        assert entity_url == 'https://api.ingest.dev.data.humancellatlas.org/processes'

    def test_get_subject_url_for_protocols(self, test_data):
        entity_url = submit.get_subject_url(test_data.links_json, 'protocols')
        assert entity_url == 'https://api.ingest.dev.data.humancellatlas.org/protocols'

    def test_get_subject_url_for_protocol_entity(self, test_data):
        entity_url = submit.get_subject_url(
            test_data.add_analysis_protocol_response, 'self'
        )
        assert (
            entity_url
            == 'https://api.ingest.integration.data.humancellatlas.org/protocols/5bcb9777593d3c0007227a54'
        )

    def test_get_subject_url_for_envelopes(self, test_data):
        entity_url = submit.get_subject_url(test_data.links_json, 'submissionEnvelopes')
        assert (
            entity_url
            == 'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes'
        )

    def test_get_envelope_url(self, requests_mock, test_data):
        submit_url = "https://api.ingest.dev.data.humancellatlas.org/"

        def _request_callback(request, context):
            context.status_code = 200
            return test_data.links_json

        requests_mock.get(submit_url, json=_request_callback)
        with HttpRequestsManager():
            envelope_url = submit.get_envelope_url(
                submit_url, test_data.headers, HttpRequests()
            )
        expected = "https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes"
        assert envelope_url == expected
        assert requests_mock.call_count == 1

    def test_get_envelope_url_retries_on_error(self, requests_mock, test_data):
        submit_url = 'https://api.ingest.dev.data.humancellatlas.org/'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.get(submit_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.get_envelope_url(submit_url, test_data.headers, HttpRequests())
        assert requests_mock.call_count == 3

    def test_get_envelope_url_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        submit_url = 'https://api.ingest.dev.data.humancellatlas.org/'

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.get(submit_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.get_envelope_url(submit_url, test_data.headers, HttpRequests())
        assert requests_mock.call_count == 3

    def test_create_submission_envelope(self, requests_mock, test_data):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes'
        )

        def _request_callback(request, context):
            context.status_code = 201
            return {
                'submissionDate': '2018-08-22T15:10:11.993Z',
                'updateDate': '2018-08-22T15:10:11.993Z',
                'user': 'fake1@clients',
                'lastModifiedUser': 'fake1@clients',
                'uuid': {'uuid': 'fake-uuid'},
                'events': [],
                'stagingDetails': None,
                'submissionState': 'Pending',
                'triggersAnalysis': True,
                'submissionErrors': [],
                'open': True,
                '_links': test_data.links_json['_links'],
            }

        requests_mock.post(envelope_url, json=_request_callback)
        with HttpRequestsManager():
            envelope = submit.create_submission_envelope(
                envelope_url, test_data.headers, HttpRequests()
            )
        assert envelope['user'] == 'fake1@clients'
        assert requests_mock.call_count == 1

    def test_create_submission_envelope_retries_on_error(
        self, requests_mock, test_data
    ):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.post(envelope_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.create_submission_envelope(
                envelope_url, test_data.headers, HttpRequests()
            )
        assert requests_mock.call_count == 3

    def test_create_submission_envelope_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        envelope_url = (
            'https://api.ingest.dev.data.humancellatlas.org/submissionEnvelopes'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.post(envelope_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.create_submission_envelope(
                envelope_url, test_data.headers, HttpRequests()
            )
        assert requests_mock.call_count == 3

    def test_get_analysis_protocol_returns_None(self, requests_mock, test_data):
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/protocols'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {}

        requests_mock.get(analysis_protocol_url, json=_request_callback)
        with HttpRequestsManager():
            analysis_protocol = submit.get_analysis_protocol(
                analysis_protocol_url,
                test_data.headers,
                test_data.analysis_protocol_id,
                HttpRequests(),
            )
        assert analysis_protocol is None

    def test_get_analysis_protocol_finds_existing_analysis_protocol(
        self, requests_mock, test_data
    ):
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/protocols'
        )

        def _request_callback(request, context):
            context.status_code = 200
            analysis_protocol = test_data.analysis_protocol
            return {'_embedded': {'protocols': [{'content': analysis_protocol}]}}

        requests_mock.get(analysis_protocol_url, json=_request_callback)
        with HttpRequestsManager():
            analysis_protocol = submit.get_analysis_protocol(
                analysis_protocol_url,
                test_data.headers,
                test_data.analysis_protocol_id,
                HttpRequests(),
            )
        assert (
            analysis_protocol['content']['protocol_core']['protocol_id']
            == test_data.analysis_protocol_id
        )

    def test_get_analysis_process_returns_None(self, requests_mock, test_data):
        analysis_process_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/processes'
        )

        def _request_callback(request, context):
            context.status_code = 200
            return {}

        requests_mock.get(analysis_process_url, json=_request_callback)
        with HttpRequestsManager():
            analysis_process = submit.get_analysis_process(
                analysis_process_url,
                test_data.headers,
                test_data.analysis_process_id,
                HttpRequests(),
            )
        assert analysis_process is None

    def test_get_analysis_process_finds_existing_analysis_process(
        self, requests_mock, test_data
    ):
        analysis_process_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/processes'
        )

        def _request_callback(request, context):
            context.status_code = 200
            analysis_process = test_data.analysis_process
            return {'_embedded': {'processes': [{'content': analysis_process}]}}

        requests_mock.get(analysis_process_url, json=_request_callback)
        with HttpRequestsManager():
            analysis_process = submit.get_analysis_process(
                analysis_process_url,
                test_data.headers,
                test_data.analysis_process_id,
                HttpRequests(),
            )
        assert (
            analysis_process['content']['process_core']['process_id']
            == test_data.analysis_process_id
        )

    def test_add_analysis_protocol(self, requests_mock, test_data):
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/protocols'
        )

        def _request_callback(request, context):
            context.status_code = 201
            return test_data.analysis_protocol

        requests_mock.post(analysis_protocol_url, json=_request_callback)
        with HttpRequestsManager():
            res = submit.add_analysis_protocol(
                analysis_protocol_url,
                test_data.headers,
                test_data.analysis_protocol,
                HttpRequests(),
            )
        assert requests_mock.call_count == 1
        assert res == test_data.analysis_protocol

    def test_add_analysis_protocol_retries_on_error(self, requests_mock, test_data):
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/protocols'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.post(analysis_protocol_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.add_analysis_protocol(
                analysis_protocol_url,
                test_data.headers,
                test_data.analysis_protocol,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_analysis_protocol_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/protocols'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.post(analysis_protocol_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.add_analysis_protocol(
                analysis_protocol_url,
                test_data.headers,
                test_data.analysis_protocol,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_analysis_process(self, requests_mock, test_data):
        analysis_process_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/processes'
        )

        def _request_callback(request, context):
            context.status_code = 201
            return test_data.analysis_process

        requests_mock.post(analysis_process_url, json=_request_callback)
        with HttpRequestsManager():
            res = submit.add_analysis_process(
                analysis_process_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 1
        assert res == test_data.analysis_process

    def test_add_analysis_process_retries_on_error(self, requests_mock, test_data):
        analysis_process_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/processes'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.post(analysis_process_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.add_analysis_process(
                analysis_process_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_analysis_process_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        analysis_process_url = (
            'https://api.ingest.dev.data.humancellatlas.org/abcde/processes'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.post(analysis_process_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.add_analysis_process(
                analysis_process_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_input_bundles(self, requests_mock, test_data):
        input_bundles_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/bundleReferences'

        def _request_callback(request, context):
            context.status_code = 201
            return {}

        requests_mock.put(input_bundles_url, json=_request_callback)
        with HttpRequestsManager():
            submit.add_input_bundles(
                input_bundles_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 1

    def test_add_input_bundles_retries_on_error(self, requests_mock, test_data):
        input_bundles_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/bundleReferences'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.put(input_bundles_url, json=_request_callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.add_input_bundles(
                input_bundles_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_input_bundles_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        input_bundles_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/bundleReferences'

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.put(input_bundles_url, json=_request_callback)
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.add_input_bundles(
                input_bundles_url,
                test_data.headers,
                test_data.analysis_process,
                HttpRequests(),
            )
        assert requests_mock.call_count == 3

    def test_add_file_reference(self, requests_mock, test_data):
        file_refs_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/fileReference'

        def _request_callback(request, context):
            context.status_code = 201
            return {}

        requests_mock.put(file_refs_url, json=_request_callback)
        file_ref = {
            'describedBy': 'https://schema.humancellatlas.org/type/file/schema_version/analysis_file',
            'schema_type': 'file',
            'file_core': {
                'file_name': 'test',
                'format': 'bam',
                'checksum': '0123456789abcdef0123456789abcdef',
            },
        }

        with HttpRequestsManager():
            submit.add_file_reference(
                file_ref, file_refs_url, test_data.headers, HttpRequests()
            )
        assert requests_mock.call_count == 1

    def test_add_file_reference_retries_on_error(self, requests_mock, test_data):
        file_refs_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/fileReference'

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.put(file_refs_url, json=_request_callback)
        file_ref = {
            'describedBy': 'https://schema.humancellatlas.org/type/file/schema_version/analysis_file',
            'schema_type': 'file',
            'file_core': {
                'file_name': 'test',
                'format': 'bam',
                'checksum': '0123456789abcdef0123456789abcdef',
            },
        }
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.add_file_reference(
                file_ref, file_refs_url, test_data.headers, HttpRequests()
            )
        assert requests_mock.call_count == 3

    def test_add_file_reference_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        file_refs_url = 'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/fileReference'

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.put(file_refs_url, json=_request_callback)
        file_ref = {
            'describedBy': 'https://schema.humancellatlas.org/type/file/schema_version/analysis_file',
            'schema_type': 'file',
            'file_core': {
                'file_name': 'test',
                'format': 'bam',
                'checksum': '0123456789abcdef0123456789abcdef',
            },
        }
        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.add_file_reference(
                file_ref, file_refs_url, test_data.headers, HttpRequests()
            )
        assert requests_mock.call_count == 3

    def test_link_analysis_protocol_to_analysis_process(self, requests_mock, test_data):
        links_url = (
            'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/protocols'
        )
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/protocols/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 201
            return {}

        requests_mock.put(links_url, json=_request_callback)

        with HttpRequestsManager():
            submit.link_analysis_protocol_to_analysis_process(
                test_data.headers, links_url, analysis_protocol_url, HttpRequests()
            )
        assert requests_mock.call_count == 1

    def test_link_analysis_protocol_to_analysis_process_retries_on_error(
        self, requests_mock, test_data
    ):
        links_url = (
            'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/protocols'
        )
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/protocols/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            return {'status': 'error', 'message': 'Internal Server Error'}

        requests_mock.put(links_url, json=_request_callback)

        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            submit.link_analysis_protocol_to_analysis_process(
                test_data.headers, links_url, analysis_protocol_url, HttpRequests()
            )
        assert requests_mock.call_count == 3

    def test_link_analysis_protocol_to_analysis_process_retries_on_read_timeout_error(
        self, requests_mock, test_data
    ):
        links_url = (
            'https://api.ingest.dev.data.humancellatlas.org/processes/abcde/protocols'
        )
        analysis_protocol_url = (
            'https://api.ingest.dev.data.humancellatlas.org/protocols/abcde'
        )

        def _request_callback(request, context):
            context.status_code = 500
            raise requests.ReadTimeout

        requests_mock.put(links_url, json=_request_callback)

        with pytest.raises(requests.ReadTimeout), HttpRequestsManager():
            submit.link_analysis_protocol_to_analysis_process(
                test_data.headers, links_url, analysis_protocol_url, HttpRequests()
            )
        assert requests_mock.call_count == 3
