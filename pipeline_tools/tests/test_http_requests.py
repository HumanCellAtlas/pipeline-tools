import json
import os
import pytest
import requests
import requests_mock as req_mock

from pipeline_tools.shared import http_requests
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.tests.http_requests_manager import HttpRequestsManager


class TestHttpRequests(object):
    def test_check_status_bad_codes(self):
        with pytest.raises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 404
            HttpRequests.check_status(response)
        with pytest.raises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 409
            HttpRequests.check_status(response)
        with pytest.raises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 500
            HttpRequests.check_status(response)
        with pytest.raises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 301
            HttpRequests.check_status(response)

    def test_check_status_acceptable_codes(self):
        try:
            response = requests.Response()
            response.status_code = 200
            HttpRequests.check_status(response)
        except requests.HTTPError as e:
            pytest.fail(str(e))

        try:
            response = requests.Response()
            response.status_code = 202
            HttpRequests.check_status(response)
        except requests.HTTPError as e:
            pytest.fail(str(e))

    def test_get_retries_then_raises_exception_on_500(self, requests_mock):
        def callback(request, response):
            response.status_code = 500
            return {}

        url = 'https://fake_url'
        requests_mock.get(url, json=callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            HttpRequests().get(url)
        assert requests_mock.call_count == 3

    def test_get_immediately_raises_exception_on_404(self, requests_mock):
        def callback(request, response):
            response.status_code = 404
            return {}

        url = 'https://fake_url'
        requests_mock.get(url, json=callback)
        with pytest.raises(requests.HTTPError), HttpRequestsManager():
            HttpRequests().get(url)
        assert requests_mock.call_count == 1

    def test_get_succeeds_on_200(self, requests_mock):
        def callback(request, response):
            response.status_code = 200
            return {}

        url = 'https://fake_url'
        requests_mock.get(url, json=callback)
        try:
            with HttpRequestsManager():
                HttpRequests().get(url)
        except requests.HTTPError as e:
            pytest.fail(str(e))

    # @mock.patch('requests.get', side_effect=requests.ConnectionError())
    def test_get_retries_on_connection_error(self, requests_mock):
        def callback(request, response):
            raise requests.ConnectionError()

        url = 'https://fake_url'
        requests_mock.get(url, json=callback)
        with pytest.raises(requests.ConnectionError), HttpRequestsManager():
            HttpRequests().get(url)
        assert requests_mock.call_count == 3

    def test_get_records_details(self, requests_mock):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}

        url = 'https://fake_url'
        adapter = req_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        requests_mock.get(url)
        with HttpRequestsManager() as temp_dir:
            HttpRequests().get(url)
            file_path = temp_dir + '/request_001.txt'
            assert os.path.isfile(file_path) is True
            assert os.stat(file_path).st_size > 0
            with open(file_path) as f:
                first_line = f.readline()
                parts = first_line.strip().split(' ')
                assert len(parts) == 2
                assert parts[0] == 'GET'
                assert parts[1] == 'https://fake_url'

    def test_post_records_details(self, requests_mock):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}

        url = 'https://fake_url'
        js = json.dumps({'foo': 'bar'})
        adapter = req_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        requests_mock.post(url, json=js)
        with HttpRequestsManager() as temp_dir:
            HttpRequests().post(url, json=js)
            file_path = temp_dir + '/request_001.txt'
            assert os.path.isfile(file_path) is True
            assert os.stat(file_path).st_size > 0
            with open(file_path) as f:
                first_line = f.readline()
                parts = first_line.strip().split(' ')
                assert len(parts) == 2
                assert parts[0] == 'POST'
                assert parts[1] == 'https://fake_url'
                rest_of_file = f.read()
                response_js = json.loads(rest_of_file)
                assert 'foo' in response_js
                assert response_js['foo'] == 'bar'

    def test_post_records_response_details(self, requests_mock):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return response

        url = 'https://fake_url'
        adapter = req_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        requests_mock.post(url, json={'foo': 'bar'})
        with HttpRequestsManager() as temp_dir:
            HttpRequests().post(url, json='{}')
            file_path = temp_dir + '/response_001.txt'
            assert os.path.isfile(file_path) is True
            assert os.stat(file_path).st_size > 0
            with open(file_path) as f:
                first_line = f.readline().strip()
                assert first_line == '200'
                rest_of_file = f.read()
                js = json.loads(rest_of_file)
                assert 'foo' in js
                assert js['foo'] == 'bar'

    def test_does_not_record_when_var_not_set(self, requests_mock):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}

        url = 'https://fake_url'
        adapter = req_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        requests_mock.get(url)
        with HttpRequestsManager(False) as temp_dir:
            HttpRequests().get(url)
            file_path = temp_dir + '/request_001.txt'
            assert os.path.isfile(file_path) is False

    def test_creates_dummy_files_even_when_record_not_on(self, requests_mock):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}

        url = 'https://fake_url'
        adapter = req_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        requests_mock.get(url)
        with HttpRequestsManager(False) as temp_dir:
            HttpRequests().get(url)
            assert os.path.isfile(temp_dir + '/request_000.txt') is True
            assert os.path.isfile(temp_dir + '/response_000.txt') is True

    def test_get_next_file_suffix_empty(self):
        assert HttpRequests._get_next_file_suffix([]) == '001'

    def test_get_next_file_suffix_one_file(self):
        assert HttpRequests._get_next_file_suffix(['/foo/bar/asdf_001.txt']) == '002'

    def test_get_next_file_suffix_11(self):
        assert HttpRequests._get_next_file_suffix(['a_002.txt', 'a_010.txt']) == '011'

    def test_get_next_file_suffix_unsorted(self):
        assert (
            HttpRequests._get_next_file_suffix(['a_002.txt', 'a_004.txt', 'a_003.txt'])
            == '005'
        )

    def test_attributes_initialized_for_empty_strings(self):
        with HttpRequestsManager():
            os.environ[http_requests.RECORD_HTTP_REQUESTS] = ''
            os.environ[http_requests.HTTP_RECORD_DIR] = ''
            os.environ[http_requests.RETRY_MAX_TRIES] = ''
            os.environ[http_requests.RETRY_MAX_INTERVAL] = ''
            os.environ[http_requests.RETRY_MULTIPLIER] = ''
            os.environ[http_requests.RETRY_TIMEOUT] = ''
            os.environ[http_requests.INDIVIDUAL_REQUEST_TIMEOUT] = ''
            hr = HttpRequests(write_dummy_files=False)
            assert hr.should_record is False
            assert hr.record_dir == '.'
            assert hr.retry_timeout == 7200
            assert hr.individual_request_timeout == 60
            assert hr.retry_max_tries == 1e4
            assert hr.retry_multiplier == 1
            assert hr.retry_max_interval == 60
            assert hr.individual_request_timeout == 60
