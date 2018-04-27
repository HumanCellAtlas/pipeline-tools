import unittest
import os
import json
import requests
import requests_mock
import unittest
from pipeline_tools.http_requests import HttpRequests
from pipeline_tools import http_requests
from .http_requests_manager import HttpRequestsManager


class TestHttpRequests(unittest.TestCase):

    def test_check_status_bad_codes(self):
        with self.assertRaises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 404
            HttpRequests.check_status(response)
        with self.assertRaises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 500
            HttpRequests.check_status(response)
        with self.assertRaises(requests.HTTPError):
            response = requests.Response()
            response.status_code = 301
            HttpRequests.check_status(response)

    def test_check_status_acceptable_codes(self):
        try:
            response = requests.Response()
            response.status_code = 200 
            HttpRequests.check_status(response)
        except requests.HTTPError as e:
            self.fail(str(e))

        try:
            response = requests.Response()
            response.status_code = 202 
            HttpRequests.check_status(response)
        except requests.HTTPError as e:
            self.fail(str(e))

    @requests_mock.mock()
    def test_get_retries_then_raises_exception_on_500(self, mock_request):
        def callback(request, response):
            response.status_code = 500
            return {}
        url = 'https://fake_url'
        mock_request.get(url, json=callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            HttpRequests().get(url)
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_get_immediately_raises_exception_on_404(self, mock_request):
        def callback(request, response):
            response.status_code = 404
            return {}
        url = 'https://fake_url'
        mock_request.get(url, json=callback)
        with self.assertRaises(requests.HTTPError), HttpRequestsManager():
            HttpRequests().get(url)
        self.assertEqual(mock_request.call_count, 1)

    @requests_mock.mock()
    def test_get_succeeds_on_200(self, mock_request):
        def callback(request, response):
            response.status_code = 200
            return {}
        url = 'https://fake_url'
        mock_request.get(url, json=callback)
        try:
            with HttpRequestsManager():
                HttpRequests().get(url)
        except requests.HTTPError as e:
            self.fail(str(e))

    #@mock.patch('requests.get', side_effect=requests.ConnectionError())
    @requests_mock.mock()
    def test_get_retries_on_connection_error(self, mock_request):
        def callback(request, response):
            raise requests.ConnectionError()
        url = 'https://fake_url'
        mock_request.get(url, json=callback)
        with self.assertRaises(requests.ConnectionError), HttpRequestsManager():
            HttpRequests().get(url)
        self.assertEqual(mock_request.call_count, 3)

    @requests_mock.mock()
    def test_get_records_details(self, mock_request):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}
        url = 'https://fake_url'
        adapter = requests_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        mock_request.get(url)
        with HttpRequestsManager() as temp_dir:
            HttpRequests().get(url)
            file_path = temp_dir + '/request_001.txt'
            self.assertTrue(os.path.isfile(file_path))
            self.assertTrue(os.stat(file_path).st_size > 0)
            with open(file_path) as f:
                first_line = f.readline()
                parts = first_line.strip().split(' ')
                self.assertEqual(len(parts), 2)
                self.assertEqual(parts[0], 'GET')
                self.assertEqual(parts[1], 'https://fake_url')

    @requests_mock.mock()
    def test_post_records_details(self, mock_request):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}
        url = 'https://fake_url'
        js = json.dumps({'foo': 'bar'})
        adapter = requests_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        mock_request.post(url, json=js)
        with HttpRequestsManager() as temp_dir:
            HttpRequests().post(url, json=js)
            file_path = temp_dir + '/request_001.txt'
            self.assertTrue(os.path.isfile(file_path))
            self.assertTrue(os.stat(file_path).st_size > 0)
            with open(file_path) as f:
                first_line = f.readline()
                parts = first_line.strip().split(' ')
                self.assertEqual(len(parts), 2)
                self.assertEqual(parts[0], 'POST')
                self.assertEqual(parts[1], 'https://fake_url')
                rest_of_file = f.read()
                response_js = json.loads(rest_of_file)
                self.assertIn('foo', response_js)
                self.assertEqual(response_js['foo'], 'bar')

    @requests_mock.mock()
    def test_post_records_response_details(self, mock_request):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return response
        url = 'https://fake_url'
        adapter = requests_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        mock_request.post(url, json={'foo': 'bar'})
        with HttpRequestsManager() as temp_dir:
            HttpRequests().post(url, json='{}')
            file_path = temp_dir + '/response_001.txt'
            self.assertTrue(os.path.isfile(file_path))
            self.assertTrue(os.stat(file_path).st_size > 0)
            with open(file_path) as f:
                first_line = f.readline().strip()
                self.assertEqual(first_line, '200')
                rest_of_file = f.read()
                js = json.loads(rest_of_file)
                self.assertIn('foo', js)
                self.assertEqual(js['foo'], 'bar')

    @requests_mock.mock()
    def test_does_not_record_when_var_not_set(self, mock_request):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}
        url = 'https://fake_url'
        adapter = requests_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        mock_request.get(url)
        with HttpRequestsManager(False) as temp_dir:
            HttpRequests().get(url)
            file_path = temp_dir + '/request_001.txt'
            self.assertFalse(os.path.isfile(file_path))

    @requests_mock.mock()
    def test_creates_dummy_files_even_when_record_not_on(self, mock_request):
        def callback(request):
            response = requests.Response()
            response.status_code = 200
            return {}
        url = 'https://fake_url'
        adapter = requests_mock.Adapter()
        session = requests.Session()
        session.mount('mock', adapter)
        adapter.add_matcher(callback)
        mock_request.get(url)
        with HttpRequestsManager(False) as temp_dir:
            HttpRequests().get(url)
            self.assertTrue(os.path.isfile(temp_dir + '/request_000.txt'))
            self.assertTrue(os.path.isfile(temp_dir + '/response_000.txt'))

    def test_get_next_file_suffix_empty(self):
        self.assertEqual(HttpRequests._get_next_file_suffix([]), '001')

    def test_get_next_file_suffix_one_file(self):
        self.assertEqual(HttpRequests._get_next_file_suffix(['/foo/bar/asdf_001.txt']), '002')

    def test_get_next_file_suffix_11(self):
        self.assertEqual(HttpRequests._get_next_file_suffix(['a_002.txt', 'a_010.txt']), '011')

    def test_get_next_file_suffix_unsorted(self):
        self.assertEqual(HttpRequests._get_next_file_suffix(['a_002.txt', 'a_004.txt', 'a_003.txt']), '005')

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
            self.assertFalse(hr.should_record)
            self.assertEqual(hr.record_dir, '.')
            self.assertEqual(hr.retry_timeout, 7200)
            self.assertEqual(hr.individual_request_timeout, 60)
            self.assertEqual(hr.retry_max_tries, 1E4)
            self.assertEqual(hr.retry_multiplier, 1)
            self.assertEqual(hr.retry_max_interval, 60)
            self.assertEqual(hr.individual_request_timeout, 60)

if __name__ == '__main__':
    unittest.main()
