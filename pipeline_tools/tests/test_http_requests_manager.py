import unittest
import os
from .http_requests_manager import HttpRequestsManager
from pipeline_tools import http_requests


class TestHttpRequestsManager(unittest.TestCase):

    def test_enter_creates_directory(self):
        with HttpRequestsManager() as temp_dir:
            self.assertTrue(os.path.isdir(temp_dir))

    def test_exit_deletes_directory(self):
        with HttpRequestsManager() as temp_dir:
            temp_dir_name = temp_dir
            self.assertTrue(os.path.isdir(temp_dir))
        self.assertFalse(os.path.isdir(temp_dir))

    def test_enter_sets_environment_vars(self):
        with HttpRequestsManager() as temp_dir:
            self.assertIn(http_requests.HTTP_RECORD_DIR, os.environ)
            self.assertEqual(os.environ[http_requests.HTTP_RECORD_DIR], temp_dir)
            self.assertIn(http_requests.RECORD_HTTP_REQUESTS, os.environ)
            self.assertEqual(os.environ[http_requests.RECORD_HTTP_REQUESTS], 'true')
            self.assertIn(http_requests.RETRY_MAX_TRIES, os.environ)
            self.assertEqual(os.environ[http_requests.RETRY_MAX_TRIES], '3')
            self.assertIn(http_requests.RETRY_MAX_INTERVAL, os.environ)
            self.assertEqual(os.environ[http_requests.RETRY_MAX_INTERVAL], '10')
            self.assertIn(http_requests.RETRY_TIMEOUT, os.environ)
            self.assertEqual(os.environ[http_requests.RETRY_TIMEOUT], '1')
            self.assertIn(http_requests.RETRY_MULTIPLIER, os.environ)
            self.assertEqual(os.environ[http_requests.RETRY_MULTIPLIER], '0.01')
            self.assertIn(http_requests.INDIVIDUAL_REQUEST_TIMEOUT, os.environ)
            self.assertEqual(os.environ[http_requests.INDIVIDUAL_REQUEST_TIMEOUT], '1')

    def test_exit_deletes_environment_var(self):
        with HttpRequestsManager() as temp_dir:
            pass
        self.assertNotIn(http_requests.HTTP_RECORD_DIR, os.environ)
        self.assertNotIn(http_requests.RECORD_HTTP_REQUESTS, os.environ)
        self.assertNotIn(http_requests.RETRY_MAX_TRIES, os.environ)
        self.assertNotIn(http_requests.RETRY_MAX_INTERVAL, os.environ)
        self.assertNotIn(http_requests.RETRY_TIMEOUT, os.environ)
        self.assertNotIn(http_requests.RETRY_MULTIPLIER, os.environ)
        self.assertNotIn(http_requests.INDIVIDUAL_REQUEST_TIMEOUT, os.environ)
