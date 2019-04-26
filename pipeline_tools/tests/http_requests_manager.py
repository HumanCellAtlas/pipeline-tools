import os

from tempfile import TemporaryDirectory
from pipeline_tools.shared import http_requests


class HttpRequestsManager(TemporaryDirectory):
    """Context manager that creates and deletes a temporary directory
    and sets and deletes environment variables needed by HttpRequests.

    Using this context manager reduces the boilerplate code needed to
    point HttpRequests at a temporary directory and clean it up when finished.

    Attributes:
            should_record (bool): whether to record requests and responses
            max_tries (int): maximum number of tries
            retry_multiplier (float): multiplier A for exponential retries A * 2^i
            timeout (float): stop retrying after this number of seconds
            max_interval (float): ceiling for interval between retries
            individual_request_timeout (float): time out any request that takes longer than this number of seconds
    """

    def __init__(
        self,
        should_record=True,
        max_tries=3,
        retry_multiplier=0.01,
        timeout=1,
        max_interval=10,
        individual_request_timeout=1,
    ):
        """Sets self.should_record attribute.

        Args:
            should_record (bool): whether to record requests and responses
            max_tries (int): maximum number of tries
            retry_multiplier (float): multiplier A for exponential retries A * 2^i
            timeout (float): stop retrying after this number of seconds
            max_interval (float): ceiling for interval between retries
            individual_request_timeout (float): time out any request that takes longer than this number of seconds
        """
        self.should_record = should_record
        self.max_tries = max_tries
        self.retry_multiplier = retry_multiplier
        self.timeout = timeout
        self.max_interval = max_interval
        self.individual_request_timeout = individual_request_timeout
        super(HttpRequestsManager, self).__init__()

    def __enter__(self):
        """Creates temp dir and sets environment variables used by HttpRequests"""
        super(HttpRequestsManager, self).__enter__()
        # We have to set HTTP_RECORD_DIR no matter what because we always
        # write a dummy request_000.txt and response_000.txt file to
        # avoid errors when using glob to gather files in WDL.
        os.environ[http_requests.HTTP_RECORD_DIR] = self.name
        os.environ[http_requests.RETRY_MAX_TRIES] = str(self.max_tries)
        os.environ[http_requests.RETRY_TIMEOUT] = str(self.timeout)
        os.environ[http_requests.RETRY_MULTIPLIER] = str(self.retry_multiplier)
        os.environ[http_requests.RETRY_MAX_INTERVAL] = str(self.max_interval)
        os.environ[http_requests.INDIVIDUAL_REQUEST_TIMEOUT] = str(
            self.individual_request_timeout
        )
        if self.should_record:
            os.environ[http_requests.RECORD_HTTP_REQUESTS] = 'true'
        return self.name

    def __exit__(self, *args):
        """Deletes temp dir and environment variables used by HttpRequests"""
        os.environ.pop(http_requests.RETRY_MAX_TRIES, None)
        os.environ.pop(http_requests.RETRY_TIMEOUT, None)
        os.environ.pop(http_requests.RETRY_MULTIPLIER, None)
        os.environ.pop(http_requests.RETRY_MAX_INTERVAL, None)
        os.environ.pop(http_requests.HTTP_RECORD_DIR, None)
        os.environ.pop(http_requests.RECORD_HTTP_REQUESTS, None)
        os.environ.pop(http_requests.INDIVIDUAL_REQUEST_TIMEOUT, None)
        super(HttpRequestsManager, self).__exit__(*args)
