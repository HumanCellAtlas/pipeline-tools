import glob
import os
import re
import requests
from datetime import datetime
from requests.packages.urllib3.util import retry as retry_utils
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)


HTTP_RECORD_DIR = 'HTTP_RECORD_DIR'
RECORD_HTTP_REQUESTS = 'RECORD_HTTP_REQUESTS'

RETRY_TIMEOUT = 'RETRY_TIMEOUT'
RETRY_MAX_TRIES = 'RETRY_MAX_TRIES'
RETRY_MULTIPLIER = 'RETRY_MULTIPLIER'
RETRY_MAX_INTERVAL = 'RETRY_MAX_INTERVAL'
INDIVIDUAL_REQUEST_TIMEOUT = 'INDIVIDUAL_REQUEST_TIMEOUT'


class RetryPolicy(retry_utils.Retry):
    """Wrapper around the urllib3.retry module that overrides which status codes should follow the retry_after header.

    Attributes:
        retry_after_status_codes (frozenset): Which status codes follow the retry_after header

    """

    def __init__(self, retry_after_status_codes={301}, **kwargs):
        super(RetryPolicy, self).__init__(**kwargs)
        self.RETRY_AFTER_STATUS_CODES = frozenset(retry_after_status_codes)


class HttpRequests(object):
    """Wraps requests library and adds ability to record requests and responses.

    When in record mode, will record requests and responses by writing them to
    files.

    Requests made through this class will raise an error if response status code
    indicates an error.

    Attributes:
        should_record (bool): whether to record requests and responses.
        record_dir (str): the directory where requests and responses
            should be written.
        retry_timeout (int): time in seconds after which we will stop retrying
        retry_max_tries (int): maximum number of retries before giving up
        retry_multiplier (float): multiplier A for exponential retries, e.g. interval will be: A x 2^i
        retry_max_interval (float): ceiling for retry interval
        individual_request_timeout (int): time in seconds after which each request will timeout
    """

    def __init__(self, write_dummy_files=True):
        """Initializes should_record and record_dir from environment variables.
        Also writes dummy request_000.txt and response_000.txt files needed to prevent
        an error when using this class in WDL and glob is used to find the outputs.
        """

        self._populate_params()

        if not write_dummy_files:
            return

        msg = """This dummy file is needed to prevent an error when running in WDL
        that collects http request and response files using the WDL glob function,
        which will throw an error if the glob matches zero files.
        """
        with open(self.record_dir + '/request_000.txt', 'w') as f:
            f.write(msg)
        with open(self.record_dir + '/response_000.txt', 'w') as f:
            f.write(msg)

    def _populate_params(self):
        def is_true(x):
            return x.lower() == 'true'

        self.should_record = self._get_param(RECORD_HTTP_REQUESTS, 'false', is_true)
        self.record_dir = self._get_param(HTTP_RECORD_DIR, '.')
        self.retry_timeout = self._get_param(RETRY_TIMEOUT, '7200', float)
        self.retry_max_tries = self._get_param(RETRY_MAX_TRIES, '10000', int)
        self.retry_multiplier = self._get_param(RETRY_MULTIPLIER, '1', float)
        self.retry_max_interval = self._get_param(RETRY_MAX_INTERVAL, '60', float)
        self.individual_request_timeout = self._get_param(
            INDIVIDUAL_REQUEST_TIMEOUT, '60', float
        )

    @staticmethod
    def _get_param(name, default, convert_fn=lambda x: x):
        param_str = os.environ.get(name, default)

        # handle case where environment var is set to empty string
        if not param_str:
            param_str = default

        return convert_fn(param_str)

    def get(self, *args, **kwargs):
        """Calls requests.get function.

        In addition to calling requests.get, this function will record the request
        and response if the HttpRequests object's should_record attribute is True.

        Args:
            All arguments that requests.get accepts are accepted here.

        Returns:
            requests.Response: An object representing the response to the request.

        Raises:
            requests.HTTPError: if 5xx error occurs
            tenacity.RetryError: if retry limit is reached and condition specified by
            retry kwarg is still not met
        """
        kwargs['method'] = 'get'
        return self._http_request_with_retry(*args, **kwargs)

    def put(self, *args, **kwargs):
        """Calls requests.put function.

        In addition to calling requests.put, this function will record the request
        and response if the HttpRequests object's should_record attribute is True.

        Args:
            All arguments that requests.put accepts are accepted here except
            for 'body' -- use 'json' instead.

        Returns:
            requests.Response: An object representing the response to the request.

        Raises:
            requests.HTTPError: if 5xx error occurs
            tenacity.RetryError: if retry limit is reached and condition specified by
            retry kwarg is still not met
        """
        kwargs['method'] = 'put'
        return self._http_request_with_retry(*args, **kwargs)

    def post(self, *args, **kwargs):
        """Calls requests.post function.

        In addition to calling requests.post, this function will record the request
        and response if the HttpRequests object's should_record attribute is True.

        Args:
            All arguments that requests.post accepts are accepted here except
            for 'body' -- use 'json' instead.

        Returns:
            requests.Response: An object representing the response to the request.

        Raises:
            requests.HTTPError: if 5xx error occurs
            tenacity.RetryError: if retry limit is reached and condition specified by
            retry kwarg is still not met
        """
        kwargs['method'] = 'post'
        return self._http_request_with_retry(*args, **kwargs)

    def _http_request_with_retry(self, *args, **kwargs):
        def is_retryable(error):
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('{0} {1}'.format(now, repr(error)))

            def is_retryable_status_code(error):
                if not isinstance(error, requests.HTTPError):
                    return False
                if error.response.status_code == 409:
                    return True
                else:
                    return not (400 <= error.response.status_code <= 499)

            return is_retryable_status_code(error) or isinstance(
                error, (requests.ConnectionError, requests.ReadTimeout)
            )

        if 'retry' in kwargs:
            retry = kwargs['retry'] | retry_if_exception(is_retryable)
            del kwargs['retry']
        else:
            retry = retry_if_exception(is_retryable)
        if 'before' in kwargs:
            before = kwargs['before']
            del kwargs['before']
        else:
            before = None

        kwargs['timeout'] = self.individual_request_timeout
        return self._http_request.retry_with(
            retry=retry,
            before=before,
            wait=wait_exponential(
                multiplier=self.retry_multiplier, max=self.retry_max_interval
            ),
            stop=(
                stop_after_delay(self.retry_timeout)
                | stop_after_attempt(self.retry_max_tries)
            ),
        )(self, *args, **kwargs)

    @retry(reraise=True)
    def _http_request(self, *args, **kwargs):
        """Calls requests function specified by the given http method.

        Records the request and response if the HttpRequests object has
        should_record set to True.

        Args:
            method: string specifying the http method, like 'get' or 'PUT'
                (case insensitive)
            All arguments that the specified requests function accepts are accepted
            here except for 'body' -- use 'json' instead.

        Returns:
            requests.Response: An object representing the response to the request.

        Raises:
            requests.HTTPError: if 5xx error occurs
            tenacity.RetryError: if retry limit is reached and condition specified by
            retry kwarg is still not met
            NotImplementedError: if method is something other than get, put, or post
        """
        # Figure out which requests function to call
        http_method = kwargs.get('method').lower()
        fn = HttpRequests._get_method(http_method)

        # Record request details
        if self.should_record:
            if not os.path.isdir(self.record_dir):
                os.makedirs(self.record_dir)
            request_files = glob.glob(self.record_dir + '/request_[0-9][0-9][0-9].txt')
            request_file_suffix = HttpRequests._get_next_file_suffix(request_files)
            # Get url and request body from args[0] and args[1] respectively if available,
            # otherwise get them from kwargs.
            if len(args) == 0:
                url = kwargs['url']
                request_body = kwargs.get('json', '{}')
            else:
                url = args[0]
                if len(args) == 1:
                    request_body = kwargs.get('json', '{}')
                else:
                    request_body = args[1]
            with open(
                self.record_dir + '/request_' + request_file_suffix + '.txt', 'w'
            ) as f:
                f.write(http_method.upper() + ' ' + url + '\n')
                f.write('{0}\n'.format(request_body))

        # Keys not expected by requests library will cause errors so must be deleted
        del kwargs['method']

        # Call through to requests library
        response = fn(*args, **kwargs)

        # Record response details
        if self.should_record:
            response_files = glob.glob(
                self.record_dir + '/response_[0-9][0-9][0-9].txt'
            )
            response_file_suffix = HttpRequests._get_next_file_suffix(response_files)
            with open(
                self.record_dir + '/response_' + response_file_suffix + '.txt', 'w'
            ) as f:
                f.write('{}\n'.format(response.status_code))
                f.write('{}\n'.format(response.text))

        HttpRequests.check_status(response)
        return response

    @staticmethod
    def _get_method(http_method):
        session = requests.Session()
        # Follow retry-after headers for 301 status codes to avoid exceeding max_redirects (defaults to 30)
        # when requesting files from the Data Store.
        retry_policy = RetryPolicy()
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_policy)
        session.mount('https://', adapter)

        if http_method == 'get':
            fn = session.get
        elif http_method == 'put':
            fn = session.put
        elif http_method == 'post':
            fn = session.post
        else:
            raise NotImplementedError('Only get, put, and post are implemented.')
        return fn

    @staticmethod
    def _get_next_file_suffix(files):
        """Returns the next suffix (one higher than the highest one given), like '006',
        given a list of strings representing files like foo_n.txt, where n is a
        zero-padded integer like 005 and foo is an arbitrary prefix.

        Args:
            files (list): list of strings representing files in a directory that look like foo_n.txt

        Returns:
            str: A string representing the next suffix, one higher than the highest one present
            in the given list.
        """
        if len(files) == 0:
            return '001'
        sorted_files = sorted(files, reverse=True)
        match = re.search('_([\d]+)\.txt', sorted_files[0])
        index = int(match.group(1)) + 1
        # Zero pad number so it has 3 digits
        return format(index, '03')

    @staticmethod
    def check_status(response):
        """Check that the response status code is in range 200-299, or 409.
        Raises a ValueError and prints response_text if status is not in the expected range. Otherwise,
        just returns silently.
        Args:
            response.status (int): The actual HTTP status code.
            response.text (str): Text to print along with status code when mismatch occurs

        Raises:
            requests.HTTPError: for 5xx errors and prints response_text if status is not in the expected range.
            Otherwise,
            just returns silently.

        Examples:
            check_status(200, 'foo') passes
            check_status(404, 'foo') raises error
            check_status(301, 'bar') raises error
        """
        status = response.status_code
        matches = 200 <= status <= 299
        if not matches:
            message = 'HTTP status code {0} is not in expected range 2xx. Response: {1}'.format(
                status, response.text
            )
            raise requests.HTTPError(message, response=response)
