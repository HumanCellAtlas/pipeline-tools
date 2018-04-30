#!/usr/bin/env python

import requests
import argparse
from .dcp_utils import check_status
from tenacity import retry, retry_if_result, retry_if_exception_type, wait_exponential, stop_after_delay, RetryError

RETRY_SECONDS = 10
TIMEOUT_SECONDS = 600


def status_is_invalid(status):
    return status != 'Valid'


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS),
       retry=(retry_if_result(status_is_invalid) | retry_if_exception_type(requests.HTTPError)))
def wait_for_valid_status(envelope_url):
    """
    Check the status of the submission. Retry until the status is "Valid", or if there is an error with the request to
    get the submission envelope.

    :param str envelope_url: Submission envelope url
    :return: str status: Status of the submission ("Valid", "Validating", etc.)
    """
    print('Getting status for {}'.format(envelope_url))
    envelope_js = get_envelope_json(envelope_url)
    status = envelope_js.get('submissionState')
    print('submissionState: {}'.format(status))
    return status


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def confirm(envelope_url):
    print('Confirming submission')
    headers = {
        'Content-type': 'application/json'
    }
    response = requests.put('{}/submissionEvent'.format(envelope_url), headers=headers)
    check_status(response.status_code, response.text)
    print(response.text)


def get_envelope_json(envelope_url):
    response = requests.get(envelope_url)
    envelope_js = response.json()
    check_status(response.status_code, response.text)
    return envelope_js


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--envelope_url', required=True)
    parser.add_argument('--retry_seconds', type=int, default=RETRY_SECONDS, help='Maximum interval for exponential retries')
    parser.add_argument('--timeout_seconds', type=int, default=TIMEOUT_SECONDS, help='Maximum duration for retrying a request')
    args = parser.parse_args()
    try:
        wait_for_valid_status.retry_with(wait=wait_exponential(multiplier=1, max=args.retry_seconds),
                                         stop=stop_after_delay(args.timeout_seconds))(args.envelope_url)
    except RetryError:
        message = 'Timed out while waiting for Valid status. Timeout seconds: {}'.format(args.timeout_seconds)
        raise ValueError(message)

    confirm.retry_with(wait=wait_exponential(multiplier=1, max=args.retry_seconds),
                       stop=stop_after_delay(args.timeout_seconds))(args.envelope_url)


if __name__ == '__main__':
    main()
