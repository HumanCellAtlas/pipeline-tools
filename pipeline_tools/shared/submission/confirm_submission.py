#!/usr/bin/env python

import argparse
from tenacity import retry_if_result, RetryError
from datetime import datetime
from pipeline_tools.shared.http_requests import HttpRequests


def wait_for_valid_status(envelope_url, http_requests):
    """
    Check the status of the submission. Retry until the status is "Valid", or if there is an error with the request to
    get the submission envelope.

    Args:
        envelope_url (str): Submission envelope url
        http_requests (HttpRequests): HttpRequests object

    Returns:
        str: Status of the submission ("Valid", "Validating", etc.)

    Raises:
        requests.HTTPError: if 4xx error or 5xx error past timeout
        tenacity.RetryError: if status is invalid past timeout
    """

    def log_before(envelope_url):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('{0} Getting status for {1}'.format(now, envelope_url))

    def status_is_invalid(response):
        envelope_js = response.json()
        status = envelope_js.get('submissionState')
        print('submissionState: {}'.format(status))
        return status != 'Valid'

    response = http_requests.get(
        envelope_url,
        before=log_before(envelope_url),
        retry=retry_if_result(status_is_invalid),
    )
    return True


def confirm(envelope_url, http_requests):
    """Confirms the submission.

    Args:
        envelope_url (str): the url for the envelope
        http_requests (HttpRequests): HttpRequests object

    Returns:
        str: The text of the response

    Raises:
        requests.HTTPError: if the response status indicates an error
    """
    print('Confirming submission')
    headers = {'Content-type': 'application/json'}
    response = http_requests.put(
        '{}/submissionEvent'.format(envelope_url), headers=headers
    )
    text = response.text
    print(text)
    return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--envelope_url', required=True)
    args = parser.parse_args()
    http_requests = HttpRequests()
    try:
        wait_for_valid_status(args.envelope_url, http_requests)
    except RetryError:
        message = 'Timed out while waiting for Valid status.'
        raise ValueError(message)

    confirm(args.envelope_url, http_requests)


if __name__ == '__main__':
    main()
