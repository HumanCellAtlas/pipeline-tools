#!/usr/bin/env python

import argparse
from tenacity import retry_if_result, RetryError
from datetime import datetime
from pipeline_tools.shared.http_requests import HttpRequests
from pipeline_tools.shared import auth_utils, exceptions


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

    def keep_polling(response):
        # Keep polling until the status is "Valid/Complete" or "Invalid"
        envelope_js = response.json()
        status = envelope_js.get('submissionState')
        print('submissionState: {}'.format(status))
        return status not in ('Valid', 'Complete', 'Invalid')

    response = http_requests.get(
        envelope_url,
        before=log_before(envelope_url),
        retry=retry_if_result(keep_polling),
    )
    return response.json()


def confirm(envelope_url, http_requests, runtime_environment, service_account_key_path):
    """Confirms the submission.

    Args:
        envelope_url (str): the url for the envelope.
        http_requests (HttpRequests): HttpRequests object.
        runtime_environment (str): Environment where the pipeline is running ('dev', 'test', 'staging' or 'prod').
        service_account_key_path (str): Path to the JSON service account key for generating a JWT.

    Returns:
        str: The text of the response

    Raises:
        requests.HTTPError: if the response status indicates an error
    """
    print('Making auth headers')
    dcp_auth_client = auth_utils.DCPAuthClient(
        service_account_key_path, runtime_environment
    )
    auth_headers = dcp_auth_client.get_auth_header()

    print('Confirming submission')
    headers = {'Content-type': 'application/json'}
    headers.update(auth_headers)
    response = http_requests.put(
        '{}/submissionEvent'.format(envelope_url), headers=headers
    )
    text = response.text
    print(text)
    return text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--envelope_url',
        required=True,
        help='The url of the submission envelope in Ingest service.',
    )
    parser.add_argument(
        '--runtime_environment',
        required=True,
        help='Environment where the pipeline is running ("dev", "test", "staging" or "prod").',
    )
    parser.add_argument(
        '--service_account_key_path',
        required=True,
        help='Path to the JSON service account key for generating a JWT.',
    )
    args = parser.parse_args()
    http_requests = HttpRequests()
    try:
        response = wait_for_valid_status(args.envelope_url, http_requests)
        status = response.get('submissionState')
    except RetryError:
        message = 'Timed out while waiting for Valid status.'
        raise ValueError(message)

    if status == 'Invalid':
        raise exceptions.SubmissionError('Invalid submission envelope.')
    elif status == 'Valid':
        confirm(
            args.envelope_url,
            http_requests,
            args.runtime_environment,
            args.service_account_key_path,
        )


if __name__ == '__main__':
    main()
