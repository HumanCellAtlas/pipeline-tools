#!/usr/bin/env python

import requests
import argparse
from tenacity import retry, retry_if_result, retry_if_exception_type, wait_exponential, stop_after_delay, RetryError

RETRY_SECONDS = 10
TIMEOUT_SECONDS = 600


def urn_is_none(urn):
    return urn is None


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS),
       retry=(retry_if_result(urn_is_none) | retry_if_exception_type(requests.HTTPError)))
def run(envelope_url):
    """
    Check the contents of the submission envelope for the staging urn. Retry until the envelope contains a
    staging urn, or if there is an error with the request.

    :param str envelope_url: Submission envelope url
    :return: str urn: staging urn in the format dcp:upl:aws:integration:12345:abcde
    """
    envelope_js = get_envelope_json(envelope_url)
    urn = get_staging_urn(envelope_js)
    return urn


def get_envelope_json(envelope_url):
    response = requests.get(envelope_url)
    response.raise_for_status()
    envelope_js = response.json()
    return envelope_js


def get_staging_urn(envelope_js):
    """
    Get the staging urn from the submission envelope.
    :param dict envelope_js: submission envelope contents
    :return: str urn: staging urn in the format dcp:upl:aws:integration:12345:abcde
    """
    details = envelope_js.get('stagingDetails')
    if not details:
        return None
    location = details.get('stagingAreaLocation')
    if not location:
        return None
    urn = location.get('value')
    return urn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--envelope_url', required=True)
    parser.add_argument('--retry_seconds', type=int, default=RETRY_SECONDS, help='Maximum interval for exponential retries')
    parser.add_argument('--timeout_seconds', type=int, default=TIMEOUT_SECONDS, help='Maximum duration for retrying a request')
    args = parser.parse_args()
    try:
        urn = run.retry_with(wait=wait_exponential(multiplier=1, max=args.retry_seconds),
                             stop=stop_after_delay(args.timeout_seconds))(args.envelope_url)
    except RetryError:
        message = 'Timed out while trying to get urn. Timeout seconds: {}'.format(args.timeout_seconds)
        raise ValueError(message)
    print(urn)


if __name__ == '__main__':
    main()
