#!/usr/bin/env python

import argparse
from tenacity import retry_if_result, RetryError
from pipeline_tools.http_requests import HttpRequests


def run(envelope_url, http_requests):
    """Check the contents of the submission envelope for the staging urn. Retry until the envelope contains a
    staging urn, or if there is an error with the request.

    Args:
        http_requests (HttpRequests): an HttpRequests object.
        envelope_url (str): the submission envelope url

    Returns:
        String giving the staging urn in the format dcp:upl:aws:integration:12345:abcde

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
        tenacity.RetryError: if urn is missing beyond timeout
    """
    def urn_is_none(response):
        envelope_js = response.json()
        urn = get_staging_urn(envelope_js)
        return urn is None

    response = (
        http_requests
        .get(
            envelope_url,
            retry=retry_if_result(urn_is_none)
        )
    )
    urn = get_staging_urn(response.json())
    return urn


def get_staging_urn(envelope_js):
    """Get the staging urn from the submission envelope.

    Args:
        envelope_js (dict): the submission envelope contents

    Returns:
        String giving the staging urn in the format dcp:upl:aws:integration:12345:abcde,
        or None if the envelope doesn't contain a urn
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
    args = parser.parse_args()
    try:
        urn = run(args.envelope_url, HttpRequests())
    except RetryError:
        message = 'Timed out while trying to get urn.'
        raise ValueError(message)
    print(urn)


if __name__ == '__main__':
    main()
