#!/usr/bin/env python

import argparse
from tenacity import retry_if_result, RetryError
from datetime import datetime
from pipeline_tools.shared.http_requests import HttpRequests


def run(envelope_url, http_requests):
    """Check the contents of the submission envelope for the upload urn. Retry until the envelope contains a
    upload urn, or if there is an error with the request.

    Args:
        http_requests (HttpRequests): an HttpRequests object.
        envelope_url (str): the submission envelope url

    Returns:
        String giving the upload urn in the format dcp:upl:aws:integration:12345:abcde

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
        tenacity.RetryError: if urn is missing beyond timeout
    """

    def urn_is_none(response):
        envelope_js = response.json()
        urn = get_upload_urn(envelope_js)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('{0} Upload urn: {1}'.format(now, urn))
        return urn is None

    response = http_requests.get(envelope_url, retry=retry_if_result(urn_is_none))
    urn = get_upload_urn(response.json())
    return urn


def get_upload_urn(envelope_js):
    """Get the upload urn from the submission envelope.

    Args:
        envelope_js (dict): the submission envelope contents

    Returns:
        String giving the upload urn in the format s3://<bucket>/<uuid>,
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
    parser.add_argument('-envelope_url', required=True)
    parser.add_argument('-output', default='upload_urn.txt')
    args = parser.parse_args()
    try:
        urn = run(args.envelope_url, HttpRequests())
    except RetryError:
        message = 'Timed out while trying to get urn.'
        raise ValueError(message)
    with open('upload_urn.txt', 'w') as f:
        f.write(urn)


if __name__ == '__main__':
    main()
