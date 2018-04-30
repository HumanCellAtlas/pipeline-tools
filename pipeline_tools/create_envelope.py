#!/usr/bin/env python

import requests
import json
import argparse
from .dcp_utils import get_auth_token, make_auth_header, check_status
from tenacity import retry, wait_exponential, stop_after_delay

RETRY_SECONDS = 10
TIMEOUT_SECONDS = 600


def run(submit_url, analysis_json_path, schema_version, retry_seconds, timeout_seconds):
    # 0. Get Auth token and make auth headers
    print('Fetching auth token from Auth0')
    auth_token = get_auth_token()
    print('Making auth headers')
    auth_headers = make_auth_header(auth_token)

    # 1. Get envelope url
    envelope_url = get_envelope_url.retry_with(wait=wait_exponential(retry_seconds),
                                               stop=stop_after_delay(timeout_seconds))(submit_url, auth_headers)

    # 2. Create envelope
    envelope_js = create_submission_envelope.retry_with(wait=wait_exponential(retry_seconds),
                                                        stop=stop_after_delay(timeout_seconds))(envelope_url, auth_headers)

    submission_url = get_entity_url(envelope_js, 'submissionEnvelope')
    with open('submission_url.txt', 'w') as f:
        f.write(submission_url)

    # 3. Create analysis
    with open(analysis_json_path) as f:
        analysis_json_contents = json.load(f)
    analyses_url = get_entity_url(envelope_js, 'processes')
    analysis_js = create_analysis.retry_with(wait=wait_exponential(retry_seconds),
                                             stop=stop_after_delay(timeout_seconds))(analyses_url, auth_headers, analysis_json_contents)

    # 4. Add input bundles
    input_bundles_url = get_entity_url(analysis_js, 'add-input-bundles')
    add_input_bundles.retry_with(wait=wait_exponential(retry_seconds),
                                 stop=stop_after_delay(timeout_seconds))(input_bundles_url, auth_headers, analysis_json_contents)

    # 5. Add file references
    file_refs_url = get_entity_url(analysis_js, 'add-file-reference')
    print('Adding file references at {0}'.format(file_refs_url))
    output_files = get_output_files(analysis_json_contents, schema_version)
    for file_ref in output_files:
        add_file_reference.retry_with(wait=wait_exponential(retry_seconds),
                                      stop=stop_after_delay(timeout_seconds))(file_ref, file_refs_url, auth_headers)


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def get_envelope_url(submit_url, auth_headers):
    print('Getting envelope url from {}'.format(submit_url))
    response = requests.get(submit_url, headers=auth_headers)
    check_status(response.status_code, response.text)
    envelope_url = get_entity_url(response.json(), 'submissionEnvelopes')
    return envelope_url


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def create_submission_envelope(envelope_url, auth_headers):
    print('Creating submission envelope at {0}'.format(envelope_url))
    response = requests.post(envelope_url, '{}', headers=auth_headers)
    check_status(response.status_code, response.text)
    envelope_js = response.json()
    return envelope_js


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def create_analysis(analyses_url, auth_headers, analysis_json_contents):
    print('Creating analysis at {0}'.format(analyses_url))
    response = requests.post(analyses_url, headers=auth_headers, data=json.dumps(analysis_json_contents))
    check_status(response.status_code, response.text)
    analysis_js = response.json()
    return analysis_js


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def add_input_bundles(input_bundles_url, auth_headers, analysis_json_contents):
    print('Adding input bundles at {0}'.format(input_bundles_url))
    input_bundle_uuid = get_input_bundle_uuid(analysis_json_contents)
    bundle_refs_js = json.dumps({"bundleUuids": [input_bundle_uuid]}, indent=2)
    print(bundle_refs_js)
    response = requests.put(input_bundles_url, headers=auth_headers, data=bundle_refs_js)
    check_status(response.status_code, response.text)


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=RETRY_SECONDS), stop=stop_after_delay(TIMEOUT_SECONDS))
def add_file_reference(file_ref, file_refs_url, auth_headers):
    print('Adding file: {}'.format(file_ref['fileName']))
    response = requests.put(file_refs_url, headers=auth_headers, data=json.dumps(file_ref))
    check_status(response.status_code, response.text)


def get_entity_url(js, entity):
    entity_url = js['_links'][entity]['href'].split('{')[0]
    print('Got url for {0}: {1}'.format(entity, entity_url))
    return entity_url


def get_input_bundle_uuid(analysis_json):
    bundle = analysis_json['input_bundles'][0]
    uuid = bundle
    print('Input bundle uuid {0}'.format(uuid))
    return uuid


def get_output_files(analysis_json, schema_version):
    outputs = analysis_json['outputs']
    output_refs = []

    for out in outputs:
        output_ref = {}
        file_name = out['file_core']['file_name']
        output_ref['fileName'] = file_name
        output_ref['content'] = {
            'describedBy': 'https://schema.humancellatlas.org/type/file/{}/analysis_file'.format(schema_version),
            'schema_type': 'file',
            'file_core': {
                'describedBy': 'https://schema.humancellatlas.org/core/file/{}/file_core'.format(schema_version),
                'file_name': file_name,
                'file_format': out['file_core']['file_format']
            }
        }
        output_refs.append(output_ref)
    return output_refs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--submit_url', required=True)
    parser.add_argument('--analysis_json_path', required=True)
    parser.add_argument('--schema_version', required=True, help='The metadata schema version that the analysis.json conforms to')
    parser.add_argument('--retry_seconds', required=False, default=RETRY_SECONDS, help='Maximum interval for exponential retries')
    parser.add_argument('--timeout_seconds', required=False, default=TIMEOUT_SECONDS, help='Maximum duration for retrying a request')
    args = parser.parse_args()
    run(args.submit_url, args.analysis_json_path, args.schema_version, args.retry_seconds, args.timeout_seconds)


if __name__ == '__main__':
    main()
