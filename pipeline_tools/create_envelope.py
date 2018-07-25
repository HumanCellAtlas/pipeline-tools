#!/usr/bin/env python

import json
import argparse
import requests
from .dcp_utils import get_auth_token, make_auth_header
from pipeline_tools.http_requests import HttpRequests


def run(submit_url, analysis_json_path, schema_url, analysis_file_version):
    """Create submission in ingest service.

    Args:
        submit_url (str): url of ingest service to use
        analysis_json_path (str): path to analysis json file
        schema_url (str): URL for retrieving HCA metadata schemas
        analysis_file_version (str): version of metadata schema that output files should conform to

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    http_requests = HttpRequests()

    # 0. Get Auth token and make auth headers
    print('Fetching auth token from Auth0')
    auth_token = get_auth_token(http_requests)
    print('Making auth headers')
    auth_headers = make_auth_header(auth_token)

    # 1. Get envelope url
    envelope_url = get_envelope_url(submit_url, auth_headers, http_requests)

    # 2. Create envelope
    envelope_js = create_submission_envelope(envelope_url, auth_headers, http_requests)

    submission_url = get_subject_url(envelope_js, 'submissionEnvelope')
    with open('submission_url.txt', 'w') as f:
        f.write(submission_url)

    # 3. Create analysis
    with open(analysis_json_path) as f:
        analysis_json_contents = json.load(f)
    analyses_url = get_subject_url(envelope_js, 'processes')

    # Check if an analysis process exists in the submission envelope from a previous attempt
    analysis_workflow_id = analysis_json_contents['protocol_core']['protocol_id']
    analysis_js = get_analysis_process(analyses_url, auth_headers, analysis_workflow_id, http_requests)
    if not analysis_js:
        analysis_js = create_analysis(analyses_url, auth_headers, analysis_json_contents, http_requests)

    # 4. Add input bundles
    input_bundles_url = get_subject_url(analysis_js, 'add-input-bundles')
    add_input_bundles(input_bundles_url, auth_headers, analysis_json_contents, http_requests)

    # 5. Add file references
    file_refs_url = get_subject_url(analysis_js, 'add-file-reference')
    print('Adding file references at {0}'.format(file_refs_url))
    output_files = get_output_files(analysis_json_contents, schema_url, analysis_file_version)

    for file_ref in output_files:
        add_file_reference(file_ref, file_refs_url, auth_headers, http_requests)


def get_envelope_url(submit_url, auth_headers, http_requests):
    """Query the ingest service to get the url for creating envelopes.

    Args:
        submit_url (str): the root url of the ingest service
        auth_headers (dict): headers to use for auth
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        envelope_url (str): A string giving the url for creating envelopes

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    print('Getting envelope url from {}'.format(submit_url))
    response = http_requests.get(submit_url,
                                 headers=auth_headers)
    envelope_url = get_subject_url(response.json(), 'submissionEnvelopes')
    return envelope_url


def create_submission_envelope(envelope_url, auth_headers, http_requests):
    """Creates a new submission envelope

    Args:
        envelope_url (str): the url for creating envelopes
        auth_headers (dict): headers to use for auth
        http_requests (HttpRequests): HttpRequests object to use

    Returns:
        envelope_js (dict): Dict representing the JSON response to the request

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    print('Creating submission envelope at {0}'.format(envelope_url))
    response = http_requests.post(envelope_url,
                                  '{}',
                                  headers=auth_headers)
    envelope_js = response.json()
    return envelope_js


def get_analysis_process(analyses_url, auth_headers, analysis_workflow_id, http_requests):
    """Checks the submission envelope for an analysis process with a protocol_id
    that matches the analysis workflow id.

    Args:
        analyses_url (str): the url for creating the analysis record
        auth_headers (dict): headers to use for auth
        analysis_workflow_id (str): Cromwell id for analysis workflow
        http_requests (HttpRequests): HttpRequests object to use

    Returns:
        analysis_js (dict): A dict represents the response JSON

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    response = http_requests.get(analyses_url, headers=auth_headers)
    data = response.json().get('_embedded')
    if data:
        processes_js = data.get('processes')
        for process in processes_js:
            process_id = process['content']['protocol_core']['protocol_id']
            if process_id == analysis_workflow_id:
                print('Found existing analysis result for workflow {} in {}'.format(analysis_workflow_id, analyses_url))
                return process
    return None


def create_analysis(analyses_url, auth_headers, analysis_json_contents, http_requests):
    """Creates the analysis record for the submission envelope

    Args:
        analyses_url (str): the url for creating the analysis record
        auth_headers (dict): headers to use for auth
        analysis_json_contents (dict): metadata describing the analysis
        http_requests (HttpRequests): HttpRequests object to use

    Returns:
        analysis_js (dict): A dict represents the response JSON

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    print('Creating analysis at {0}'.format(analyses_url))
    response = http_requests.post(analyses_url,
                                  headers=auth_headers,
                                  json=analysis_json_contents)
    analysis_js = response.json()
    return analysis_js


def add_input_bundles(input_bundles_url, auth_headers, analysis_json_contents, http_requests):
    """Adds references to the input bundle(s) used in the analysis.

    Args:
        input_bundles_url (str): the url to use for adding input bundles
        auth_headers (dict): headers to use for auth
        analysis_json_contents (dict): metadata describing the analysis
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        dict: Dict representing the JSON response to the request

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    print('Adding input bundles at {0}'.format(input_bundles_url))
    input_bundle_uuid = get_input_bundle_uuid(analysis_json_contents)
    bundle_refs_js = {"bundleUuids": [input_bundle_uuid]}
    print(bundle_refs_js)
    response = http_requests.put(input_bundles_url,
                                 headers=auth_headers,
                                 json=bundle_refs_js)
    return response.json()


def add_file_reference(file_ref, file_refs_url, auth_headers, http_requests):
    """Adds a file reference to the analysis metadata in a submission envelope

    Args:
        file_ref (dict): metadata about the file
        file_refs_url (str): the url for adding file references
        auth_headers (dict): headers to use for auth
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        dict: Dict representing the JSON response to the request

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    print('Adding file: {}'.format(file_ref['fileName']))
    response = http_requests.put(file_refs_url,
                                 headers=auth_headers,
                                 json=file_ref)
    return response.json()


def get_subject_url(js, subject):
    """Get the ingest service url for the given subject

    Args:
        js (dict): the JSON response from the root ingest service url
        entity (str): the name of the subject to look for (e.g.
        'submissionEnvelope', 'add-file-reference')

    Returns:
        subject_url (str): A string giving the url for the given subject
    """
    subject_url = js['_links'][subject]['href'].split('{')[0]
    print('Got url for {0}: {1}'.format(subject, subject_url))
    return subject_url


def get_input_bundle_uuid(analysis_json):
    """Get the uuid of the input bundle used in the analysis

    Args:
        analysis_json (dict): metadata describing the analysis

    Returns:
        uuid (str): A string representing the input bundle uuid
    """
    bundle = analysis_json['input_bundles'][0]
    uuid = bundle
    print('Input bundle uuid {0}'.format(uuid))
    return uuid


def get_output_files(analysis_json, schema_url, analysis_file_version):
    """Get the metadata describing the outputs of the analysis

    Args:
        analysis_json (dict): metadata describing the analysis
        schema_url (str): URL for retrieving HCA metadata schemas
        analysis_file_version (str): the schema version that file references will conform to

    Returns:
        output_refs (dict): A dict of metadata describing the output files produced by the analysis
    """
    outputs = analysis_json['outputs']
    output_refs = []

    for out in outputs:
        output_ref = {}
        file_name = out['file_core']['file_name']
        output_ref['fileName'] = file_name
        output_ref['content'] = {
            'describedBy': '{}/type/file/{}/analysis_file'.format(schema_url, analysis_file_version),
            'schema_type': 'file',
            'file_core': {
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
    parser.add_argument('--schema_url', required=True, help='URL for retrieving HCA metadata schemas')
    parser.add_argument('--analysis_file_version', required=True, help='The metadata schema version that the analysis files conform to')
    args = parser.parse_args()
    schema_url = args.schema_url.strip('/')
    run(args.submit_url, args.analysis_json_path, schema_url, args.analysis_file_version)


if __name__ == '__main__':
    main()
