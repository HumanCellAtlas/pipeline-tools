#!/usr/bin/env python
import argparse
import json

from pipeline_tools.shared import auth_utils
from pipeline_tools.shared.http_requests import HttpRequests


def build_envelope(
    submit_url,
    analysis_protocol_path,
    analysis_process_path,
    outputs_file_path,
    raw_schema_url,
    analysis_file_version,
    runtime_environment,
    service_account_key_path,
):
    """Create the submission envelope in Ingest service.

    Args:
        submit_url (str): URL of Ingest service to perform the submission.
        analysis_protocol_path (str): Path to the analysis_protocol json file.
        analysis_process_path (str): Path to the analysis_process json file.
        outputs_file_path (str): Path to the outputs json file.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        analysis_file_version (str): Version of the metadata schema that the analysis_file conforms to.
        runtime_environment (str): Environment where the pipeline is running ('dev', 'test', 'staging' or 'prod').
        service_account_key_path (str): Path to the JSON service account key for generating a JWT.
    """
    # Instantiate a HttpRequests object
    http_requests = HttpRequests()

    # === 0. Get Auth token and make auth headers ===
    # According to Ingest service, we only need to include the auth_headers for creating the envelope step, but
    # to be safe, we are sending the token at each step, except the linking step, which requires a totally different
    # content-type in the header.

    print('Making auth headers')
    dcp_auth_client = auth_utils.DCPAuthClient(
        service_account_key_path, runtime_environment
    )
    auth_headers = dcp_auth_client.get_auth_header()

    # === 1. Get envelope url ===
    envelope_url = get_envelope_url(submit_url, auth_headers, http_requests)

    # === 2. Create envelope and get submission_url ===
    envelope_dict = create_submission_envelope(
        envelope_url, auth_headers, http_requests
    )
    submission_url = get_subject_url(
        endpoint_dict=envelope_dict, subject='submissionEnvelope'
    )

    # save submission_url to disk
    with open('submission_url.txt', 'w') as f:
        f.write(submission_url)

    # === 3. Create analysis_protocol ===
    with open(analysis_protocol_path) as f:
        analysis_protocol_dict = json.load(f)

    # URL for getting and creating the analysis protocol, e.g.
    # https://api.ingest.{deployment}.data.humancellatlas.org/submissionEnvelopes/{envelope_id}/protocols
    analysis_protocol_url = get_subject_url(
        endpoint_dict=envelope_dict, subject='protocols'
    )

    # Check if an analysis_protocol already exists in the submission envelope from a previous attempt
    pipeline_version = analysis_protocol_dict['protocol_core']['protocol_id']
    analysis_protocol_response = get_analysis_protocol(
        analysis_protocol_url=analysis_protocol_url,
        auth_headers=auth_headers,
        protocol_id=pipeline_version,
        http_requests=http_requests,
    )

    # Create analysis_protocol if this is the first attempt
    if not analysis_protocol_response:
        analysis_protocol_response = add_analysis_protocol(
            analysis_protocol_url=analysis_protocol_url,
            auth_headers=auth_headers,
            analysis_protocol=analysis_protocol_dict,
            http_requests=http_requests,
        )

    # === 4. Create analysis_process ===
    with open(analysis_process_path) as f:
        analysis_process_dict = json.load(f)

    analysis_process_url = get_subject_url(
        endpoint_dict=envelope_dict, subject='processes'
    )

    # Check if an analysis_process already exists in the submission envelope from a previous attempt
    analysis_workflow_id = analysis_process_dict['process_core']['process_id']
    analysis_process_response = get_analysis_process(
        analysis_process_url=analysis_process_url,
        auth_headers=auth_headers,
        process_id=analysis_workflow_id,
        http_requests=http_requests,
    )

    # Create analysis_process if this is the first attempt
    if not analysis_process_response:
        analysis_process_response = add_analysis_process(
            analysis_process_url=analysis_process_url,
            auth_headers=auth_headers,
            analysis_process=analysis_process_dict,
            http_requests=http_requests,
        )

    # === 5. Link analysis_protocol to analysis_process ===
    link_url = get_subject_url(
        endpoint_dict=analysis_process_response, subject='protocols'
    )

    # URL for linking the analysis protocol to analysis process, e.g.
    # https://api.ingest.integration.data.humancellatlas.org/protocols/{protocol_document_id}
    analysis_protocol_entity_url = get_subject_url(analysis_protocol_response, 'self')

    print(
        'Linking analysis_protocol {0} to analysis_process at {1}'.format(
            analysis_protocol_entity_url, link_url
        )
    )
    link_analysis_protocol_to_analysis_process(
        auth_headers=auth_headers,
        link_url=link_url,
        analysis_protocol_url=analysis_protocol_entity_url,
        http_requests=http_requests,
    )

    # === 6. Add input bundle references ===
    input_bundles_url = get_subject_url(
        endpoint_dict=analysis_process_response, subject='add-input-bundles'
    )
    print('Adding input bundles at {0}'.format(input_bundles_url))
    add_input_bundles(
        input_bundles_url=input_bundles_url,
        auth_headers=auth_headers,
        analysis_process=analysis_process_dict,
        http_requests=http_requests,
    )

    # === 7. Add file references ===
    file_refs_url = get_subject_url(
        endpoint_dict=analysis_process_response, subject='add-file-reference'
    )
    print('Adding file references at {0}'.format(file_refs_url))

    with open(outputs_file_path) as f:
        outputs_dict = json.load(f)

    # TODO: parallelize this to speed up
    for file_ref in outputs_dict:  # TODO: parallelize this to speed up
        add_file_reference(
            file_ref=file_ref,
            file_refs_url=file_refs_url,
            auth_headers=auth_headers,
            http_requests=http_requests,
        )


def get_subject_url(endpoint_dict, subject):
    """Get the Ingest service url for a given subject.

    Args:
        endpoint_dict (dict): Dict representing the JSON response from the root Ingest service url.
        subject (str): The name of the subject to look for. (e.g. 'submissionEnvelope', 'add-file-reference')

    Returns:
        subject_url (str): A string giving the url for the given subject.
    """
    subject_url = endpoint_dict['_links'][subject]['href'].split('{')[0]
    print(
        'Got {subject} URL: {subject_url}'.format(
            subject=subject, subject_url=subject_url
        )
    )
    return subject_url


def get_envelope_url(submit_url, auth_headers, http_requests):
    """Query the Ingest service to get the url for creating envelopes.

    Args:
        submit_url (str): The root url of the Ingest service for submissions.
        auth_headers (dict): Dict representing headers to use for auth.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        envelope_url (str): A string giving the url for creating envelopes.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Getting envelope url from {}'.format(submit_url))
    response = http_requests.get(submit_url, headers=auth_headers)
    envelope_url = get_subject_url(
        endpoint_dict=response.json(), subject='submissionEnvelopes'
    )
    return envelope_url


def create_submission_envelope(envelope_url, auth_headers, http_requests):
    """Creates a new submission envelope and return its content.

    Args:
        envelope_url (str): The url for creating envelopes.
        auth_headers (dict): Dict representing headers to use for auth.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        envelope_dict (dict): Dict representing the JSON response to the request of creating the envelope.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Creating submission envelope at {0}'.format(envelope_url))
    response = http_requests.post(envelope_url, '{}', headers=auth_headers)
    envelope_dict = response.json()
    return envelope_dict


def get_analysis_protocol(
    analysis_protocol_url, auth_headers, protocol_id, http_requests
):
    """Checks the submission envelope for an analysis_protocol with a protocol_id that matches the pipeline version.

    Args:
        analysis_protocol_url (str): The url for creating the analysis_protocol.
        auth_headers (dict): Dict representing headers to use for auth.
        protocol_id (str): Required field of the analysis_protocol, the version of the given pipeline.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        protocol (dict or None): A dict represents the analysis_protocol.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    response = http_requests.get(analysis_protocol_url, headers=auth_headers)
    content = response.json().get('_embedded')

    if content:
        for protocol in content.get('protocols'):
            if protocol['content']['protocol_core']['protocol_id'] == protocol_id:
                print(
                    'Found existing analysis_protocol for pipeline version {0} in {1}'.format(
                        protocol_id, analysis_protocol_url
                    )
                )
                return protocol

    print("Cannot find any existing analysis_protocol with id: {0}".format(protocol_id))
    return None


def get_analysis_process(analysis_process_url, auth_headers, process_id, http_requests):
    """Checks the submission envelope for an analysis_process with a process_id that matches the analysis workflow id.

    Args:
        analysis_process_url (str): The url for creating the analysis_process.
        auth_headers (dict): Dict representing headers to use for auth.
        process_id (str): Required field of the analysis_process, the UUID of the analysis workflow in Cromwell.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        process (dict or None): A dict represents the analysis_process.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    response = http_requests.get(analysis_process_url, headers=auth_headers)
    content = response.json().get('_embedded')

    if content:
        for process in content.get('processes'):
            if process['content']['process_core']['process_id'] == process_id:
                print(
                    'Found existing analysis_process for workflow {0} in {1}'.format(
                        process_id, analysis_process_url
                    )
                )
                return process
    return None


def add_analysis_protocol(
    analysis_protocol_url, auth_headers, analysis_protocol, http_requests
):
    """Add the analysis_protocol record for the submission envelope.

    Args:
        analysis_protocol_url (str): The url for creating the analysis_protocol.
        auth_headers (dict): Dict representing headers to use for auth.
        analysis_protocol (dict): A dict representing the analysis_protocol json file to be submitted.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        analysis_protocol_response (dict): A dict represents the JSON response from adding the analysis protocol.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Adding analysis_protocol at {0}'.format(analysis_protocol_url))
    response = http_requests.post(
        analysis_protocol_url, headers=auth_headers, json=analysis_protocol
    )
    analysis_protocol_response = response.json()
    return analysis_protocol_response


def add_analysis_process(
    analysis_process_url, auth_headers, analysis_process, http_requests
):
    """Add the analysis_process record for the submission envelope.

    Args:
        analysis_process_url (str): The url for creating the analysis_protocol.
        auth_headers (dict): Dict representing headers to use for auth.
        analysis_process (dict): A dict representing the analysis_process json file to be submitted.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        analysis_process_response (dict): A dict represents the JSON response from adding the analysis process.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Adding analysis_process at {0}'.format(analysis_process_url))
    response = http_requests.post(
        analysis_process_url, headers=auth_headers, json=analysis_process
    )
    analysis_process_response = response.json()
    return analysis_process_response


def add_input_bundles(input_bundles_url, auth_headers, analysis_process, http_requests):
    """Add references to the input bundle(s) used in the analysis.

    Args:
        input_bundles_url(str): The url to use for adding input bundles.
        auth_headers (dict): Dict representing headers to use for auth.
        analysis_process (dict): A dict representing the analysis_process json file to be submitted.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        dict: Dict representing the JSON response to the request for adding the reference to input bundles.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Adding input bundles at {0}'.format(input_bundles_url))
    input_bundle_uuid = analysis_process['input_bundles'][
        0
    ]  # Note: it's a placeholder here
    bundle_refs_dict = {'bundleUuids': [input_bundle_uuid]}
    response = http_requests.put(
        input_bundles_url, headers=auth_headers, json=bundle_refs_dict
    )
    print('Added input bundle reference: {0}'.format(bundle_refs_dict))
    return response.json()


def add_file_reference(file_ref, file_refs_url, auth_headers, http_requests):
    """Add a file reference to the analysis metadata in a submission envelope.

    Args:
        file_ref (dict): HCA metadata stub about the file.
        file_refs_url (str): The url for adding file references.
        auth_headers (dict): Dict representing headers to use for auth.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        dict: Dict representing the JSON response to the request that adding the file reference.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    # Format payload for ingest file reference API endpoint
    file_payload = {'fileName': file_ref['file_core']['file_name'], 'content': file_ref}
    print('Adding file reference: {}'.format(file_payload))
    response = http_requests.put(file_refs_url, headers=auth_headers, json=file_payload)
    return response.json()


def link_analysis_protocol_to_analysis_process(
    auth_headers, link_url, analysis_protocol_url, http_requests
):
    """Make the analysis process to be associated with the analysis_protocol to let Ingest create the links.json.

    Args:
        auth_headers (dict): Dict representing headers to use for auth.
        link_url (str): The url for link protocols to processes.
        analysis_protocol_url (str): The url for creating the analysis_protocol.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        dict: Dict representing the JSON response to the request that adding the file reference.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    link_headers = {'content-type': 'text/uri-list'}
    link_headers.update(auth_headers)
    response = http_requests.put(
        link_url, headers=link_headers, data=analysis_protocol_url
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--submit_url',
        required=True,
        help='The root url of the Ingest service for submissions.',
    )
    parser.add_argument(
        '--analysis_process_path',
        required=True,
        help='Path to the analysis_process.json file.',
    )
    parser.add_argument(
        '--outputs_file_path', required=True, help='Path to the outputs.json file.'
    )
    parser.add_argument(
        '--analysis_protocol_path',
        required=True,
        help='Path to the analysis_protocol.json file.',
    )
    parser.add_argument(
        '--schema_url', required=True, help='URL for retrieving HCA metadata schemas.'
    )
    parser.add_argument(
        '--analysis_file_version',
        required=True,
        help='The metadata schema version that the output files(analysis_file) conform to.',
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

    schema_url = args.schema_url.strip('/')

    build_envelope(
        submit_url=args.submit_url,
        analysis_protocol_path=args.analysis_protocol_path,
        analysis_process_path=args.analysis_process_path,
        outputs_file_path=args.outputs_file_path,
        raw_schema_url=schema_url,
        analysis_file_version=args.analysis_file_version,
        runtime_environment=args.runtime_environment,
        service_account_key_path=args.service_account_key_path,
    )


if __name__ == '__main__':
    main()
