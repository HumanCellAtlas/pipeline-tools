#!/usr/bin/env python
import argparse
import json
from typing import List

from pipeline_tools.dcp_utils import get_auth_token, make_auth_header
from pipeline_tools.http_requests import HttpRequests


def build_envelope(submit_url, analysis_protocol_path, analysis_process_path, raw_schema_url,
                   analysis_file_version):
    """Create the submission envelope in Ingest service.

    Args:
        submit_url (str): URL of Ingest service to perform the submission.
        analysis_protocol_path (str): Path to the analysis_protocol json file.
        analysis_process_path (str): Path to the analysis_process json file.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        analysis_file_version (str): Version of the metadata schema that the analysis_file conforms to.
    """
    # Instantiate a HttpRequests object
    http_requests = HttpRequests()

    # === 0. Get Auth token and make auth headers ===
    print('Fetching auth token from Auth0')
    auth_token = get_auth_token(http_requests)
    print('Making auth headers')
    auth_headers = make_auth_header(auth_token)

    # === 1. Get envelope url ===
    envelope_url = get_envelope_url(submit_url, auth_headers, http_requests)

    # === 2. Create envelope and get submission_url ===
    envelope_dict = create_submission_envelope(envelope_url, auth_headers, http_requests)
    submission_url = get_subject_url(endpoint_dict=envelope_dict, subject='submissionEnvelope')

    # save submission_url to disk
    with open('submission_url.txt', 'w') as f:
        f.write(submission_url)

    # === 3. Create analysis_protocol ===
    with open(analysis_protocol_path) as f:
        analysis_protocol_dict = json.load(f)

    analysis_protocol_url = get_subject_url(endpoint_dict=envelope_dict, subject='protocols')

    # Check if an analysis_protocol already exists in the submission envelope from a previous attempt
    pipeline_version = analysis_protocol_dict['protocol_core']['protocol_id']
    analysis_protocol = get_analysis_protocol(analysis_protocol_url=analysis_protocol_url,
                                              auth_headers=auth_headers,
                                              protocol_id=pipeline_version,
                                              http_requests=http_requests)

    # Create analysis_protocol if this is the first attempt
    if not analysis_protocol:
        _analysis_protocol = add_analysis_protocol(analysis_protocol_url=analysis_protocol_url,
                                                   auth_headers=auth_headers,
                                                   analysis_protocol=analysis_protocol_dict,
                                                   http_requests=http_requests)

    # === 4. Create analysis_process ===
    with open(analysis_process_path) as f:
        analysis_process_dict = json.load(f)

    analysis_process_url = get_subject_url(endpoint_dict=envelope_dict, subject='processes')

    # Check if an analysis_process already exists in the submission envelope from a previous attempt
    analysis_workflow_id = analysis_process_dict['process_core']['process_id']
    analysis_process = get_analysis_process(analysis_process_url=analysis_process_url,
                                            auth_headers=auth_headers,
                                            process_id=analysis_workflow_id,
                                            http_requests=http_requests)

    # Create analysis_process if this is the first attempt
    if not analysis_process:
        analysis_process = add_analysis_process(analysis_process_url=analysis_process_url,
                                                auth_headers=auth_headers,
                                                analysis_process=analysis_process_dict,
                                                http_requests=http_requests)

    # === 5. Link analysis_protocol to analysis_process ===
    link_url = get_subject_url(endpoint_dict=analysis_process, subject='protocols')
    print('Linking analysis_protocol to analysis_process at {0}'.format(link_url))
    link_analysis_protocol_to_analysis_process(link_url=link_url,
                                               analysis_protocol_url=analysis_protocol_url,
                                               http_requests=http_requests)

    # === 6. Add input bundle references ===
    input_bundles_url = get_subject_url(endpoint_dict=analysis_process, subject='add-input-bundles')
    print('Adding input bundles at {0}'.format(input_bundles_url))
    add_input_bundles(input_bundles_url=input_bundles_url,
                      auth_headers=auth_headers,
                      analysis_process=analysis_process_dict,
                      http_requests=http_requests)

    # === 7. Add file references ===
    file_refs_url = get_subject_url(endpoint_dict=analysis_process, subject='add-file-reference')
    print('Adding file references at {0}'.format(file_refs_url))
    output_files = get_output_files(analysis_process=analysis_process_dict,
                                    raw_schema_url=raw_schema_url,
                                    analysis_file_version=analysis_file_version)

    # TODO: parallelize this to speed up
    for file_ref in output_files:  # TODO: parallelize this to speed up
        add_file_reference(file_ref=file_ref,
                           file_refs_url=file_refs_url,
                           auth_headers=auth_headers,
                           http_requests=http_requests)


def get_subject_url(endpoint_dict, subject):
    """Get the Ingest service url for a given subject.

    Args:
        endpoint_dict (dict): Dict representing the JSON response from the root Ingest service url. A typical dict will
                              look like below:
        ```
        {'_links': {'biomaterials'       : {'href'     : 'ingest_root_url/biomaterials{?page,size,sort,projection}',
                                            'templated': True},
                    'bundleManifests'    : {'href'     : 'ingest_root_url/bundleManifests{?page,size,sort}',
                                            'templated': True,
                                            'title'    : 'Access or create bundle manifests (describing which '
                                                         'submitted contents went into which bundle in the datastore)'},
                    'files'              : {'href'     : 'ingest_root_url/files{?page,size,sort}',
                                            'templated': True,
                                            'title'    : 'Access or create, within a submission envelope, a new assay'},
                    'processes'          : {'href'     : 'ingest_root_url/processes{?page,size,sort,projection}',
                                            'templated': True},
                    'profile'            : {'href': 'ingest_root_url/profile'},
                    'projects'           : {'href'     : 'ingest_root_url/projects{?page,size,sort}',
                                            'templated': True,
                                            'title'    : 'Access or create projects. Creation can only be done inside '
                                                         'a submission envelope'},
                    'protocols'          : {'href'     : 'ingest_root_url/protocols{?page,size,sort}',
                                            'templated': True,
                                            'title'    : 'Access or create protocols'},
                    'schemas'            : {'href'     : 'ingest_root_url/schemas{?page,size,sort}',
                                            'templated': True},
                    'submissionEnvelopes': {'href'     : 'ingest_root_url/submissionEnvelopes{?page,size,sort}',
                                            'templated': True,
                                            'title'    : 'Access or create new submission envelopes'},
                    'submissionManifests': {'href'     : 'ingest_root_url/submissionManifests{?page,size,sort}',
                                            'templated': True},
                    'user'               : {'href': 'ingest_root_url/user'}}}
        ```
        subject (str): The name of the subject to look for. (e.g. 'submissionEnvelope', 'add-file-reference')

    Returns:
        subject_url (str): A string giving the url for the given subject.
    """
    subject_url = endpoint_dict['_links'][subject]['href'].split('{')[0]
    print('Got {subject} URL: {subject_url}'.format(subject=subject, subject_url=subject_url))
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
    envelope_url = get_subject_url(endpoint_dict=response.json(), subject='submissionEnvelopes')
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


# FIXME: questionable in new bundle structure :(
def get_analysis_protocol(analysis_protocol_url, auth_headers, protocol_id, http_requests):
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
                print('Found existing analysis_protocol for pipeline version {0} in {1}'.format(
                        protocol_id, analysis_protocol_url))
                return protocol
    return None


# FIXME: questionable in new bundle structure :(
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
        for process in content.get('protocols'):
            if process['content']['process_core']['process_id'] == process_id:
                print('Found existing analysis_process for workflow {0} in {1}'.format(
                        process_id, analysis_process_url))
                return process
    return None


def add_analysis_protocol(analysis_protocol_url, auth_headers, analysis_protocol, http_requests):
    """Add the analysis_protocol record for the submission envelope.

    Args:
        analysis_protocol_url (str): The url for creating the analysis_protocol.
        auth_headers (dict): Dict representing headers to use for auth.
        analysis_protocol (dict): A dict representing the analysis_protocol json file to be submitted.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        analysis_protocol (dict): A dict represents the JSON response from adding the analysis protocol.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Adding analysis_protocol at {0}'.format(analysis_protocol_url))
    response = http_requests.post(analysis_protocol_url, headers=auth_headers, json=analysis_protocol)
    analysis_protocol = response.json()
    return analysis_protocol


def add_analysis_process(analysis_process_url, auth_headers, analysis_process, http_requests):
    """Add the analysis_process record for the submission envelope.

    Args:
        analysis_process_url (str): The url for creating the analysis_protocol.
        auth_headers (dict): Dict representing headers to use for auth.
        analysis_process (dict): A dict representing the analysis_process json file to be submitted.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Returns:
        analysis_process (dict): A dict represents the JSON response from adding the analysis process.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    print('Adding analysis_process at {0}'.format(analysis_process_url))
    response = http_requests.post(analysis_process_url, headers=auth_headers, json=analysis_process)
    analysis_process = response.json()
    return analysis_process


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
    input_bundle_uuid = analysis_process['input_bundles'][0]
    bundle_refs_dict = {'bundleUuids': [input_bundle_uuid]}
    response = http_requests.put(input_bundles_url, headers=auth_headers, json=bundle_refs_dict)
    print('Added input bundle reference: {0}'.format(bundle_refs_dict))
    return response.json()


def get_output_files(analysis_process, raw_schema_url, analysis_file_version):
    """Get the metadata describing the outputs of the analysis process.

    TODO: Implement the dataclass in "https://github.com/HumanCellAtlas/metadata-api/blob/1b7192cecbef43b5befecc4153bf
    2e2f4db5bb16/src/humancellatlas/data/metadata/__init__.py#L445" so we can use the `metadata-api` directly to create
    the analysis_file metadata file.

    Args:
        analysis_process (dict): A dict representing the analysis_process json file to be submitted.
        raw_schema_url (str): URL prefix for retrieving HCA metadata schemas.
        analysis_file_version (str): Version of the metadata schema that the analysis_file conforms to.

    Returns:
        output_refs (List[dict]): A list of metadata dicts describing the output files produced by the analysis_process.
    """
    outputs = analysis_process['outputs']
    output_refs = [
        {
            'fileName': out['file_core']['file_name'],
            'content' : {
                'describedBy': '{0}/type/file/{1}/analysis_file'.format(raw_schema_url, analysis_file_version),
                'schema_type': 'file',
                'file_core'  : {
                    'file_name'  : out['file_core']['file_name'],
                    'file_format': out['file_core']['file_format']
                }
            }
        } for out in outputs
    ]
    return output_refs


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
    print('Adding file: {0} to the file reference.'.format(file_ref['fileName']))
    response = http_requests.put(file_refs_url, headers=auth_headers, json=file_ref)
    return response.json()


def link_analysis_protocol_to_analysis_process(link_url, analysis_protocol_url, http_requests):
    """Make the analysis process to be associated with the analysis_protocol to let Ingest create the links.json.

    Args:
        link_url (str): The url for link protocols to processes.
        analysis_protocol_url (str): The url for creating the analysis_protocol.
        http_requests (http_requests.HttpRequests): The HttpRequests object to use for talking to Ingest.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond timeout.
    """
    link_headers = {'content-type': 'text/uri-list'}
    response = http_requests.put(link_url, headers=link_headers, data=analysis_protocol_url)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--submit_url',
                        required=True,
                        help='The root url of the Ingest service for submissions.')
    parser.add_argument('--analysis_process_path',
                        required=True,
                        help='Path to the analysis_process.json file.')
    parser.add_argument('--analysis_protocol_path',
                        required=True,
                        help='Path to the analysis_protocol.json file.')
    parser.add_argument('--schema_url',
                        required=True,
                        help='URL for retrieving HCA metadata schemas.')
    parser.add_argument('--analysis_file_version',
                        required=True,
                        help='The metadata schema version that the output files(analysis_file) conform to.')
    args = parser.parse_args()

    schema_url = args.schema_url.strip('/')

    build_envelope(submit_url=args.submit_url,
                   analysis_protocol_path=args.analysis_protocol_path,
                   analysis_process_path=args.analysis_process_path,
                   raw_schema_url=schema_url,
                   analysis_file_version=args.analysis_file_version)


if __name__ == '__main__':
    main()
