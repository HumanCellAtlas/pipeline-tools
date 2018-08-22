import argparse
import json
from cromwell_tools import cromwell_tools
from requests.auth import HTTPBasicAuth

from pipeline_tools.http_requests import HttpRequests


def get_analysis_workflow_id(analysis_output_path):
    """Parse the analysis workflow id from one of its output paths, and write the id to a file so that it is available
    outside of the get_analysis task.

    Args:
        analysis_output_path (str): path to workflow output file.

    Returns:
        workflow_id (str): string giving Cromwell UUID of the workflow.
    """
    url = analysis_output_path
    calls = url.split('/call-')
    workflow_id = calls[1].split('/')[-1]
    print('Got analysis workflow UUID: {0}'.format(workflow_id))
    with open('workflow_id.txt', 'w') as f:
        f.write(workflow_id)
    return workflow_id


def get_adapter_workflow_id(analysis_output_path):
    """Parse the adapter workflow id from one of its analysis workflow output paths.

    Args:
        analysis_output_path (str): Path to workflow output file.

    Returns:
        workflow_id (str): String giving Cromwell UUID of the adapter workflow.
    """
    url = analysis_output_path
    calls = url.split('/call-')
    adapter_workflow_id = calls[0].split('/')[-1]
    print('Got adapter workflow UUID: {0}'.format(adapter_workflow_id))
    with open('adapter_workflow_id.txt', 'w') as f:
        f.write(adapter_workflow_id)
    return adapter_workflow_id


def get_adapter_workflow_version(cromwell_url,
                                 adapter_workflow_id,
                                 http_requests,
                                 use_caas=False,
                                 caas_key_file=None):
    """Get the version of the adapter workflow from its workflow id and write the version to a file so that it is
    available outside of the get_analysis task.

    Args:
        cromwell_url (str): Url to the cromwell environment the workflow was run in.
        adapter_workflow_id (str): String giving Cromwell UUID of the adapter workflow.
        http_requests: `http_requests.HttpRequests` instance, a wrapper around requests provides better retry and
                       logging.
        use_caas (bool): whether or not to use Cromwell-as-a-Service.
        caas_key_file (str): path to CaaS service account JSON key file.

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond the timeout
    """

    def log_before(workflowId):
        print('Getting the version for adapter workflow {}'.format(workflowId))

    cromwell_url = cromwell_url

    if use_caas:
        json_credentials = caas_key_file or "/cromwell-metadata/caas_key.json"
        headers = cromwell_tools.generate_auth_header_from_key_file(json_credentials)
        auth = None
    else:
        headers = None
        auth = get_auth()
    url = '{0}/query?id={1}&additionalQueryResultFields=labels'.format(cromwell_url, adapter_workflow_id)
    response = http_requests.get(url, auth=auth, headers=headers, before=log_before(adapter_workflow_id))

    workflow_labels = response.json().get('results')[0].get('labels')

    workflow_version = workflow_labels.get('workflow-version') if workflow_labels else None

    with open('pipeline_version.txt', 'w') as f:
        f.write(workflow_version)


def get_auth(credentials_file=None):
    """Parse cromwell username and password from credentials file.

    Args:
        credentials_file (str): Path to the file containing cromwell authentication credentials.

    Returns:
        requests.auth.HTTPBasicAuth: An object to be used for cromwell requests.
    """
    credentials_file = credentials_file or '/cromwell-metadata/cromwell_credentials.txt'
    with open(credentials_file) as f:
        credentials = f.read().split()
    user = credentials[0]
    password = credentials[1]
    return HTTPBasicAuth(user, password)


def get_metadata(cromwell_url, workflow_id, http_requests, use_caas=False, caas_key_file=None):
    """Get metadata for analysis workflow from Cromwell and write it to a JSON file. Retry the request with
    exponentially increasing wait times if there is an error.

    Args:
        cromwell_url (str): Url to the cromwell environment the workflow was run in.
        workflow_id (str): The analysis workflow id.
        http_requests: `http_requests.HttpRequests` instance, a wrapper around requests provides better retry and
                       logging.
        use_caas (bool): Whether or not to use Cromwell-as-a-Service.
        caas_key_file (str): Path to CaaS service account JSON key file.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond the timeout
    """

    def log_before(workflowId):
        print('Getting metadata for workflow {}'.format(workflowId))

    cromwell_url = cromwell_url

    if use_caas:
        json_credentials = caas_key_file or "/cromwell-metadata/caas_key.json"
        headers = cromwell_tools.generate_auth_header_from_key_file(json_credentials)
        auth = None
    else:
        headers = None
        auth = get_auth()
    url = '{0}/{1}/metadata?expandSubWorkflows=true'.format(cromwell_url, workflow_id)
    response = http_requests.get(url, auth=auth, headers=headers, before=log_before(workflow_id))
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--cromwell_url', required=True)
    parser.add_argument('--use_caas', required=True)
    parser.add_argument('--caas_key_file', required=False, default=None)
    args = parser.parse_args()

    use_caas = True if args.use_caas.lower() == 'true' else False

    print('Using analysis output path: {0}'.format(args.analysis_output_path))

    # Get the workflow id and metadata, write them to files
    workflow_id = get_analysis_workflow_id(analysis_output_path=args.analysis_output_path)
    get_metadata(cromwell_url=args.cromwell_url,
                 workflow_id=workflow_id,
                 http_requests=HttpRequests(),
                 use_caas=use_caas,
                 caas_key_file=args.caas_key_file)

    # Get the pipeline version and write to file
    adapter_workflow_id = get_adapter_workflow_id(analysis_output_path=args.analysis_output_path)
    get_adapter_workflow_version(cromwell_url=args.cromwell_url,
                                 adapter_workflow_id=adapter_workflow_id,
                                 http_requests=HttpRequests(),
                                 use_caas=use_caas,
                                 caas_key_file=args.caas_key_file)


if __name__ == '__main__':
    main()
