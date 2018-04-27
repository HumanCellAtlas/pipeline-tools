import argparse
import json
from requests.auth import HTTPBasicAuth
from cromwell_tools import cromwell_tools
from pipeline_tools.http_requests import HttpRequests


def get_workflow_id(analysis_output_path):
    """Parse the analysis workflow id from one of its output paths, and write the id to a file so that it is available
    outside of the get_analysis task.

    Args:
        analysis_output_path (str): path to workflow output file.

    Returns:
        workflow_id: string giving Cromwell UUID of the workflow.
    """
    url = analysis_output_path
    hash_end = url.rfind("/call-")
    hash_start = url.rfind('/', 0, hash_end) + 1
    workflow_id = url[hash_start:hash_end]
    with open('workflow_id.txt', 'w') as f:
        f.write(workflow_id)
    return workflow_id


def get_auth(credentials_file=None):
    """Parse cromwell username and password from credentials file.

    Args:
        credentials_file (str): Path to the file containing cromwell authentication credentials.

    Returns:
        requests.auth.HTTPBasicAuth object to use for cromwell requests
    """
    credentials_file = credentials_file or '/cromwell-metadata/cromwell_credentials.txt'
    with open(credentials_file) as f:
        credentials = f.read().split()
    user = credentials[0]
    password = credentials[1]
    return HTTPBasicAuth(user, password)


def get_metadata(runtime_environment, workflow_id, http_requests, use_caas=False, caas_key_file=None):
    """Get metadata for analysis workflow from Cromwell and write it to a JSON file. Retry the request with exponentially
    increasing wait times if there is an error.

    Args:
        runtime_environment (str): the cromwell environment the workflow was run in.
        workflow_id (str): the analysis workflow id.
        use_caas (bool): whether or not to use Cromwell-as-a-Service.
        caas_key_file (str): path to CaaS service account JSON key file.

    Returns:
        Nothing returned

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond the timeout
    """
    def log_before(workflow_id):
        print('Getting metadata for workflow {}'.format(workflow_id))

    if use_caas:
        cromwell_url = 'https://cromwell.caas-dev.broadinstitute.org/api/workflows/v1'
        json_credentials = caas_key_file or "/cromwell-metadata/caas_key.json"
        headers = cromwell_tools.generate_auth_header_from_key_file(json_credentials)
        auth = None
    else:
        cromwell_url = 'https://cromwell.mint-{}.broadinstitute.org/api/workflows/v1'.format(runtime_environment)
        headers = None
        auth = get_auth()
    url = '{}/{}/metadata?expandSubWorkflows=true'.format(cromwell_url, workflow_id)
    response = http_requests.get(url, auth=auth, headers=headers, before=log_before(workflow_id))
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--runtime_environment', required=True)
    parser.add_argument('--use_caas', required=True)
    parser.add_argument('--caas_key_file', required=False, default=None)
    args = parser.parse_args()
    workflow_id = get_workflow_id(args.analysis_output_path)
    use_caas = True if args.use_caas.lower() == 'true' else False
    get_metadata(args.runtime_environment, workflow_id, HttpRequests(), use_caas, args.caas_key_file)


if __name__ == '__main__':
    main()
