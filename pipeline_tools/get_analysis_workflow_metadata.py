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

    def log_before(workflow_id):
        print('Getting metadata for workflow {}'.format(workflow_id))

    cromwell_url = cromwell_url + '/api/workflows/v1`

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


if __name__ == '__main__':
    main()
