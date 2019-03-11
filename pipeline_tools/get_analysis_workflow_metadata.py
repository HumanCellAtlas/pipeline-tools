import argparse
import json
import google.auth
import google.auth.transport.requests
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


def get_auth_headers():
    """ Get a bearer token from the default google account credentials on the machine that executes 
    this function. The credentials must have the scopes "https://www.googleapis.com/auth/userinfo.email"
    and "https://www.googleapis.com/auth/userinfo.profile", which Cromwell will add automatically if 
    it is confiugred to use the Pipelines API v2 backend.

    Returns:
        headers (dict): authorization header containing bearer token {'Authorization': 'bearer 12345'}
    """
    credentials, project = google.auth.default()
    if not credentials.valid:
        credentials.refresh(google.auth.transport.requests.Request())
    headers = {}
    credentials.apply(headers)
    return headers


def get_metadata(cromwell_url, workflow_id, http_requests):
    """Get metadata for analysis workflow from Cromwell and write it to a JSON file. This is only 
    compatible with instances of Cromwell that use SAM for Identity Access Management (IAM), such 
    as Cromwell-as-a-Service. 

    Args:
        cromwell_url (str): Url to the cromwell environment the workflow was run in.
        workflow_id (str): The analysis workflow id.
        http_requests: `http_requests.HttpRequests` instance, a wrapper around requests provides better retry and
                       logging.

    Raises:
        requests.HTTPError: For 4xx errors or 5xx errors beyond the timeout
    """

    def log_before(workflow_id):
        print('Getting metadata for workflow {}'.format(workflow_id))

    headers = get_auth_headers()
    base_url = cromwell_url.strip('/')
    url = '{0}/api/workflows/v1/{1}/metadata?expandSubWorkflows=true'.format(base_url, workflow_id)
    response = http_requests.get(url, headers=headers, before=log_before(workflow_id))
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--cromwell_url', required=True)
    args = parser.parse_args()

    print('Using analysis output path: {0}'.format(args.analysis_output_path))

    # Get the workflow id and metadata, write them to files
    workflow_id = get_analysis_workflow_id(analysis_output_path=args.analysis_output_path)
    get_metadata(cromwell_url=args.cromwell_url,
                 workflow_id=workflow_id,
                 http_requests=HttpRequests())


if __name__ == '__main__':
    main()
