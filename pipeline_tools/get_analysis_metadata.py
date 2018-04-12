import argparse
import json
import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, wait_exponential, stop_after_delay


def get_workflow_id(analysis_output_path):
    """
    Parse the analysis workflow id from one of its output paths, and write the id to a file so that it is available
    outside of the get_analysis task.
    :param str analysis_output_path: Path to workflow output file.
    :return: str workflow_id: Cromwell UUID of the workflow.
    """
    url = analysis_output_path
    hash_end = url.rfind("/call-")
    hash_start = url.rfind('/', 0, hash_end) + 1
    workflow_id = url[hash_start:hash_end]
    with open('workflow_id.txt', 'w') as f:
        f.write(workflow_id)
    return workflow_id


def get_auth(credentials_file=None):
    """
    Parse cromwell username and password from credentials file.
    :param str credentials_file: Path to the file containing cromwell authentication credentials.
    :return: requests.auth.HTTPBasicAuth: Auth object to use for cromwell requests.
    """
    credentials_file = credentials_file or '/cromwell-metadata/cromwell_credentials.txt'
    with open(credentials_file) as f:
        credentials = f.read().split()
    user = credentials[0]
    password = credentials[1]
    return HTTPBasicAuth(user, password)


@retry(reraise=True, wait=wait_exponential(multiplier=1, max=10), stop=stop_after_delay(20))
def get_metadata(runtime_environment, workflow_id):
    """
    Get metadata for analysis workflow from Cromwell and write it to a JSON file. Retry the request with exponentially
    increasing wait times if there is an error.
    :param str runtime_environment: The cromwell environment the workflow was run in.
    :param str workflow_id: The analysis workflow id.
    :return:
    """
    print('Getting metadata for workflow {}'.format(workflow_id))
    url = 'https://cromwell.mint-{}.broadinstitute.org/api/workflows/v1/{}/metadata?expandSubWorkflows=true'.format(runtime_environment, workflow_id)
    response = requests.get(url, auth=get_auth())
    response.raise_for_status()
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--runtime_environment', required=True)
    parser.add_argument('--retry_exponential_multiplier', required=False, default=1)
    parser.add_argument('--retry_exponential_max', required=False, default=10)
    parser.add_argument('--retry_timeout', required=False, default=20)
    args = parser.parse_args()
    workflow_id = get_workflow_id(args.analysis_output_path)
    get_metadata.retry_with(reraise=True,
                            wait=wait_exponential(multiplier=args.retry_exponential_multiplier, max=args.retry_exponential_max),
                            stop=stop_after_delay(args.retry_timeout))(args.runtime_environment, workflow_id)
    print(get_metadata.retry.statistics)


if __name__ == '__main__':
    main()
