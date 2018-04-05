import argparse
import json
import requests
from requests.auth import HTTPBasicAuth


def get_workflow_id(analysis_output_path):
    url = analysis_output_path
    hash_end = url.rfind("/call-")
    hash_start = url.rfind('/', 0, hash_end) + 1
    return url[hash_start:hash_end]


def get_auth():
    credentials_file = '/cromwell-metadata/cromwell_credentials.txt'
    with open(credentials_file) as f:
        credentials = f.read().split('\t')
    user = credentials[0]
    password = credentials[1]
    return HTTPBasicAuth(user, password)


def get_metadata(runtime_environment, workflow_id):
    print('Getting metadata for workflow {}'.format(workflow_id))
    url = 'https://cromwell.mint-{}.broadinstitute.org/api/workflows/v1/{}/metadata?expandSubWorkflows=true'.format(runtime_environment, workflow_id)
    response = requests.get(url, auth=get_auth())
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--runtime_environment', required=True)
    args = parser.parse_args()
    workflow_id = get_workflow_id(args.analysis_output_path)
    get_metadata(args.runtime_environment, workflow_id)


if __name__ == '__main__':
    main()
