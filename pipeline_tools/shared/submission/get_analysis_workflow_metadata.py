import argparse
import json
import google.auth
import google.auth.transport.requests
import re
from pipeline_tools.shared.http_requests import HttpRequests


def get_analysis_workflow_id(analysis_output_path):
    """Parse the analysis workflow id from one of its output paths, and write the id to a file so that it is available
    outside of the get_analysis task.

    Args:
        analysis_output_path (str): path to workflow output file.

    Returns:
        workflow_id (str): string giving Cromwell UUID of the workflow.
    """
    # Get the last match for UUID prior to the file name (in case the file is
    # named with a UUID) to ensure it is the subworkflow id
    url = analysis_output_path.rsplit('/', 1)[0]
    uuid_regex = r"([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})"
    workflow_id = re.findall(uuid_regex, url)[-1]
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


def get_metadata(cromwell_url,
                 workflow_id,
                 http_requests,
                 include_keys=[],
                 include_subworkflows="False"
):
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

    # There is an issue with the current CromIAM that huge response content can
    # break the gzip compressing and session, which will raise an error:
    # `requests.exceptions.ChunkedEncodingError`. Check here:
    # https://github.com/broadinstitute/cromwell/issues/4708 for the details.
    # As a workaround, we need to pass `Accept-Encoding: identity` to the header
    # to force disabling the compressing
    headers['Accept-Encoding'] = 'identity'

    base_url = cromwell_url.strip('/')
    url = '{0}/api/workflows/v1/{1}/metadata?expandSubWorkflows={2}'.format(base_url,
                                                                            workflow_id,
                                                                            include_subworkflows.lower()
    )

    if include_keys:
        print(f'Including keys: {", ".join(include_keys)}')
        key_query = f'&includeKey={"&includeKey=".join(include_keys)}'
        url += key_query

    response = http_requests.get(url, headers=headers, before=log_before(workflow_id))
    with open('metadata.json', 'w') as f:
        json.dump(response.json(), f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis_output_path', required=True)
    parser.add_argument('--cromwell_url', required=True)
    parser.add_argument('--include_keys', required=False, nargs='+')
    parser.add_argument('--include_subworkflows', default=False)
    args = parser.parse_args()

    print('Using analysis output path: {0}'.format(args.analysis_output_path))

    # Get the workflow id and metadata, write them to files
    workflow_id = get_analysis_workflow_id(
        analysis_output_path=args.analysis_output_path
    )
    get_metadata(
        cromwell_url=args.cromwell_url,
        workflow_id=workflow_id,
        http_requests=HttpRequests(),
        include_keys=args.include_keys,
    )


if __name__ == '__main__':
    main()
