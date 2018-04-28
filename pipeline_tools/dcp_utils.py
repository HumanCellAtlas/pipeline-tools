import requests
import logging
import time


def get_file_by_uuid(file_id, dss_url):
    """Retrieve a file from the Human Cell Atlas data storage service by its id.

    :param str file_id: The id of the file to retrieve.
    :param str dss_url: The url for the HCA data storage service, e.g. "https://dss.staging.data.humancellatlas.org/v1".
    :return dict: file contents.
    """
    url = '{dss_url}/files/{file_id}?replica=gcp'.format(
        dss_url=dss_url, file_id=file_id)
    logging.info('GET {0}'.format(url))
    response = requests.get(url)
    logging.info(response.status_code)
    logging.info(response.text)
    return response.json()


def get_manifest(bundle_uuid, bundle_version, dss_url, timeout_seconds, retry_seconds):
    """Retrieve manifest.json file for a given bundle uuid and version.

    :param str bundle_uuid: Bundle unique id
    :param str bundle_version: Timestamp of bundle creation, e.g. "2017-10-23T17:50:26.894Z"
    :param str dss_url: The url for the Human Cell Atlas data storage service, e.g. "https://dss.staging.data.humancellatlas.org/v1"
    :param int timeout_seconds: Seconds before allowing the request to timeout
    :param int retry_seconds: Seconds between retrying the request to get the manifest file
    :return dict:
        ::

            {
                'name_to_meta': dict mapping <str file name>: <dict file metadata>,
                'url_to_name': dict mapping <str file url>: <str file name>
            }
    """
    url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
        dss_url=dss_url, bundle_uuid=bundle_uuid, bundle_version=bundle_version)
    start = time.time()
    current = start
    # Retry in a loop because of intermittent 5xx errors from dss
    while current - start < timeout_seconds:
        logging.info('GET {0}'.format(url))
        response = requests.get(url)
        logging.info(response.status_code)
        logging.info(response.text)
        if 200 <= response.status_code <= 299:
            break
        time.sleep(retry_seconds)
        current = time.time()
    manifest = response.json()

    return manifest


def get_manifest_file_dicts(manifest):
    bundle = manifest['bundle']
    name_to_meta = {}
    url_to_name = {}
    for f in bundle['files']:
        name_to_meta[f['name']] = f
        url_to_name[f['url']] = f['name']
    return {
        'name_to_meta': name_to_meta,
        'url_to_name': url_to_name
    }


def get_file_uuid(manifest_file_dicts, file_name):
    return manifest_file_dicts['name_to_meta'][file_name]['uuid']


def get_file_url(manifest_file_dicts, file_name):
    return manifest_file_dicts['name_to_meta'][file_name]['url']


def get_auth_token(url="https://danielvaughan.eu.auth0.com/oauth/token",
                   client_id="Zdsog4nDAnhQ99yiKwMQWAPc2qUDlR99",
                   client_secret="t-OAE-GQk_nZZtWn-QQezJxDsLXmU7VSzlAh9cKW5vb87i90qlXGTvVNAjfT9weF",
                   audience="http://localhost:8080",
                   grant_type="client_credentials"):
    """Request and get the access token for a trusted client from Auth0.

    .. note::

        The parameters and credentials here are meant to be hard coded, the authentication is purely for identifying a user it doesn't give any permissions.

    :param str url: The url to the Auth0 domain oauth endpoint.
    :param str client_id: The value of the Client ID field of the Non Interactive Client of Auth0.
    :param str client_secret: The value of the Client Secret field of the Non Interactive Client of Auth0.
    :param str audience: The value of the Identifier field of the Auth0 Management API.
    :param str grant_type: Denotes which OAuth 2.0 flow you want to run. e.g. client_credentials
    :return dict auth_token: JSON response of the signed JWT (JSON Web Token), with when it expires (24h by default),
     the scopes granted, and the token type.
    """
    url = url
    headers = {
        "content-type": "application/json"
    }
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": grant_type
    }
    response = requests.post(url=url, headers=headers, json=payload)
    response.raise_for_status()
    auth_token = response.json()
    return auth_token


def make_auth_header(auth_token):
    """Make the authorization headers to communicate with endpoints which implement Auth0 authentication API.

    :param dict auth_token: Obtained from the Auth0 domain oauth endpoint, a dictionary of the
     signed JWT (JSON Web Token), with when it expires (24h by default), the scopes granted, and the token type.
    :return dict headers: A header with necessary token information to talk to Auth0 authentication required endpoints.
    """
    token_type = auth_token['token_type']
    access_token = auth_token['access_token']

    headers = {
        "Content-type": "application/json",
        "Authorization": "{token_type} {access_token}".format(token_type=token_type, access_token=access_token)
    }
    return headers
