import logging

from pipeline_tools.http_requests import HttpRequests  # noqa


def get_file_by_uuid(file_id, dss_url, http_requests):
    """Retrieve a JSON file from the Human Cell Atlas data storage service by its id.
    Retry with exponentially increasing wait times between requests if there are any failures.

    Args:
        file_id (str): the id of the file to retrieve.
        dss_url (str): the url for the HCA data storage service, e.g. "https://dss.staging.data.humancellatlas.org/v1".
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        dict: dict representing the contents of the JSON file

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    url = '{dss_url}/files/{file_id}?replica=gcp'.format(
        dss_url=dss_url, file_id=file_id
    )
    logging.info('GET {0}'.format(url))
    response = http_requests.get(url)
    logging.info(response.status_code)
    logging.info(response.text)
    return response.json()


def get_manifest(bundle_uuid, bundle_version, dss_url, http_requests):
    """Retrieve manifest JSON file for a given bundle uuid and version.

    Retry with exponentially increasing wait times between requests if there are any failures.

    TODO: Reduce the number of lines of code by switching to use DSS Python API client.

    Instead of talking to the DSS API directly, using the DSS Python API can avoid a lot of potential issues,
    especially those related to the Checkout Service. A simple example of using the DSS Python client and the
    metadata-api to get the manifest would be:

    ```python
    from humancellatlas.data.metadata.helpers.dss import download_bundle_metadata, dss_client

    client = dss_client()
    version, manifest, metadata_files = download_bundle_metadata(client, 'gcp', bundle_uuid, directurls=True)
    ```

    Args:
        bundle_uuid (str): the uuid of the bundle
        bundle_version (str): the bundle version, e.g. "2017-10-23T17:50:26.894Z"
        dss_url (str): The url for the Human Cell Atlas data storage service,
        e.g. "https://dss.staging.data.humancellatlas.org/v1"
        http_requests (HttpRequests): the HttpRequests object to use

    Returns:
        dict: A dict representing the full bundle manifest, with `directurls` for each file.

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    url = '{dss_url}/bundles/{bundle_uuid}?version={bundle_version}&replica=gcp&directurls=true'.format(
        dss_url=dss_url, bundle_uuid=bundle_uuid, bundle_version=bundle_version
    )
    logging.info('GET {0}'.format(url))
    response = http_requests.get(url)
    logging.info(response.status_code)
    logging.info(response.text)
    manifest = response.json()
    return manifest


def get_auth_token(
    http_requests,
    url="https://danielvaughan.eu.auth0.com/oauth/token",
    client_id="Zdsog4nDAnhQ99yiKwMQWAPc2qUDlR99",
    client_secret="t-OAE-GQk_nZZtWn-QQezJxDsLXmU7VSzlAh9cKW5vb87i90qlXGTvVNAjfT9weF",
    audience="http://localhost:8080",
    grant_type="client_credentials",
):
    """Request and get the access token for a trusted client from Auth0.

    .. note::

        We have hard-coded some test credentials here temporarily, which do not give any special
        permissions in the ingest service.

    Args:
        http_requests (HttpRequests): the HttpRequests object to use
        url (str): the url to the Auth0 domain oauth endpoint.
        client_id (str): the value of the Client ID field of the Non Interactive Client of Auth0.
        client_secret (str): the value of the Client Secret field of the Non Interactive Client of Auth0.
        audience (str): the value of the Identifier field of the Auth0 Management API.
        grant_type (str): type of OAuth 2.0 flow you want to run. e.g. client_credentials

    Returns:
        auth_token (dict): A dict containing the JWT (JSON Web Token) and its expiry (24h by default),
            the scopes granted, and the token type.

    Raises:
        requests.HTTPError: for 4xx errors or 5xx errors beyond timeout
    """
    url = url
    headers = {"content-type": "application/json"}
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": audience,
        "grant_type": grant_type,
    }
    response = http_requests.post(url=url, headers=headers, json=payload)
    response.raise_for_status()
    auth_token = response.json()
    return auth_token


def make_auth_header(auth_token):
    """Make the authorization headers to communicate with endpoints which implement Auth0 authentication API.

    Args:
        auth_token (dict): a dict obtained from the Auth0 domain oauth endpoint, containing the signed JWT
            (JSON Web Token), its expiry, the scopes granted, and the token type.

    Returns:
        headers (dict): A dict representing the headers with necessary token information to talk to Auth0 authentication
            required endpoints.
    """
    token_type = auth_token['token_type']
    access_token = auth_token['access_token']

    headers = {
        "Content-type": "application/json",
        "Authorization": "{token_type} {access_token}".format(
            token_type=token_type, access_token=access_token
        ),
    }
    return headers
