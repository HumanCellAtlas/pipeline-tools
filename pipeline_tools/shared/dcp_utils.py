from pipeline_tools.shared.http_requests import HttpRequests  # noqa


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
