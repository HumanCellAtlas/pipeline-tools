import time
import json
import jwt


class DCPAuthClient(object):
    dev_deployments = ('dev', 'integration', 'test', 'staging')

    def __init__(self, path_to_json_key, trusted_google_project):
        self.path_to_json_key = path_to_json_key
        self.trusted_google_project = trusted_google_project

        # TODO: check the liveliness of the token and make it as singleton if applicable
        self.issue_time = None
        self.expire_time = None

    @property
    def audience(self):
        if not any(
            deployment in self.trusted_google_project
            for deployment in DCPAuthClient.dev_deployments
        ):
            return "https://data.humancellatlas.org/"
        return "https://dev.data.humancellatlas.org/"

    @property
    def token(self):
        credentials = DCPAuthClient._from_json(self.path_to_json_key)
        token = DCPAuthClient.get_service_jwt(
            service_credentials=credentials, audience=self.audience
        )
        return token

    @staticmethod
    def _from_json(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    @staticmethod
    def get_service_jwt(service_credentials, audience):
        iat = time.time()
        exp = iat + 3600
        payload = {
            'iss': service_credentials["client_email"],
            'sub': service_credentials["client_email"],
            'aud': audience,
            'iat': iat,
            'exp': exp,
            'https://auth.data.humancellatlas.org/email': service_credentials[
                "client_email"
            ],
            'https://auth.data.humancellatlas.org/group': 'hca',
            'scope': ["openid", "email", "offline_access"],
        }
        additional_headers = {'kid': service_credentials["private_key_id"]}
        signed_jwt = jwt.encode(
            payload,
            service_credentials["private_key"],
            headers=additional_headers,
            algorithm='RS256',
        ).decode()
        return signed_jwt

    def get_auth_header(self):
        return {'Authorization': 'Bearer {}'.format(self.token)}
