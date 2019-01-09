import time
import jwt


def get_service_jwt(service_credentials):
    print(service_credentials["client_email"])
    iat = time.time()
    exp = iat + 3600
    payload = {
        'iss': service_credentials["client_email"],
        'sub': service_credentials["client_email"],
        'aud': 'https://dev.data.humancellatlas.org/',
        'iat': iat,
        'exp': exp,
        'https://auth.data.humancellatlas.org/email': service_credentials["client_email"],
        'https://auth.data.humancellatlas.org/group': 'hca',
        'scope': ["openid", "email", "offline_access"]
    }
    additional_headers = {'kid': service_credentials["private_key_id"]}
    signed_jwt = jwt.encode(payload, service_credentials["private_key"], headers=additional_headers,
                            algorithm='RS256').decode()
    return signed_jwt


def get_auth_header(signed_jwt):
    return {'Authorization': 'Bearer {}'.format(signed_jwt)}
