# flowfabric_http.py
import requests
from .auth import flowfabric_get_token

# GET request
def flowfabric_get(endpoint, token=None, verbose=False):
    base_url = "https://flowfabric-api.lynker-spatial.com"
    if token is None:
        # get a token
        token = flowfabric_get_token()['id_token']
        print("No token provided, using token from flowfabric_get_token()") if verbose else None
    try:
        headers = {
            "Authorization": f"Bearer {token.strip()}",
            "Accept": "application/json"
        }
        url = "".join([base_url, endpoint])
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json() if 'application/json' in response.headers.get('Content-Type', '') else response.text
    except requests.exceptions.Timeout:
        return {"Error": "Request timed out"}
    except requests.exceptions.RequestException as e:
        return{"Error": str(e)}

# POST request
def flowfabric_post(endpoint, body, token=None, verbose=False):
    base_url = "https://flowfabric-api.lynker-spatial.com"
    if token is None:
        # get a token
        token = flowfabric_get_token()['id_token']
        print("No token provided, using token from flowfabric_get_token()") if verbose else None
    try:
        headers = {
            "Authorization": f"Bearer {token.strip()}",
            "Accept": "application/json"
        }
        url = "".join([base_url, endpoint])
        response = requests.post(url, json=body, headers=headers, timeout=10)
        return response.json() if 'application/json' in response.headers.get('Content-Type', '') else response.text
    except requests.exceptions.Timeout:
        return{"Error": "Request timed out"}
    except requests.exceptions.RequestException as e:
        return{"Error": str(e)}
