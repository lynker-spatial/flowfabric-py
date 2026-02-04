# auth.py
import json
import os
import threading
import webbrowser
import time
from threading import Lock
from pathlib import Path

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
token_file = Path.home() / ".flowfabric_token.json"

import base64
import hashlib
import secrets
import requests
from requests_oauthlib import OAuth2Session
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# token lock
token_lock = Lock()

# helper function to load token
def load_token():
    if not token_file.exists():
        return None
    try:
        with token_file.open("r") as f:
            return json.load(f)
    except Exception:
        return None

# helper function to save the token
def save_token(token):
    token_file.parent.mkdir(parents=True, exist_ok=True)
    with token_file.open("w") as f:
        json.dump(token, f)

# helper function
# only returns True if both the expiration time & token exist & are not expired
def token_is_valid(token):
    if not token:
        return False

    expires_at = token.get("expires_at")
    if not expires_at:
        return False

    return expires_at - 60 > time.time() # extra time as a buffer

# handle the OAuth callback
class OAuthCallbackHandler(BaseHTTPRequestHandler):
    auth_code = None
    auth_state = None

    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)
        OAuthCallbackHandler.auth_code = query.get("code", [None])[0]
        OAuthCallbackHandler.auth_state = query.get("state", [None])[0]

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Authentication complete. You may close this window.")

        threading.Thread(target=self.server.shutdown).start()

# login and get a token
def flowfabric_get_token(force_refresh=False):
    # dumps the cached token
    if force_refresh:
        token_file.unlink(missing_ok=True)

    # return current token if it is still valid
    token = load_token()
    if token_is_valid(token):
        return token

    # otherwise generate a new token
    with token_lock:
        provider = requests.get(
            "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_em0hAPqnS/.well-known/openid-configuration"
        )
        json_prov = provider.json()

        client_id = "1he6ti5109b9t6r1ifd4brecpl"
        redirect_uri = "http://localhost:57777"
        scope = ["openid", "profile", "email", "phone"]

        # PKCE
        code_verifier = secrets.token_urlsafe(64)
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode()).digest()
        ).decode().rstrip("=")

        oauth = OAuth2Session(
            client_id=client_id,
            redirect_uri=redirect_uri,
            scope=scope,
        )

        authorization_url, state = oauth.authorization_url(
            json_prov["authorization_endpoint"],
            code_challenge=code_challenge,
            code_challenge_method="S256",
        )

        # Start callback server
        server = HTTPServer(("localhost", 57777), OAuthCallbackHandler)
        thread = threading.Thread(target=server.serve_forever)
        thread.daemon = True
        thread.start()

        webbrowser.open(authorization_url)

        timeout = time.time() + 60
        # Wait for redirect
        while OAuthCallbackHandler.auth_code is None:
            if time.time() > timeout:
                server.shutdown()
                thread.join()
                server.server_close()
            time.sleep(0.1)

        server.shutdown()
        thread.join()
        server.server_close()

        token = oauth.fetch_token(
            json_prov["token_endpoint"],
            code=OAuthCallbackHandler.auth_code,
            code_verifier=code_verifier,
            include_client_id=True,
        )

        provider.close()
        save_token(token)
        return token

# refresh token
def flowfabric_refresh_token():
    flowfabric_get_token(force_refresh=True)
