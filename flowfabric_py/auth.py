# auth.py
import os
import threading
import webbrowser
import time
from threading import Lock

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

import base64
import hashlib
import secrets
import requests
from requests_oauthlib import OAuth2Session
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# token cache and lock
token_cache = None
token_lock = Lock()

# helper function
# only returns True if both the expiration time & token exist & are not expired
def token_is_valid(token):
    if not token:
        return False

    expires_at = token.get("expires_at")
    if not expires_at:
        return False

    return expires_at > time.time()

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
    global token_cache
    # dumps the cached token - probably a safer way to do this
    if force_refresh:
        token_cache = None

    # if token exists and is not expired, return it
    with token_lock:
        if token_is_valid(token_cache):
            return token_cache

    # otherwise generate a new token
    provider = requests.get(
        "https://cognito-idp.us-west-2.amazonaws.com/us-west-2_em0hAPqnS/.well-known/openid-configuration"
    )
    json = provider.json()

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
        json["authorization_endpoint"],
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
        json["token_endpoint"],
        code=OAuthCallbackHandler.auth_code,
        code_verifier=code_verifier,
        include_client_id=True,
    )

    provider.close()
    with token_lock:
        token_cache = token

    return token
