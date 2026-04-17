"""APS OAuth 2.0: PKCE browser login, refresh tokens, client credentials, and token file cache."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import threading
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from .config import HubConfig

AUTHORIZE_URL = "https://developer.api.autodesk.com/authentication/v2/authorize"
TOKEN_URL = "https://developer.api.autodesk.com/authentication/v2/token"

DEFAULT_REDIRECT_URI = "http://127.0.0.1:8089/callback"
DEFAULT_SCOPES = "data:read data:write account:read account:write"


def oauth_redirect_uri() -> str:
    """Return the registered callback URL for 3-legged OAuth (must match the APS app)."""
    return os.getenv("APS_REDIRECT_URI", DEFAULT_REDIRECT_URI).strip()


def oauth_scopes() -> str:
    """Return space-separated OAuth scopes from env or :data:`DEFAULT_SCOPES`."""
    return os.getenv("APS_SCOPES", DEFAULT_SCOPES).strip()


def oauth_use_client_credentials() -> bool:
    """
    Two-legged OAuth: client_id + client_secret, no browser.
    Set APS_AUTH_MODE=client_credentials (or two_legged, or APS_USE_CLIENT_CREDENTIALS=1).
    """
    flag = (os.getenv("APS_USE_CLIENT_CREDENTIALS") or "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        return True
    mode = (os.getenv("APS_AUTH_MODE") or "").strip().lower()
    return mode in (
        "client_credentials",
        "client-credentials",
        "two_legged",
        "two-legged",
        "2legged",
        "2-legged",
    )


def token_path_for_hub(hub_key: str) -> Path:
    """Path to the JSON file storing access/refresh tokens for a hub key."""
    d = Path("data") / "tokens"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{hub_key}.json"


@dataclass
class TokenBundle:
    """In-memory OAuth token set with optional refresh token and wall-clock expiry."""

    access_token: str
    refresh_token: str | None
    expires_at: float  # unix timestamp (wall clock)

    def is_expired(self, *, skew_seconds: float = 120) -> bool:
        """True if current time is past expiry minus a small skew (default 120s)."""
        return time.time() >= (self.expires_at - skew_seconds)


def _b64url(data: bytes) -> str:
    """URL-safe base64 without padding (PKCE challenge)."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _pkce_pair() -> tuple[str, str]:
    """Generate PKCE verifier and S256 challenge for the authorize request."""
    verifier = secrets.token_urlsafe(64)
    challenge = _b64url(hashlib.sha256(verifier.encode("utf-8")).digest())
    return verifier, challenge


def load_tokens(hub_key: str) -> TokenBundle | None:
    """Load a :class:`TokenBundle` from disk, or ``None`` if missing or invalid."""
    path = token_path_for_hub(hub_key)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        access = raw.get("access_token")
        refresh = raw.get("refresh_token")
        expires_at = raw.get("expires_at")
        if not isinstance(access, str) or not access:
            return None
        if not isinstance(expires_at, (int, float)):
            return None
        return TokenBundle(
            access_token=access,
            refresh_token=str(refresh) if isinstance(refresh, str) and refresh else None,
            expires_at=float(expires_at),
        )
    except Exception:  # noqa: BLE001
        return None


def save_tokens(hub_key: str, bundle: TokenBundle) -> None:
    """Persist access token, refresh token, and expiry to the hub’s token JSON file."""
    path = token_path_for_hub(hub_key)
    path.write_text(
        json.dumps(
            {
                "access_token": bundle.access_token,
                "refresh_token": bundle.refresh_token,
                "expires_at": bundle.expires_at,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _parse_token_response(data: dict) -> TokenBundle:
    """Map APS token JSON to :class:`TokenBundle` (computes ``expires_at`` from ``expires_in``)."""
    access = data.get("access_token")
    if not isinstance(access, str) or not access:
        raise ValueError("Token response missing access_token")
    refresh = data.get("refresh_token")
    refresh_s = str(refresh) if isinstance(refresh, str) and refresh else None
    expires_in = data.get("expires_in")
    if not isinstance(expires_in, (int, float)):
        expires_in = 3600
    expires_at = time.time() + float(expires_in)
    return TokenBundle(access_token=access, refresh_token=refresh_s, expires_at=expires_at)


def exchange_authorization_code(
    *,
    code: str,
    redirect_uri: str,
    hub: HubConfig,
    code_verifier: str,
) -> TokenBundle:
    """Exchange authorization ``code`` + PKCE verifier for tokens (3-legged completion)."""
    if not hub.client_id:
        raise ValueError("Hub missing CLIENT_ID")
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": hub.client_id,
        "code_verifier": code_verifier,
    }
    if hub.client_secret:
        payload["client_secret"] = hub.client_secret
    resp = requests.post(TOKEN_URL, data=payload, timeout=60)
    resp.raise_for_status()
    return _parse_token_response(resp.json())


def fetch_client_credentials_token(*, hub: HubConfig) -> TokenBundle:
    """
    Two-legged token (app-only). Requires client_secret. No refresh_token; obtain a new token when expired.
    """
    if not hub.client_id:
        raise ValueError("Hub missing CLIENT_ID (set HUB_<key>_CLIENT_ID)")
    if not hub.client_secret:
        raise ValueError(
            "Client credentials flow requires HUB_<key>_CLIENT_SECRET (confidential app)"
        )
    payload = {
        "grant_type": "client_credentials",
        "client_id": hub.client_id,
        "client_secret": hub.client_secret,
        "scope": oauth_scopes(),
    }
    resp = requests.post(TOKEN_URL, data=payload, timeout=60)
    resp.raise_for_status()
    return _parse_token_response(resp.json())


def refresh_access_token(*, hub: HubConfig, refresh_token: str) -> TokenBundle:
    """Obtain a new access token using a refresh token; retain prior refresh if omitted in response."""
    if not hub.client_id:
        raise ValueError("Hub missing CLIENT_ID")
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": hub.client_id,
    }
    if hub.client_secret:
        payload["client_secret"] = hub.client_secret
    resp = requests.post(TOKEN_URL, data=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    bundle = _parse_token_response(data)
    # APS may omit refresh_token on refresh; keep the old one
    if bundle.refresh_token is None:
        bundle = TokenBundle(
            access_token=bundle.access_token,
            refresh_token=refresh_token,
            expires_at=bundle.expires_at,
        )
    return bundle


def build_authorize_url(*, hub: HubConfig, redirect_uri: str, state: str, code_challenge: str) -> str:
    """Build the browser URL for the OAuth2 authorize endpoint with PKCE S256."""
    if not hub.client_id:
        raise ValueError("Hub missing CLIENT_ID")
    q = {
        "response_type": "code",
        "client_id": hub.client_id,
        "redirect_uri": redirect_uri,
        "scope": oauth_scopes(),
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{AUTHORIZE_URL}?{urlencode(q)}"


def interactive_browser_login(hub: HubConfig) -> TokenBundle:
    """
    Open system browser for 3-legged login; local HTTP server receives ?code=.
    """
    if not hub.client_id:
        raise ValueError("Hub missing CLIENT_ID (set HUB_<key>_CLIENT_ID)")

    redirect_uri = oauth_redirect_uri()
    parsed = urlparse(redirect_uri)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError(f"Invalid APS_REDIRECT_URI: {redirect_uri}")
    port = parsed.port
    if port is None:
        port = 80 if parsed.scheme == "http" else 443
    if parsed.scheme == "https":
        raise ValueError("Local callback must use http://127.0.0.1 for this CLI (use http, not https).")

    state = secrets.token_urlsafe(32)
    code_verifier, code_challenge = _pkce_pair()

    result: dict[str, str | None] = {"code": None, "error": None, "state": None}

    class Handler(BaseHTTPRequestHandler):
        """Minimal HTTP handler to capture ``code`` / ``error`` on the OAuth redirect."""

        def do_GET(self) -> None:  # noqa: N802
            """Parse query string for OAuth result and respond with a short HTML status page."""
            if self.path.startswith("/favicon"):
                self.send_response(404)
                self.end_headers()
                return
            qs = urlparse(self.path)
            params = parse_qs(qs.query)
            result["code"] = (params.get("code") or [None])[0]
            result["error"] = (params.get("error") or [None])[0]
            result["state"] = (params.get("state") or [None])[0]
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            if result.get("error"):
                body = "<h1>Authorization failed</h1><p>You can close this window.</p>"
            else:
                body = "<h1>Success</h1><p>You can close this window and return to the terminal.</p>"
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            """Silence default stderr request logging from :class:`BaseHTTPRequestHandler`."""
            return

    server = HTTPServer((parsed.hostname or "127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        url = build_authorize_url(
            hub=hub,
            redirect_uri=redirect_uri,
            state=state,
            code_challenge=code_challenge,
        )
        webbrowser.open(url)

        deadline = time.time() + 300
        while time.time() < deadline:
            if result["code"] or result["error"]:
                break
            time.sleep(0.1)

        if result["error"]:
            raise RuntimeError(f"OAuth error: {result['error']}")
        if not result["code"]:
            raise TimeoutError("No authorization response within 5 minutes (timeout).")
        if result["state"] != state:
            raise RuntimeError("OAuth state mismatch (possible CSRF).")

        bundle = exchange_authorization_code(
            code=str(result["code"]),
            redirect_uri=redirect_uri,
            hub=hub,
            code_verifier=code_verifier,
        )
        return bundle
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def access_token_after_401(hub_key: str, hub: HubConfig) -> str:
    """
    Call from Construction Admin (etc.) 401 handlers. Two-legged always fetches a new token;
    three-legged uses normal refresh / relogin without forcing a new client_credentials call
    when the cached token is still within expiry.
    """
    return ensure_access_token(
        hub_key, hub, force_refresh=oauth_use_client_credentials()
    ).access_token


def ensure_access_token(
    hub_key: str,
    hub: HubConfig,
    *,
    on_relogin: Callable[[], TokenBundle] | None = None,
    force_refresh: bool = False,
) -> TokenBundle:
    """
    Return a valid access token.

    Three-legged: use cache, refresh_token, or browser login (on_relogin if set).
    Two-legged (APS_AUTH_MODE=client_credentials): client_credentials grant; optional file cache.
    When force_refresh is True and two-legged is enabled, always request a new token.
    """
    if oauth_use_client_credentials():
        if not force_refresh:
            bundle = load_tokens(hub_key)
            # Ignore token files from an earlier 3-legged login (they carry refresh_token).
            if (
                bundle
                and not bundle.refresh_token
                and not bundle.is_expired()
            ):
                return bundle
        b = fetch_client_credentials_token(hub=hub)
        save_tokens(hub_key, b)
        return b

    relogin = on_relogin or (lambda: interactive_browser_login(hub))

    bundle = load_tokens(hub_key)
    if bundle is None:
        b = relogin()
        save_tokens(hub_key, b)
        return b

    if not bundle.is_expired():
        return bundle

    if not bundle.refresh_token:
        b = relogin()
        save_tokens(hub_key, b)
        return b

    try:
        new_bundle = refresh_access_token(hub=hub, refresh_token=bundle.refresh_token)
        save_tokens(hub_key, new_bundle)
        return new_bundle
    except Exception:  # noqa: BLE001
        b = relogin()
        save_tokens(hub_key, b)
        return b
