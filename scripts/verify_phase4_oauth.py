"""
Verify Phase 4 behavior without opening a browser:
- Expired access token + refresh fails -> ensure_access_token uses relogin callback and saves new token.
"""
from __future__ import annotations

import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from provisioner.config import HubConfig
from provisioner.oauth_aps import TokenBundle, ensure_access_token, load_tokens, save_tokens


def main() -> int:
    """Exercise ``ensure_access_token`` refresh/relogin paths with mocked token storage (no browser)."""
    hub = HubConfig(
        key="testhub",
        name=None,
        hub_id="hub-id",
        client_id="client-id",
        client_secret="client-secret",
    )

    with tempfile.TemporaryDirectory() as td:
        token_file = Path(td) / "testhub.json"

        def token_path(_key: str) -> Path:
            """Redirect hub token reads/writes to a temporary file for this test run."""
            return token_file

        with patch("provisioner.oauth_aps.token_path_for_hub", token_path):
            # Fresh token -> no refresh
            save_tokens(
                "testhub",
                TokenBundle(access_token="a1", refresh_token=None, expires_at=time.time() + 3600),
            )
            b = ensure_access_token("testhub", hub, on_relogin=lambda: TokenBundle("x", None, time.time() + 10))
            assert b.access_token == "a1", b

            # Expired, no refresh -> relogin
            save_tokens(
                "testhub",
                TokenBundle(access_token="old", refresh_token=None, expires_at=time.time() - 10),
            )
            b2 = ensure_access_token(
                "testhub",
                hub,
                on_relogin=lambda: TokenBundle("new1", "r1", time.time() + 3600),
            )
            assert b2.access_token == "new1", b2

            # Expired + refresh fails -> relogin
            save_tokens(
                "testhub",
                TokenBundle(access_token="old", refresh_token="r0", expires_at=time.time() - 10),
            )

            def bad_refresh(*_a, **_k):
                """Force the refresh path to fail so ``ensure_access_token`` falls back to relogin."""
                raise RuntimeError("refresh failed")

            def good_relogin():
                """Supply a fresh bundle when interactive relogin is invoked."""
                return TokenBundle("new2", "r2", time.time() + 3600)

            with patch("provisioner.oauth_aps.refresh_access_token", bad_refresh):
                b3 = ensure_access_token("testhub", hub, on_relogin=good_relogin)
            assert b3.access_token == "new2", b3
            loaded = load_tokens("testhub")
            assert loaded is not None and loaded.access_token == "new2", loaded

    print("OK: Phase 4 ensure_access_token paths verified (no browser).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
