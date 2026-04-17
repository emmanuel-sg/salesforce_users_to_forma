"""Verify Phase 6 retry behavior without calling Autodesk."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from provisioner.acc_import import post_users_import_batch

logger = logging.getLogger("verify_phase6")
logger.addHandler(logging.NullHandler())


def test_429_then_success() -> None:
    """Assert ``post_users_import_batch`` retries once after HTTP 429 then succeeds."""
    calls: list[int] = []

    def mock_post(*_a, **_k):
        """First response 429, second 200 to simulate rate limit then success."""
        calls.append(1)
        r = MagicMock()
        if len(calls) == 1:
            r.status_code = 429
            r.text = "retry"
        else:
            r.status_code = 200
            r.text = "ok"
        return r

    with patch("provisioner.acc_import.requests.post", mock_post):
        ok = post_users_import_batch(
            "proj-1",
            [{"email": "a@b.c", "products": [], "roleIds": ["r"], "companyId": "c"}],
            access_token="tok",
            refresh_access_token=None,
            max_retries=5,
            base_backoff_seconds=0.01,
            logger=logger,
        )
    assert ok, "expected success after 429"
    assert len(calls) == 2, calls


def test_400_no_retry() -> None:
    calls: list[int] = []

    def mock_post(*_a, **_k):
        """Always return 400 so the importer must not spin on retries."""
        calls.append(1)
        r = MagicMock()
        r.status_code = 400
        r.text = "validation"
        return r

    with patch("provisioner.acc_import.requests.post", mock_post):
        ok = post_users_import_batch(
            "proj-1",
            [{"email": "a@b.c", "products": [], "roleIds": ["r"], "companyId": "c"}],
            access_token="tok",
            refresh_access_token=None,
            max_retries=5,
            base_backoff_seconds=0.01,
            logger=logger,
        )
    assert not ok
    assert len(calls) == 1, calls


def main() -> int:
    """Run mocked import batch tests and print a one-line success marker."""
    test_429_then_success()
    test_400_no_retry()
    print("OK: Phase 6 import retry logic verified (mocked).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
