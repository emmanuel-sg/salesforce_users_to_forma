"""Entry point when running ``python -m provisioner``; delegates to :func:`provisioner.cli.main`."""

from __future__ import annotations  # Store type annotations as strings (postponed evaluation) for forward refs and lower import-time typing overhead.

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
