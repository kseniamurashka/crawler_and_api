"""Backward-compatible ASGI application export."""

from job_market.api import app, run


if __name__ == "__main__":
    run()
