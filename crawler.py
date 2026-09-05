"""Backward-compatible launcher for the collector.

Prefer the installed ``job-market-collect`` command; this file remains so links to
the original portfolio project do not break.
"""

from job_market.cli import main


if __name__ == "__main__":
    main()
