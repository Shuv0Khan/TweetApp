"""Reads credentials from environment variables instead of hard-coding them.

Values come from the process environment. If python-dotenv is installed, a
`.env` file in the project root is loaded first (see `.env.example`). The
`.env` file is git-ignored, so real keys never get committed.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass


def get(name, default=None):
    """Return an optional setting, or `default` when it is not set."""
    return os.environ.get(name, default)


def require(name):
    """Return a required setting, failing with a clear message when it is missing."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set. Copy .env.example to .env and fill it in.")
    return value
