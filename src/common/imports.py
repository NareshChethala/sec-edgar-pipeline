"""
code/common/imports.py

Centralized imports + dependency checks for the SEC pipeline.
Keep shared imports here so all pipeline steps stay consistent.
"""

# Standard libs used across pipeline scripts
import argparse
import time
from pathlib import Path

# Core dependency (required for all modes)
import requests


def require_gcs() -> None:
    """
    Fail fast if GCS dependencies aren't installed.
    """
    try:
        from google.cloud import storage  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Missing dependency for GCS.\n"
            "Install with: pip install google-cloud-storage"
        ) from e


def require_s3() -> None:
    """
    Fail fast if S3 dependencies aren't installed.
    """
    try:
        import boto3  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "Missing dependency for AWS S3.\n"
            "Install with: pip install boto3"
        ) from e
