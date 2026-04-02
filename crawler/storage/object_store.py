"""
Object storage backend for raw payloads.

Supports:
- Local filesystem (dev)  — when OBJECT_STORE_BUCKET starts with '.' or '/'
- S3-compatible (prod)    — any other value treated as bucket name
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

_BUCKET = os.environ.get("OBJECT_STORE_BUCKET", "./data/raw")
_USE_S3 = not (_BUCKET.startswith(".") or _BUCKET.startswith("/"))


def _s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _local_path(key: str) -> Path:
    p = Path(_BUCKET) / key
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def put_raw(payload: bytes, source_key: str, fmt: str) -> tuple[str, bytes]:
    """
    Store *payload* in object storage.

    Returns:
        (object_key, sha256_digest)
    """
    digest = hashlib.sha256(payload).digest()
    hex_digest = digest.hex()
    object_key = f"{source_key}/{hex_digest[:2]}/{hex_digest}.{fmt}"

    if _USE_S3:
        client = _s3_client()
        client.put_object(
            Bucket=_BUCKET,
            Key=object_key,
            Body=payload,
            ContentType=_content_type(fmt),
            ChecksumSHA256=hex_digest,
        )
        log.debug("s3_put", bucket=_BUCKET, key=object_key)
    else:
        path = _local_path(object_key)
        path.write_bytes(payload)
        log.debug("local_put", path=str(path))

    return object_key, digest


def get_raw(object_key: str) -> Optional[bytes]:
    """Retrieve payload from object storage. Returns None if not found."""
    if _USE_S3:
        client = _s3_client()
        try:
            resp = client.get_object(Bucket=_BUCKET, Key=object_key)
            return resp["Body"].read()
        except client.exceptions.NoSuchKey:
            return None
    else:
        path = _local_path(object_key)
        return path.read_bytes() if path.exists() else None


def _content_type(fmt: str) -> str:
    return {
        "json": "application/json",
        "rss": "application/rss+xml",
        "atom": "application/atom+xml",
        "html": "text/html",
        "pdf": "application/pdf",
    }.get(fmt, "application/octet-stream")
