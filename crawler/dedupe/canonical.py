"""
Exact-match deduplication using canonical URL and stable fingerprints.

Strategy:
1. Normalise URL (strip tracking params, lowercase scheme/host, sort query params).
2. Compute a stable SHA-256 fingerprint of (source_key, external_id or url, title).
3. Look up existing news_items by canonical_url — if found, merge/update.
"""

from __future__ import annotations

import hashlib
import uuid
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

_TRACKING_PARAMS = frozenset(
    [
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
        "ref", "referrer", "source", "fbclid", "gclid", "_ga",
    ]
)


def normalise_url(url: str) -> str:
    """
    Return a stable canonical URL by:
    - Lowercasing scheme and host
    - Removing fragment
    - Removing known tracking query params
    - Sorting remaining query params alphabetically
    """
    try:
        p = urlparse(url)
        # lowercase scheme + netloc
        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        # strip tracking params, sort the rest
        qs = parse_qs(p.query, keep_blank_values=False)
        clean_qs = {k: v for k, v in qs.items() if k.lower() not in _TRACKING_PARAMS}
        sorted_query = urlencode(sorted(clean_qs.items()), doseq=True)
        canonical = urlunparse((scheme, netloc, p.path, p.params, sorted_query, ""))
        return canonical
    except Exception:
        return url


def make_fingerprint(source_key: str, external_id: str | None, title: str) -> bytes:
    """
    Stable SHA-256 fingerprint for exact-match dedup within a source.

    Uses external_id if available (most reliable), otherwise falls back to title.
    """
    key = external_id or title
    payload = f"{source_key}|{key}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).digest()


def assign_cluster_id(item: dict, existing_cluster_id: uuid.UUID | None = None) -> uuid.UUID:
    """
    Return a cluster UUID for this item.
    If *existing_cluster_id* is provided (cross-source near-dup match), reuse it.
    Otherwise allocate a new UUID.
    """
    return existing_cluster_id or uuid.uuid4()
