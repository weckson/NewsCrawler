"""
Near-duplicate detection using SimHash.

SimHash produces a 64-bit fingerprint of a text document such that
similar documents have fingerprints with low Hamming distance.

Threshold: Hamming distance <= 3 is treated as a near-duplicate
(empirically good for headline-level similarity; tune for your corpus).

Window: Only compare items published within WINDOW_HOURS of each other
to bound the search space.
"""

from __future__ import annotations

import hashlib
import re
import struct
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

HAMMING_THRESHOLD = 6
WINDOW_HOURS = 6
SIMHASH_BITS = 64

# In-memory store: (simhash_int, cluster_id, published_utc)
# For production, push to Redis or DB index.
_index: list[tuple[int, uuid.UUID, datetime]] = []


def _tokenise(text: str) -> list[str]:
    """Simple word-level tokeniser with stop-word removal."""
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    stop = {
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
        "for", "of", "with", "is", "are", "was", "were", "it", "its",
        "this", "that", "from", "by", "be", "has", "have", "had",
    }
    return [t for t in tokens if t not in stop and len(t) > 1]


def _hash_token(token: str) -> int:
    """64-bit FNV-1a hash of a token."""
    digest = hashlib.md5(token.encode()).digest()[:8]
    return struct.unpack("<Q", digest)[0]


def compute(text: str) -> int:
    """Compute 64-bit SimHash of *text*."""
    tokens = _tokenise(text)
    if not tokens:
        return 0

    v = [0] * SIMHASH_BITS
    for token in tokens:
        h = _hash_token(token)
        for i in range(SIMHASH_BITS):
            bit = (h >> i) & 1
            v[i] += 1 if bit else -1

    result = 0
    for i in range(SIMHASH_BITS):
        if v[i] > 0:
            result |= 1 << i
    return result


def hamming_distance(a: int, b: int) -> int:
    """Count differing bits between two 64-bit integers."""
    x = a ^ b
    count = 0
    while x:
        count += x & 1
        x >>= 1
    return count


def find_cluster(
    simhash: int,
    published_utc: Optional[datetime],
    threshold: int = HAMMING_THRESHOLD,
    window_hours: int = WINDOW_HOURS,
) -> Optional[uuid.UUID]:
    """
    Search the in-memory index for a near-duplicate within the time window.
    Returns the cluster_id of the nearest match, or None.
    """
    now = published_utc or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=window_hours)

    best_dist = threshold + 1
    best_cluster: Optional[uuid.UUID] = None

    for stored_hash, cluster_id, stored_time in reversed(_index):
        if stored_time < cutoff:
            break  # index is newest-first; older entries are irrelevant
        dist = hamming_distance(simhash, stored_hash)
        if dist <= threshold and dist < best_dist:
            best_dist = dist
            best_cluster = cluster_id

    return best_cluster


def register(simhash: int, cluster_id: uuid.UUID, published_utc: Optional[datetime]) -> None:
    """Add an item to the in-memory near-dup index."""
    ts = published_utc or datetime.now(timezone.utc)
    _index.append((simhash, cluster_id, ts))
    # Prune entries older than 2x window
    cutoff = datetime.now(timezone.utc) - timedelta(hours=WINDOW_HOURS * 2)
    while _index and _index[0][2] < cutoff:
        _index.pop(0)


def dedupe_item(item: dict) -> dict:
    """
    Compute SimHash for *item* title+summary, find or create cluster, and
    attach dedupe fields to the item dict.

    Modifies item in-place and returns it.
    """
    text = " ".join(filter(None, [item.get("title"), item.get("summary")]))
    sh = compute(text)
    published = item.get("published_time_utc")

    existing_cluster = find_cluster(sh, published)
    if existing_cluster:
        cluster_id = existing_cluster
        score = 1.0 - hamming_distance(sh, _get_centroid_hash(existing_cluster)) / SIMHASH_BITS
        log.debug("near_dup_found", cluster_id=str(cluster_id), score=score)
    else:
        from crawler.dedupe.canonical import assign_cluster_id
        cluster_id = assign_cluster_id(item)
        score = 1.0

    register(sh, cluster_id, published)

    item["dedupe_cluster_id"] = cluster_id
    item["dedupe_score"] = score
    return item


def _get_centroid_hash(cluster_id: uuid.UUID) -> int:
    """Return the first hash registered for a cluster (used as centroid proxy)."""
    for stored_hash, cid, _ in _index:
        if cid == cluster_id:
            return stored_hash
    return 0
