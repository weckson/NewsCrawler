"""
RSS / Atom feed parser.

Handles PR Newswire, Business Wire, Company IR, ASIC, and any standard
RSS 2.0 / Atom 1.0 feed.

Output: list of normalised item dicts ready for dedupe + storage.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs

import feedparser
import structlog

log = structlog.get_logger(__name__)

_TRACKING_PARAMS = frozenset(
    [
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
        "ref", "referrer", "source",
    ]
)


def parse(raw_bytes: bytes, source_key: str, feed_url: str, store_body: bool = False) -> list[dict]:
    """
    Parse RSS/Atom bytes into a list of normalised item dicts.

    Args:
        raw_bytes:   raw feed payload
        source_key:  e.g. 'prnewswire_rss_financial'
        feed_url:    original request URL (for provenance)
        store_body:  if False, body is set to None regardless of feed content
    """
    text = raw_bytes.decode("utf-8", errors="replace")
    feed = feedparser.parse(text)

    if feed.bozo and not feed.entries:
        log.warning("rss_parse_error", source_key=source_key, error=str(feed.bozo_exception))
        return []

    items = []
    for entry in feed.entries:
        try:
            item = _normalise_entry(entry, source_key, feed_url, store_body)
            if item:
                items.append(item)
        except Exception as exc:
            log.warning("rss_entry_error", source_key=source_key, error=str(exc))

    log.info("rss_parsed", source_key=source_key, count=len(items))
    return items


def _normalise_entry(
    entry: feedparser.FeedParserDict,
    source_key: str,
    feed_url: str,
    store_body: bool,
) -> dict | None:
    link = _canonical_url(getattr(entry, "link", None))
    guid = getattr(entry, "id", None) or link
    if not link and not guid:
        return None

    title = _clean_text(getattr(entry, "title", None))
    if not title:
        return None

    summary = _clean_text(getattr(entry, "summary", None))
    body = None
    if store_body:
        content_list = getattr(entry, "content", [])
        if content_list:
            body = _clean_text(content_list[0].get("value"))

    published = _parse_dt(getattr(entry, "published_parsed", None))
    updated = _parse_dt(getattr(entry, "updated_parsed", None))

    authors = []
    if hasattr(entry, "author"):
        authors = [entry.author]
    elif hasattr(entry, "authors"):
        authors = [a.get("name", "") for a in entry.authors if a.get("name")]

    # Fingerprint: stable hash of (source_key, guid or link, title)
    fp_input = f"{source_key}|{guid or link}|{title}".encode()
    fingerprint = hashlib.sha256(fp_input).digest()

    return {
        "canonical_source": source_key,
        "canonical_url": link or guid,
        "canonical_external_id": guid,
        "title": title,
        "summary": summary,
        "body": body,
        "language": "en",
        "published_time_utc": published,
        "updated_time_utc": updated,
        "authors": authors or None,
        "source_attribution": {"feed_url": feed_url, "source_key": source_key},
        "content_type": _infer_content_type(source_key, title),
        "tickers": [],
        "topics": [],
        "fingerprint_v1": fingerprint,
        "provenance_entry": {"raw_source": source_key, "feed_url": feed_url},
    }


def _canonical_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url)
    qs = {k: v for k, v in parse_qs(parsed.query).items() if k not in _TRACKING_PARAMS}
    clean = parsed._replace(query=urlencode(qs, doseq=True), fragment="")
    return urlunparse(clean)


def _clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _parse_dt(parsed_time) -> Optional[datetime]:
    if not parsed_time:
        return None
    import time as _time
    try:
        ts = _time.mktime(parsed_time)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def _infer_content_type(source_key: str, title: str) -> str:
    lower = (source_key + title).lower()
    if any(w in lower for w in ("press release", "prnewswire", "businesswire", "pr newswire")):
        return "press_release"
    if any(w in lower for w in ("asic", "regulator", "enforcement", "banning", "warning")):
        return "regulatory"
    return "news"
