"""
Minimal HTML parser for web sources (ASX, ASIC, etc.).

Extracts headline, published date, and canonical link from simple
press-release pages. Full-body extraction is intentionally limited —
only extract body if source policy permits (store_full_text: true).

Does NOT attempt to bypass paywalls or access controls.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

import structlog

log = structlog.get_logger(__name__)

_TRACKING_PARAMS = frozenset(
    ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "ref"]
)


def parse(
    raw_bytes: bytes,
    url: str,
    source_key: str,
    store_body: bool = False,
) -> dict | None:
    """
    Extract a single news item from an HTML page.

    Returns None if the page doesn't look like a news article.
    """
    try:
        from html.parser import HTMLParser
    except ImportError:
        log.error("html_parser_import_failed")
        return None

    text = raw_bytes.decode("utf-8", errors="replace")

    title = _extract_og_or_title(text)
    if not title:
        return None

    summary = _extract_meta_description(text)
    body = _extract_article_text(text) if store_body else None
    published = _extract_published_date(text)
    canonical = _extract_canonical(text, url) or _canonical_url(url)

    fp_input = f"{source_key}|{canonical}|{title}".encode()
    fingerprint = hashlib.sha256(fp_input).digest()

    return {
        "canonical_source": source_key,
        "canonical_url": canonical,
        "canonical_external_id": None,
        "title": title,
        "summary": summary,
        "body": body,
        "language": "en",
        "published_time_utc": published,
        "updated_time_utc": None,
        "authors": None,
        "source_attribution": {"source_key": source_key, "scraped_url": url},
        "content_type": _infer_content_type(source_key, title),
        "tickers": [],
        "topics": [],
        "fingerprint_v1": fingerprint,
        "provenance_entry": {"raw_source": source_key, "url": url},
    }


# ── Simple regex-based extractors (no heavy deps) ─────────────────────────────

def _extract_og_or_title(html: str) -> Optional[str]:
    # og:title
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html, re.I)
    if m:
        return _clean(m.group(1))
    # <title>
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if m:
        return _clean(m.group(1))
    return None


def _extract_meta_description(html: str) -> Optional[str]:
    m = re.search(
        r'<meta[^>]+(?:name=["\']description["\']|property=["\']og:description["\'])[^>]+content=["\'](.*?)["\']',
        html, re.I
    )
    return _clean(m.group(1)) if m else None


def _extract_canonical(html: str, base_url: str) -> Optional[str]:
    m = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\'](.*?)["\']', html, re.I)
    if m:
        href = m.group(1).strip()
        if href.startswith("http"):
            return _canonical_url(href)
        return _canonical_url(urljoin(base_url, href))
    return None


def _extract_published_date(html: str) -> Optional[datetime]:
    # Try JSON-LD datePublished
    m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html)
    if m:
        return _parse_iso(m.group(1))
    # Try <time datetime="...">
    m = re.search(r'<time[^>]+datetime=["\']([\d\-T:+Z]+)["\']', html, re.I)
    if m:
        return _parse_iso(m.group(1))
    # Try meta article:published_time
    m = re.search(
        r'<meta[^>]+property=["\']article:published_time["\'][^>]+content=["\'](.*?)["\']',
        html, re.I
    )
    if m:
        return _parse_iso(m.group(1))
    return None


def _extract_article_text(html: str) -> Optional[str]:
    """Very simple article body extraction — strip tags from <article> if present."""
    m = re.search(r"<article[^>]*>(.*?)</article>", html, re.I | re.S)
    if m:
        return _clean(re.sub(r"<[^>]+>", " ", m.group(1)))
    # Fallback: strip all tags from body
    m = re.search(r"<body[^>]*>(.*?)</body>", html, re.I | re.S)
    if m:
        text = _clean(re.sub(r"<[^>]+>", " ", m.group(1)))
        # Limit to 5000 chars to avoid storing whole pages
        return text[:5000] if text else None
    return None


def _clean(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _canonical_url(url: str) -> str:
    parsed = urlparse(url)
    qs = {k: v for k, v in parse_qs(parsed.query).items() if k not in _TRACKING_PARAMS}
    clean = parsed._replace(query=urlencode(qs, doseq=True), fragment="")
    return urlunparse(clean)


def _parse_iso(value: str) -> Optional[datetime]:
    try:
        from dateutil import parser as dp
        dt = dp.parse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _infer_content_type(source_key: str, title: str) -> str:
    lower = (source_key + " " + title).lower()
    if any(w in lower for w in ("asic", "enforcement", "banning", "licence", "penalty")):
        return "regulatory"
    if any(w in lower for w in ("asx", "announcement")):
        return "press_release"
    return "news"
