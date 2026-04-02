"""
Benzinga News API parser (REST JSON).

Handles:
- GET /api/v2/news  (Newsfeed)
- GET /api/v2/press-releases

Reference: https://docs.benzinga.com/api-reference/news-api/get-news-items

Authentication: Authorization: token <API_KEY>  (header-based)
Delta pulls:    updatedSince  (Unix timestamp)
Page size:      pageSize <= 100 (as documented)

This module is pure parsing — no HTTP; the fetcher.py handles that.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

log = structlog.get_logger(__name__)

BENZINGA_BASE_URL = "https://api.benzinga.com/api/v2"
_MAX_PAGE_SIZE = 100


def build_news_url(
    page: int = 0,
    page_size: int = _MAX_PAGE_SIZE,
    updated_since: Optional[int] = None,
    tickers: Optional[list[str]] = None,
) -> str:
    """
    Construct Benzinga News API URL with delta params.

    Args:
        page:          0-based page index
        page_size:     max 100 (per docs)
        updated_since: Unix timestamp for delta pulls
        tickers:       optional ticker filter list
    """
    params: list[str] = [
        f"pageSize={min(page_size, _MAX_PAGE_SIZE)}",
        f"page={page}",
    ]
    if updated_since is not None:
        params.append(f"updatedSince={updated_since}")
    if tickers:
        params.append(f"tickers={','.join(tickers)}")
    return f"{BENZINGA_BASE_URL}/news?{'&'.join(params)}"


def build_press_release_url(page: int = 0, page_size: int = _MAX_PAGE_SIZE) -> str:
    return f"{BENZINGA_BASE_URL}/press-releases?pageSize={min(page_size, _MAX_PAGE_SIZE)}&page={page}"


def auth_headers() -> dict:
    """
    Return the Authorization header for Benzinga.
    Key is loaded from BENZINGA_API_KEY env var.

    IMPORTANT: Never log or store these headers; fetcher.py redacts them.
    """
    api_key = os.environ.get("BENZINGA_API_KEY", "")
    if not api_key:
        log.warning("benzinga_api_key_missing")
    return {
        "Authorization": f"token {api_key}",
        "Accept": "application/json",
    }


def parse_news_response(
    data: list[dict] | dict,
    source_key: str = "benzinga_news_api",
    store_body: bool = True,
) -> list[dict]:
    """
    Normalise Benzinga /news JSON response into item dicts.

    The API returns either a list directly or a dict with a 'news' key.
    """
    if isinstance(data, dict):
        items_raw = data.get("news", data.get("data", []))
    else:
        items_raw = data

    items = []
    for raw in items_raw:
        try:
            item = _normalise_news_item(raw, source_key, store_body)
            if item:
                items.append(item)
        except Exception as exc:
            log.warning("benzinga_item_error", error=str(exc), id=raw.get("id"))

    log.info("benzinga_parsed", source_key=source_key, count=len(items))
    return items


def parse_press_release_response(
    data: list[dict] | dict,
    source_key: str = "benzinga_press_releases",
    store_body: bool = True,
) -> list[dict]:
    if isinstance(data, dict):
        items_raw = data.get("press-releases", data.get("data", []))
    else:
        items_raw = data

    items = []
    for raw in items_raw:
        try:
            item = _normalise_news_item(raw, source_key, store_body, content_type="press_release")
            if item:
                items.append(item)
        except Exception as exc:
            log.warning("benzinga_pr_error", error=str(exc), id=raw.get("id"))

    return items


def _normalise_news_item(
    raw: dict,
    source_key: str,
    store_body: bool,
    content_type: str = "news",
) -> dict | None:
    item_id = str(raw.get("id", ""))
    url = raw.get("url") or raw.get("link") or raw.get("canonical_url") or ""
    if not url and not item_id:
        return None

    title = _clean(raw.get("title") or raw.get("headline") or "")
    if not title:
        return None

    summary = _clean(raw.get("teaser") or raw.get("summary") or "")
    body = _clean(raw.get("body") or "") if store_body else None

    published = _parse_benzinga_dt(raw.get("created") or raw.get("published"))
    updated = _parse_benzinga_dt(raw.get("updated"))

    # Tickers: Benzinga returns a 'stocks' list with 'name' fields
    tickers = [
        s["name"] for s in raw.get("stocks", []) if s.get("name")
    ]

    # Authors
    authors = []
    if raw.get("author"):
        authors = [raw["author"]]
    elif raw.get("authors"):
        authors = raw["authors"] if isinstance(raw["authors"], list) else [raw["authors"]]

    # Topics / channels
    topics = [
        c.get("name", "") for c in raw.get("channels", []) if c.get("name")
    ]

    canonical_url = url or f"https://www.benzinga.com/news/{item_id}"

    # Fingerprint: stable hash of (source_key, benzinga_id, title)
    fp_input = f"benzinga|{item_id}|{title}".encode()
    fingerprint = hashlib.sha256(fp_input).digest()

    return {
        "canonical_source": source_key,
        "canonical_url": canonical_url,
        "canonical_external_id": item_id or None,
        "title": title,
        "summary": summary or None,
        "body": body or None,
        "language": "en",
        "published_time_utc": published,
        "updated_time_utc": updated,
        "authors": authors or None,
        "source_attribution": {
            "provider": "benzinga",
            "item_id": item_id,
            "source_key": source_key,
        },
        "content_type": content_type,
        "tickers": tickers,
        "topics": topics,
        "fingerprint_v1": fingerprint,
        "provenance_entry": {"raw_source": source_key, "benzinga_id": item_id},
    }


def _clean(text: Any) -> str:
    if not text:
        return ""
    import re
    text = str(text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_benzinga_dt(value: Any) -> Optional[datetime]:
    """Parse Benzinga timestamps: Unix int, ISO string, or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    return None
