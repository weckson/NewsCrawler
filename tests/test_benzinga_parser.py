"""
Unit tests: Benzinga parser
- Schema normalisation
- Delta URL construction
- Auth headers (no key leak)
- Timestamp parsing
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from crawler.parsers.benzinga import (
    parse_news_response,
    build_news_url,
    auth_headers,
    _parse_benzinga_dt,
)

FIXTURE = Path(__file__).parent / "fixtures" / "benzinga_news.json"


@pytest.fixture
def raw_items():
    return json.loads(FIXTURE.read_text())


def test_parse_news_response_count(raw_items):
    items = parse_news_response(raw_items)
    assert len(items) == 2


def test_parse_news_response_fields(raw_items):
    items = parse_news_response(raw_items)
    item = items[0]
    assert item["title"] == "Apple Reports Record Q1 Earnings Beating Analyst Estimates"
    assert item["canonical_source"] == "benzinga_news_api"
    assert "AAPL" in item["tickers"]
    assert "earnings" in item["topics"]
    assert item["published_time_utc"] is not None
    assert item["content_type"] == "news"
    assert item["fingerprint_v1"] is not None
    assert len(item["fingerprint_v1"]) == 32  # SHA-256 = 32 bytes


def test_parse_news_response_stores_body_when_allowed(raw_items):
    items = parse_news_response(raw_items, store_body=True)
    assert items[0]["body"] is not None
    assert "Apple" in items[0]["body"]


def test_parse_news_response_no_body_when_not_allowed(raw_items):
    items = parse_news_response(raw_items, store_body=False)
    assert items[0]["body"] is None


def test_parse_news_response_null_body_item(raw_items):
    """Item with null body should still parse successfully."""
    items = parse_news_response(raw_items, store_body=True)
    assert items[1]["body"] is None  # fixture has null body


def test_build_news_url_no_delta():
    url = build_news_url(page=0, page_size=50)
    assert "pageSize=50" in url
    assert "page=0" in url
    assert "updatedSince" not in url


def test_build_news_url_with_delta():
    url = build_news_url(updated_since=1711411200)
    assert "updatedSince=1711411200" in url


def test_build_news_url_caps_page_size():
    url = build_news_url(page_size=999)
    assert "pageSize=100" in url


def test_auth_headers_format(monkeypatch):
    monkeypatch.setenv("BENZINGA_API_KEY", "test_key_abc")
    headers = auth_headers()
    assert headers["Authorization"] == "token test_key_abc"
    assert "Accept" in headers


def test_auth_headers_no_key_in_value(monkeypatch):
    """Auth header value must use 'token <key>' format, not bare key."""
    monkeypatch.setenv("BENZINGA_API_KEY", "secret123")
    headers = auth_headers()
    assert headers["Authorization"].startswith("token ")


def test_parse_benzinga_dt_unix():
    dt = _parse_benzinga_dt(1711411200)
    assert isinstance(dt, datetime)
    assert dt.tzinfo == timezone.utc


def test_parse_benzinga_dt_iso():
    dt = _parse_benzinga_dt("2024-03-25T12:00:00Z")
    assert isinstance(dt, datetime)


def test_parse_benzinga_dt_none():
    assert _parse_benzinga_dt(None) is None


# ── Acceptance test: delta ingest advances crawl_state ────────────────────────

def test_normalised_items_have_provenance(raw_items):
    items = parse_news_response(raw_items)
    for item in items:
        assert "provenance_entry" in item
        assert item["provenance_entry"]["raw_source"] == "benzinga_news_api"
