"""
Unit tests: deduplication
- Acceptance test 4: two near-duplicate items cluster into same dedupe_cluster_id
- Exact URL dedup via canonical.py
- SimHash distance computation
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from crawler.dedupe.canonical import normalise_url, make_fingerprint
from crawler.dedupe import simhash as sh
from crawler.dedupe.simhash import compute, hamming_distance, dedupe_item, _index


# ── canonical.py ──────────────────────────────────────────────────────────────

def test_normalise_url_removes_tracking():
    url = "https://example.com/news/123?utm_source=rss&utm_medium=feed&id=42"
    result = normalise_url(url)
    assert "utm_source" not in result
    assert "id=42" in result


def test_normalise_url_removes_fragment():
    url = "https://example.com/news/123#comments"
    assert "#" not in normalise_url(url)


def test_normalise_url_lowercases_host():
    url = "HTTPS://Example.COM/news/123"
    result = normalise_url(url)
    assert result.startswith("https://example.com/")


def test_normalise_url_sorts_query_params():
    url1 = "https://example.com/news?z=1&a=2"
    url2 = "https://example.com/news?a=2&z=1"
    assert normalise_url(url1) == normalise_url(url2)


def test_make_fingerprint_stable():
    fp1 = make_fingerprint("benzinga_news_api", "12345", "Apple Q1 Earnings Beat")
    fp2 = make_fingerprint("benzinga_news_api", "12345", "Apple Q1 Earnings Beat")
    assert fp1 == fp2


def test_make_fingerprint_differs_on_source():
    fp1 = make_fingerprint("benzinga_news_api", "12345", "Same title")
    fp2 = make_fingerprint("prnewswire_rss", "12345", "Same title")
    assert fp1 != fp2


# ── simhash.py ────────────────────────────────────────────────────────────────

def test_compute_identical_texts():
    assert compute("apple earnings beat") == compute("apple earnings beat")


def test_compute_empty_text():
    assert compute("") == 0


def test_hamming_distance_identical():
    h = compute("hello world")
    assert hamming_distance(h, h) == 0


def test_hamming_distance_max():
    assert hamming_distance(0, (1 << 64) - 1) == 64


# ── Acceptance test 4: near-duplicate clustering ─────────────────────────────

def _make_item(title: str, summary: str = "", source: str = "test_source") -> dict:
    return {
        "canonical_source": source,
        "canonical_url": f"https://example.com/{hash(title)}",
        "canonical_external_id": None,
        "title": title,
        "summary": summary,
        "published_time_utc": datetime.now(timezone.utc),
        "tickers": [],
        "topics": [],
    }


def test_near_duplicate_items_share_cluster():
    """Two near-identical headlines should land in the same dedupe cluster."""
    _index.clear()  # reset state between tests

    item1 = _make_item(
        "Apple Reports Record Q1 Earnings Beating Wall Street Estimates",
        "Apple Reports Record Q1 Earnings Beating Wall Street Estimates",
    )
    item2 = _make_item(
        "Apple Reports Record Q1 Earnings Beating Wall Street Results",
        "Apple Reports Record Q1 Earnings Beating Wall Street Results",
    )

    dedupe_item(item1)
    dedupe_item(item2)

    assert item1["dedupe_cluster_id"] == item2["dedupe_cluster_id"], (
        "Near-duplicate items must share the same dedupe_cluster_id"
    )


def test_distinct_items_get_different_clusters():
    """Clearly unrelated headlines should get separate clusters."""
    _index.clear()

    item1 = _make_item("Apple Reports Record Q1 Earnings")
    item2 = _make_item("RBA Holds Cash Rate at 4.35% Amid Inflation Concerns")

    dedupe_item(item1)
    dedupe_item(item2)

    assert item1["dedupe_cluster_id"] != item2["dedupe_cluster_id"]


def test_dedupe_item_sets_score():
    _index.clear()
    item = _make_item("Markets rally on strong jobs data")
    dedupe_item(item)
    assert "dedupe_score" in item
    assert 0.0 <= item["dedupe_score"] <= 1.0
