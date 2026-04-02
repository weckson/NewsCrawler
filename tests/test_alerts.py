"""
Unit tests: alert rules engine
- Keyword matching
- Ticker matching
- Source matching
- Dedup-aware (one alert per cluster)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from crawler.alerts.rules import evaluate, load_rules, _alerted_clusters


def setup_function():
    _alerted_clusters.clear()
    load_rules([
        {
            "name": "market_keywords",
            "keywords": ["earnings", "acquisition", "profit warning"],
            "notify": ["slack"],
        },
        {
            "name": "asx_watchlist",
            "tickers": ["BHP", "CBA"],
            "notify": ["slack"],
        },
        {
            "name": "regulatory_actions",
            "sources": ["asic_newsroom"],
            "notify": ["slack", "email"],
        },
    ])


def _item(title="Test", tickers=None, source="benzinga_news_api", cluster_id=None):
    return {
        "title": title,
        "summary": "",
        "tickers": tickers or [],
        "topics": [],
        "canonical_source": source,
        "canonical_url": f"https://example.com/{uuid.uuid4()}",
        "published_time_utc": datetime.now(timezone.utc),
        "dedupe_cluster_id": cluster_id or uuid.uuid4(),
    }


def test_keyword_match():
    item = _item(title="Apple Q1 earnings beat estimates")
    matched = evaluate(item)
    assert any(r["name"] == "market_keywords" for r in matched)


def test_keyword_case_insensitive():
    item = _item(title="COMPANY ANNOUNCES ACQUISITION")
    matched = evaluate(item)
    assert any(r["name"] == "market_keywords" for r in matched)


def test_ticker_match():
    item = _item(title="BHP reports full-year results", tickers=["BHP"])
    matched = evaluate(item)
    assert any(r["name"] == "asx_watchlist" for r in matched)


def test_ticker_case_insensitive():
    item = _item(title="Bank reports results", tickers=["cba"])
    matched = evaluate(item)
    assert any(r["name"] == "asx_watchlist" for r in matched)


def test_source_match():
    item = _item(title="ASIC bans financial adviser", source="asic_newsroom")
    matched = evaluate(item)
    assert any(r["name"] == "regulatory_actions" for r in matched)


def test_no_match():
    item = _item(title="Weather update: sunny tomorrow")
    matched = evaluate(item)
    assert matched == []


def test_dedup_aware_second_alert_suppressed():
    """Second item with same cluster_id must not fire another alert."""
    cluster = uuid.uuid4()
    item1 = _item(title="Apple earnings beat estimates", cluster_id=cluster)
    item2 = _item(title="Apple Q1 earnings beat analyst expectations", cluster_id=cluster)

    matched1 = evaluate(item1)
    matched2 = evaluate(item2)

    assert len(matched1) > 0
    assert matched2 == [], "Second item in same cluster must be suppressed"


def test_notify_channels_present():
    item = _item(title="Profit warning: major retailer cuts outlook")
    matched = evaluate(item)
    assert matched
    assert "notify" in matched[0]
    assert "slack" in matched[0]["notify"]
