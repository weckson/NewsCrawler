"""Tests for the IBKR TWS wire-news channel (crawler/sources/ibkr_news.py)
and its integration points in crawl_news.py. No network / no TWS required."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from crawler.sources.ibkr_news import (
    article_from_headline,
    clean_headline,
    html_to_text,
    is_fragment,
    provider_meta,
    _parse_news_time,
)


# ── headline hygiene ────────────────────────────────────────────────────────

def test_clean_headline_strips_ibkr_tags():
    raw = "{A:800015:L:en}Broadcom Legal Chief Sells Shares After Apple Partnership Deal -- Barron's"
    assert clean_headline(raw) == (
        "Broadcom Legal Chief Sells Shares After Apple Partnership Deal -- Barron's"
    )


def test_clean_headline_strips_bullet_and_multiple_tags():
    raw = "{A:1:L:en}{K:n}* Etched Also Raising Funding at $10 Billion Valuation -- WSJ"
    assert clean_headline(raw).startswith("Etched Also Raising")


def test_clean_headline_plain_passthrough():
    assert clean_headline("Nvidia Narrowly Holds Off Apple") == "Nvidia Narrowly Holds Off Apple"


def test_fragment_detection_continuation_pages():
    assert is_fragment("Up & Down Wall Street: Silicon Valley Legend -2-")
    assert is_fragment("How A Homegrown Chinese Chip Maker Became The -3-")
    assert not is_fragment("Nvidia Could Lose Its Crown. Here's the Stock Taking Its Place.")


def test_fragment_detection_stubs():
    assert is_fragment("Review")           # too short to carry signal
    assert not is_fragment("Apple's AI Spending Earned an Upgrade")


# ── provider mapping ────────────────────────────────────────────────────────

def test_provider_meta_dow_jones_family_trust3():
    for code in ("DJ-N", "DJ-RT", "DJ-RTA", "DJ-RTE", "DJ-RTG", "DJNL"):
        name, domain, trust = provider_meta(code)
        assert domain == "dowjones.com"
        assert trust == 3


def test_provider_meta_briefing_split():
    assert provider_meta("BRFUPDN")[2] == 3   # analyst actions — high value
    assert provider_meta("BRFG")[2] == 2      # general market columns


def test_provider_meta_unknown_dj_prefix_and_fallback():
    assert provider_meta("DJ-XX") == ("Dow Jones", "dowjones.com", 3)
    name, domain, trust = provider_meta("MYSTERY")
    assert trust == 2 and domain == "interactivebrokers.com"


# ── time parsing ────────────────────────────────────────────────────────────

def test_parse_news_time_naive_datetime_is_utc():
    dt = _parse_news_time(datetime(2026, 7, 18, 1, 31, 0))
    assert dt.tzinfo is not None
    assert dt.isoformat() == "2026-07-18T01:31:00+00:00"


def test_parse_news_time_string_form():
    dt = _parse_news_time("2026-07-18 01:31:00.0")
    assert dt.isoformat() == "2026-07-18T01:31:00+00:00"


def test_parse_news_time_garbage_falls_back_to_now():
    dt = _parse_news_time("not-a-time")
    assert abs((datetime.now(timezone.utc) - dt).total_seconds()) < 5


# ── article construction ────────────────────────────────────────────────────

def _mk_hn(headline, provider="DJ-N", article_id="DJ-N$abc123",
           when=None):
    return SimpleNamespace(
        headline=headline, providerCode=provider, articleId=article_id,
        time=when or datetime(2026, 7, 18, 1, 31, 0),
    )


def test_article_from_headline_schema():
    art = article_from_headline(
        _mk_hn("{A:800015:L:en}Rocket Lab Initiated at Neutral by Piper Sandler"),
        "RKLB",
    )
    assert art is not None
    assert art["title"] == "Rocket Lab Initiated at Neutral by Piper Sandler"
    assert art["url"] == "ibkr-news://DJ-N$abc123"
    assert art["_channel"] == "ibkr_news"
    assert art["_trust"] == 3
    assert art["_source_domain"] == "dowjones.com"
    assert art["_ticker"] == "RKLB"
    assert art["_tickers"] == ["RKLB"]
    assert art["published"] == "2026-07-18T01:31:00+00:00"
    assert art["_meta"]["ibkr_provider"] == "DJ-N"
    assert art["_meta"]["ibkr_article_id"] == "DJ-N$abc123"


def test_article_from_headline_drops_fragments():
    assert article_from_headline(_mk_hn("Story Title -2-"), "NVDA") is None
    assert article_from_headline(_mk_hn(""), "NVDA") is None


def test_article_url_is_stable_across_runs():
    a1 = article_from_headline(_mk_hn("Some Headline"), "NVDA")
    a2 = article_from_headline(_mk_hn("Some Headline"), "NVDA")
    assert a1["url"] == a2["url"]
    assert a1["id"] == a2["id"]


# ── body extraction ─────────────────────────────────────────────────────────

def test_html_to_text_dj_body():
    body = ("<p>&#10;  By Mackenzie Tatananni </p>&#10;<p>&#10;  A high-ranking "
            "insider at Broadcom capitalized on a short-lived rebound in the chip "
            "maker&apos;s stock, selling nearly $20 million of shares. </p>")
    text = html_to_text(body)
    assert "<p>" not in text
    assert "By Mackenzie Tatananni" in text
    assert "chip maker's stock" in text


def test_html_to_text_empty():
    assert html_to_text("") == ""
    assert html_to_text(None) == ""


# ── channel disable switch ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_env_disable_short_circuits(monkeypatch):
    from crawler.sources.ibkr_news import fetch_ibkr_news_for_tickers
    monkeypatch.setenv("NEWSCRAWLER_IBKR_NEWS", "0")
    assert await fetch_ibkr_news_for_tickers(["NVDA"]) == {}


@pytest.mark.asyncio
async def test_no_tws_listening_returns_empty(monkeypatch):
    """With no TWS on an unlikely port, the channel skips fast and clean."""
    from crawler.sources.ibkr_news import fetch_ibkr_news_for_tickers
    monkeypatch.delenv("NEWSCRAWLER_IBKR_NEWS", raising=False)
    monkeypatch.setenv("NEWSCRAWLER_IBKR_PORT", "59999")
    assert await fetch_ibkr_news_for_tickers(["NVDA"]) == {}


# ── quality_filter integration ──────────────────────────────────────────────

def test_quality_filter_accepts_ibkr_article_without_company_name():
    """DJ headlines that never name the company (insider Form-4 style) must
    survive quality_filter via the authoritative-attribution relevance floor."""
    import crawl_news

    now = datetime.now(timezone.utc)
    art = article_from_headline(
        _mk_hn("VP Papermaster Sells 6,000 Shares Of Advanced Micro Devices",
               when=now - timedelta(hours=2)),
        "AMD",
    )
    passed = crawl_news.quality_filter([art], "AMD", hours=48)
    assert len(passed) == 1
    assert passed[0]["_relevance"] >= 0.30
    assert passed[0]["_channel"] == "ibkr_news"


def test_weak_signal_gate_still_applies_to_aggregators():
    """The ibkr_news weak-signal exemption must NOT leak to other channels:
    the same 13F-churn phrasing from an aggregator still dies."""
    import crawl_news

    assert crawl_news.is_weak_signal(
        "Some Capital Management sells 6,000 shares of Advanced Micro Devices",
        "marketbeat.com",
    )


def test_quality_filter_still_drops_stale_ibkr_articles():
    import crawl_news

    old = datetime.now(timezone.utc) - timedelta(hours=400)
    art = article_from_headline(
        _mk_hn("Ancient Headline That Should Not Survive The Window",
               when=old),
        "AMD",
    )
    passed = crawl_news.quality_filter([art], "AMD", hours=48)
    assert passed == []
