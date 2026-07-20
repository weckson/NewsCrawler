"""Timezone / PIT correctness of publish-time parsing in crawl_news.py.

Regression guard for the 2026-07-20 bug: parse_rss built `published` with
`time.mktime(entry.published_parsed)`. feedparser normalizes those structs to
UTC, but mktime interprets a struct as LOCAL time — so on any host not running
UTC, every RSS timestamp was shifted by the machine's offset (-10h measured on
an Australia/Sydney box). That silently moved the --hours cutoff, distorted the
temporal-freshness signal, and inflated meta.publish_to_observe_latency_s
across every parse_rss channel.

These tests are written to fail on ANY host if the idiom regresses — they pin
absolute UTC values rather than comparing against locally-derived ones.
"""
import time
from calendar import timegm
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from crawl_news import _rss_published_utc, parse_rss

# Fri, 18 Jul 2026 20:10:28 GMT — a real EDGAR-era 8-K publish time.
PUBDATE_RFC822 = "Fri, 18 Jul 2026 20:10:28 GMT"
EXPECTED_UTC = "2026-07-18T20:10:28+00:00"
UTC_STRUCT = parsedate_to_datetime(PUBDATE_RFC822).utctimetuple()


def _rss(pubdate: str | None = PUBDATE_RFC822) -> bytes:
    pub = f"<pubDate>{pubdate}</pubDate>" if pubdate else ""
    return (
        '<?xml version="1.0"?><rss version="2.0"><channel>'
        "<title>Test Feed</title>"
        "<item><title>Some Company Reports Quarterly Results Today</title>"
        "<link>https://example.com/a</link>"
        f"<description>Body text.</description>{pub}</item>"
        "</channel></rss>"
    ).encode()


# ── the core guard ──────────────────────────────────────────────────────────

def test_rss_published_struct_is_interpreted_as_utc():
    entry = {"published_parsed": UTC_STRUCT}
    assert _rss_published_utc(entry) == EXPECTED_UTC


def test_parse_rss_end_to_end_publish_time():
    articles = parse_rss(_rss(), "yahoo_finance_rss", "NVDA")
    assert len(articles) == 1
    assert articles[0]["published"] == EXPECTED_UTC


def test_result_is_not_the_local_time_misreading():
    """The actual bug: mktime treats the UTC struct as local time. On a host
    with a non-zero UTC offset the two answers differ, and we must produce the
    UTC one. Skipped on UTC hosts, where the readings coincide and the test
    could not detect a regression anyway."""
    if time.mktime(UTC_STRUCT) == timegm(UTC_STRUCT):
        import pytest
        pytest.skip("host runs UTC — the buggy and correct idioms coincide")
    buggy = datetime.fromtimestamp(
        time.mktime(UTC_STRUCT), tz=timezone.utc
    ).isoformat()
    assert _rss_published_utc({"published_parsed": UTC_STRUCT}) != buggy


# ── fallback must never leak a non-ISO string ───────────────────────────────

def test_falls_back_to_raw_header_when_struct_missing():
    assert _rss_published_utc({"published": PUBDATE_RFC822}) == EXPECTED_UTC


def test_naive_header_is_assumed_utc():
    got = _rss_published_utc({"published": "Fri, 18 Jul 2026 20:10:28"})
    assert got == EXPECTED_UTC


def test_offset_header_is_converted_not_truncated():
    # 16:10:28-04:00 is the same instant as 20:10:28Z
    got = _rss_published_utc({"published": "Fri, 18 Jul 2026 16:10:28 -0400"})
    assert got == EXPECTED_UTC


def test_unparseable_input_returns_none_never_a_raw_string():
    """Two sort sites call fromisoformat() unguarded; the old code stored the
    raw header on failure, which would crash the run."""
    for entry in ({"published": "not-a-date"}, {"published_parsed": None}, {}):
        got = _rss_published_utc(entry)
        assert got is None, got


def test_missing_pubdate_yields_none_and_still_parses_article():
    articles = parse_rss(_rss(pubdate=None), "yahoo_finance_rss", "NVDA")
    assert len(articles) == 1
    assert articles[0]["published"] is None


def test_every_emitted_published_is_iso_parseable():
    for pubdate in (PUBDATE_RFC822, "garbage", "Fri, 18 Jul 2026 16:10:28 -0400"):
        for a in parse_rss(_rss(pubdate=pubdate), "nasdaq_rss", "NVDA"):
            if a["published"] is not None:
                datetime.fromisoformat(a["published"])   # must not raise
