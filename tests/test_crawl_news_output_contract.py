"""Output-contract tests — lock the AIStock NewsItem interface.

These are the safety net for internal refactors: they assert the *shape* of
what NewsCrawler emits (top-level fields, meta keys, time formats, fixed
markers) rather than any particular scoring behaviour. If an internal change
alters the schema AIStock consumes, one of these fails.

AIStock reads:
  - top-level: first_seen_at_utc (PIT windowing), trust_tier / source_quality
    (filtering), ticker / tickers_hint (routing), timestamp_utc (display)
  - meta: event_types, event_signals, sentiment, classifier_version,
    publish_to_observe_latency_s

Do NOT loosen these without a matching change on the AIStock consumer side.
"""

from datetime import datetime, timezone

from crawl_news import (
    CLASSIFIER_VERSION,
    build_aistock_payload,
    to_news_item,
    to_news_items,
)

# The exact top-level key set AIStock's NewsItem contract depends on.
REQUIRED_TOP_LEVEL_KEYS = {
    "id",
    "timestamp_utc",
    "first_seen_at_utc",
    "source",
    "url",
    "title",
    "body",
    "author",
    "language",
    "ticker",
    "tickers_hint",
    "source_quality",
    "publisher_raw",
    "trust_tier",
    "body_kind",
    "ingest_source",
    "meta",
}


def _full_article():
    return {
        "id": "article-1",
        "title": "AMD wins major AI server deal worth $5 billion",
        "url": "https://example.com/amd-deal",
        "published": "2026-03-29T10:30:45+00:00",
        "_first_seen_at": "2026-03-29T10:38:00+00:00",
        "summary": "AMD secured a new hyperscaler contract worth $5 billion.",
        "_source_name": "Reuters",
        "_source_domain": "www.reuters.com",
        "_trust": 3,
        "_relevance": 0.92,
        "_quality_score": 0.85,
        "_channel": "google_broad",
        "_ticker": "AMD",
        "_tickers": ["AMD"],
        "_alt_sources": ["CNBC"],
    }


def _iso_utc_parseable(value: str) -> bool:
    """True if value is an ISO-8601 string that carries a UTC offset."""
    try:
        dt = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return False
    return dt.tzinfo is not None and dt.utcoffset() == timezone.utc.utcoffset(None)


# ── top-level shape ──────────────────────────────────────────────────────────

def test_newsitem_has_exactly_the_expected_top_level_keys():
    item = to_news_item(_full_article())
    assert set(item.keys()) == REQUIRED_TOP_LEVEL_KEYS


def test_minimal_article_still_emits_all_required_keys():
    # An article with only a title must not drop any contract key.
    item = to_news_item({"title": "bare headline"})
    assert REQUIRED_TOP_LEVEL_KEYS.issubset(item.keys())


def test_fixed_markers_are_stable():
    item = to_news_item(_full_article())
    assert item["ingest_source"] == "newscrawler_local"
    assert item["language"] == "en"
    assert item["author"] is None
    assert item["meta"]["event_origin"] == "news"


# ── time fields ──────────────────────────────────────────────────────────────

def test_time_fields_are_iso_utc():
    item = to_news_item(_full_article())
    assert _iso_utc_parseable(item["timestamp_utc"])
    assert _iso_utc_parseable(item["first_seen_at_utc"])


def test_first_seen_at_mirrored_in_meta_for_legacy_consumers():
    item = to_news_item(_full_article())
    assert item["meta"]["first_seen_at_utc"] == item["first_seen_at_utc"]


def test_publish_to_observe_latency_is_nonnegative_int():
    item = to_news_item(_full_article())
    latency = item["meta"]["publish_to_observe_latency_s"]
    assert isinstance(latency, int)
    assert latency >= 0
    # 2026-03-29T10:30:45 -> 10:38:00 == 435s
    assert latency == 435


# ── meta / classifier lineage ────────────────────────────────────────────────

def test_classifier_version_is_stamped():
    item = to_news_item(_full_article())
    assert item["meta"]["classifier_version"] == CLASSIFIER_VERSION


def test_meta_drops_none_values_but_keeps_channel():
    # meta is built with a `v is not None` filter — a present channel survives.
    item = to_news_item(_full_article())
    assert item["meta"]["_channel"] == "google_broad"
    assert all(v is not None for v in item["meta"].values())


# ── routing fields ───────────────────────────────────────────────────────────

def test_tickers_hint_is_sorted_unique_list():
    article = dict(_full_article(), _ticker=None, _tickers=["NVDA", "AMD", "AMD"])
    item = to_news_item(article)
    assert item["tickers_hint"] == ["AMD", "NVDA"]
    assert item["ticker"] == "AMD"  # primary = first after sort


def test_trust_and_quality_pass_through():
    item = to_news_item(_full_article())
    assert item["trust_tier"] == 3
    assert item["source_quality"] == "high"


# ── array + payload envelope ─────────────────────────────────────────────────

def test_to_news_items_preserves_array_shape():
    items = to_news_items([_full_article(), {"title": "second"}])
    assert isinstance(items, list) and len(items) == 2
    for it in items:
        assert REQUIRED_TOP_LEVEL_KEYS.issubset(it.keys())


def test_build_aistock_payload_envelope_keys():
    news_items = [to_news_item(_full_article())]
    payload = build_aistock_payload(news_items)
    # The envelope AIStock's connector expects.
    for key in ("hard_event_news", "soft_event_news", "alt_sentiment_news", "news"):
        assert key in payload
    assert payload["news"] == news_items
    assert payload["soft_event_news"] == news_items
