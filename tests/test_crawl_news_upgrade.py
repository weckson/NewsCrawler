"""Tests for the 2026-07 upgrade: URL canonicalization + importance scoring.

Both are additive:
  - canonicalize_url() collapses tracking-param variants of the same story so
    dedup / the url-keyed SQLite store treat them as one article.
  - importance_score() (surfaced as meta.importance) gives AIStock a single
    rules-only scalar to prioritise watchlist news, without touching any
    existing scoring or gating.
"""

from crawl_news import (
    canonicalize_url,
    importance_score,
    to_news_item,
)


# ── canonicalize_url ─────────────────────────────────────────────────────────

def test_strips_utm_and_click_ids():
    url = "https://example.com/a?utm_source=twitter&utm_medium=social&id=42&fbclid=xyz"
    assert canonicalize_url(url) == "https://example.com/a?id=42"


def test_tracking_variants_collapse_to_same_key():
    a = canonicalize_url("https://site.com/story?utm_source=rss")
    b = canonicalize_url("https://site.com/story?utm_source=newsletter&gclid=abc")
    assert a == b == "https://site.com/story"


def test_drops_fragment_and_lowercases_host():
    url = "https://Example.COM/Path?b=2&a=1#section"
    # host lowercased, fragment gone, query sorted, path case preserved
    assert canonicalize_url(url) == "https://example.com/Path?a=1&b=2"


def test_meaningful_query_params_are_preserved():
    # A real content selector (article id, page) must NOT be stripped.
    url = "https://news.site/article?story=12345&page=2"
    assert canonicalize_url(url) == "https://news.site/article?page=2&story=12345"


def test_google_news_redirector_is_left_opaque():
    # Must survive untouched until resolve_article_url decodes it.
    url = "https://news.google.com/rss/articles/CBMiabc123?oc=5&hl=en-US"
    assert canonicalize_url(url) == url


def test_malformed_or_empty_url_returned_unchanged():
    assert canonicalize_url("") == ""
    assert canonicalize_url("not a url") == "not a url"


# ── importance_score ─────────────────────────────────────────────────────────

def _art(**kw):
    base = {"_trust": 2, "_relevance": 0.8, "_channel": "google_broad", "_alt_sources": []}
    base.update(kw)
    return base


def test_importance_is_bounded():
    # Even a maximal article can't exceed 1.0.
    art = _art(_channel="sec_edgar", _trust=3, _alt_sources=["a", "b", "c"])
    sig = {"deal_size_class": "large", "guidance_direction": "raised",
           "regulatory_outcome": "approval"}
    s = importance_score(art, ["ma_activity"], sig)
    assert 0.0 <= s <= 1.0


def test_ma_deal_scores_higher_than_price_move():
    deal = importance_score(_art(), ["ma_activity"], {"deal_size_class": "large"})
    move = importance_score(_art(), ["price_action"], None)
    assert deal > move


def test_primary_source_outranks_reblog_for_same_event():
    filing = importance_score(_art(_channel="sec_edgar"), ["insider_activity"], None)
    reblog = importance_score(_art(_channel="google_broad", _trust=1), ["insider_activity"], None)
    assert filing > reblog


def test_low_relevance_is_penalized():
    on_topic = importance_score(_art(_relevance=0.8), ["regulatory"], None)
    off_topic = importance_score(_art(_relevance=0.1), ["regulatory"], None)
    assert off_topic < on_topic


def test_no_event_types_gets_floor():
    s = importance_score(_art(), [], None)
    assert 0.0 <= s <= 0.4  # only the base floor + maybe a small source bump


def test_importance_surfaces_in_newsitem_meta():
    item = to_news_item({
        "title": "BigCo agrees to acquire SmallCo for $12 billion",
        "url": "https://example.com/deal",
        "_ticker": "BIG",
        "_trust": 3,
        "_relevance": 0.9,
        "_channel": "wire_tripwire",
    })
    assert "importance" in item["meta"]
    assert isinstance(item["meta"]["importance"], float)
    assert item["meta"]["importance"] > 0.8  # large M&A from a primary wire
