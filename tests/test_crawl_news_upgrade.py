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


# ── Business Wire source added to the tripwire (2026-07-30) ──────────────────

def test_business_wire_feed_registered():
    import crawl_news as cn
    assert any("businesswire.com" in u for u in cn._WIRE_TRIPWIRE_FEEDS), \
        "Business Wire feed missing from _WIRE_TRIPWIRE_FEEDS"


def test_business_wire_item_labeled_correctly():
    # A Business Wire release flowing through the wire_tripwire parse branch
    # must be attributed to Business Wire (not defaulted to PR Newswire) at
    # trust tier 3.
    import crawl_news as cn
    rss = (
        b'<?xml version="1.0"?><rss version="2.0"><channel>'
        b'<item><title>Acme Corp to Acquire Beta Inc for $3 billion</title>'
        b'<link>https://www.businesswire.com/news/home/20260730/en/Acme-Beta</link>'
        b'<description>Acme announced a definitive agreement.</description>'
        b'<pubDate>Wed, 30 Jul 2026 09:00:00 GMT</pubDate></item></channel></rss>'
    )
    arts = cn.parse_rss(rss, "wire_tripwire", "")
    assert arts, "parse_rss returned no articles"
    a = arts[0]
    assert a["_source_name"] == "Business Wire"
    assert a["_source_domain"] == "businesswire.com"
    assert a["_trust"] == 3


def test_wire_tripwire_still_labels_prnewswire_and_globenewswire():
    # Regression guard: adding Business Wire must not change the other two.
    import crawl_news as cn
    for dom, expect in (
        ("https://www.prnewswire.com/news-releases/x.html", "PR Newswire"),
        ("https://www.globenewswire.com/news-release/x", "GlobeNewswire"),
    ):
        rss = (
            b'<?xml version="1.0"?><rss version="2.0"><channel><item>'
            b'<title>Company reports Q3 earnings beat</title>'
            b'<link>' + dom.encode() + b'</link>'
            b'<description>x</description></item></channel></rss>'
        )
        a = cn.parse_rss(rss, "wire_tripwire", "")[0]
        assert a["_source_name"] == expect, (dom, a["_source_name"])


# ── Widened tripwire event gate: contracts + capital investment (2026-07-30) ──

def test_major_contract_and_capital_investment_classified():
    import crawl_news as cn
    contract = cn.classify_events("Booz Allen Wins $1 Billion Army Cyber Contract")
    assert "major_contract" in contract
    order = cn.classify_events("Lockheed Receives Order for 50 F-35 Jets")
    assert "major_contract" in order
    capex = cn.classify_events("Intel to Invest $20 Billion in New Arizona Fab")
    assert "capital_investment" in capex
    plant = cn.classify_events("PMI U.S. Opens $1.2 Billion Aurora Campus")
    assert "capital_investment" in plant


def test_new_categories_are_in_tripwire_gate():
    import crawl_news as cn
    assert {"major_contract", "capital_investment", "partnership"} <= cn._TRIPWIRE_EVENT_TYPES


def test_contract_capex_categories_do_not_flag_consumer_pr():
    # Recall guard: real Business Wire consumer/PR noise must NOT trip the new
    # high-value categories (verified against the live feed's fluff).
    import crawl_news as cn
    noise = [
        "The Cheesecake Factory Celebrates National Cheesecake Day July 30",
        "Open Farm Introduces Pets Are Perfect Brand Evolution",
        "La-Z-Boy and Kristin Juszczyk Team Up to Create the Jer-Z-Boy",
        "Fieldwork Appoints Dr. Cleo Valentine as Advisor",
    ]
    for t in noise:
        ets = set(cn.classify_events(t))
        assert "major_contract" not in ets, t
        assert "capital_investment" not in ets, t


