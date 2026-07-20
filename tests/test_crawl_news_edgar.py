"""Tests for the SEC EDGAR filings channel (sec_edgar) in crawl_news.py.
No network required — feeds are inline Atom fixtures, the CIK map is a dict.

Covers the 宁缺毋滥 contract: only event-class forms (4 / 8-K / 13D/G) for
watchlist CIKs enter the pipeline; NPORT-P-style fund noise and prefix-match
pollution (424B5, 425) never do. Also covers PIT correctness of the parsed
acceptance timestamp (offset-aware → UTC, no local-zone assumption).
"""
from datetime import datetime, timedelta, timezone

import crawl_news
from crawl_news import (
    _build_cik_to_ticker,
    _edgar_title,
    _fetch_edgar_filings,
    _parse_edgar_atom,
    classify_events,
    dedup_articles,
    quality_filter,
    select_articles_for_fulltext,
    to_news_item,
)

CIK_MAP = {1045810: "NVDA", 1819994: "RKLB", 1554859: "SMLR"}


def _atom(entries: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="ISO-8859-1" ?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        "<title>Latest Filings</title>"
        f"{entries}</feed>"
    ).encode()


def _entry(title, form, acc, updated="2026-07-18T17:02:41-04:00",
           link=None):
    link = link or f"https://www.sec.gov/Archives/edgar/data/{acc}-index.htm"
    return (
        "<entry>"
        f"<title>{title}</title>"
        f'<link rel="alternate" type="text/html" href="{link}"/>'
        f'<category scheme="https://www.sec.gov/" label="form type" term="{form}"/>'
        f"<summary type=\"html\">Filed: 2026-07-18 AccNo: {acc}</summary>"
        f"<updated>{updated}</updated>"
        f"<id>urn:tag:sec.gov,2008:accession-number={acc}</id>"
        "</entry>"
    )


FORM4_ACC = "0001045810-26-000101"
FORM8K_ACC = "0001819994-26-000055"
SC13D_ACC = "0002222222-26-000007"
SC13G_ACC = "0001045810-26-000222"

SAMPLE_FEED = _atom(
    # Form 4 pair: Reporting (person) + Issuer (company) share one accession
    _entry("4 - Kress Colette (0001178579) (Reporting)", "4", FORM4_ACC)
    + _entry("4 - NVIDIA CORP (0001045810) (Issuer)", "4", FORM4_ACC)
    # 8-K from a watchlist company
    + _entry("8-K - Rocket Lab Corp (0001819994) (Filer)", "8-K", FORM8K_ACC)
    # 8-K from a NON-watchlist CIK — must be dropped
    + _entry("8-K - Unrelated Industries Inc (0009999999) (Filer)", "8-K",
             "0009999999-26-000001")
    # Fund-holdings noise with a WATCHLIST CIK — form whitelist must drop it
    + _entry("NPORT-P - NVIDIA CORP (0001045810) (Filer)", "NPORT-P",
             "0001045810-26-000303")
    # Prefix-match pollution from the type=4 query — must be dropped
    + _entry("424B5 - NVIDIA CORP (0001045810) (Filer)", "424B5",
             "0001045810-26-000404")
    # SC 13D pair: Subject (target) + Filed by (acquiring fund)
    + _entry("SC 13D - Semler Scientific, Inc. (0001554859) (Subject)",
             "SC 13D", SC13D_ACC)
    + _entry("SC 13D - Big Activist Fund LP (0002222222) (Filed by)",
             "SC 13D", SC13D_ACC)
    # Amended passive-stake schedule on a watchlist company
    + _entry("SC 13G/A - NVIDIA CORP (0001045810) (Subject)", "SC 13G/A",
             SC13G_ACC)
)


# ── Atom parsing: whitelist, roles, attribution ─────────────────────────────

def test_parse_keeps_only_event_forms_for_watchlist_ciks():
    articles, raw = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    assert raw == 9
    got = {(a["_meta"]["edgar_form"], a["_ticker"]) for a in articles}
    assert got == {("4", "NVDA"), ("8-K", "RKLB"), ("SC 13D", "SMLR"),
                   ("SC 13G/A", "NVDA")}


def test_form4_article_schema_and_pit_timestamp():
    articles, _ = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    art = next(a for a in articles if a["_meta"]["edgar_form"] == "4")
    assert art["_channel"] == "sec_edgar"
    assert art["_trust"] == 3
    assert art["_source_name"] == "SEC EDGAR"
    assert art["_source_domain"] == "sec.gov"
    assert art["_ticker"] == "NVDA" and art["_tickers"] == ["NVDA"]
    assert art["url"].endswith(f"{FORM4_ACC}-index.htm")
    # 17:02:41-04:00 → 21:02:41 UTC. Must NOT depend on the local timezone.
    assert art["published"] == "2026-07-18T21:02:41+00:00"
    # Issuer row wins; person name grafted from the paired Reporting row.
    assert "SEC Form 4" in art["title"]
    assert "NVIDIA CORP" in art["title"]
    assert "Kress Colette" in art["title"]
    assert FORM4_ACC in art["summary"]


def test_one_article_per_accession_and_ticker():
    """Accession dedup is keyed by (accession, ticker), not accession alone —
    the paired Issuer/Reporting rows of one Form 4 collapse to one article,
    while a co-registrant accession still surfaces for each watchlist company
    (see test_co_registrant_accession_surfaces_for_both_tickers)."""
    articles, _ = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    keys = [(a["_meta"]["edgar_accession"], a["_ticker"]) for a in articles]
    assert len(keys) == len(set(keys))
    # The Form 4 Reporting row and the 13D Filed-by row must not emit articles
    assert all(a["_meta"]["edgar_cik"] in CIK_MAP for a in articles)


def test_co_registrant_accession_surfaces_for_both_tickers():
    """One 8-K filed jointly by two watchlist companies (e.g. merger parties)
    shares a single accession. Keying dedup on accession ALONE silently
    dropped one of them — both must reach their own ticker."""
    shared_acc = "0001045810-26-000777"
    feed = _atom(
        _entry("8-K - NVIDIA CORP (0001045810) (Filer)", "8-K", shared_acc)
        + _entry("8-K - Rocket Lab Corp (0001819994) (Filer)", "8-K", shared_acc)
    )
    articles, _ = _parse_edgar_atom(feed, CIK_MAP)
    assert {a["_ticker"] for a in articles} == {"NVDA", "RKLB"}
    assert all(a["_meta"]["edgar_accession"] == shared_acc for a in articles)


def test_13d_title_names_the_acquiring_fund():
    articles, _ = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    art = next(a for a in articles if a["_meta"]["edgar_form"] == "SC 13D")
    assert "Semler Scientific" in art["title"]
    assert "Big Activist Fund LP" in art["title"]


def test_amended_13g_marked_amended():
    articles, _ = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    art = next(a for a in articles if a["_meta"]["edgar_form"] == "SC 13G/A")
    assert "(amended)" in art["title"]
    assert "passive stake" in art["title"]


# ── CIK map building ────────────────────────────────────────────────────────

def test_build_cik_map_normalizes_share_classes_and_filters_watchlist():
    company_tickers = {
        "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        "1": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
        "2": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."},
        "3": {"cik_str": 1067983, "ticker": "BRK-B", "title": "Berkshire Hathaway"},
        "4": {"cik_str": 789019, "ticker": "MSFT", "title": "Microsoft"},
    }
    cik_map = _build_cik_to_ticker(
        company_tickers, ["AAPL", "GOOGL", "GOOG", "BRK.B"]
    )
    # SEC's BRK-B matches the watchlist's BRK.B, emitted in watchlist form
    assert cik_map[1067983] == "BRK.B"
    # shared-CIK collision: first file entry wins, one ticker per CIK
    assert cik_map[1652044] == "GOOGL"
    assert cik_map[320193] == "AAPL"
    # not on the watchlist → excluded
    assert 789019 not in cik_map


# ── Event-type mapping (tax_v7 patterns must survive rolling re-class) ──────

def test_synthetic_titles_classify_to_prescribed_event_types():
    assert "insider_activity" in classify_events(
        _edgar_title("4", "NVIDIA CORP", "NVDA", "Kress Colette"), "")
    assert "regulatory" in classify_events(
        _edgar_title("8-K", "Rocket Lab Corp", "RKLB", ""), "")
    assert "ma_activity" in classify_events(
        _edgar_title("SC 13D", "Semler Scientific, Inc.", "SMLR",
                     "Big Activist Fund LP"), "")
    assert "ma_activity" in classify_events(
        _edgar_title("SC 13G/A", "NVIDIA CORP", "NVDA", ""), "")


# ── quality_filter integration ──────────────────────────────────────────────

def _mk_edgar_article(hours_ago=2.0, form="4"):
    published = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)) \
        .replace(microsecond=0).isoformat()
    title = _edgar_title(form, "NVIDIA CORP", "NVDA", "Kress Colette")
    return {
        "id": "test-id",
        "title": title,
        "url": "https://www.sec.gov/Archives/edgar/data/0001045810-26-000101-index.htm",
        "_source_name": "SEC EDGAR",
        "_source_domain": "sec.gov",
        "_trust": 3,
        "_channel": "sec_edgar",
        "_ticker": "NVDA",
        "_tickers": ["NVDA"],
        "published": published,
        "summary": f"{form} accepted {published}, accession 0001045810-26-000101. "
                   f"EDGAR: 4 - NVIDIA CORP (0001045810) (Issuer)",
        "_meta": {"edgar_form": form, "edgar_accession": "0001045810-26-000101",
                  "edgar_cik": 1045810},
    }


def test_quality_filter_accepts_edgar_article_via_relevance_floor():
    """The synthetic title barely mentions the ticker, so relevance_score
    would gate it out — the authoritative-CIK bypass must carry it through,
    and the weak-signal insider patterns must not kill it."""
    passed = quality_filter([_mk_edgar_article()], "NVDA", hours=48)
    assert len(passed) == 1
    assert passed[0]["_relevance"] >= 0.55
    assert passed[0]["_channel"] == "sec_edgar"


def test_quality_filter_drops_stale_edgar_article():
    passed = quality_filter([_mk_edgar_article(hours_ago=400)], "NVDA", hours=48)
    assert passed == []


# ── dedup: distinct filings must never collapse ─────────────────────────────

def _mk_form4(insider: str, url: str, company="Natera, Inc.", ticker="NTRA"):
    return {
        "title": _edgar_title("4", company, ticker, insider),
        "url": url,
        "_source_name": "SEC EDGAR",
        "_source_domain": "sec.gov",
        "_trust": 3,
        "_relevance": 0.55,
        "_channel": "sec_edgar",
        "_ticker": ticker,
    }


def test_same_day_form4s_by_different_insiders_survive_dedup():
    """Observed 2026-07-20: two Natera Form 4s filed seconds apart by different
    insiders collapsed to one. SimHash kept them apart (hamming 14 > 8) but the
    fuzzy pass merged them (token_set_ratio 88 >= 85) — the shared synthetic
    boilerplate dominates the token set, so the insider name carries too little
    weight. Each accession is a distinct legal filing, and "N insiders sold
    today" is a materially stronger signal than one, so the magnitude must
    survive."""
    articles = [
        _mk_form4("Sheena Jonathan", "https://sec.gov/a/0001-26-01-index.htm"),
        _mk_form4("Chapman Rowan E", "https://sec.gov/a/0001-26-02-index.htm"),
    ]
    assert len(dedup_articles(articles)) == 2


def test_dedup_exemption_does_not_leak_to_other_channels():
    """The sec_edgar exemption must be channel-scoped: near-identical titles
    on a normal channel still cluster (that's the whole point of the fuzzy
    pass — paraphrased re-reports of one story)."""
    articles = [
        dict(_mk_form4("Sheena Jonathan", "https://x.com/1"), _channel="yahoo_finance_rss"),
        dict(_mk_form4("Chapman Rowan E", "https://x.com/2"), _channel="yahoo_finance_rss"),
    ]
    assert len(dedup_articles(articles)) == 1


def test_edgar_dedup_survives_full_quality_filter():
    """End-to-end: both filings must still be there after the whole pipeline
    (quality_filter runs dedup internally)."""
    now = datetime.now(timezone.utc) - timedelta(hours=2)
    published = now.replace(microsecond=0).isoformat()
    articles = []
    for insider, n in (("Sheena Jonathan", 1), ("Chapman Rowan E", 2)):
        a = _mk_form4(insider, f"https://sec.gov/a/0001-26-0{n}-index.htm")
        a["published"] = published
        a["summary"] = f"4 accepted {published}, accession 0001554859-26-00{n}."
        articles.append(a)
    passed = quality_filter(articles, "NTRA", hours=48)
    assert len(passed) == 2


def test_fulltext_selector_skips_edgar_index_pages():
    """trust=3 + relevance 0.55 would normally select for fulltext, but EDGAR
    links are filing *index* pages — extraction would return table junk."""
    art = _mk_edgar_article()
    art["_relevance"] = 0.55
    assert select_articles_for_fulltext([art], mode="high-value") == []


# ── NewsItem export ─────────────────────────────────────────────────────────

def test_to_news_item_edgar_fields():
    articles, _ = _parse_edgar_atom(SAMPLE_FEED, CIK_MAP)
    art = next(a for a in articles if a["_meta"]["edgar_form"] == "4")
    art["_relevance"] = 0.55
    item = to_news_item(art)
    assert item["trust_tier"] == 3
    assert item["source"] == "sec_edgar"
    assert item["ticker"] == "NVDA"
    assert item["timestamp_utc"] == "2026-07-18T21:02:41+00:00"
    assert "insider_activity" in (item["meta"]["event_types"] or [])
    assert item["meta"]["_channel"] == "sec_edgar"


# ── channel disable switch ──────────────────────────────────────────────────

async def test_env_disable_short_circuits(monkeypatch):
    """NEWSCRAWLER_SEC_EDGAR=0 must return before any HTTP/session use
    (session=None would explode otherwise)."""
    monkeypatch.setenv("NEWSCRAWLER_SEC_EDGAR", "0")
    assert await _fetch_edgar_filings(None, ["NVDA"]) == {}
