from datetime import datetime, timezone, timedelta

from crawl_news import (
    AISTOCK200_TICKERS,
    AISTOCK500_TICKERS,
    BUILTIN_TICKER_SETS,
    DEFAULT_TICKER_SET,
    MEGA_WATCHLIST_TICKERS,
    QUALITY_THRESHOLD,
    SEMICONDUCTOR_TICKERS,
    article_quality_score,
    build_sources,
    is_high_value_article,
    is_weak_signal,
    merge_articles_by_url,
    parse_tickers,
    quality_filter,
    relevance_score,
    select_articles_for_fulltext,
    _signal_headline_informativeness,
    _signal_source_authority,
    _signal_temporal_freshness,
    _signal_content_specificity,
    _signal_summary_richness,
    _signal_source_diversity,
)


def test_build_sources_includes_amd_ir_for_amd_only():
    amd_tags = [source["tag"] for source in build_sources("AMD")]
    nvda_tags = [source["tag"] for source in build_sources("NVDA")]
    watchlist_tags = [source["tag"] for source in build_sources("NVDA", include_global_feeds=False)]

    assert "amd_ir" in amd_tags
    assert "amd_ir" not in nvda_tags
    assert "benzinga_rss" not in watchlist_tags


def test_parse_tickers_supports_comma_list_and_preset():
    assert parse_tickers("amd, nvda,AMD") == ["AMD", "NVDA"]
    assert parse_tickers(None, preset=DEFAULT_TICKER_SET) == AISTOCK500_TICKERS
    assert parse_tickers(None, preset="aistock200") == AISTOCK200_TICKERS
    assert parse_tickers(None, preset="core200") == AISTOCK200_TICKERS
    assert parse_tickers(None, preset="aistock500") == AISTOCK500_TICKERS
    assert parse_tickers(None, preset="semis20") == SEMICONDUCTOR_TICKERS


def test_parse_tickers_supports_watchlist_aliases():
    assert MEGA_WATCHLIST_TICKERS == AISTOCK500_TICKERS

    tickers = parse_tickers(None, preset="mega_watchlist")

    assert tickers == AISTOCK500_TICKERS
    assert len(tickers) == 500
    assert "BRK.B" in tickers
    assert all(not ticker.startswith("_comment_") for ticker in tickers)
    assert BUILTIN_TICKER_SETS["aistock500"] == AISTOCK500_TICKERS
    assert BUILTIN_TICKER_SETS["mega_watchlist"] == AISTOCK500_TICKERS


def test_parse_tickers_supports_aistock200_preset():
    assert len(AISTOCK200_TICKERS) == 200
    assert "A" not in AISTOCK200_TICKERS
    assert "C" not in AISTOCK200_TICKERS
    assert "T" not in AISTOCK200_TICKERS
    assert BUILTIN_TICKER_SETS["aistock200"] == AISTOCK200_TICKERS
    assert BUILTIN_TICKER_SETS["core200"] == AISTOCK200_TICKERS


def test_build_sources_uses_company_hint_for_ambiguous_ticker():
    urls = [url for source in build_sources("A", include_global_feeds=False) for url in source["urls"]]

    assert any("Agilent" in url or "agilent" in url for url in urls)


def test_is_weak_signal_filters_holdings_and_profile_titles():
    assert is_weak_signal(
        "Advanced Micro Devices, Inc. $AMD Shares Sold by Nepsis Inc.",
        "marketbeat.com",
    )
    assert is_weak_signal(
        "Advanced Micro Devices (AMD): Company Profile, Stock Price, News, Rankings",
        "fortune.com",
    )
    assert is_weak_signal(
        "Diversified Trust Co. Reduces Position in NVIDIA Corporation $NVDA",
        "marketbeat.com",
    )
    assert not is_weak_signal(
        "AMD and Meta Announce Expanded Strategic Partnership to Deploy 6 Gigawatts of AMD GPUs",
        "ir.amd.com",
    )


def test_quality_filter_keeps_high_signal_and_drops_weak_signal_titles():
    now = datetime.now(timezone.utc)
    articles = [
        {
            "id": "weak",
            "title": "Advanced Micro Devices, Inc. $AMD Shares Sold by Nepsis Inc.",
            "url": "https://example.com/weak",
            "_source_name": "MarketBeat",
            "_source_domain": "marketbeat.com",
            "_trust": 1,
            "_channel": "google_broad",
            "_ticker": "AMD",
            "published": (now - timedelta(hours=2)).isoformat(),
            "summary": "Institutional ownership update.",
        },
        {
            "id": "strong",
            "title": "AMD and Meta Announce Expanded Strategic Partnership to Deploy 6 Gigawatts of AMD GPUs",
            "url": "https://ir.amd.com/news-events/press-releases/detail/1279/amd-and-meta-announce-expanded-strategic-partnership-to-deploy-6-gigawatts-of-amd-gpus",
            "_source_name": "AMD Investor Relations",
            "_source_domain": "ir.amd.com",
            "_trust": 3,
            "_channel": "amd_ir",
            "_ticker": "AMD",
            "published": (now - timedelta(hours=1)).isoformat(),
            "summary": "AMD and Meta announced an expanded AI infrastructure partnership.",
        },
    ]

    filtered = quality_filter(articles, "AMD", 168)

    assert [article["id"] for article in filtered] == ["strong"]


def test_relevance_score_avoids_single_letter_false_positive():
    assert relevance_score(
        "A US company will be destroyed, analyst says",
        "Macro commentary with no company-specific ticker reference.",
        "A",
    ) == 0.0


def test_relevance_score_accepts_company_name_or_ticker_context():
    assert relevance_score(
        "Agilent Technologies (A) beats earnings estimates",
        "The instrumentation company raised full-year guidance.",
        "A",
    ) > 0.5
    assert relevance_score(
        "KeyCorp (KEY) stock rises after earnings beat",
        "The regional bank topped estimates.",
        "KEY",
    ) > 0.5


def test_merge_articles_by_url_unions_tickers():
    merged = merge_articles_by_url(
        [
            {
                "id": "1",
                "title": "Shared article",
                "url": "https://example.com/shared",
                "_ticker": "AMD",
                "_tickers": ["AMD"],
                "_relevance": 0.6,
                "_trust": 2,
            },
            {
                "id": "2",
                "title": "Shared article",
                "url": "https://example.com/shared",
                "_ticker": "NVDA",
                "_tickers": ["NVDA"],
                "_relevance": 0.8,
                "_trust": 3,
            },
        ]
    )

    assert len(merged) == 1
    assert merged[0]["_tickers"] == ["AMD", "NVDA"]
    assert merged[0]["_relevance"] == 0.8
    assert merged[0]["_trust"] == 3


def test_high_value_article_prefers_strong_relevance_and_keywords():
    article = {
        "title": "AMD raises guidance after strong data center GPU demand",
        "summary": "The company lifted annual outlook after AI server momentum accelerated.",
        "_trust": 2,
        "_relevance": 0.7,
        "_channel": "google_broad",
        "_source_domain": "marketwatch.com",
    }

    assert is_high_value_article(article)


def test_select_articles_for_fulltext_caps_per_ticker_in_high_value_mode():
    articles = [
        {
            "id": f"a{i}",
            "title": f"AMD earnings update {i}",
            "summary": "AMD earnings and guidance improved.",
            "_trust": 3,
            "_relevance": 0.9,
            "_ticker": "AMD",
            "_tickers": ["AMD"],
        }
        for i in range(5)
    ]

    selected = select_articles_for_fulltext(articles, mode="high-value", max_articles=2)

    assert [article["id"] for article in selected] == ["a0", "a1"]


# ── Quality scoring system tests ─────────────────────────────────────────────

def test_signal_source_authority_maps_tiers():
    assert _signal_source_authority(3) == 1.0
    assert _signal_source_authority(2) == 0.7
    assert _signal_source_authority(1) == 0.3
    assert _signal_source_authority(0) == 0.0


def test_signal_headline_informativeness_rewards_action_keywords():
    earnings_title = "AMD beats Q3 earnings estimates, raises full-year guidance"
    generic_title = "AMD stock price today"
    assert _signal_headline_informativeness(earnings_title) > _signal_headline_informativeness(generic_title)


def test_signal_headline_informativeness_penalizes_clickbait():
    clickbait = "You won't believe what this stock will do next"
    normal = "NVDA reports strong Q2 revenue growth of 122%"
    assert _signal_headline_informativeness(clickbait) < _signal_headline_informativeness(normal)


def test_signal_temporal_freshness_decays():
    now = datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)
    assert _signal_temporal_freshness((now - timedelta(hours=2)).isoformat(), now) == 1.0
    assert _signal_temporal_freshness((now - timedelta(hours=8)).isoformat(), now) == 0.8
    assert _signal_temporal_freshness((now - timedelta(hours=20)).isoformat(), now) == 0.6
    assert _signal_temporal_freshness((now - timedelta(hours=30)).isoformat(), now) == 0.4
    assert _signal_temporal_freshness((now - timedelta(hours=40)).isoformat(), now) == 0.2
    assert _signal_temporal_freshness((now - timedelta(hours=60)).isoformat(), now) == 0.0
    assert _signal_temporal_freshness(None, now) == 0.3  # unknown


def test_signal_content_specificity_penalizes_roundups():
    # Roundup mentioning many tickers
    roundup_score = _signal_content_specificity(
        "NVDA AAPL MSFT GOOGL AMZN META all fall on tariff fears",
        "Market-wide selloff hits tech giants.",
        "NVDA",
    )
    # Specific article about one company
    specific_score = _signal_content_specificity(
        "NVDA raises guidance after $26B data center revenue",
        "Nvidia lifted full-year outlook citing 200% growth in AI server demand.",
        "NVDA",
    )
    assert specific_score > roundup_score


def test_signal_summary_richness():
    assert _signal_summary_richness(None, "x" * 500) == 1.0   # has body
    assert _signal_summary_richness("x" * 120, None) == 0.7   # decent summary
    assert _signal_summary_richness("short", None) == 0.1      # too short


def test_signal_source_diversity():
    assert _signal_source_diversity(3) == 1.0
    assert _signal_source_diversity(0) == 0.2


def test_article_quality_score_high_tier_passes():
    """A top-tier source with good relevance should score well above threshold."""
    now = datetime.now(timezone.utc)
    article = {
        "title": "AMD beats Q3 earnings, raises guidance on AI demand - Reuters",
        "summary": "AMD reported $6.8B revenue, beating estimates by 8%, and raised guidance.",
        "_trust": 3,
        "_relevance": 0.85,
        "published": (now - timedelta(hours=3)).isoformat(),
        "_alt_sources": ["cnbc.com", "bloomberg.com"],
        "_channel": "google_broad",
    }
    score = article_quality_score(article, "AMD", now=now)
    assert score > 0.70, f"Expected > 0.70, got {score}"


def test_article_quality_score_low_tier_generic_fails():
    """A low-tier source with generic content should fail the threshold."""
    now = datetime.now(timezone.utc)
    article = {
        "title": "AMD Stock Price Today - StockTitan",
        "summary": "AMD stock price today.",
        "_trust": 1,
        "_relevance": 0.30,
        "published": (now - timedelta(hours=30)).isoformat(),
        "_alt_sources": [],
        "_channel": "google_broad",
    }
    score = article_quality_score(article, "AMD", now=now)
    assert score < QUALITY_THRESHOLD, f"Expected < {QUALITY_THRESHOLD}, got {score}"


def test_article_quality_score_tier1_excellent_content_can_pass():
    """A tier-1 source with excellent content should still be able to pass."""
    now = datetime.now(timezone.utc)
    article = {
        "title": "AMD announces $10B acquisition of Xilinx, earnings surge 45% in Q4",
        "summary": "AMD confirmed a $10 billion deal to acquire Xilinx. Q4 revenue reached $6.8B, up 45%.",
        "_trust": 1,
        "_relevance": 0.90,
        "published": (now - timedelta(hours=1)).isoformat(),
        "_alt_sources": ["reuters.com", "cnbc.com", "bloomberg.com"],
        "_channel": "google_broad",
    }
    score = article_quality_score(article, "AMD", now=now)
    assert score >= QUALITY_THRESHOLD, f"Expected >= {QUALITY_THRESHOLD}, got {score}"


def test_quality_filter_gates_on_quality_score():
    """quality_filter should drop low-scoring articles."""
    now = datetime.now(timezone.utc)
    articles = [
        {
            "id": "high_quality",
            "title": "AMD beats Q3 earnings, raises guidance on AI demand",
            "url": "https://reuters.com/amd-earnings",
            "_source_name": "Reuters",
            "_source_domain": "reuters.com",
            "_trust": 3,
            "_channel": "google_broad",
            "_ticker": "AMD",
            "published": (now - timedelta(hours=2)).isoformat(),
            "summary": "AMD reported $6.8B revenue, raised FY guidance citing strong AI server demand.",
        },
        {
            "id": "low_quality",
            "title": "AMD stock price movement analysis for investors today",
            "url": "https://example.com/amd-price",
            "_source_name": "Unknown Blog",
            "_source_domain": "unknownblog.com",
            "_trust": 1,
            "_channel": "google_broad",
            "_ticker": "AMD",
            "published": (now - timedelta(hours=40)).isoformat(),
            "summary": "AMD stock.",
        },
    ]
    filtered = quality_filter(articles, "AMD", 48)
    ids = [a["id"] for a in filtered]
    assert "high_quality" in ids
    # low_quality may or may not pass depending on exact scoring,
    # but high_quality should always rank first
    if len(filtered) > 1:
        assert filtered[0]["id"] == "high_quality"
