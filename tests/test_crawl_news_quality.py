from crawl_news import (
    AISTOCK200_TICKERS,
    AISTOCK500_TICKERS,
    BUILTIN_TICKER_SETS,
    DEFAULT_TICKER_SET,
    MEGA_WATCHLIST_TICKERS,
    SEMICONDUCTOR_TICKERS,
    build_sources,
    is_high_value_article,
    is_weak_signal,
    merge_articles_by_url,
    parse_tickers,
    quality_filter,
    relevance_score,
    select_articles_for_fulltext,
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
            "published": "2026-03-29T10:30:00+00:00",
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
            "published": "2026-03-29T10:31:00+00:00",
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
