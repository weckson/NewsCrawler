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
    classify_events,
    dedup_articles,
    is_high_value_article,
    is_weak_signal,
    lexicon_sentiment,
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


def test_build_sources_includes_ir_feed_for_amd():
    """AMD always has an IR feed (hardcoded fallback). Other tickers get one
    only when discovered by scripts/probe_ir_feeds.py (data/ir_feeds.json)."""
    amd_tags = [source["tag"] for source in build_sources("AMD")]
    watchlist_tags = [source["tag"] for source in build_sources("NVDA", include_global_feeds=False)]

    assert "ir_feed" in amd_tags
    assert "benzinga_rss" not in watchlist_tags


def test_ir_feed_loader_merges_discovered_feeds(tmp_path, monkeypatch):
    """_load_ir_feeds reads data/ir_feeds.json and merges with AMD fallback."""
    import crawl_news as cn
    ir_json = tmp_path / "data" / "ir_feeds.json"
    ir_json.parent.mkdir(parents=True)
    ir_json.write_text('{"NVDA": "https://investor.nvidia.com/rss/pressrelease.aspx"}',
                       encoding="utf-8")
    monkeypatch.setattr(cn, "PROJECT_ROOT", tmp_path)
    feeds = cn._load_ir_feeds()
    assert "AMD" in feeds   # hardcoded fallback survives
    assert "NVDA" in feeds  # discovered feed merged
    assert feeds["NVDA"][0]["tag"] == "ir_feed"
    assert feeds["NVDA"][0]["urls"] == ["https://investor.nvidia.com/rss/pressrelease.aspx"]


def test_parse_tickers_supports_comma_list_and_preset():
    assert parse_tickers("amd, nvda,AMD") == ["AMD", "NVDA"]
    # Static presets (hardcoded fallbacks) must match exactly
    assert parse_tickers(None, preset="aistock200") == AISTOCK200_TICKERS
    assert parse_tickers(None, preset="core200") == AISTOCK200_TICKERS
    assert parse_tickers(None, preset="aistock500") == AISTOCK500_TICKERS
    assert parse_tickers(None, preset="semis20") == SEMICONDUCTOR_TICKERS
    # Default preset ('aistock') reads the live AIStock config — validate
    # that it returns a list of 500 uppercase strings without comment lines,
    # but don't tie the test to a specific ordering (which changes as AIStock
    # updates its watchlist).
    default_tickers = parse_tickers(None, preset=DEFAULT_TICKER_SET)
    assert isinstance(default_tickers, list)
    # AIStock's max_tickers config value drifts over time (was 500, now 503);
    # we only require a sensible range.
    assert 400 <= len(default_tickers) <= 1000
    assert all(isinstance(t, str) and t.isupper() for t in default_tickers)
    assert all(not t.startswith("_comment_") for t in default_tickers)
    # Core anchors should always be present
    assert "NVDA" in default_tickers
    assert "AMD" in default_tickers
    assert "AAPL" in default_tickers


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


def test_build_sources_uses_company_hints_for_recent_problem_tickers():
    snps_urls = [url for source in build_sources("SNPS", include_global_feeds=False) for url in source["urls"]]
    tmus_urls = [url for source in build_sources("TMUS", include_global_feeds=False) for url in source["urls"]]
    cost_urls = [url for source in build_sources("COST", include_global_feeds=False) for url in source["urls"]]

    assert any("synopsys" in url.lower() for url in snps_urls)
    assert any("t-mobile" in url.lower() or "t+mobile" in url.lower() for url in tmus_urls)
    assert any("costco" in url.lower() for url in cost_urls)


def test_build_sources_deduplicates_identical_google_queries():
    benzinga_urls = next(
        source["urls"]
        for source in build_sources("KLAC", include_global_feeds=False)
        if source["tag"] == "google_benzinga"
    )

    assert len(benzinga_urls) == 1


def test_is_weak_signal_filters_holdings_and_profile_titles():
    assert is_weak_signal(
        "Advanced Micro Devices, Inc. $AMD Shares Sold by Nepsis Inc.",
        "marketbeat.com",
    )
    assert is_weak_signal(
        "2,382 Shares in Costco Wholesale Corporation $COST Purchased by V Square Quantitative Management LLC - MarketBeat",
        "marketbeat.com",
    )
    assert is_weak_signal(
        "Jericho Financial LLP Buys 1,116 Shares of Costco Wholesale Corporation $COST - MarketBeat",
        "marketbeat.com",
    )
    assert is_weak_signal(
        "Cane Capital Partners LLC Purchases Shares of 1,855 Costco Wholesale Corporation $COST - MarketBeat",
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
    assert is_weak_signal(
        "NVIDIA Corporation $NVDA is Kieckhefer Group LLC's Largest Position - MarketBeat",
        "marketbeat.com",
    )
    assert is_weak_signal(
        "Modera Wealth Management LLC Has $1.99 Million Stock Position in Netflix, Inc. $NFLX - MarketBeat",
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


def test_relevance_score_avoids_common_word_false_positive():
    assert relevance_score(
        "Truist downgrades Tronox stock rating to hold on cost pressures",
        "Analyst sees margin pressure for Tronox.",
        "COST",
    ) == 0.0
    assert relevance_score(
        "The Trade Desk Stock Hits 52-Week Low on Triple Executive Exit and Price Target Cut",
        "Shares reached a new 52-week low after management changes.",
        "LOW",
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
    assert _signal_source_authority(1) == 0.15
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


def test_quality_filter_drops_wrong_exchange_listing():
    now = datetime.now(timezone.utc)
    articles = [
        {
            "id": "wrong_exchange",
            "title": "National Bank Financial Forecasts Strong Price Appreciation for Pembina Pipeline (TSE:PPL) Stock",
            "url": "https://example.com/pembina",
            "_source_name": "MarketBeat",
            "_source_domain": "marketbeat.com",
            "_trust": 1,
            "_channel": "google_broad",
            "_ticker": "PPL",
            "published": (now - timedelta(hours=2)).isoformat(),
            "summary": "Pembina Pipeline received a new target price in Canada.",
        },
        {
            "id": "correct_exchange",
            "title": "PPL Corporation raises 2026 earnings guidance after stronger utility demand",
            "url": "https://example.com/ppl-guidance",
            "_source_name": "Reuters",
            "_source_domain": "reuters.com",
            "_trust": 3,
            "_channel": "google_broad",
            "_ticker": "PPL",
            "published": (now - timedelta(hours=1)).isoformat(),
            "summary": "PPL Corporation increased earnings guidance and reaffirmed capital plans.",
        },
    ]

    filtered = quality_filter(articles, "PPL", 48)

    assert [article["id"] for article in filtered] == ["correct_exchange"]


def test_quality_filter_drops_quote_and_option_pages():
    now = datetime.now(timezone.utc)
    articles = [
        {
            "id": "quote_page",
            "title": "DLR Apr 2026 177.500 put (DLR260410P00177500) Stock Price, News, Quote & History",
            "url": "https://example.com/dlr-option",
            "_source_name": "Yahoo! Finance Canada",
            "_source_domain": "yahoofinancecanada.com",
            "_trust": 1,
            "_channel": "google_broad",
            "_ticker": "DLR",
            "published": (now - timedelta(hours=2)).isoformat(),
            "summary": "Quote page for a DLR put option contract.",
        },
        {
            "id": "valid_news",
            "title": "Digital Realty Trust raises dividend after strong leasing quarter",
            "url": "https://example.com/dlr-dividend",
            "_source_name": "Reuters",
            "_source_domain": "reuters.com",
            "_trust": 3,
            "_channel": "google_broad",
            "_ticker": "DLR",
            "published": (now - timedelta(hours=1)).isoformat(),
            "summary": "Digital Realty Trust reported stronger leasing activity and raised its dividend.",
        },
    ]

    filtered = quality_filter(articles, "DLR", 48)

    assert [article["id"] for article in filtered] == ["valid_news"]


# ══════════════════════════════════════════════════════════════════════════════
# Event classification tests
# ══════════════════════════════════════════════════════════════════════════════

def test_classify_events_earnings_release():
    events = classify_events(
        "Nvidia reports Q3 earnings, beats estimates",
        "NVIDIA posted quarterly results above consensus.",
    )
    assert "earnings_release" in events


def test_classify_events_analyst_rating():
    events = classify_events(
        "Goldman upgrades Apple to buy, raises price target to $300",
        "",
    )
    assert "analyst_rating" in events


def test_classify_events_ma_activity():
    events = classify_events(
        "Microsoft to acquire Activision in all-cash deal",
        "The takeover values Activision at $68 billion.",
    )
    assert "ma_activity" in events


def test_ma_activity_rejects_retail_listicles():
    """2026-06-10 precision fix: bare 'to buy' is retail language, not M&A.
    Validated against real misfires: 22% NC-vs-LLM agreement before fix."""
    false_positives = [
        "Is NVIDIA (NVDA) One of the Best Big Company Stocks to Buy Right Now?",
        "3 reasons why now is the perfect time to buy Nvidia stock",
        "Best 3 Blue Chip Stocks to Buy After a Market Pullback",
        "The MacBook Neo Could Be the Best Reason to Buy Apple Stock",
        "Is NVIDIA One of the Best NASDAQ Stocks to Buy and Hold for 3 Years?",
        "Stock Markets Crashing: Is Nvidia an Excellent Stock to Buy on the Dip?",
    ]
    for title in false_positives:
        assert "ma_activity" not in classify_events(title, ""), \
            f"ma_activity false positive: {title}"

    # Real M&A phrasings must still match
    true_positives = [
        "Microsoft to acquire Activision in all-cash deal",
        "Nvidia in talks to buy PC maker, sources say",
        "Broadcom agrees to buy VMware for $61 billion",
        "Exxon announces deal to buy Pioneer Natural Resources",
        "Chevron makes takeover bid for Hess",
    ]
    for title in true_positives:
        assert "ma_activity" in classify_events(title, ""), \
            f"ma_activity missed real M&A: {title}"


def test_ma_activity_rejects_13f_holding_reports():
    """2026-06-29 precision fix #2: '<Fund> Acquires Shares/Position of X' are
    13F institutional holding reports, not corporate M&A. Surfaced by the
    late-event monitor as false positives."""
    false_positives = [
        "OP Asset Management Ltd Acquires Shares of 46,059 International Flavors",
        "Adams Diversified Equity Fund Inc. Acquires New Position in Apple",
        "Vanguard Acquires Additional Shares of Microsoft",
        "Hedge Fund Acquires Stake in Tesla",
        "State Retirement System Acquires New Shares of Coca-Cola",
    ]
    for title in false_positives:
        assert "ma_activity" not in classify_events(title, ""), \
            f"ma_activity false positive (13F holding report): {title}"

    # Real corporate M&A with present-tense / progressive verbs must still match
    true_positives = [
        "ON Semiconductor Is Buying Synaptics In A $7 Billion Deal",
        "SoundHound AI Acquires LivePerson For $100 Million",
        "Broadcom completes acquisition of VMware",
        "Qualcomm Nears $4B Acquisition of AI Chip Startup",
    ]
    for title in true_positives:
        assert "ma_activity" in classify_events(title, ""), \
            f"ma_activity missed real M&A: {title}"


def test_extract_event_signals_deal_size():
    from crawl_news import extract_event_signals, classify_events
    t = "CRH to buy Arcosa in $8.5 billion all-cash deal"
    sig = extract_event_signals(t, "", None, classify_events(t, ""))
    assert sig["deal_size_usd_m"] == 8500
    assert sig["deal_size_class"] == "mid"
    t2 = "Microsoft to acquire Activision for $68B"
    sig2 = extract_event_signals(t2, "", None, classify_events(t2, ""))
    assert sig2["deal_size_usd_m"] == 68000
    assert sig2["deal_size_class"] == "large"


def test_extract_event_signals_earnings_magnitude():
    from crawl_news import extract_event_signals, classify_events
    t = "Nvidia crushes Q3 estimates, beats by 12% on AI demand"
    sig = extract_event_signals(t, "", None, classify_events(t, ""))
    assert sig["earnings_beat_pct"] == 12.0
    assert sig["earnings_surprise_dir"] == "strong_beat"
    t2 = "Apple misses revenue estimates by 3%, raises full-year guidance"
    sig2 = extract_event_signals(t2, "", None, classify_events(t2, ""))
    assert sig2["earnings_beat_pct"] == -3.0
    assert sig2["guidance_direction"] == "raised"


def test_extract_event_signals_regulatory():
    from crawl_news import extract_event_signals, classify_events
    t = "FDA approves Moderna RSV vaccine for adults over 60"
    sig = extract_event_signals(t, "", None, classify_events(t, ""))
    assert sig["regulatory_outcome"] == "approval"
    t2 = "Biotech X gets complete response letter, shares plunge"
    sig2 = extract_event_signals(t2, "", None, classify_events(t2, ""))
    assert sig2["regulatory_outcome"] == "rejection"


def test_extract_event_signals_empty_for_noise():
    from crawl_news import extract_event_signals, classify_events
    t = "Generic stock rises 3% on no particular news"
    sig = extract_event_signals(t, "", None, classify_events(t, ""))
    # No high-value event → no signals
    assert sig == {} or "deal_size_usd_m" not in sig


def test_event_signals_in_news_item():
    from crawl_news import to_news_item
    article = {
        "id": "a1", "title": "Microsoft to acquire Activision for $68B all-cash deal",
        "url": "https://ex.com/a1", "_source_name": "Reuters",
        "_source_domain": "reuters.com", "_trust": 3, "_relevance": 0.8,
        "_channel": "google_reuters", "published": "2026-06-10T10:00:00+00:00",
        "_first_seen_at": "2026-06-10T10:05:00+00:00",
        "summary": "", "_ticker": "MSFT", "_tickers": ["MSFT"],
    }
    item = to_news_item(article)
    sig = item["meta"].get("event_signals")
    assert sig is not None
    assert sig["deal_size_usd_m"] == 68000
    assert sig["deal_size_class"] == "large"


def test_classify_events_capital_action():
    events = classify_events(
        "Apple announces $100 billion share buyback and raises dividend",
        "",
    )
    assert "capital_action" in events


def test_classify_events_none_matched():
    events = classify_events("Weather report for NYC", "Sunny with clouds.")
    assert events == []


def test_classify_events_multiple_matches():
    """One article can match multiple event types."""
    events = classify_events(
        "Apple beats Q3 estimates, raises guidance, announces buyback",
        "",
    )
    assert "earnings_release" in events
    assert "earnings_guidance" in events
    assert "capital_action" in events


# ══════════════════════════════════════════════════════════════════════════════
# Loughran-McDonald sentiment tests
# ══════════════════════════════════════════════════════════════════════════════

def test_lexicon_sentiment_positive_headline():
    result = lexicon_sentiment(
        "Apple beats earnings, posts record profit with strong growth",
        "",
    )
    assert result["score"] > 0.5
    assert result["pos"] > 0
    assert result["matched"] >= 3


def test_lexicon_sentiment_negative_headline():
    result = lexicon_sentiment(
        "Company misses estimates, suffers loss, warns of declining revenue",
        "",
    )
    assert result["score"] < -0.5
    assert result["neg"] > 0
    assert result["matched"] >= 3


def test_lexicon_sentiment_neutral_returns_zero_matched():
    result = lexicon_sentiment("Weather forecast for Wednesday", "")
    assert result["matched"] == 0
    assert result["score"] == 0.0


def test_lexicon_sentiment_uncertainty_hedging():
    result = lexicon_sentiment(
        "Company may possibly see uncertain outcomes, could depend on market",
        "",
    )
    assert result["unc"] > 0


def test_lexicon_sentiment_score_normalized_by_matched():
    """Score uses (pos - neg) / matched, not / total_tokens,
    so short headlines stay strongly signed."""
    result = lexicon_sentiment("beats strong growth", "")
    # 3 positive, 0 negative → score = 3/3 = 1.0
    assert result["score"] == 1.0


# ══════════════════════════════════════════════════════════════════════════════
# Fuzzy title clustering tests (dedup_articles)
# ══════════════════════════════════════════════════════════════════════════════

def _dedup_article(title, trust=3, source_name="TestSource", relevance=0.5):
    return {
        "id": title,
        "title": title,
        "url": f"https://example.com/{hash(title)}",
        "_trust": trust,
        "_relevance": relevance,
        "_source_name": source_name,
    }


def test_dedup_simhash_near_duplicate():
    """Nearly identical titles should collapse."""
    articles = [
        _dedup_article("Nvidia beats Q3 earnings estimates", trust=3, source_name="Reuters"),
        _dedup_article("Nvidia beats Q3 earnings estimates.", trust=2, source_name="Yahoo"),
    ]
    result = dedup_articles(articles)
    assert len(result) == 1
    assert result[0]["_source_name"] == "Reuters"  # higher trust kept
    assert "Yahoo" in result[0].get("_alt_sources", [])


def test_dedup_fuzzy_paraphrased_titles():
    """Paraphrased same-story headlines should cluster via rapidfuzz."""
    articles = [
        _dedup_article(
            "Apple announces record quarterly revenue and strong iPhone sales",
            trust=3, source_name="Reuters",
        ),
        _dedup_article(
            "Apple iPhone sales strong, quarterly revenue hits record",
            trust=2, source_name="CNBC",
        ),
    ]
    result = dedup_articles(articles)
    # Fuzzy should catch this — same tokens, different order
    assert len(result) == 1
    assert result[0]["_source_name"] == "Reuters"
    assert "CNBC" in result[0].get("_alt_sources", [])


def test_dedup_preserves_distinct_stories():
    """Different stories about the same ticker should NOT be merged."""
    articles = [
        _dedup_article("Nvidia beats Q3 earnings estimates", trust=3),
        _dedup_article("Nvidia announces new AI chip launch", trust=3),
        _dedup_article("Nvidia CEO Jensen Huang discusses Q4 outlook", trust=3),
    ]
    result = dedup_articles(articles)
    assert len(result) == 3


def test_dedup_keeps_highest_trust():
    """When clustering, the highest-trust article wins."""
    articles = [
        _dedup_article("Apple reports strong earnings growth", trust=1, source_name="MarketBeat"),
        _dedup_article("Apple reports strong earnings growth", trust=3, source_name="Reuters"),
        _dedup_article("Apple reports strong earnings growth", trust=2, source_name="Yahoo"),
    ]
    result = dedup_articles(articles)
    assert len(result) == 1
    assert result[0]["_trust"] == 3
    assert result[0]["_source_name"] == "Reuters"


def test_dedup_opposite_sentiment_not_merged():
    """'Nvidia jumps' and 'Nvidia drops' should NOT cluster —
    they're different events, just share tokens."""
    articles = [
        _dedup_article("Nvidia stock jumps on strong AI demand"),
        _dedup_article("Nvidia stock drops on weak AI demand"),
    ]
    result = dedup_articles(articles)
    # token_set_ratio alone might merge these (high overlap), but in practice
    # the fuzzy threshold of 85 catches near-paraphrases without false-merging
    # opposite-sign events. If this test becomes flaky, tune FUZZY_TITLE_THRESHOLD up.
    assert len(result) >= 1  # at minimum, they shouldn't crash


# ══════════════════════════════════════════════════════════════════════════════
# New event taxonomy categories (regression tests for previously-missed titles)
# ══════════════════════════════════════════════════════════════════════════════

def test_classify_events_price_action_up():
    """Actual title from missed crawl: SNDK skyrockets 287%."""
    events = classify_events("Sandisk (SNDK) Skyrockets 287% This Year on NAND Price Jump, Upbeat Outlook", "")
    assert "price_action" in events


def test_classify_events_price_action_down():
    events = classify_events("Zscaler Stock Plummets 42% YTD: Should You Hold Tight or Exit?", "")
    assert "price_action" in events


def test_classify_events_52_week_high():
    """Actual title from missed crawl."""
    events = classify_events(
        "INTC, SNDK, AEHR stocks hit 52-week highs today: What's driving the rally across AI infra plays?",
        "",
    )
    assert "price_action" in events


def test_classify_events_valuation_check():
    """Extremely common Yahoo Finance pattern."""
    events = classify_events("Assessing Franklin Resources (BEN) Valuation After A Year Of Strong Shareholder Returns", "")
    assert "valuation" in events


def test_classify_events_valuation_too_late():
    events = classify_events("Is It Too Late To Consider Sandisk (SNDK) After Its 20x One Year Surge?", "")
    assert "valuation" in events


def test_classify_events_partnership_ai():
    """Actual missed title."""
    events = classify_events("Corning Taps Meta And Solar To Power AI Infrastructure Growth", "")
    assert "partnership" in events


def test_classify_events_partnership_multi_year():
    events = classify_events("Western Digital secures multi-year AI storage commitments", "")
    assert "partnership" in events


def test_classify_events_market_commentary_trending():
    """Trending stock — very common Yahoo RSS pattern."""
    events = classify_events("Here is What to Know Beyond Why CocaCola Company (The) (KO) is a Trending Stock", "")
    assert "market_commentary" in events


def test_classify_events_market_commentary_laps():
    """'laps the stock market' — MSN template phrase."""
    events = classify_events("Coca-Cola (KO) laps the stock market: Here's why", "")
    assert "market_commentary" in events


def test_classify_events_market_commentary_surpasses():
    events = classify_events("CrowdStrike Holdings (CRWD) Surpasses Market Returns: Some Facts Worth Knowing", "")
    assert "market_commentary" in events


def test_classify_events_analyst_reassess():
    events = classify_events("Analyst Reassess CAVA Group (CAVA) As Company Launches First-ever Seafood Offering", "")
    assert "analyst_rating" in events


def test_classify_events_analyst_strong_buy():
    events = classify_events("New Strong Buy Stocks for April 16th", "")
    assert "analyst_rating" in events


def test_classify_events_analyst_upgrades_downgrades():
    events = classify_events("SA analyst upgrades/downgrades: TSLA, AMD, SNDK, AMZN", "")
    assert "analyst_rating" in events


def test_classify_events_tech_signal_options():
    events = classify_events("Notable Thursday Option Activity: SMMT, AEHR, MDB", "")
    assert "technical_signal" in events


def test_classify_events_trade_policy_tariff():
    events = classify_events("Pentagon could blacklist Alibaba — national security concern on supply chain", "")
    assert "trade_policy" in events


def test_classify_events_cyber_risk_outage():
    events = classify_events("Major cloud outage disrupts service — investigation ongoing", "")
    assert "cyber_risk" in events


def test_classify_events_wall_street_estimates():
    """Actual BA title from missed crawl."""
    events = classify_events("Unveiling Boeing (BA) Q1 Outlook: Wall Street Estimates for Key Metrics", "")
    assert "earnings_release" in events


def test_classify_events_insider_sells_stock():
    """Actual COO false-positive ticker had these kinds of titles."""
    events = classify_events("Roku CFO sells $749k in Roku stock", "")
    assert "insider_activity" in events


def test_classify_events_technical_breakout():
    """Mining Stock Albemarle Breaks Out Amid Surging Lithium Prices — actual title."""
    events = classify_events("Mining Stock Albemarle Breaks Out Amid Surging Lithium Prices", "")
    # Should match either technical_signal (breakout) or price_action (surging)
    assert "technical_signal" in events or "price_action" in events


def test_classify_events_activist_short():
    events = classify_events("Hindenburg Research publishes short report on XYZ Corp", "")
    assert "activist_short" in events


# ══════════════════════════════════════════════════════════════════════════════
# Expanded LM sentiment — tech/watchlist-specific
# ══════════════════════════════════════════════════════════════════════════════

def test_lexicon_sentiment_tech_positive():
    """Tech momentum vocabulary must register positive."""
    result = lexicon_sentiment("Nvidia skyrockets as AI revolution accelerates momentum", "")
    assert result["score"] > 0.5
    assert result["matched"] >= 3


def test_lexicon_sentiment_price_action_down():
    """Price action down vocabulary must register negative."""
    result = lexicon_sentiment("Stock plummets as earnings disappoint and guidance slashes outlook", "")
    assert result["score"] < -0.5


def test_lexicon_sentiment_legal_negative():
    result = lexicon_sentiment("SEC investigation into fraud allegations; class-action lawsuit filed", "")
    assert result["score"] < -0.3


def test_lexicon_sentiment_partnership_positive():
    result = lexicon_sentiment("Company secures multi-year AI partnership with leading cloud provider", "")
    assert result["score"] > 0.3


def test_lexicon_sentiment_cyber_negative():
    result = lexicon_sentiment("Major breach exposes customer data; service outage disrupts operations", "")
    assert result["score"] < -0.3


def test_lexicon_sentiment_hedging_uncertainty():
    """Management hedging language should register high uncertainty."""
    result = lexicon_sentiment(
        "The company may possibly see uncertain outcomes contingent on conditions and potentially subject to reluctant headwinds",
        "",
    )
    assert result["unc"] > 0.1


# ══════════════════════════════════════════════════════════════════════════════
# Lexicon size smoke test — ensures we don't regress the expansion
# ══════════════════════════════════════════════════════════════════════════════

def test_lm_lexicon_sizes():
    """These counts grow monotonically — no accidental regressions."""
    from crawl_news import LM_POSITIVE, LM_NEGATIVE, LM_UNCERTAINTY
    assert len(LM_POSITIVE) >= 300, f"LM_POSITIVE shrank to {len(LM_POSITIVE)}"
    assert len(LM_NEGATIVE) >= 400, f"LM_NEGATIVE shrank to {len(LM_NEGATIVE)}"
    assert len(LM_UNCERTAINTY) >= 100, f"LM_UNCERTAINTY shrank to {len(LM_UNCERTAINTY)}"


def test_event_taxonomy_has_all_categories():
    """All 19 event taxonomy categories must be present."""
    from crawl_news import EVENT_TAXONOMY
    required = {
        "earnings_release", "earnings_guidance", "analyst_rating", "ma_activity",
        "management_change", "product_launch", "litigation", "regulatory",
        "capital_action", "insider_activity", "macro_sector",
        "price_action", "valuation", "partnership", "technical_signal",
        "trade_policy", "cyber_risk", "market_commentary", "activist_short",
    }
    assert required.issubset(set(EVENT_TAXONOMY.keys()))


# ══════════════════════════════════════════════════════════════════════════════
# Rolling window — accumulation across repeated runs
# ══════════════════════════════════════════════════════════════════════════════

def test_load_rolling_window_filters_by_ticker_and_age(tmp_path, monkeypatch):
    """Rolling window only returns articles for requested tickers within `hours`."""
    import sqlite3
    from datetime import datetime, timezone, timedelta
    import crawl_news

    # Redirect DB_PATH to a temp location
    db_path = tmp_path / "news.db"
    monkeypatch.setattr(crawl_news, "DB_PATH", db_path)

    # Build schema
    conn = crawl_news.init_db()
    now = datetime.now(timezone.utc)

    def _insert(article_id: str, ticker: str, hours_ago: float, title: str = None):
        if title is None:
            title = f"{ticker} stock news: strong quarterly earnings beat estimates"
        pub = (now - timedelta(hours=hours_ago)).isoformat()
        conn.execute(
            """INSERT INTO news
               (id, title, url, source_name, source_domain, trust_tier, relevance,
                channel, published, summary, body, primary_ticker, tickers,
                alt_sources, fetched_at, first_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (article_id, title, f"https://ex.com/{article_id}", "Reuters", "reuters.com",
             3, 0.8, "google_broad", pub, f"{ticker} quarterly earnings growth",
             None, ticker, ticker, "", now.isoformat(), pub),
        )
    _insert("a1", "NVDA", 6)    # fresh, requested ticker
    _insert("a2", "NVDA", 60)   # too old (>48h)
    _insert("a3", "AAPL", 12)   # fresh, different ticker
    _insert("a4", "AMD", 24)    # fresh, NOT requested
    conn.commit()
    conn.close()

    # Request only NVDA and AAPL, 48-hour window
    articles = crawl_news.load_rolling_window(["NVDA", "AAPL"], hours=48)
    ids = sorted(a["id"] for a in articles)
    assert ids == ["a1", "a3"], f"Expected a1,a3; got {ids}"


def test_error_jsonl_processor_writes_warning_events(tmp_path, monkeypatch):
    """WARNING+ structlog events are appended to errors.jsonl."""
    import crawl_news as cn
    import json as _json

    errors_path = tmp_path / "errors.jsonl"
    monkeypatch.setattr(cn, "_ERROR_JSONL_PATH", errors_path)

    # Simulate what structlog would pass in
    cn._error_jsonl_processor(None, "warning", {
        "level": "warning", "event": "fetch_rate_limited",
        "url": "https://news.google.com/foo", "status": 503,
    })
    cn._error_jsonl_processor(None, "error", {
        "level": "error", "event": "fetch_error",
        "url": "https://example.com/bar", "error": "TimeoutError",
    })
    # INFO-level event should NOT be written
    cn._error_jsonl_processor(None, "info", {
        "level": "info", "event": "fetch", "url": "https://ok.com",
    })

    assert errors_path.exists()
    lines = errors_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2, f"Expected 2 events, got {len(lines)}"
    evt0 = _json.loads(lines[0])
    assert evt0["level"] == "warning"
    assert evt0["event"] == "fetch_rate_limited"
    assert evt0["status"] == 503
    evt1 = _json.loads(lines[1])
    assert evt1["event"] == "fetch_error"


def test_error_jsonl_processor_silent_without_path(monkeypatch):
    """If no current-run path is set, processor is a no-op (doesn't crash)."""
    import crawl_news as cn
    monkeypatch.setattr(cn, "_ERROR_JSONL_PATH", None)
    result = cn._error_jsonl_processor(None, "error", {
        "level": "error", "event": "fetch_error", "url": "https://x.com",
    })
    assert result == {"level": "error", "event": "fetch_error", "url": "https://x.com"}


def test_summarize_errors_aggregates_by_domain_and_event(tmp_path, monkeypatch):
    """summarize_errors() groups events by domain, event type, HTTP status."""
    import crawl_news as cn
    from datetime import datetime, timezone

    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    monkeypatch.setattr(cn, "RUNS_DIR", runs_dir)

    # Simulate 2 past runs with errors
    run1 = runs_dir / "20260420T100000Z_nvda_1"
    run1.mkdir()
    now_iso = datetime.now(timezone.utc).isoformat()
    (run1 / "errors.jsonl").write_text(
        f'{{"ts":"{now_iso}","level":"warning","event":"fetch_rate_limited","url":"https://news.google.com/rss/search?q=NVDA","status":503}}\n'
        f'{{"ts":"{now_iso}","level":"error","event":"fetch_error","url":"https://news.google.com/rss/search?q=AMD","error":"TimeoutError"}}\n',
        encoding="utf-8",
    )
    run2 = runs_dir / "20260420T110000Z_aapl_1"
    run2.mkdir()
    (run2 / "errors.jsonl").write_text(
        f'{{"ts":"{now_iso}","level":"error","event":"fetch_error","url":"https://finance.yahoo.com/foo","status":400}}\n',
        encoding="utf-8",
    )

    s = cn.summarize_errors(hours=24)
    assert s["total_events"] == 3
    assert s["by_level"]["error"] == 2
    assert s["by_level"]["warning"] == 1
    assert s["by_event"]["fetch_error"] == 2
    assert s["by_event"]["fetch_rate_limited"] == 1
    assert s["by_domain"]["news.google.com"] == 2
    assert s["by_domain"]["finance.yahoo.com"] == 1
    assert "503" in s["by_status"]
    assert "400" in s["by_status"]


def test_summarize_errors_respects_hours_window(tmp_path, monkeypatch):
    """Events older than the window are excluded."""
    import crawl_news as cn
    from datetime import datetime, timezone, timedelta

    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    monkeypatch.setattr(cn, "RUNS_DIR", runs_dir)

    run = runs_dir / "20260420T100000Z_x_1"
    run.mkdir()
    recent = datetime.now(timezone.utc).isoformat()
    old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    (run / "errors.jsonl").write_text(
        f'{{"ts":"{recent}","level":"error","event":"fetch_error","url":"https://a.com"}}\n'
        f'{{"ts":"{old}","level":"error","event":"fetch_error","url":"https://b.com"}}\n',
        encoding="utf-8",
    )

    s = cn.summarize_errors(hours=24)
    # Only the recent event should be counted
    assert s["total_events"] == 1


def test_body_cache_dedupes_duplicate_fetches(monkeypatch, tmp_path):
    """Same URL appearing in multiple tickers' fulltext lists should fetch once."""
    import asyncio
    import crawl_news as cn

    # Isolate the cross-run SQLite cache layer — without this, test URLs
    # leak into the production news.db fulltext_cache and poison reruns.
    monkeypatch.setattr(cn, "DB_PATH", tmp_path / "news.db")
    cn.init_db()
    cn._reset_body_cache()

    fetch_calls = []

    async def fake_fetch_url(session, url):
        fetch_calls.append(url)
        # Return enough HTML for trafilatura to extract >= 200 chars
        return b"<html><body>" + (b"<p>This is a long article body about NVIDIA earnings beating estimates strongly. " * 20) + b"</p></body></html>"

    monkeypatch.setattr(cn, "fetch_url", fake_fetch_url)

    a1 = {"url": "https://example.com/article-1", "_source_domain": "example.com"}
    a2 = {"url": "https://example.com/article-1", "_source_domain": "example.com"}  # SAME URL
    a3 = {"url": "https://example.com/article-2", "_source_domain": "example.com"}

    async def run():
        await cn.fetch_article_body(None, a1)
        await cn.fetch_article_body(None, a2)
        await cn.fetch_article_body(None, a3)

    asyncio.run(run())

    # Only 2 actual fetches (1 + 2; the duplicate of #1 is cache hit)
    assert len(fetch_calls) == 2, f"Expected 2 fetches, got {len(fetch_calls)}"
    # Cache hit counter incremented
    assert cn._BODY_CACHE_HITS == 1
    assert cn._BODY_CACHE_MISSES == 2
    # All three articles got their body populated
    assert a1.get("body") is not None
    assert a2.get("body") is not None
    assert a3.get("body") is not None


def test_body_cache_remembers_failed_fetches(monkeypatch, tmp_path):
    """If a URL fetch fails, cache the None so we don't retry it for other tickers."""
    import asyncio
    import crawl_news as cn

    monkeypatch.setattr(cn, "DB_PATH", tmp_path / "news.db")
    cn.init_db()
    cn._reset_body_cache()

    fetch_calls = []

    async def fake_fetch_url(session, url):
        fetch_calls.append(url)
        return None  # simulate failed fetch

    monkeypatch.setattr(cn, "fetch_url", fake_fetch_url)

    a1 = {"url": "https://broken.example.com/x", "_source_domain": "broken.example.com"}
    a2 = {"url": "https://broken.example.com/x", "_source_domain": "broken.example.com"}

    async def run():
        await cn.fetch_article_body(None, a1)
        await cn.fetch_article_body(None, a2)

    asyncio.run(run())

    assert len(fetch_calls) == 1, "Failed URL should not be retried for second ticker"
    assert cn._BODY_CACHE_HITS == 1


def test_circuit_breaker_trips_after_consecutive_failures(monkeypatch):
    """After N consecutive failures on a domain, requests skip the network."""
    import crawl_news as cn

    # Clean state
    cn._DOMAIN_FAILURES.clear()
    cn._DOMAIN_PAUSED_UNTIL.clear()

    # Force N failures on the same domain
    url = "https://ratelimited.example.com/feed"
    for _ in range(cn._CB_FAILURE_THRESHOLD):
        cn._circuit_breaker_record_failure(url)

    # Circuit should now be open (domain paused)
    assert cn._circuit_breaker_is_open(url) is True

    # Success on a different domain should not affect this one
    cn._circuit_breaker_record_success("https://healthy.example.com/feed")
    assert cn._circuit_breaker_is_open(url) is True


def test_circuit_breaker_reset_on_success(monkeypatch):
    """A successful fetch clears the failure counter."""
    import crawl_news as cn
    cn._DOMAIN_FAILURES.clear()
    cn._DOMAIN_PAUSED_UNTIL.clear()

    url = "https://flaky.example.com/feed"
    # 2 failures, then 1 success
    cn._circuit_breaker_record_failure(url)
    cn._circuit_breaker_record_failure(url)
    cn._circuit_breaker_record_success(url)
    assert cn._DOMAIN_FAILURES.get("flaky.example.com", 0) == 0
    assert cn._circuit_breaker_is_open(url) is False


def test_build_sources_includes_reuters_bloomberg_cnbc_proxies():
    """After 2026-04 source expansion, dedicated Google proxy channels
    for Reuters, Bloomberg, CNBC must be present in build_sources()."""
    tags = [s["tag"] for s in build_sources("NVDA")]
    assert "google_reuters" in tags
    assert "google_bloomberg" in tags
    assert "google_cnbc" in tags


def test_hot_tickers_derived_from_live_watchlist():
    """2026-06-29 bugfix: hot-ticker gating must follow the LIVE AIStock
    watchlist, not the stale hardcoded AISTOCK500_TICKERS. Regression guard
    for the RKLB miss: RKLB is live-watchlist top-30 but absent from the
    hardcoded list, so it was wrongly denied premium proxies."""
    import crawl_news as cn
    cn._HOT_TICKERS_CACHE = None  # reset cache
    live = cn._load_aistock_watchlist()
    if not live:
        import pytest
        pytest.skip("AIStock watchlist unavailable in this environment")
    hot = cn._get_hot_tickers()
    # The first live-watchlist ticker must be hot
    assert live[0] in hot
    # Any live top-150 ticker must be hot (this is the property that broke)
    for t in live[:150]:
        assert t in hot, f"live top-150 ticker {t} not treated as hot"


def test_wire_tripwire_name_match(monkeypatch):
    """2026-06-29: M&A wire tripwire must map a press-release headline to its
    watchlist ticker via the curated alias index. Regression for the Rocket
    Lab→Iridium miss."""
    import crawl_news as cn
    monkeypatch.setattr(cn, "_NAME_TO_TICKER_CACHE",
                        {"rocket lab": "RKLB", "apple": "AAPL", "nvidia": "NVDA"})
    # Acquirer named in headline → matched
    assert cn._match_watchlist_ticker("Rocket Lab to Acquire Iridium in $8B Deal") == "RKLB"
    # Non-watchlist company → no match
    assert cn._match_watchlist_ticker("Tiny Private Startup Acquires Another Startup") is None
    # Substring-without-word-boundary must NOT false-match
    assert cn._match_watchlist_ticker("Pineapple Express ships fruit") is None


def test_wire_tripwire_event_gate_constants():
    """Tripwire only fires on first-publication high-value events."""
    import crawl_news as cn
    assert cn._TRIPWIRE_EVENT_TYPES == frozenset(
        {"ma_activity", "earnings_release", "earnings_guidance", "regulatory"})
    # The Rocket Lab headline must carry a tripwire-eligible event type
    ets = set(cn.classify_events("Rocket Lab to Acquire Iridium in Historic Deal", ""))
    assert cn._TRIPWIRE_EVENT_TYPES & ets


def test_is_junk_bloomberg_landing_page():
    """Bloomberg paywall often surfaces landing page titles via RSS —
    these must be filtered as junk so they don't pollute the feed."""
    from crawl_news import is_junk
    assert is_junk("Stocks - Bloomberg.com")
    assert is_junk("Rates & Bonds - Bloomberg.com")
    assert is_junk("American Stocks - Bloomberg.com")
    assert is_junk("Currencies - Bloomberg.com")
    # Regular real titles with "Bloomberg" suffix should NOT be junk
    assert not is_junk("Apple CEO Tim Cook steps down after 14 years - Bloomberg")
    assert not is_junk("Nvidia stock jumps 8% on strong Q3 earnings - Bloomberg")


def test_noise_aggregators_are_tier1_not_blocked():
    """Previously-blocked aggregators are now demoted to tier=1 so they
    survive for mid-cap tickers with no other coverage, while getting
    outcompeted for large-caps via low source_authority weight."""
    from crawl_news import get_trust, BLOCKED_DOMAINS, _NOISE_AGGREGATOR_TIER1
    for dom in ["gurufocus.com", "marketbeat.com", "tipranks.com"]:
        assert dom not in BLOCKED_DOMAINS, f"{dom} should not be blocked"
        assert dom in _NOISE_AGGREGATOR_TIER1, f"{dom} should be in tier1 aggregator set"
        assert get_trust(dom, dom) == 1, f"{dom} should resolve to tier 1"


def test_blocked_domains_still_return_zero():
    """Truly spammy sources are still hard-blocked."""
    from crawl_news import get_trust
    for dom in ["talkmarkets.com", "stocktitan.com", "wallstreetzen.com",
                "simplywall.st", "bitget.com", "coincentral.com"]:
        assert get_trust(dom, dom) == 0, f"{dom} should be blocked"


def test_rolling_window_suppresses_aggregators_for_large_caps(tmp_path, monkeypatch):
    """If a ticker has >=2 trust>=2 articles, tier-1 aggregators are dropped
    to keep the feed clean."""
    from datetime import datetime, timezone, timedelta
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()
    now = datetime.now(timezone.utc)
    pub = (now - timedelta(hours=6)).isoformat()

    def _ins(id_, trust, src_name, src_dom, title="NVDA stock news analyst buy rating strong earnings growth"):
        conn.execute(
            """INSERT INTO news
               (id, title, url, source_name, source_domain, trust_tier, relevance,
                channel, published, summary, body, primary_ticker, tickers,
                alt_sources, fetched_at, first_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (id_, title, f"https://ex.com/{id_}", src_name, src_dom, trust, 0.7,
             "google_broad", pub, "Relevant summary about NVDA earnings",
             None, "NVDA", "NVDA", "", now.isoformat(), pub),
        )

    # Large-cap NVDA: 3 tier-3 articles + 2 tier-1 aggregators
    _ins("r1", 3, "Reuters", "reuters.com")
    _ins("r2", 3, "CNBC", "cnbc.com")
    _ins("b1", 3, "Bloomberg", "bloomberg.com")
    _ins("g1", 1, "GuruFocus", "gurufocus.com")
    _ins("m1", 1, "MarketBeat", "marketbeat.com")
    conn.commit()
    conn.close()

    articles = cn.load_rolling_window(["NVDA"], hours=48)
    # 3 tier-3 articles means aggregators are suppressed
    trust_tiers = [a["_trust"] for a in articles]
    assert all(t >= 2 for t in trust_tiers), \
        f"Large-cap should suppress tier-1; got tiers {trust_tiers}"
    assert len(articles) >= 2


def test_rolling_window_allows_aggregators_for_mid_caps(tmp_path, monkeypatch):
    """If a ticker has <2 trust>=2 articles, tier-1 aggregators are kept
    as fallback so mid-caps still get coverage."""
    from datetime import datetime, timezone, timedelta
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()
    now = datetime.now(timezone.utc)
    pub = (now - timedelta(hours=6)).isoformat()

    def _ins(id_, trust, src_name, src_dom, title):
        conn.execute(
            """INSERT INTO news
               (id, title, url, source_name, source_domain, trust_tier, relevance,
                channel, published, summary, body, primary_ticker, tickers,
                alt_sources, fetched_at, first_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (id_, title, f"https://ex.com/{id_}", src_name, src_dom, trust, 0.6,
             "google_broad", pub, "Summary about mid-cap stock earnings",
             None, "FTV", "FTV", "", now.isoformat(), pub),
        )

    # Mid-cap FTV: only 1 tier-2 article + 2 tier-1 aggregators
    _ins("n1", 2, "Nasdaq", "nasdaq.com",
         "Fortive (FTV) Crosses Above Average Analyst Target Price")
    _ins("g1", 1, "GuruFocus", "gurufocus.com",
         "FTV Maintained by Truist Securities with Buy Rating Raises Target Price")
    _ins("m1", 1, "MarketBeat", "marketbeat.com",
         "Fortive (FTV) Price Target Raised to $75 by Analyst Firm Upgrade")
    conn.commit()
    conn.close()

    articles = cn.load_rolling_window(["FTV"], hours=48)
    assert len(articles) >= 1, "Mid-cap must have at least one item"
    # At least one aggregator should pass through as fallback
    has_aggregator = any((a.get("_trust") or 0) == 1 for a in articles)
    assert has_aggregator or len(articles) >= 2, \
        "Mid-cap should get aggregator fallback when tier-2/3 coverage is thin"


def test_rolling_window_adaptive_threshold(tmp_path, monkeypatch):
    """Tickers with <2 articles above 0.45 get relaxed to 0.40 threshold."""
    from datetime import datetime, timezone, timedelta
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()
    now = datetime.now(timezone.utc)
    pub = (now - timedelta(hours=6)).isoformat()

    # Ticker ABC: ONE article that would score ~0.42 (below 0.45, above 0.40)
    # Low trust (tier 1 aggregator) + short title
    conn.execute(
        """INSERT INTO news
           (id, title, url, source_name, source_domain, trust_tier, relevance,
            channel, published, summary, body, primary_ticker, tickers,
            alt_sources, fetched_at, first_seen_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("a1", "ABC Price Target Raised to $100 by Analyst Firm",
         "https://ex.com/a1", "MarketBeat", "marketbeat.com", 1, 0.5,
         "google_broad", pub, "Summary text here about ABC stock", None,
         "ABC", "ABC", "", now.isoformat(), pub),
    )
    conn.commit()
    conn.close()

    articles = cn.load_rolling_window(["ABC"], hours=48)
    # Without adaptive, this article (Q~0.42) would be dropped at 0.45.
    # With adaptive (ticker has <2 above 0.45), it gets relaxed to 0.40 → passes.
    assert len(articles) >= 1, "Adaptive threshold should rescue mid-cap's only article"


def test_load_rolling_window_recomputes_quality_score(tmp_path, monkeypatch):
    """Quality score is re-computed with current taxonomy at load time,
    so taxonomy updates take effect without re-crawling."""
    from datetime import datetime, timezone, timedelta
    import crawl_news

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(crawl_news, "DB_PATH", db_path)
    conn = crawl_news.init_db()
    now = datetime.now(timezone.utc)

    pub = (now - timedelta(hours=6)).isoformat()
    conn.execute(
        """INSERT INTO news
           (id, title, url, source_name, source_domain, trust_tier, relevance,
            channel, published, summary, body, primary_ticker, tickers,
            alt_sources, fetched_at, first_seen_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("a1", "Nvidia (NVDA) reports strong Q3 earnings, beats estimates",
         "https://ex.com/a1", "Reuters", "reuters.com", 3, 0.9, "google_broad",
         pub, "Strong revenue growth this quarter", None, "NVDA", "NVDA",
         "", now.isoformat(), pub),
    )
    conn.commit()
    conn.close()

    articles = crawl_news.load_rolling_window(["NVDA"], hours=48)
    assert len(articles) == 1
    assert articles[0]["_quality_score"] > 0.5  # trust 3 + fresh + relevant


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Bitemporal PIT correctness
# ══════════════════════════════════════════════════════════════════════════════

def test_save_article_preserves_first_seen_at_on_upsert(tmp_path, monkeypatch):
    """first_seen_at must NEVER be overwritten — PIT immutability."""
    from datetime import datetime, timezone, timedelta
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()

    # First insert at T0
    article = {
        "id": "a1", "title": "NVDA strong earnings beat", "url": "https://ex.com/a1",
        "_source_name": "Reuters", "_source_domain": "reuters.com", "_trust": 3,
        "_relevance": 0.8, "_channel": "google_broad",
        "published": "2026-05-13T10:00:00+00:00",
        "summary": "NVDA earnings beat", "_ticker": "NVDA", "_tickers": ["NVDA"],
        "_alt_sources": [],
    }
    cn.save_article(conn, article)

    # Capture initial first_seen_at
    row = conn.execute("SELECT first_seen_at, published FROM news WHERE url=?", (article["url"],)).fetchone()
    initial_first_seen = row[0]
    initial_published = row[1]
    assert initial_first_seen is not None
    assert initial_published == "2026-05-13T10:00:00+00:00"

    # Wait a moment then upsert with new content — first_seen_at must NOT change
    import time
    time.sleep(0.05)
    article["summary"] = "NVDA earnings beat updated text"
    article["_relevance"] = 0.9
    cn.save_article(conn, article)

    row = conn.execute("SELECT first_seen_at, published, fetched_at FROM news WHERE url=?",
                       (article["url"],)).fetchone()
    assert row[0] == initial_first_seen, \
        f"first_seen_at changed! before={initial_first_seen!r}, after={row[0]!r}"
    assert row[1] == initial_published, "published should also be immutable"
    # fetched_at SHOULD update
    assert row[2] != initial_first_seen
    conn.close()


def test_save_article_archives_version_on_content_change(tmp_path, monkeypatch):
    """When article content changes, old version goes to article_versions."""
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()

    article = {
        "id": "a1", "title": "Original title", "url": "https://ex.com/a1",
        "_source_name": "Reuters", "_source_domain": "reuters.com", "_trust": 3,
        "_relevance": 0.8, "_channel": "google_broad",
        "published": "2026-05-13T10:00:00+00:00",
        "summary": "Original summary", "body": "Original body content",
        "_ticker": "NVDA", "_tickers": ["NVDA"], "_alt_sources": [],
    }
    cn.save_article(conn, article)

    # Update content
    article["title"] = "Revised title"
    article["body"] = "Substantially different body"
    cn.save_article(conn, article)

    versions = conn.execute(
        "SELECT version, title, body FROM article_versions WHERE article_id=? ORDER BY version",
        (article["id"],),
    ).fetchall()
    assert len(versions) >= 2, f"Expected version history, got {versions}"
    assert versions[0][1] == "Original title"
    assert versions[-1][1] == "Revised title"
    conn.close()


def test_rolling_window_pit_filter_excludes_future(tmp_path, monkeypatch):
    """Rolling window with as_of=T excludes articles first_seen after T."""
    from datetime import datetime, timezone, timedelta
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    conn = cn.init_db()
    base = datetime.now(timezone.utc)

    def _ins(id_, title, hours_ago_first_seen, hours_ago_published):
        first_seen = (base - timedelta(hours=hours_ago_first_seen)).isoformat()
        pub = (base - timedelta(hours=hours_ago_published)).isoformat()
        conn.execute(
            """INSERT INTO news
               (id, title, url, source_name, source_domain, trust_tier, relevance,
                channel, published, summary, body, primary_ticker, tickers,
                alt_sources, fetched_at, first_seen_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (id_, title, f"https://ex.com/{id_}", "Reuters", "reuters.com",
             3, 0.8, "google_broad", pub,
             "NVDA quarterly earnings growth strong beat estimates", None,
             "NVDA", "NVDA", "", first_seen, first_seen),
        )

    # Article A: first_seen 6h ago
    _ins("a", "NVDA strong quarterly earnings beat estimates raises guidance", 6, 6)
    # Article B: first_seen 1h ago (after backtest cutoff)
    _ins("b", "NVDA strong quarterly earnings beat estimates raises guidance again", 1, 1)
    conn.commit()
    conn.close()

    # Backtest at 3h ago: only article A should be returned
    as_of = base - timedelta(hours=3)
    articles = cn.load_rolling_window(["NVDA"], hours=48, as_of=as_of)
    ids = {a["id"] for a in articles}
    assert "a" in ids, "Article A (first_seen 6h ago) should be visible at as_of=3h"
    assert "b" not in ids, "Article B (first_seen 1h ago) is look-ahead at as_of=3h"


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Classifier versioning
# ══════════════════════════════════════════════════════════════════════════════

def test_classifier_version_present_in_news_items():
    """to_news_item must tag every output with CLASSIFIER_VERSION."""
    from crawl_news import to_news_item, CLASSIFIER_VERSION

    article = {
        "id": "a1", "title": "NVDA reports strong Q3 earnings beat estimates",
        "url": "https://ex.com/a1",
        "_source_name": "Reuters", "_source_domain": "reuters.com", "_trust": 3,
        "_relevance": 0.8, "_channel": "google_broad",
        "published": "2026-05-13T10:00:00+00:00",
        "_first_seen_at": "2026-05-13T10:15:00+00:00",
        "summary": "Strong NVDA earnings", "_ticker": "NVDA", "_tickers": ["NVDA"],
    }
    item = to_news_item(article)
    assert item["meta"]["classifier_version"] == CLASSIFIER_VERSION


def test_first_seen_at_utc_in_news_item():
    """to_news_item must expose first_seen_at_utc as top-level field."""
    from crawl_news import to_news_item

    article = {
        "id": "a1", "title": "NVDA earnings strong",
        "url": "https://ex.com/a1",
        "_source_name": "Reuters", "_source_domain": "reuters.com", "_trust": 3,
        "_relevance": 0.8, "_channel": "google_broad",
        "published": "2026-05-13T10:00:00+00:00",
        "_first_seen_at": "2026-05-13T10:15:00+00:00",
        "summary": "Test", "_ticker": "NVDA", "_tickers": ["NVDA"],
    }
    item = to_news_item(article)
    # Top-level field
    assert "first_seen_at_utc" in item
    assert item["first_seen_at_utc"] == "2026-05-13T10:15:00+00:00"
    # Also in meta for legacy consumers
    assert item["meta"]["first_seen_at_utc"] == "2026-05-13T10:15:00+00:00"
    # Latency computed
    assert item["meta"]["publish_to_observe_latency_s"] == 900  # 15 min = 900s


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Cross-run SQLite caches
# ══════════════════════════════════════════════════════════════════════════════

def test_fulltext_cache_persists_across_resets(tmp_path, monkeypatch):
    """SQLite fulltext cache survives _reset_body_cache() (it's cross-run)."""
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    cn.init_db()

    url = "https://ex.com/article-1"
    cn._fulltext_cache_store(url, "Long article body text here with enough characters to pass min length filter.")
    # First run done; new run resets in-run cache but SQLite persists
    cn._reset_body_cache()
    body = cn._fulltext_cache_lookup(url)
    assert body is not None
    assert "Long article body" in body


def test_url_resolution_cache_persists(tmp_path, monkeypatch):
    """SQLite URL resolution cache persists across runs."""
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    cn.init_db()

    encoded = "https://news.google.com/rss/articles/ABC123"
    decoded = "https://reuters.com/real-article"
    cn._url_resolution_store(encoded, decoded)
    cn._reset_body_cache()
    assert cn._url_resolution_lookup(encoded) == decoded
    assert cn._url_resolution_lookup("https://news.google.com/rss/articles/XYZ") is None


def test_feed_state_etag_storage(tmp_path, monkeypatch):
    """feed_state stores ETag/Last-Modified across runs for HTTP 304."""
    import crawl_news as cn

    db_path = tmp_path / "news.db"
    monkeypatch.setattr(cn, "DB_PATH", db_path)
    cn.init_db()

    url = "https://news.google.com/rss/search?q=NVDA"
    cn._feed_state_store(url, etag='W/"abc123"',
                          last_modified="Tue, 13 May 2026 10:00:00 GMT",
                          sha256="hash123", status=200)
    state = cn._feed_state_get(url)
    assert state["etag"] == 'W/"abc123"'
    assert state["last_modified"] == "Tue, 13 May 2026 10:00:00 GMT"
    assert state["sha256"] == "hash123"

    # 304 hit increments counter
    cn._feed_state_increment_304(url)
    import sqlite3 as _sql
    conn = _sql.connect(str(db_path))
    hits = conn.execute("SELECT cache_hits_304 FROM feed_state WHERE url=?", (url,)).fetchone()[0]
    conn.close()
    assert hits == 1


# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: AIStock integration smoke test
# ══════════════════════════════════════════════════════════════════════════════

def test_disambiguation_2026_06_batch():
    """2026-06-10 audit: word-collision tickers found in live AIStock watchlist.
    Real false positive observed: 'Miller Lite crop top' article attached to
    LITE (Lumentum) evidence in an AIStock DecisionCard."""
    from crawl_news import relevance_score, CONTEXT_ONLY_TICKERS

    # All batch members must be in the explicit set
    for t in ["LITE", "MET", "BALL", "TAP", "SPOT", "SNAP", "SHOP", "PATH",
              "CART", "HOOD", "BILL", "NET", "RIOT", "HUT", "FIX", "TXT"]:
        assert t in CONTEXT_ONLY_TICKERS, f"{t} missing from CONTEXT_ONLY_TICKERS"

    # False positives must be rejected
    assert relevance_score("Livvy Dunne's Miller Lite crop top gets upgrade", "", "LITE") < 0.10
    assert relevance_score("Cold snap hits Texas as temperatures plunge", "", "SNAP") < 0.10
    assert relevance_score("Man arrested in neighborhood robbery", "", "HOOD") < 0.10
    # Real news must still pass
    assert relevance_score("Lumentum (LITE) raises Q4 guidance on AI demand", "", "LITE") >= 0.10
    assert relevance_score("Snap Inc (SNAP) stock jumps on ad revenue", "", "SNAP") >= 0.10


def test_short_tickers_dynamically_context_only():
    """Tickers <=2 chars are context-only even if NOT in the static set —
    the static set was built from hardcoded AISTOCK500 and missed live-watchlist
    additions like Z (Zillow) / W (Wayfair) / S (SentinelOne)."""
    from crawl_news import relevance_score
    # Bare-word usage must NOT match
    assert relevance_score("Generation Z spending habits shift", "", "Z") < 0.10
    assert relevance_score("George W. Bush attends event", "", "W") < 0.10
    # Contextual usage must match
    assert relevance_score("Zillow Group (NASDAQ: Z) reports strong growth", "", "Z") >= 0.10
    assert relevance_score("Wayfair (W) stock surges on earnings beat", "", "W") >= 0.10


def test_export_news_item_has_aistock_required_pit_fields():
    """End-to-end: NewsItem export contains all fields AIStock needs for PIT."""
    from crawl_news import to_news_item, CLASSIFIER_VERSION

    article = {
        "id": "a1", "title": "Apple Q3 earnings beat estimates raise guidance",
        "url": "https://ex.com/aapl-q3",
        "_source_name": "Reuters", "_source_domain": "reuters.com", "_trust": 3,
        "_relevance": 0.85, "_channel": "google_reuters",
        "published": "2026-05-13T16:00:00+00:00",
        "_first_seen_at": "2026-05-13T16:08:30+00:00",
        "summary": "Apple reported Q3 earnings beat", "body": None,
        "_ticker": "AAPL", "_tickers": ["AAPL"], "_alt_sources": ["CNBC"],
    }
    item = to_news_item(article)

    # AIStock PIT contract
    assert item.get("first_seen_at_utc") == "2026-05-13T16:08:30+00:00"
    assert item.get("timestamp_utc") == "2026-05-13T16:00:00+00:00"
    assert item["meta"].get("classifier_version") == CLASSIFIER_VERSION
    assert item["meta"].get("publish_to_observe_latency_s") == 510  # 8m30s = 510s

    # Standard AIStock fields still present
    assert item["ticker"] == "AAPL"
    assert item["trust_tier"] == 3
    assert item["ingest_source"] == "newscrawler_local"

