# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler for US equities. No API key required.
Produces per-ticker JSON exports consumed by [`D:\AIStock`](../AIStock) as a curated news data source.

**Explicitly excludes** sources already in AIStock (Polygon, SEC/EDGAR, FINRA, Finnhub, Reddit, FRED, yfinance).

## Sources

**Per-ticker sources** (6 channels per ticker):
| Channel | Method | Volume |
|---------|--------|--------|
| Google News → Benzinga (`site:benzinga.com when:2d`) | Google as BZ proxy | ~100 articles |
| Google News → SeekingAlpha (`site:seekingalpha.com when:2d`) | Google as SA proxy (SA's own RSS is 98% stale) | ~100 articles |
| Google News broad (`when:1d` + `when:2d`) | All sources, 5 query variants for freshness + coverage | variable |
| Yahoo Finance RSS (`/rss/headline?s={TICKER}`) | Per-ticker headlines, trust=3 | ~20 articles |
| Nasdaq RSS (`/feed/rssoutbound?symbol={TICKER}`) | Curated, best fulltext rate (63%) | ~15 articles |
| Company IR RSS | RSS polling | per-company |

**Shared** (fetched once per run):
| Channel | Method |
|---------|--------|
| Benzinga RSS (`/feed`, `/news/feed`) | Official RSS, distributed to all tickers |

No direct HTML scraping — all feeds are structured RSS, no Cloudflare challenge.
Google News queries use `when:2d` to return only recent articles, dramatically improving freshness.
Full article text is extracted with [trafilatura](https://trafilatura.readthedocs.io/) when available (boilerplate-free main content), falling back to a regex-based extractor.

## Quick Start

```bash
# Install
pip install -e ".[dev]"

# Default run: reads AIStock watchlist (config/default.json), last 48h
python crawl_news.py

# 200-ticker preset
python crawl_news.py --ticker-set aistock200

# Single ticker
python crawl_news.py --ticker NVDA

# Custom tickers
python crawl_news.py --tickers NVDA,AMD,MSFT

# Human-readable report
python crawl_news.py --ticker NVDA --output pretty

# Custom data directory (Linux/cron)
python crawl_news.py --data-dir /var/lib/newscrawler

# Tests
pytest tests/ -q
```

## Watchlist & Ticker Sets

| Preset | Source | Count |
|--------|--------|-------|
| `aistock` **(default)** | Reads `D:\AIStock\config\default.json` live | matches AIStock exactly |
| `aistock500` | Hardcoded fallback (used if AIStock repo not found) | 500 |
| `aistock200` | Legacy core watchlist (noisy single-letter symbols removed) | ~200 |
| `semis20` | Semiconductor focus | 20 |

The default `aistock` preset reads AIStock's watchlist at runtime so crawl tickers always stay in sync — no manual list maintenance needed.

## CLI Flags

```
--ticker NVDA                  Single ticker
--tickers NVDA,AMD,MSFT        Comma-separated list
--ticker-set aistock200        Built-in preset (see table above)
--hours 48                     Look-back window in hours (default: 48)
--fulltext-mode high-value     off | high-value | all (default: high-value)
--fulltext-max-articles 20     Per-ticker cap for full-text fetch (default: 20)
--output newsitem-json         newsitem-json (default, for piping) | pretty
--data-dir /path/to/data       Override base data directory
--no-rolling                   Disable rolling-window export (default: enabled)
--summarize-errors 24          Print error audit report for last N hours and exit
```

## Error Audit

Every WARNING/ERROR event is written to `data/runs/{timestamp}/errors.jsonl`
as structured JSON (only created when the run produces warnings/errors).

```bash
# Quick audit: what went wrong in the last 24h?
python crawl_news.py --summarize-errors 24

# Sample output:
# === Error audit: last 24h ===
# Total events: 12
# By level: {'warning': 10, 'error': 2}
# By event type: {fetch_rate_limited: 8, circuit_breaker_tripped: 2, ...}
# By domain: {news.google.com: 9, finance.yahoo.com: 3}
# By HTTP status: {'503': 8, '400': 3, '429': 1}
```

Programmatic API: `summarize_errors(hours=24)` returns a dict suitable for
cron-based alerting.

## Rolling Window

By default, the exported `latest_news_items.json` and `by_ticker/{TICKER}/*`
contain **all articles from the last `--hours` window** (across runs), read
from SQLite. Repeated runs accumulate coverage instead of overwriting.

Taxonomy and disambiguation rules are re-applied at export time, so updates
take effect on next run without re-crawling.

Use `--no-rolling` to export only this run's fresh fetches.

## Output Layout

```
data/
  news.db                        SQLite article store (dedup by URL)
  aistock/
    latest_news_items.json       Flat list of all NewsItems (all tickers)
    latest_payload.json          AIStock payload envelope
    by_ticker/
      NVDA/
        latest_news_items.json   NewsItems for NVDA only  ← read by AIStock
        latest_articles.json     Raw filtered articles
        latest_payload.json      Payload envelope
        latest_summary.json      Counts / metadata
      AMD/
        ...
  raw/                           Raw RSS XML payloads (debug)
  runs/
    20260406T120000Z_nvda_3/     Per-run artifacts
      run.log
      summary.json
      filtered_articles.json
      news_items.json
      by_ticker/
```

**AIStock reads** `data/aistock/by_ticker/{TICKER}/latest_news_items.json` per requested ticker
(configured via `crawler_news_by_ticker_dir` in AIStock `config/default.json`).

## NewsItem Schema

Each item in `latest_news_items.json` follows the AIStock `NewsItem` contract:

```json
{
  "id":             "uuid-v5",
  "timestamp_utc":  "2026-04-06T12:00:00+00:00",
  "source":         "yahoo_finance",
  "url":            "https://...",
  "title":          "Article headline",
  "body":           "Full text or summary snippet (nullable)",
  "author":         null,
  "language":       "en",
  "ticker":         "NVDA",
  "tickers_hint":   ["NVDA", "AMD"],
  "source_quality": "high",
  "publisher_raw":  "finance.yahoo.com",
  "trust_tier":     3,
  "body_kind":      "article_text",
  "ingest_source":  "newscrawler_local",
  "meta": {
    "source_name":   "Yahoo Finance",
    "source_domain": "finance.yahoo.com",
    "event_origin":  "news",
    "relevance":     0.75,
    "quality_score": 0.82,
    "event_types":   ["earnings_release", "analyst_rating"],
    "sentiment":     {"score": 0.67, "pos": 0.032, "neg": 0.006, "unc": 0.010, "matched": 12},
    "_channel":      "google_broad"
  }
}
```

| Field | Values | Description |
|-------|--------|-------------|
| `trust_tier` | 3 / 2 / 1 / 0 | 3=TOP (WSJ/Bloomberg), 2=OK (Yahoo/SA), 1=low, 0=blocked |
| `body_kind` | `article_text` / `summary_snippet` | Full text fetched vs RSS excerpt |
| `ingest_source` | `newscrawler_local` | Fixed marker; AIStock scorer applies 15% discount to snippets |
| `source_quality` | `high` / `medium` / `low` | Derived from trust_tier |

## Quality Pipeline

```
Raw RSS articles
  → Time recency    (hard cutoff = now - hours)
  → Blocked domains (trust_tier == 0 dropped)
  → Relevance       (ticker mention density ≥ 0.10)
  → Junk filter     (listicles, sponsored content)
  → Weak-signal     (holdings / profile fluff)
  → Quality score   (7-signal composite, threshold 0.45)
  → SimHash dedup   (Hamming ≤ 6 = same article)
  → Re-score        (source_diversity signal updated post-dedup)
  → Quality gate    (drop below threshold; adaptive 0.40 fallback)
  → Sort: quality_score ↓  newest first
  → [optional] Full-text fetch for high-value articles
```

### Quality Scoring System

Each article receives a composite quality score [0.0, 1.0] from 7 weighted signals
(modeled on RavenPack / Bloomberg Terminal / GDELT evaluation methods):

| Signal | Weight | Description |
|--------|--------|-------------|
| Source Authority | 0.30 | trust_tier mapping: TOP=1.0, OK=0.7, low=0.3 |
| Ticker Relevance | 0.25 | Reuses existing relevance scoring |
| Headline Informativeness | 0.15 | Financial action keywords, title length, clickbait penalty |
| Temporal Freshness | 0.10 | Continuous decay: ≤4h=1.0 → ≤48h=0.2 → >48h=0.0 |
| Content Specificity | 0.10 | Multi-ticker roundup penalty + entity density ($, %) |
| Summary Richness | 0.05 | Full text=1.0, summary ≥100ch=0.7, nothing=0.1 |
| Source Diversity | 0.05 | Multi-source confirmation: ≥3 sources=1.0, 0=0.2 |

**Threshold**: 0.45 (adaptive fallback to 0.40 for tickers with <2 articles).

Effect: low-quality articles reduced from 72% to 18% of output.

### Near-Duplicate Clustering

Same-story articles across sources are collapsed into a single cluster head
with the other publishers listed in `meta._alt_sources`. Two-pass approach:

1. **SimHash** (Hamming ≤ 8) — catches near-identical titles
2. **Fuzzy token-set ratio** (rapidfuzz, threshold 85) — catches paraphrased rewrites

The highest-trust source wins; alt_sources drive the source_diversity signal,
so corroborated events score higher in the quality pipeline.

### Financial Sentiment (Loughran-McDonald)

Each article carries a pre-computed LM sentiment score in `meta.sentiment`:

```json
{"score": 0.67, "pos": 0.032, "neg": 0.006, "unc": 0.010, "matched": 12}
```

- `score` — normalized net sentiment in `[-1, 1]` = `(pos_count - neg_count) / matched`
- `pos` / `neg` — fraction of tokens matching LM positive / negative lexicon
- `unc` — fraction matching uncertainty/hedging words (correlates with downside surprises)
- `matched` — total LM dictionary hits (0 → sentiment field is `null`)

Uses a curated subset (~300 high-signal terms) of the Loughran-McDonald 2018
financial sentiment dictionary — calibrated to financial text, unlike
general-purpose lexicons (VADER, AFINN). Null when no dictionary words matched.

### Event Classification

Each article is tagged with matching event types via regex taxonomy (stored in `meta.event_types`).
AIStock can route articles by event type instead of just trust tier.

| Event type | Examples |
|------------|----------|
| `earnings_release` | Q3 results, beats estimates, quarterly report |
| `earnings_guidance` | raises/cuts guidance, reaffirms outlook |
| `analyst_rating` | upgrades/downgrades, price target changes |
| `ma_activity` | acquisitions, mergers, takeovers, spin-offs |
| `management_change` | CEO/CFO appointments and departures |
| `product_launch` | new product announcements, debuts |
| `litigation` | lawsuits, SEC probes, class-actions, settlements |
| `regulatory` | FDA/FTC approvals, phase trials |
| `capital_action` | dividends, buybacks, stock splits, offerings |
| `insider_activity` | Form 4, insider buying/selling |
| `macro_sector` | Fed rates, inflation, tariffs, GDP |

## Performance

500 tickers complete in ~11 minutes (down from ~67 min sequential).

Optimizations:
- **Per-domain rate limiting** — Google News 0.3s, Yahoo Finance 0.5s, Nasdaq 1.0s, others 1.0s
- **5 tickers in parallel** — `asyncio.Semaphore` with shared HTTP connection pool
- **Shared Benzinga RSS** — fetched once globally, distributed to all tickers
- **DNS caching** — 300s TTL on the shared `aiohttp.TCPConnector`

Tunable constants at the top of `crawl_news.py`: `TICKER_CONCURRENCY`, `DOMAIN_DELAY`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEWSCRAWLER_DATA_DIR` | `./data` | Base data directory |
| `NEWSCRAWLER_DB_PATH` | `{DATA_DIR}/news.db` | SQLite database path |
| `NEWSCRAWLER_RAW_DIR` | `{DATA_DIR}/raw` | Raw RSS payloads |
| `NEWSCRAWLER_RUNS_DIR` | `{DATA_DIR}/runs` | Per-run artifact directories |
| `NEWSCRAWLER_AISTOCK_EXPORT_DIR` | `{DATA_DIR}/aistock` | AIStock export root |
| `NEWSCRAWLER_TICKER_EXPORTS_DIR` | `{AISTOCK_DIR}/by_ticker` | Per-ticker exports |
| `NEWSCRAWLER_SNAPSHOT_PATH` | *(from config)* | Override flat snapshot path |
| `NEWSCRAWLER_BY_TICKER_DIR` | *(from config)* | Override per-ticker dir path |

All relative paths resolve from the script location (`PROJECT_ROOT`), so
`python /opt/NewsCrawler/crawl_news.py` works correctly from any working directory.

## Viewing Data

```bash
# Latest run artifacts
ls data/runs/ | sort -r | head -3

# Per-ticker news for NVDA
cat data/aistock/by_ticker/NVDA/latest_news_items.json | python -m json.tool | head -60

# SQLite queries
sqlite3 data/news.db "select title, tickers, published from news order by published desc limit 20;"
sqlite3 data/news.db "select count(*) from news where tickers like '%NVDA%';"
sqlite3 data/news.db "select count(*) from news where body is not null and trim(body) <> '';"
```

## Integration with AIStock

```
crawl_news.py
  └─ writes → data/aistock/by_ticker/{TICKER}/latest_news_items.json
                        ↓
  D:\AIStock\data_sources\connectors\crawler_news_source.py
    reads per-ticker files → normalizes → trust-tier filter → stream routing
                        ↓
  soft_event_stream (trust_tier ≥ 2)  or  alt_sentiment (if route_low_quality_to_alt)
                        ↓
  Event Engine → Sentiment Engine → Orchestrator → DecisionCards
```

AIStock config keys (`config/default.json`):

```json
"crawler_news_enabled": true,
"crawler_news_by_ticker_dir": "../NewsCrawler/data/aistock/by_ticker",
"crawler_news_path": "../NewsCrawler/data/aistock/latest_news_items.json",
"crawler_news_min_trust_tier": 2,
"crawler_news_allow_article_text_override": false,
"crawler_news_route_low_quality_to_alt": false
```

## Tests

```bash
pytest tests/ -q    # 100 tests
```

| Test file | Coverage |
|-----------|---------|
| `tests/test_crawl_news_quality.py` | Quality scoring signals, composite scorer, filter gating, ticker parsing, presets |
| `tests/test_crawl_news_output.py` | NewsItem schema, payload envelope, full-text extraction, Google URL decoding |
| `tests/test_crawl_news_paths.py` | Path resolution, env var overrides, relative path scoping |
| `tests/test_rss_parser.py` | RSS parsing, URL canonicalization, content type inference |
| `tests/test_benzinga_parser.py` | Benzinga-specific RSS parsing |
| `tests/test_fetcher.py` | HTTP retry, conditional GET, rate limiting, captcha handling |
| `tests/test_alerts.py` | Alert generation and filtering |
| `tests/test_compliance.py` | Compliance checks and validation |
| `tests/test_dedupe.py` | SimHash fingerprinting, Hamming distance, near-duplicate clustering |
