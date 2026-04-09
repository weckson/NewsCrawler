# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler for US equities. No API key required.
Produces per-ticker JSON exports consumed by [`D:\AIStock`](../AIStock) as a curated news data source.

**Explicitly excludes** sources already in AIStock (Polygon, SEC/EDGAR, FINRA, Finnhub, Reddit, FRED, yfinance).

## Sources

| Channel | Method | Volume |
|---------|--------|--------|
| Benzinga RSS (`/feed`, `/news/feed`) | Public RSS, no CF block | ~10–15 articles |
| Google News → Benzinga (`site:benzinga.com`) | Google News RSS filtered to BZ | ~100 articles |
| Google News broad (`{ticker} stock news`) | All sources, quality-scored | variable |
| Company IR RSS | RSS polling | per-company |

No direct HTML scraping — all feeds are structured RSS, no Cloudflare challenge.

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
--fulltext-max-articles 8      Per-ticker cap for full-text fetch
--output newsitem-json         newsitem-json (default, for piping) | pretty
--data-dir /path/to/data       Override base data directory
```

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
  → Quality score   (7-signal composite, threshold 0.40)    ← NEW
  → SimHash dedup   (Hamming ≤ 6 = same article)
  → Re-score        (source_diversity signal updated post-dedup)
  → Quality gate    (drop below threshold; adaptive 0.35 fallback)
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

**Threshold**: 0.40 (adaptive fallback to 0.35 for tickers with <2 articles).

Effect: low-quality articles reduced from 72% to 18% of output.

## Performance

500 tickers complete in ~11 minutes (down from ~67 min sequential).

Optimizations:
- **Per-domain rate limiting** — Google News 0.3s, Benzinga 1.5s, others 1.0s (vs fixed 2s)
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
pytest tests/ -q    # 95 tests
```

| Test file | Coverage |
|-----------|---------|
| `tests/test_crawl_news_quality.py` | Quality scoring signals, composite scorer, filter gating, ticker parsing, presets |
| `tests/test_crawl_news_output.py` | NewsItem schema, payload envelope, full-text extraction, Google URL decoding |
| `tests/test_crawl_news_paths.py` | Path resolution, env var overrides, relative path scoping |
