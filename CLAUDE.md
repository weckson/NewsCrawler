# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler producing AIStock-compatible per-ticker exports.
No API key required. Sources: Benzinga RSS, Google News RSS, company IR feeds.
Single-file design — everything lives in `crawl_news.py`.

## Quick Start

```bash
# Default: reads AIStock watchlist live, last 48h
python crawl_news.py

# 200-ticker preset
python crawl_news.py --ticker-set aistock200

# Single ticker
python crawl_news.py --ticker NVDA

# Custom list
python crawl_news.py --tickers NVDA,AMD,MSFT --hours 24

# Human-readable terminal report
python crawl_news.py --ticker NVDA --output pretty

# Custom data root (Linux/cron)
python crawl_news.py --data-dir /var/lib/newscrawler

# Tests
pytest tests/ -q
```

## Ticker Presets

| Preset | Source | Count |
|--------|--------|-------|
| `aistock` **(default)** | Reads `D:\AIStock\config\default.json` at runtime | matches AIStock exactly |
| `aistock500` | Hardcoded fallback (used if AIStock repo not found) | 500 |
| `aistock200` | Legacy core watchlist | ~200 |
| `semis20` | Semiconductor focus | 20 |

`_load_aistock_watchlist()` reads `PROJECT_ROOT/../AIStock/config/default.json` — `watchlist` array
filtered by `max_tickers`. Falls back to `aistock500` if file not found.

## CLI Flags

```
--ticker NVDA                   Single ticker
--tickers NVDA,AMD,MSFT         Comma-separated list
--ticker-set aistock200         Preset (aistock | aistock500 | aistock200 | semis20)
--hours 48                      Look-back window (default: 48)
--fulltext-mode high-value      off | high-value | all (default: high-value)
--fulltext-max-articles 8       Per-ticker full-text cap (default: 8)
--output newsitem-json          newsitem-json (default, stdout) | pretty (stderr report)
--data-dir /path/to/data        Override base data directory
```

## Data Contract with AIStock

### How AIStock reads news

**Primary (per-ticker mode):**
```
data/aistock/by_ticker/{TICKER}/latest_news_items.json
```
AIStock reads each requested ticker's file individually via `_fetch_by_ticker_dir()`.
Config key: `"crawler_news_by_ticker_dir": "../NewsCrawler/data/aistock/by_ticker"`.

**Fallback (flat mode):**
```
data/aistock/latest_news_items.json
```
Used when `crawler_news_by_ticker_dir` is not configured or the directory is missing.
Config key: `"crawler_news_path": "../NewsCrawler/data/aistock/latest_news_items.json"`.

### Full export layout

```
data/aistock/
  latest_news_items.json         All tickers flat list
  latest_payload.json            AIStock payload envelope
  by_ticker/
    NVDA/
      latest_news_items.json     ← primary read target for AIStock
      latest_articles.json
      latest_payload.json
      latest_summary.json
    AMD/
      ...
```

### NewsItem schema

Top-level fields (match `shared/models.py NewsItem` in AIStock):

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

| Field | Values | Notes |
|-------|--------|-------|
| `trust_tier` | 3/2/1/0 | 3=TOP (WSJ/Bloomberg), 2=OK (Yahoo/SA), 1=low, 0=blocked |
| `body_kind` | `article_text` / `summary_snippet` | Snippets get 15% scorer discount in AIStock |
| `ingest_source` | `newscrawler_local` | Fixed; used by AIStock validator and scorer |
| `tickers_hint` | list[str] | All relevant tickers; drives routing + entity linking |
| `source_quality` | `high`/`medium`/`low` | Derived from trust_tier |

AIStock drops items where `trust_tier < min_trust_tier` (default 2) AND `source_quality == "low"`.

## Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `_load_aistock_watchlist()` | line ~924 | Read ticker list from AIStock config |
| `parse_tickers()` | line ~947 | Resolve preset / comma-list / default |
| `build_runtime_paths()` | line ~82 | Resolve all data paths from env/args |
| `configure_runtime_paths()` | line ~123 | Apply resolved paths to module globals |
| `_DomainRateLimiter` | line ~147 | Per-domain async rate limiter |
| `article_quality_score()` | line ~895 | 7-signal composite quality scorer |
| `quality_filter()` | line ~1238 | Full quality pipeline + score gating |
| `crawl()` | line ~1736 | Fetch + filter single ticker |
| `_fetch_shared_benzinga_rss()` | line ~1858 | Fetch BZ RSS once, share globally |
| `crawl_watchlist()` | line ~1877 | Parallel tickers, merge, save |
| `to_news_item()` | line ~1973 | Raw article → AIStock NewsItem |
| `export_articles_by_ticker()` | line ~1071 | Write stable per-ticker files |
| `persist_run_artifacts()` | line ~1136 | Write run dir + stable exports |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEWSCRAWLER_DATA_DIR` | `./data` | Base data directory |
| `NEWSCRAWLER_DB_PATH` | `{DATA_DIR}/news.db` | SQLite database |
| `NEWSCRAWLER_RAW_DIR` | `{DATA_DIR}/raw` | Raw RSS payloads |
| `NEWSCRAWLER_RUNS_DIR` | `{DATA_DIR}/runs` | Per-run artifact directories |
| `NEWSCRAWLER_AISTOCK_EXPORT_DIR` | `{DATA_DIR}/aistock` | AIStock export root |
| `NEWSCRAWLER_TICKER_EXPORTS_DIR` | `{AISTOCK_DIR}/by_ticker` | Per-ticker export dir |
| `NEWSCRAWLER_SNAPSHOT_PATH` | *(config)* | Override flat snapshot path |
| `NEWSCRAWLER_BY_TICKER_DIR` | *(config)* | Override per-ticker dir path |

All relative paths resolve from `PROJECT_ROOT` (script location).
`python /opt/NewsCrawler/crawl_news.py` works correctly from any cwd.

## Performance

500 tickers complete in ~11 minutes (previously ~67 minutes sequential).

| Tuning constant | Default | Purpose |
|-----------------|---------|---------|
| `TICKER_CONCURRENCY` | 5 | Tickers fetched in parallel |
| `DOMAIN_DELAY["news.google.com"]` | 0.3s | Google News RSS rate limit |
| `DOMAIN_DELAY["www.benzinga.com"]` | 1.5s | Benzinga RSS rate limit |
| `DOMAIN_DELAY["default"]` | 1.0s | All other domains |
| `ARTICLE_FETCH_CONCURRENCY` | 6 | Concurrent full-text fetches |

Optimizations:
- **Per-domain rate limiter**: `_DomainRateLimiter` applies different delays per host
- **Parallel ticker processing**: `asyncio.Semaphore(TICKER_CONCURRENCY)` runs N tickers concurrently
- **Shared Benzinga RSS**: `_fetch_shared_benzinga_rss()` fetches once, distributes to all tickers
- **Shared HTTP connection pool**: single `aiohttp.ClientSession` with DNS cache for all tickers

## Terminal Output

Logs go to **stderr**; JSON output goes to **stdout** (safe to pipe).

```
[14:57:51] ============================================================
[14:57:51] NewsCrawler  tickers=500  hours=48h  fulltext=high-value
[14:57:51] concurrency = 5 tickers  delays = {news.google.com: 0.3, www.benzinga.com: 1.5, default: 1.0}
[14:57:51] data_dir  = D:\NewsCrawler\data
[14:57:51] run_dir   = D:\NewsCrawler\data\runs\20260406T145751Z_nvda_avgo_mu_500
[14:57:52]   [global] Benzinga RSS fetched once  articles=25
[14:58:15] -- progress 10/500 tickers  (24s)
[14:58:27] -- progress 20/500 tickers  (36s)
...
[15:08:49] -- progress 500/500 tickers  (658s)
[15:08:51] -- total crawl time: 659.9s (11.0min)
[15:08:52] ============================================================
[15:08:52] DONE  500-ticker watchlist  in 661.0s (11.0min)
[15:08:52]   raw=140085  filtered=157  news_items=157  new_to_db=157
[15:08:52]   trust  TOP:104  OK:24  low:29
[15:08:52]   bz=23
[15:08:52] ============================================================
```

## Quality Scoring System

7-signal composite scoring model (inspired by RavenPack / Bloomberg / GDELT).
Each article gets a quality score [0.0, 1.0]; articles below `QUALITY_THRESHOLD` (0.40) are dropped.

| Signal | Weight | Source |
|--------|--------|--------|
| Source Authority | 0.30 | trust_tier: TOP=1.0, OK=0.7, low=0.3 |
| Ticker Relevance | 0.25 | Existing relevance_score() |
| Headline Informativeness | 0.15 | Action keywords, length, clickbait/listicle penalty |
| Temporal Freshness | 0.10 | Continuous decay: 4h=1.0, 12h=0.8, 24h=0.6, 48h=0.2 |
| Content Specificity | 0.10 | Multi-ticker roundup penalty + entity density ($, %) |
| Summary Richness | 0.05 | Full text=1.0, summary>=100ch=0.7, none=0.1 |
| Source Diversity | 0.05 | Multi-source confirmation: 3+=1.0, 0=0.2 |

**Threshold**: 0.40 standard, 0.35 adaptive fallback (for tickers with <2 articles).
**Effect**: low-quality articles dropped from 72% to 18% of output; AIStock usable rate ~82%.

The quality score is exported in `meta.quality_score` for AIStock to optionally use.

## Architecture Notes

- **Single-file**: all logic in `crawl_news.py`, no submodules
- **No API key**: public RSS only — Benzinga RSS, Google News RSS
- **No JS rendering**: RSS feeds bypass Cloudflare challenge
- **Per-domain rate limiting**: different delays for different hosts (Google 0.3s, Benzinga 1.5s)
- **Parallel tickers**: 5 concurrent tickers via asyncio.Semaphore
- **Shared Benzinga RSS**: fetched once globally, shared across all tickers
- **7-signal quality scoring**: composite score gates articles at 0.40 threshold
- **SQLite dedup**: articles keyed by URL; re-runs are fast
- **Async I/O**: `aiohttp` + `asyncio` with shared connection pool
- **Structured logging**: `structlog` → stderr; stdout = pure JSON
