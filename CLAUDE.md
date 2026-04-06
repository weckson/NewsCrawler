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
| `_load_aistock_watchlist()` | line ~655 | Read ticker list from AIStock config |
| `parse_tickers()` | line ~676 | Resolve preset / comma-list / default |
| `build_runtime_paths()` | line ~73 | Resolve all data paths from env/args |
| `configure_runtime_paths()` | line ~114 | Apply resolved paths to module globals |
| `crawl()` | line ~1410 | Fetch + filter single ticker |
| `crawl_watchlist()` | line ~1510 | Loop tickers, merge, save |
| `quality_filter()` | line ~924 | Full quality pipeline |
| `to_news_item()` | line ~1570 | Raw article → AIStock NewsItem |
| `export_articles_by_ticker()` | line ~773 | Write stable per-ticker files |
| `persist_run_artifacts()` | line ~838 | Write run dir + stable exports |

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

## Terminal Output

Logs go to **stderr**; JSON output goes to **stdout** (safe to pipe).

```
[12:00:01] ============================================================
[12:00:01] NewsCrawler  tickers=500  hours=48h  fulltext=high-value
[12:00:01] data_dir  = D:\NewsCrawler\data
[12:00:01] run_dir   = D:\NewsCrawler\data\runs\20260406T120001Z_nvda_3_500
[12:00:02] ── ticker 1/500: NVDA
[12:00:02]   [NVDA] fetching 3 channel(s)
[12:00:04]   [NVDA] 1/3 Benzinga RSS  raw=12
[12:00:06]   [NVDA] 2/3 Google→BZ    raw=47
[12:00:08]   [NVDA] 3/3 Google broad  raw=23
[12:00:08]   [NVDA] quality filter  raw=82
[12:00:08]   [NVDA] quality filter done  passed=14
[12:00:08]   [NVDA] saved to DB  new=3  total=14
...
[12:05:00] DONE  500-ticker watchlist
[12:05:00]   raw=12400  filtered=2800  news_items=2800  new_to_db=340
[12:05:00]   trust  BLK:12  OK:1800  TOP:900  low:88
```

## Architecture Notes

- **Single-file**: all logic in `crawl_news.py`, no submodules
- **No API key**: public RSS only — Benzinga RSS, Google News RSS
- **No JS rendering**: RSS feeds bypass Cloudflare challenge
- **Polite crawling**: 2s delay between requests
- **SQLite dedup**: articles keyed by URL; re-runs are fast
- **Async I/O**: `aiohttp` + `asyncio`
- **Structured logging**: `structlog` → stderr; stdout = pure JSON
