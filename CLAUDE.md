# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler producing AIStock-compatible exports.
No Benzinga API key required. Sources: Benzinga RSS, Google News RSS, company IR feeds.

## Quick Start

```bash
# Default run: aistock500 watchlist (500 tickers), last 48h
python crawl_news.py

# Single ticker
python crawl_news.py --ticker NVDA

# Custom tickers
python crawl_news.py --tickers NVDA,AMD,MSFT --hours 24

# Built-in presets: semis20 | aistock200 | aistock500
python crawl_news.py --ticker-set semis20

# Custom data directory (for Linux deployment)
python crawl_news.py --data-dir /var/lib/newscrawler

# Human-readable output
python crawl_news.py --ticker NVDA --output pretty

# Tests
pytest tests/ -q
```

## Output & Data Contract with AIStock

### Primary exports (read by D:\AIStock)

| Path | Description |
|------|-------------|
| `data/aistock/latest_news_items.json` | Flat list of all NewsItems from last run |
| `data/aistock/latest_articles.json` | Raw filtered articles |
| `data/aistock/latest_payload.json` | Envelope with metadata |
| `data/aistock/by_ticker/{TICKER}/latest_news_items.json` | Per-ticker split |

AIStock reads `data/aistock/latest_news_items.json` via `crawler_news_source.py`.
Config key in AIStock: `"crawler_news_path": "../NewsCrawler/data/aistock/latest_news_items.json"`.

### NewsItem schema (top-level fields)

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

**Key fields for AIStock:**
- `trust_tier` — `3`=top (WSJ/Bloomberg/Reuters), `2`=ok (Yahoo/Seeking Alpha), `1`=low, `0`=blocked
- `body_kind` — `"article_text"` (full body fetched) or `"summary_snippet"` (RSS excerpt only)
- `ingest_source` — always `"newscrawler_local"` — used by AIStock scorer for 15% discount on snippets
- `tickers_hint` — all tickers the article is relevant to; used for routing in AIStock event engine
- `source_quality` — `"high"` / `"medium"` / `"low"` (derived from trust_tier)

AIStock filters: items with `trust_tier < 2` AND `source_quality == "low"` are dropped
(configurable via `crawler_news_min_trust_tier` in AIStock `config/default.json`).

## Quality Pipeline

```
Raw RSS articles
  → Time recency (cutoff = now - hours)
  → Trust tier filter (blocked sources dropped)
  → Relevance score (ticker mention density, ≥0.15 pass)
  → Junk title filter
  → Weak-signal filter (holdings/profile content)
  → SimHash near-dedup (Hamming distance ≤ 3 = same article)
  → Sort: trust_tier desc → relevance desc → newest first
  → [optional] Full-text fetch for high-value articles
```

## Trust Tier Reference

| Tier | Label | Examples |
|------|-------|---------|
| 3 | TOP | WSJ, Bloomberg, Reuters, CNBC, FT |
| 2 | OK  | Yahoo Finance, Seeking Alpha, Barron's, MarketWatch |
| 1 | low | Unknown/generic sources |
| 0 | BLK | Spam/blocked domains |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEWSCRAWLER_DATA_DIR` | `./data` | Base data directory |
| `NEWSCRAWLER_DB_PATH` | `{DATA_DIR}/news.db` | SQLite database |
| `NEWSCRAWLER_RAW_DIR` | `{DATA_DIR}/raw` | Raw RSS payloads |
| `NEWSCRAWLER_RUNS_DIR` | `{DATA_DIR}/runs` | Per-run artifact directories |
| `NEWSCRAWLER_AISTOCK_EXPORT_DIR` | `{DATA_DIR}/aistock` | AIStock export root |
| `NEWSCRAWLER_TICKER_EXPORTS_DIR` | `{AISTOCK_DIR}/by_ticker` | Per-ticker exports |

All paths resolve relative to the script location (`PROJECT_ROOT`) when given as relative paths.
This means `python /opt/NewsCrawler/crawl_news.py` works correctly from Linux cron/systemd.

## CLI Flags

```
--ticker NVDA                  Single ticker
--tickers NVDA,AMD,MSFT        Comma-separated list
--ticker-set aistock500        Built-in preset (semis20 | aistock200 | aistock500)
--hours 48                     Look-back window in hours (default: 48)
--fulltext-mode high-value     off | high-value | all (default: high-value)
--fulltext-max-articles 8      Per-ticker cap for full-text fetch
--output newsitem-json         newsitem-json (default) | pretty
--data-dir /path/to/data       Override base data directory
```

## Run Artifacts

Each run creates `data/runs/{timestamp}_{label}/`:
- `run.log` — structured log of the full run
- `summary.json` — metadata (tickers, counts, timing)

## Files

```
crawl_news.py              Main crawler (single-file, no submodules)
tests/
  test_crawl_news_paths.py Path resolution unit tests
data/
  news.db                  SQLite article store
  aistock/                 AIStock export (consumed by D:\AIStock)
  raw/                     Raw RSS XML payloads (debug)
  runs/                    Per-run artifacts
```

## Architecture Notes

- **Single-file design**: everything in `crawl_news.py` — no submodules.
- **No API key**: uses public RSS feeds only (Benzinga RSS, Google News RSS).
- **No JS rendering**: avoids Cloudflare-blocked HTML scraping.
- **Polite crawling**: 2s delay between requests, conditional GET headers.
- **SQLite dedup**: articles stored with URL as key; re-runs skip already-saved items.
- **Async I/O**: `aiohttp` for concurrent fetching, `asyncio` event loop.
- **Logs to stderr**: structured logs go to stderr; stdout is pure JSON for piping.
