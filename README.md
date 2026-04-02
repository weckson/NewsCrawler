# NewsCrawler — Financial News Ingestion System

Production-grade financial-news crawling system. Includes **Benzinga** and explicitly **excludes** sources already in `weckson/AIStock` (Polygon, SEC/EDGAR, FINRA, Finnhub, Reddit, FRED, yfinance).

The `data/aistock/` export in this repo is produced specifically to serve `D:\AIStock` as a local news-input source. AIStock reads these snapshots as an auxiliary curated news feed; this repo is the upstream producer for that data contract.

## Sources

| Priority | Source | Method |
|---|---|---|
| 1 | Benzinga (News API + Press Releases) | Licensed API + delta pulls |
| 2 | Reuters via LSEG/RDP | Vendor feed adapter (stub — requires entitlement) |
| 3 | Company IR RSS | RSS polling |
| 4 | PR Newswire RSS | RSS polling |
| 5 | Business Wire RSS | RSS polling |
| 6 | ASX ComNews / announcements | Licensed or web |
| 7 | ASIC newsroom | RSS / low-frequency web |

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

### 2. Prepare config

PowerShell:

```powershell
Copy-Item sample.env .env
Copy-Item config\sources.example.yaml config\sources.yaml
```

Bash:

```bash
cp sample.env .env
cp config/sources.example.yaml config/sources.yaml
```

Then edit `.env`.

Minimum settings for the scheduler:

```env
BENZINGA_API_KEY=your_key_here
DATABASE_URL=postgresql://crawler:crawler@localhost:5432/newscrawler
OBJECT_STORE_BUCKET=./data/raw
```

### 3. Start PostgreSQL

```bash
docker-compose up -d db
```

Apply the schema if your database is empty:

```bash
psql postgresql://crawler:crawler@localhost:5432/newscrawler -f migrations/001_initial.sql
```

## How To Run

This repo has two runnable entry points.

### Option A: Main scheduler

Use this for the full configurable crawler under `crawler/`.

Run one pass:

```bash
python -m crawler --once
```

Run one source only:

```bash
python -m crawler --once --source benzinga_news_api
```

Run continuously:

```bash
python -m crawler
```

Installed script `newscrawler` is equivalent to `python -m crawler`.

Useful flags:

```text
--config config/sources.yaml
--once
--source <source_key>
```

### Option B: Standalone multi-ticker news crawler

`crawl_news.py` is the generic SQLite-based crawler. It does not require PostgreSQL or a Benzinga API key.

Default run:

```bash
python crawl_news.py
```

This defaults to the built-in `aistock500` preset.

Single ticker:

```bash
python crawl_news.py --ticker NVDA
```

Small watchlist:

```bash
python crawl_news.py --tickers AMD,NVDA,AVGO
```

Built-in watchlists:

```bash
python crawl_news.py --ticker-set semis20
python crawl_news.py --ticker-set aistock200
python crawl_news.py --ticker-set aistock500
```

`mega_watchlist` remains available as a backward-compatible alias for `aistock500`.
`aistock200` is the legacy 203-ticker core watchlist with the noisiest single-letter symbols `A`, `C`, and `T` removed.

Full-text modes:

```bash
python crawl_news.py --ticker-set aistock500 --fulltext-mode off
python crawl_news.py --ticker-set aistock500 --fulltext-mode high-value
python crawl_news.py --ticker-set aistock500 --fulltext-mode all
```

Pretty console report:

```bash
python crawl_news.py --ticker-set aistock500 --output pretty
```

Legacy entry point:

```bash
python crawl_amd.py --ticker-set aistock500
```

Useful flags:

```text
--ticker <symbol>
--tickers AMD,NVDA,...
--ticker-set semis20|aistock200|core200|aistock500|mega_watchlist
--hours 48
--fulltext-mode off|high-value|all
--fulltext-max-articles 8
--output newsitem-json|pretty
```

Primary outputs:

- Runs: `data/runs/<run_id>/`
- SQLite: `data/news.db`
- AIStock-compatible exports: `data/aistock/`

## How To View Data

### 1. Check the latest run output

PowerShell:

```powershell
Get-ChildItem data\runs | Sort-Object Name -Descending | Select-Object -First 3 Name, FullName
```

Each run directory contains:

- `summary.json`
- `filtered_articles.json`
- `news_items.json`
- `aistock_payload.json`
- `by_ticker/`
- `report.txt`
- `run.log`

### 2. Open the SQLite database

If `sqlite3.exe` is on your machine:

```powershell
sqlite3 data\news.db
```

If your local path is `D:\SQL\sqlite3.exe`:

```powershell
D:\SQL\sqlite3.exe D:\NewsCrawler\data\news.db
```

### 3. AIStock-compatible exports

Stable aggregate files:

- `data/aistock/latest_news_items.json`
- `data/aistock/latest_payload.json`

These files are intended for direct consumption by `D:\AIStock`. If you change their schema or path layout, update AIStock's local crawler connector at the same time.

Stable per-ticker files:

- `data/aistock/by_ticker/NVDA/latest_news_items.json`
- `data/aistock/by_ticker/NVDA/latest_payload.json`
- `data/aistock/by_ticker/NVDA/latest_summary.json`

`latest_payload.json` matches AIStock collector-style keys:

```json
{
  "hard_event_news": [],
  "soft_event_news": [...],
  "alt_sentiment_news": [],
  "news": [...]
}
```

### 4. Common SQL queries

Show tables:

```sql
.tables
```

Inspect schema:

```sql
.schema news
```

Count all news rows:

```sql
select count(*) from news;
```

Count rows with full text:

```sql
select count(*) from news where body is not null and trim(body) <> '';
```

View latest 20 rows:

```sql
select title, tickers, published
from news
order by published desc
limit 20;
```

View latest rows for one ticker:

```sql
select title, tickers, published, url
from news
where tickers like '%NVDA%'
order by published desc
limit 20;
```

View only rows with full text:

```sql
select title, tickers, substr(body, 1, 200)
from news
where body is not null and trim(body) <> ''
order by published desc
limit 20;
```

## Project Structure

```
crawler/
  scheduler.py          # cron-like planner + CLI entry point
  fetcher.py            # async HTTP, rate limiting, backoff, conditional requests
  parsers/
    benzinga.py         # Benzinga News API JSON parser
    rss.py              # RSS/Atom parser (PR Newswire, Business Wire, IR, ASIC)
    html.py             # minimal HTML parser (ASX, web sources)
  dedupe/
    canonical.py        # URL normalisation + stable fingerprints
    simhash.py          # near-duplicate detection (64-bit SimHash)
  storage/
    postgres.py         # async psycopg3 data access
    object_store.py     # S3-compatible or local filesystem raw payload store
  compliance/
    robots.py           # robots.txt fetch + cache (RFC 9309)
    policy.py           # per-source licensing + redistribution flags
    captcha.py          # CAPTCHA detection + stop-and-escalate (NO solving)
  alerts/
    rules.py            # alert rule evaluation (keyword/ticker/source/topic)
    dispatcher.py       # Slack/email/webhook dispatch
migrations/
  001_initial.sql       # PostgreSQL DDL (5 tables)
tests/                  # unit + integration tests
config/
  sources.example.yaml  # source configuration template
```

## Compliance Policy

- **No CAPTCHA solving.** On detection: stop the domain, log an incident, escalate to operator.
- **No paywall bypassing.** Use licensed API feeds; not scraped logged-in pages.
- **Full text** stored only when `store_full_text: true` in source config (API licence required).
- **Redistribution** blocked by default (`no_redistribution` compliance flag).
- **robots.txt** honoured per RFC 9309.
- **API keys** stored via environment variables only; never in source code or logs.

## Excluded Sources

The following are **NOT** implemented here (already in `weckson/AIStock`):

- Polygon (including Polygon News)
- SEC/EDGAR
- FINRA
- Finnhub
- Reddit
- FRED
- yfinance / Yahoo Finance

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `BENZINGA_API_KEY` | Yes | Benzinga Cloud API key |
| `DATABASE_URL` | Yes | PostgreSQL connection URL |
| `OBJECT_STORE_BUCKET` | Yes | S3 bucket name or local path (e.g., `./data/raw`) |
| `SLACK_WEBHOOK_URL` | No | Slack incoming webhook for alerts |
| `ALERT_EMAIL` | No | Email address for regulatory alerts |
| `METRICS_PORT` | No | Prometheus metrics port (default: 9090) |
| `RDP_CLIENT_ID` | No | Reuters/RDP OAuth client ID (future adapter) |

## Metrics (Prometheus `/metrics`)

| Metric | Description |
|---|---|
| `crawler_fetch_total` | Total HTTP fetches by source + status |
| `crawler_fetch_latency_seconds` | Fetch latency histogram |
| `crawler_rate_limit_total` | 429/503 rate-limit events |
| `crawler_detection_total` | CAPTCHA/403 detection events |
| `crawler_sources_active` | Number of enabled sources |
| `crawler_items_ingested_total` | Items ingested per source |

## Running Tests

```bash
pytest tests/ -v
```

All 5 acceptance tests from the design doc are implemented:
1. **Benzinga delta ingest** — `test_benzinga_parser.py`
2. **Retry-After test** — `test_fetcher.py::test_retry_after_respected`
3. **Conditional GET test** — `test_fetcher.py::test_conditional_get_etag`
4. **Dedupe test** — `test_dedupe.py::test_near_duplicate_items_share_cluster`
5. **Forbidden sources audit** — `test_compliance.py::test_no_forbidden_sources_in_config`
