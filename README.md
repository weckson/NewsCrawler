# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler for US equities. No API key required.
Produces per-ticker JSON exports consumed by [`D:\AIStock`](../AIStock) as a curated news data source.

Single-file design — everything lives in `crawl_news.py`. Full-text extraction via trafilatura.

> `AGENTS.md` / `CLAUDE.md` carry the deep operator notes (per-channel rationale,
> bitemporal storage, integration harness, key-function line map). This README is
> the quick start.

## Sources

**Per-ticker sources:**
| Channel | Method | Volume |
|---------|--------|--------|
| Google News → Benzinga (`site:benzinga.com when:2d`) | Google as BZ proxy | ~100 articles |
| Google News → SeekingAlpha (`site:seekingalpha.com when:2d`) | Google as SA proxy (SA's own RSS is 98% stale) | ~100 articles |
| Google News broad (`when:1d` + `when:2d`) + site proxies for Reuters/Bloomberg/CNBC (top-150 hot tickers) | All sources | variable |
| Yahoo Finance RSS (`/rss/headline?s={TICKER}`) | Per-ticker headlines, trust=3 | ~20 articles |
| Nasdaq RSS (`/feed/rssoutbound?symbol={TICKER}`) | Curated, best fulltext rate | ~15 articles |
| Company IR RSS (`data/ir_feeds.json`, 158 feeds) | PRIMARY sources — zero re-reporting latency, trust=3 | per-company |
| Finnhub company news (optional, if `FINNHUB_API_KEY` set) | REST API | per-company |

**Global tripwire / broker channels** (fetched once per run, watchlist runs only):
| Channel | Method | Notes |
|---------|--------|-------|
| SEC EDGAR filings (`sec_edgar`) | EDGAR "latest filings" Atom feed | Form 4 / 8-K / SC 13D/G with exact acceptance timestamps; `--no-sec-edgar` disables |
| IBKR TWS wire news (`ibkr_news`) | `ib_insync` readonly socket to a running TWS/IB Gateway | Dow Jones + Briefing.com headlines; silently absent when TWS closed; `--no-ibkr-news` disables |
| M&A wire tripwire (`wire_tripwire`) | PR Newswire M&A feed + GlobeNewswire | Catches breaking deals 6-12h before secondary aggregators |
| Benzinga RSS | Official RSS | Shared fetch is now a no-op (Cloudflare 403); BZ flows via `google_benzinga` proxy |

No direct HTML scraping — all feeds are structured RSS, no Cloudflare challenge.
Google News queries use `when:2d` to return only recent articles, dramatically improving freshness.
Full article text is extracted with [trafilatura](https://trafilatura.readthedocs.io/) when available (boilerplate-free main content), falling back to a regex-based extractor.
Re-discover IR feeds with `python scripts/probe_ir_feeds.py` (quarterly / after watchlist changes).

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

# Fast breaking-event pass: ONLY the global tripwire channels (~5s vs ~14min)
python crawl_news.py --fast-only

# Tests
pytest tests/ -q
```

## Watchlist & Ticker Sets

| Preset | Source | Count |
|--------|--------|-------|
| `aistock` **(default)** | Reads `D:\AIStock\config\default.json` live | matches AIStock exactly |
| `moomoo` | Reads moomoo OpenD (:11111) — watchlist groups ∪ US positions; falls back to `aistock` if OpenD down | your live moomoo list |
| `aistock500` / `mega_watchlist` | Hardcoded fallback (used if AIStock repo not found) | 500 |
| `aistock200` / `core200` | Legacy core watchlist (noisy single-letter symbols removed) | ~200 |
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
--no-ibkr-news                 Disable IBKR TWS wire-news channel (auto-skipped if TWS closed)
--no-sec-edgar                 Disable SEC EDGAR filings channel (Form 4 / 8-K / 13D/G)
--fast-only                    Run ONLY the global tripwire channels (wire+sec+ibkr),
                               skip per-ticker RSS + Finnhub. Keep rolling ENABLED.
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
  "id":                "uuid-v5",
  "timestamp_utc":     "2026-04-06T12:00:00+00:00",
  "first_seen_at_utc": "2026-04-06T12:08:30+00:00",
  "source":            "yahoo_finance",
  "url":               "https://...",
  "title":             "Article headline",
  "body":              "Full text or summary snippet (nullable)",
  "author":            null,
  "language":          "en",
  "ticker":            "NVDA",
  "tickers_hint":      ["NVDA", "AMD"],
  "source_quality":    "high",
  "publisher_raw":     "finance.yahoo.com",
  "trust_tier":        3,
  "body_kind":         "article_text",
  "ingest_source":     "newscrawler_local",
  "meta": {
    "source_name":   "Yahoo Finance",
    "source_domain": "finance.yahoo.com",
    "event_origin":  "news",
    "relevance":     0.75,
    "quality_score": 0.82,
    "importance":    0.65,
    "event_types":   ["earnings_release", "analyst_rating"],
    "sentiment":     {"score": 0.67, "pos": 0.032, "neg": 0.006, "unc": 0.010, "matched": 12},
    "classifier_version":          "tax_v7-lm_v3-disambig_v2",
    "first_seen_at_utc":           "2026-04-06T12:08:30+00:00",
    "publish_to_observe_latency_s": 510,
    "_channel":      "google_broad"
  }
}
```

| Field | Values | Description |
|-------|--------|-------------|
| `timestamp_utc` | ISO-8601 | Source-claimed publish time (**valid time**) — display / narrative |
| `first_seen_at_utc` | ISO-8601 | When NewsCrawler first observed the URL (**transaction time**) — **immutable, use for PIT/backtest windowing** |
| `trust_tier` | 3 / 2 / 1 / 0 | 3=TOP (WSJ/Bloomberg), 2=OK (Yahoo/SA), 1=low, 0=blocked |
| `body_kind` | `article_text` / `summary_snippet` | Full text fetched vs RSS excerpt |
| `ingest_source` | `newscrawler_local` | Fixed marker; AIStock scorer applies 15% discount to snippets |
| `source_quality` | `high` / `medium` / `low` | Derived from trust_tier |
| `meta.classifier_version` | e.g. `tax_v7-lm_v3-disambig_v2` | Which taxonomy/lexicon produced the derived signals; backtests can pin |
| `meta.publish_to_observe_latency_s` | int ≥ 0 | publish → observe lag; AIStock scorer uses for staleness penalty |

> **Backtest replay MUST window on `first_seen_at_utc`, never `timestamp_utc`** —
> sources occasionally backfill old articles with wrong publish times, so using
> valid time creates look-ahead bias. `first_seen_at_utc` is immutable across
> re-fetches. See the Bitemporal Storage section of `AGENTS.md` for the PIT
> query API (`load_rolling_window(..., as_of=T)`).

## Quality Pipeline

```
Raw RSS articles
  → URL canonicalize (strip utm_*/fbclid/gclid tracking params → dedup key)
  → Time recency    (hard cutoff = now - hours)
  → Blocked domains (trust_tier == 0 dropped)
  → Relevance       (ticker mention density ≥ 0.10)
  → Junk filter     (listicles, sponsored content)
  → Weak-signal     (holdings / profile fluff)
  → Quality score   (7-signal composite, threshold 0.45)
  → SimHash dedup   (Hamming ≤ 6 = same article)
  → Re-score        (source_diversity signal updated post-dedup)
  → Noise floor     (drop hype/fluff; per-ticker minimum retention)
  → Quality gate    (drop below threshold; adaptive 0.40 fallback)
  → Sort: quality_score ↓  newest first
  → [optional] Full-text fetch for high-value articles (extraction off-loop)
```

Each exported item also carries `meta.importance` (0.0–1.0) — a rules-only
priority hint (event type + primary source + magnitude + corroboration +
relevance) for downstream ranking. It is advisory: it never drops or reorders
what gets exported.

The **noise floor** drops obvious promotional / engagement-bait headlines
("unstoppable stock", "is attracting investor attention", "reasons to buy") that
slip past the quality gate on trust-3 sources, but guarantees a per-ticker
minimum (`NEWSCRAWLER_NOISE_MIN_RETAIN`, default 2) so a thinly-covered ticker
is never zeroed. Disable with `NEWSCRAWLER_NOISE_FILTER=0`.

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

500 tickers complete in ~12-15 minutes (down from ~67 min sequential).

Optimizations:
- **Per-domain rate limiting** — Google News 0.6s, Yahoo Finance 0.5s, Nasdaq 1.5s, SEC 0.15s, others 1.0s
- **5 tickers in parallel** — `asyncio.Semaphore` with shared HTTP connection pool
- **Shared Benzinga RSS** — fetched once globally, distributed to all tickers
- **DNS caching** — 300s TTL on the shared `aiohttp.TCPConnector`
- **Cross-run caches** — SQLite-backed body extraction, Google URL resolution, and HTTP `ETag`/`Last-Modified` conditional GET. Warm caches give ~3-4x speedup on a re-run.

Tunable constants at the top of `crawl_news.py`: `TICKER_CONCURRENCY`, `DOMAIN_DELAY`.

## Ops Scripts & Monitoring

| Script | Role | Writes to production? |
|--------|------|-----------------------|
| `crawl_news.py` | The only production entry point — full crawl / `--fast-only` breaking-event pass | Yes — `news.db` + AIStock exports |
| `scripts/monitor_late_events.py` | Daily watchdog: flags high-value events caught only via slow channels >6h late. `data/validation/late_events_latest.{md,json}` | No (read-only, always exits 0) |
| `scripts/probe_ir_feeds.py` | Re-discover company IR RSS feeds → `data/ir_feeds.json` (run quarterly / after watchlist changes) | Rewrites `data/ir_feeds.json` |
| `scripts/ibkr_ensure_tws.py` | Unattended TWS/IB Gateway login + news-farm warm-up. Run from the account owner's own scheduler, NEVER from the crawl process | No (crawl stays credential-free) |

`crawler/` (except `crawler/sources/`) and `crawl_amd.py` are **legacy** — not
imported by `crawl_news.py`. Only touch `crawl_news.py` and
`tests/test_crawl_news_*.py` for production work.

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
| `NEWSCRAWLER_SEC_EDGAR` | `1` | Set `0` to disable the SEC EDGAR filings channel |
| `NEWSCRAWLER_SEC_USER_AGENT` | *(declared UA)* | UA sent to SEC per its fair-access policy |
| `NEWSCRAWLER_IBKR_NEWS` | `1` | Set `0` to disable the IBKR TWS wire-news channel |
| `NEWSCRAWLER_IBKR_PORT` | *(probe 7496/4001/7497/4002)* | Pin the TWS/Gateway socket port |
| `NEWSCRAWLER_IBKR_CLIENT_ID` | `23` | `ib_insync` clientId (keep distinct from other consumers) |
| `NEWSCRAWLER_IBKR_MAX_TICKERS` | `600` | Cap on tickers queried for IBKR wire news |
| `NEWSCRAWLER_NOISE_FILTER` | `1` | Set `0` to disable the hype/fluff noise filter |
| `NEWSCRAWLER_NOISE_MIN_RETAIN` | `2` | Per-ticker floor the noise filter never drops below |
| `FINNHUB_API_KEY` | *(unset)* | Enables the optional Finnhub company-news channel when present |

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
pytest tests/ -q    # active crawl_news suite: 203 passed, 1 skipped
```

The active suite is `tests/test_crawl_news_*.py` (covers `crawl_news.py`). The
other files cover the legacy `crawler/` package and include network-touching
cases; run the active suite alone offline with:

```bash
pytest tests/test_crawl_news_*.py -q
```

| Test file | Coverage |
|-----------|---------|
| `tests/test_crawl_news_quality.py` | Quality scoring signals, composite scorer, filter gating, ticker parsing, presets |
| `tests/test_crawl_news_output.py` | NewsItem schema, payload envelope, full-text extraction, Google URL decoding |
| `tests/test_crawl_news_output_contract.py` | AIStock output-contract lock (schema keys, time formats, meta) — safety net for refactors |
| `tests/test_crawl_news_upgrade.py` | URL canonicalization + `importance_score` (2026-07-29) |
| `tests/test_crawl_news_noise.py` | Noise filter recall guard + minimum-retention floor (2026-07-29) |
| `tests/test_crawl_news_paths.py` | Path resolution, env var overrides, relative path scoping |
| `tests/test_crawl_news_edgar.py` | SEC EDGAR channel: form-type whitelist, CIK gating, synthetic titles, dedup guard |
| `tests/test_crawl_news_ibkr.py` | IBKR wire-news channel: DJ noise filter, attribution, cold-farm retry |
| `tests/test_crawl_news_time.py` | RSS publish-time UTC parsing (guards the `calendar.timegm` skew fix) |
| `tests/test_rss_parser.py` | RSS parsing, URL canonicalization, content type inference |
| `tests/test_benzinga_parser.py` | Benzinga-specific RSS parsing |
| `tests/test_fetcher.py` | HTTP retry, conditional GET, rate limiting, captcha handling |
| `tests/test_alerts.py` | Alert generation and filtering |
| `tests/test_compliance.py` | Compliance checks and validation |
| `tests/test_dedupe.py` | SimHash fingerprinting, Hamming distance, near-duplicate clustering |
