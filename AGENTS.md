# NewsCrawler — Financial News Ingestion System

Quality-filtered financial news crawler producing AIStock-compatible per-ticker exports.
No API key required. Per-ticker channels:
- **SEC EDGAR filings** (`sec_edgar`, 2026-07-19) — Form 4 (insider), 8-K
  (material event), SC 13D/G (>5% stakes) straight from EDGAR's "latest
  filings" Atom feed (`action=getcurrent`), one GLOBAL request per form family
  per run + weekly-cached `company_tickers.json` (ticker↔CIK). Motivated by
  the 2026-07 moomoo evaluation: its best-signal items were SEC filing
  re-posts but date-only (PIT-unusable); EDGAR carries exact acceptance
  timestamps. Two-stage filter: exact form-type whitelist (宁缺毋滥 — NPORT-P
  / 13F-HR / 424B* never enter) → issuer/subject CIK ∈ watchlist. Synthetic
  titles phrased to hit tax_v7 patterns so event_types survive rolling-window
  re-classification: Form 4→insider_activity, 8-K→regulatory, 13D/G→
  ma_activity. trust=3, authoritative CIK attribution → relevance floor 0.55
  gate bypassed, weak-signal exempt, fulltext skipped (links are filing index
  pages). SEC fair-access: declared UA (`NEWSCRAWLER_SEC_USER_AGENT`) +
  `www.sec.gov` DOMAIN_DELAY 0.15s. `--no-sec-edgar` or
  `NEWSCRAWLER_SEC_EDGAR=0` disables. Watchlist runs only (≥2 tickers).
- **IBKR TWS wire news** (`ibkr_news`, 2026-07-19) — Dow Jones (DJ-N/RT/RTA/RTE/
  RTG/DJNL) + Briefing.com headlines from a RUNNING TWS/IB Gateway via
  `ib_insync` readonly socket (`crawler/sources/ibkr_news.py`). Validated vs
  RSS channels: 73.8% of headlines unique, matched stories observed median
  6.8h earlier, full Barron's/WSJ paywalled text via reqNewsArticle (gated to
  high-value events, ≤40 bodies/run). trust=3 (DJ), authoritative conid
  attribution → relevance floor 0.30 no hard gate, weak-signal exempt (DJ
  insider/stake wire stories are signal, not 13F churn). Source-level noise
  filter (`is_low_signal_dj`) drops DJ auto market-data columns ("X Stock
  Slides 5.3%, Underperforms Peers") + "Dow Jones Futures" macro roundups —
  ~16% of a 150-ticker run, zero incremental signal; applied at fetch AND at
  rolling-window reload so it prunes stored rows immediately. Market-Talk and
  real price-move stories are preserved. FULL-watchlist coverage (2026-07-20:
  old top-150 hot-gate starved 17/27 flagged late M&A/regulatory events on
  mid-caps — UBER/KKR/LLY/BIIB/DUK/SCCO sat outside top-150 so DJ wire never
  queried them; a no-news ticker returns empty fast so full coverage costs
  request count not data; `NEWSCRAWLER_IBKR_MAX_TICKERS` caps the list, default
  600). Silently absent when TWS closed; `--no-ibkr-news` or
  `NEWSCRAWLER_IBKR_NEWS=0` disables. Watchlist runs only (≥2 tickers).
  Cold-farm resilience: a timed-out `reqHistoricalNews` (common right after a
  fresh login/restart) retries once after a 3s warm-up. UNATTENDED LOGIN:
  `scripts/ibkr_ensure_tws.py` idempotently brings TWS to a logged-in state —
  launches TWS, drives the Swing login by click-points, enters username/password
  from `D:/AIStock/.env` + IBKR Mobile Authenticator TOTP via AIStock's
  `shared.ibkr_launcher.current_totp_code()` (reused). After a COLD login it
  also warms the news farm (`warm_news_farm`: probes AAPL reqHistoricalNews
  until it responds, ≤90s) before returning, so the crawl that follows gets
  full coverage instead of the thin cold-farm harvest (observed 26 vs ~100
  items on 2026-07-20). Run it from the account
  owner's own scheduler (Task Scheduler at logon / a daily_run pre-step), NEVER
  from the crawl process (which stays credential-free). Pairs with TWS native
  Auto-Restart; only the weekly IBKR reset forces a re-auth.
- **M&A wire tripwire** (`wire_tripwire`, 2026-06-29) — fetches PR Newswire's
  dedicated M&A feed + GlobeNewswire + Business Wire (2026-07-30) ONCE per run;
  two-stage cheap→expensive filter (high-value event-type gate → watchlist
  company-name match) keeps only breaking events about watchlist names. Catches
  M&A 6-12h before secondary aggregators (Yahoo/Benzinga) surface them. Root
  case: Rocket Lab→Iridium $8B broke 01:00 UTC, secondary sources didn't carry
  it until ~12:00; the tripwire reads the company's own press release at
  announcement. trust=3, authoritative attribution, relevance gate bypassed.
  The event-type gate `_TRIPWIRE_EVENT_TYPES` was widened 2026-07-30 from
  {ma_activity, earnings_release, earnings_guidance, regulatory} to also include
  {major_contract, capital_investment, partnership} — a $1B contract win or a
  $1.2B plant build is also a first-publication event on these wires. Business
  Wire's broad all-news feed is fine because the two-stage filter drops the
  consumer/PR fluff (verified 2026-07-30 against the live feed).
- **Company IR feeds** — 158 discovered IR RSS feeds (`data/ir_feeds.json`, tag `ir_feed`).
  PRIMARY sources: zero media-re-reporting latency, trust=3, authoritative attribution.
  Re-discover with `python scripts/probe_ir_feeds.py` (quarterly / after watchlist changes).
- **Google News** (per-ticker queries + site-specific proxies for Benzinga/SeekingAlpha/Reuters/Bloomberg/CNBC with when:1d/2d freshness)
- **Yahoo Finance RSS** (per-ticker, authoritative attribution)
- **Nasdaq RSS** (per-ticker, highest fulltext rate)

**AIStock agreement boost** (2026-06-10, accuracy-first):
In AIStock's `event_engine/processor.py`, `meta.event_types` is used ONLY as
a corroboration signal: when the LLM's classification and NC's regex tag
independently agree, confidence rises +0.10 (cap 0.99). NC tags never decide
or override a classification.

**Why nothing stronger**: validated against 789 real classified items —
NC-vs-LLM agreement measured 22% (ma_activity), 27% (earnings_release),
70% (analyst_rating), 83% (macro), 95% (litigation). The taxonomy is
coverage-oriented (momentum candidate detection), NOT classification-precise.
A pre-gate (skip LLM) and an OTHER-rescue (override LLM's OTHER) were both
built, validated, and REMOVED on these numbers. 宁缺毋滥.

The worst regex offender (`to buy` matching "Stocks to Buy Now" listicles)
was fixed in `ma_activity` — bare "to buy" now requires a deal-context anchor
("in talks to buy", "agrees to buy", "deal to buy").

Single-file design — everything lives in `crawl_news.py`. Full-text extraction via trafilatura.

## Active vs Legacy Files

| File / Dir | Status | Notes |
|------------|--------|-------|
| `crawl_news.py` | **Active** — the only production entry point | Single-file async crawler |
| `tests/test_crawl_news_*.py` | **Active** — covers `crawl_news.py` | Run via `pytest tests/ -q` |
| `crawler/sources/` | **Active** | `finnhub_api.py` + `ibkr_news.py` — imported by `crawl_news.py` |
| `crawler/` (rest) | Legacy | Earlier multi-module prototype; not imported by `crawl_news.py` |
| `crawl_amd.py` | Legacy | Early single-ticker experiment; superseded by `crawl_news.py` |

Only touch `crawl_news.py` and `tests/test_crawl_news_*.py` for production work.

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
| `moomoo` | Reads moomoo OpenD (:11111) — custom watchlist groups ∪ US positions | your live moomoo list; falls back to `aistock` if OpenD down |
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
--fulltext-max-articles 20      Per-ticker full-text cap (default: 20)
--output newsitem-json          newsitem-json (default, stdout) | pretty (stderr report)
--data-dir /path/to/data        Override base data directory
--no-rolling                    Disable rolling-window export (default: enabled)
--no-ibkr-news                  Disable IBKR TWS wire-news channel (auto-skipped if TWS closed)
--no-sec-edgar                  Disable SEC EDGAR filings channel (Form 4 / 8-K / 13D/G)
--fast-only                     Run ONLY the global tripwire channels (wire+sec+ibkr),
                                skip per-ticker RSS + Finnhub. ~5s vs ~14min. For a
                                high-frequency (every 1-2h) breaking-event job; keep
                                rolling ENABLED (accumulates into news.db).
--summarize-errors 24           Audit mode: print error/warning report for last N hours and exit
```

## Late-Event Monitor (daily watchdog, 2026-06-29)

`scripts/monitor_late_events.py` is the standing guard against the Rocket Lab→
Iridium failure mode (breaking M&A caught 11h late via a slow secondary source).
Daily it scans `news.db` (last 24h), clusters high-value events (`ma_activity`,
`regulatory`, `earnings_guidance`), and FLAGS any that were:
- caught only via SLOW channels (no `wire_tripwire` / `ir_feed`), AND
- first observed >6h after the article's claimed publish time.

Each flag is a concrete "we were slow / a source gap exists" lead (e.g. a deal
that first broke on BusinessWire or via SEC 8-K). Writes
`data/validation/late_events_latest.{md,json}`. Informational — always exit 0.

Run standalone: `python scripts/monitor_late_events.py [--hours 24]`

Wired into AIStock `daily_run.py` as step **0d** (`_step_late_event_monitor`),
runs after the integration check (0c), in the NewsCrawler dir via subprocess.
The flagged count + top-3 examples surface in the Telegram summary so a missed
deal is visible the next morning (e.g.
`⚠️ 2 late high-value events — ON(ma_activity,40h), GILD(regulatory,34h)`).

## Integration Health Harness (AIStock side, 2026-06)

`D:/AIStock/scripts/selfcheck_news_integration.py` validates the full
NewsCrawler→AIStock chain offline (8 isolated checks): export freshness,
connector reads, PIT fields (first_seen_at/classifier_version), event_signals
flowing, **impact.py functional consumption** (synthetic large-M&A → severity
0.80), source diversity, trust-tier health, **broker-channel liveness**
(2026-07-20: WARN when ibkr_news / sec_edgar have 0 items in the rolling
export — these channels soft-fail inside NewsCrawler, so a silently dead
channel is otherwise invisible; the WARN carries where-to-look hints). Writes
`data/validation/news_integration_selfcheck_latest.md`, exit 0/1.

Wired into `scripts/daily_run.py` as step **0c** (`_step_news_integration_check`),
runs right after the NewsCrawler refresh (step 0b). Soft step — a FAIL surfaces
in the Telegram summary + ledger but never aborts the run (demo_live still
consumes whatever's in the export). Skipped alongside `--skip-news`. Catches
silent integration regressions (schema drift dropping event_signals, a dead
channel, trust collapse) that would otherwise quietly degrade the news layer.

Run standalone: `python scripts/selfcheck_news_integration.py [--quick]`

## Error Audit

Every WARNING / ERROR / CRITICAL event is written to a structured per-run file:

```
data/runs/{timestamp}_{label}/errors.jsonl
```

Each line is a JSON object with:
```json
{"ts": "2026-04-20T13:32:34+00:00", "level": "warning", "event": "fetch_rate_limited",
 "url": "https://news.google.com/...", "status": 503}
```

Files are only created if the run actually produced warnings/errors (clean runs
don't leave empty files). Typical events logged:

| Event | Level | Trigger |
|-------|-------|---------|
| `fetch_rate_limited` | warning | 429/503 after all retries exhausted |
| `fetch_error` | error | Final giveup with full exception type + message |
| `fetch_giveup` | warning | Non-retryable 4xx (e.g. 403 paywall) |
| `circuit_breaker_tripped` | warning | Domain paused after 5 consecutive failures |
| `aistock_watchlist_not_found` | warning | AIStock config missing, falling back to aistock500 |
| `wire_feed_error` | warning | Global wire feed fetch failed |

### Audit commands

```bash
# Aggregated summary of last 24h
python crawl_news.py --summarize-errors 24

# Last 7 days
python crawl_news.py --summarize-errors 168

# Raw grep across all runs
grep -l fetch_error data/runs/*/errors.jsonl

# Error rate by domain (past 24h)
python -c "from crawl_news import summarize_errors; import json; \
  print(json.dumps(summarize_errors(hours=24)['by_domain'], indent=2))"
```

The in-code API (`iter_error_events()` / `summarize_errors()`) can also be
imported for programmatic alerting (e.g. a cron job that emails if error
count > 100 in the last hour).

## Top-tier Source Coverage

Measured output share per 48h window (500-ticker run):

| Source | Articles | Notes |
|--------|---------:|-------|
| yahoo_finance.com | ~1000 | Per-ticker RSS — strongest backbone |
| seekingalpha.com | ~340 | Via Google proxy |
| benzinga.com | ~190 | Via Google proxy |
| barchart.com | ~70 | |
| fool.com | ~50 | |
| marketwatch.com | ~40 | |
| barrons.com | ~38 | |
| cnbc.com | ~20 | Via Google proxy (`site:cnbc.com`) |
| reuters.com | ~10 | Via Google proxy; Reuters focuses on macro/wire, not per-ticker |
| bloomberg.com | ~5 | Paywall limits; Google returns mostly landing pages |
| wsj.com / ft.com | ~3 each | Heavy paywalls |

**Reuters and Bloomberg are intrinsically low-coverage** because they focus on
macro/wire content rather than per-ticker analysis. Bloomberg's paywall
further restricts what Google News can surface. We proxy them through Google
for completeness, but don't rely on them for ticker-level depth.

## Rolling Window (accumulation across runs)

By default, `latest_news_items.json` and the per-ticker files contain **all
articles from the last `--hours` window**, not just this run's results.

How it works:
1. Every crawl writes fetched articles to SQLite (`news.db`)
2. After filtering, `load_rolling_window()` reads back all articles with
   `published >= now - hours` for the requested tickers
3. Current taxonomy/disambiguation is re-applied at load time (stale records
   that fail updated rules are auto-pruned)
4. Quality score is re-computed with latest signal definitions
5. Export uses the rolling window, not just this-run articles

Effect:
- **Repeated runs accumulate coverage** — a ticker with 0 articles this run
  still shows its articles from previous runs (if within `--hours`)
- **Taxonomy updates take effect immediately** without needing to re-crawl
  (next run's export uses new rules on historical DB)
- **3 runs spaced ~15 min apart typically yields 1.5-2x the single-run coverage**

Use `--no-rolling` to disable (e.g. for debugging single-run behavior).

## Bitemporal Storage (2026-05-13)

Quant-grade storage with **point-in-time (PIT) correctness** for backtest replay.

### Two time axes per article

| Column | Mutability | Semantics |
|--------|-----------|-----------|
| `published` (valid time) | Immutable on UPSERT | Source's claimed publish timestamp |
| `first_seen_at` (transaction time) | **Immutable forever** | When NewsCrawler first observed the URL |
| `fetched_at` | Updates on every refresh | Last time we re-fetched / touched the row |

⚠️ **Historical `published` skew (rows written before 2026-07-20).** `parse_rss`
built `published` with `time.mktime(entry.published_parsed)`. feedparser
normalizes those structs to UTC but `mktime` reads them as LOCAL time, so on a
non-UTC host every RSS timestamp was shifted by the machine's offset (−10h on
the Australia/Sydney box that wrote this DB; −11h for rows whose publish date
falls in AEDT). Fixed in `_rss_published_utc()` (`calendar.timegm`), guarded by
`tests/test_crawl_news_time.py`. Affects only the parse_rss channels
(`google_*`, `yahoo_finance_rss`, `nasdaq_rss`, `ir_feed`, `wire_tripwire`) —
~252k of 317k rows. **`first_seen_at` was never affected**, so PIT/backtest
correctness holds and the rolling-window export (which windows on
`first_seen_at`) self-heals within 48h. Pre-fix rows keep the skewed
`published`; anything reading `published` for age on historical rows should
add back the offset. Exact per-row inverse if a backfill is ever wanted:
`timegm(time.localtime(stored_epoch))` — must run on the same host timezone.

### SQLite tables added

| Table | Purpose |
|-------|---------|
| `news` (extended) | Now has `first_seen_at`, `content_hash`, `classifier_version` |
| `article_versions` | Content revision history (title/body changes over time) |
| `fulltext_cache` | Cross-run body extraction cache (skip trafilatura on known URLs) |
| `url_resolution` | Cross-run Google News URL → resolved URL cache |
| `feed_state` | HTTP `ETag` / `Last-Modified` per RSS feed for conditional GET |

### Classifier versioning

`CLASSIFIER_VERSION = "tax_v7-lm_v3-disambig_v2"` (constant in `crawl_news.py`).
Bumped manually when event taxonomy, LM lexicon, or disambiguation rules change.
Every `to_news_item()` tags its output with the current version. Backtests can
pin to a historical version by filtering `meta.classifier_version`.

### PIT query API

```python
from crawl_news import load_rolling_window
from datetime import datetime, timezone

# Live mode (default) — articles seen in last 48h, as of now
articles = load_rolling_window(["NVDA", "AAPL"], hours=48)

# Backtest mode — only articles available at simulated time T
T = datetime(2026, 5, 13, 9, 30, tzinfo=timezone.utc)
articles = load_rolling_window(["NVDA"], hours=48, as_of=T)
# → SQL: WHERE first_seen_at <= T AND first_seen_at >= T - 48h
# → Guaranteed zero look-ahead bias
```

### Cache effectiveness

End-of-run summary reports cache stats:
```
body_cache  in_run=12  sqlite=234  misses=18  hit_rate=93%
google_url  in_run=8   sqlite=140
http_304    hits=42/87  (48%)
```

- `body_cache.sqlite` hits = articles already fulltext-extracted in a previous run
- `http_304` hits = RSS feeds that returned 304 Not Modified (skipped parse)

Measured speedup on 2nd run of same crawl: **~3-4x faster** when caches are warm.

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

### NewsItem schema (bitemporal, 2026-05-13)

Top-level fields (match `shared/models.py NewsItem` in AIStock):

```json
{
  "id":                            "uuid-v5",
  "timestamp_utc":                 "2026-05-13T10:00:00+00:00",
  "first_seen_at_utc":             "2026-05-13T10:08:30+00:00",
  "source":                        "yahoo_finance",
  "url":                           "https://...",
  "title":                         "Article headline",
  "body":                          "Full text or summary snippet (nullable)",
  "author":                        null,
  "language":                      "en",
  "ticker":                        "NVDA",
  "tickers_hint":                  ["NVDA", "AMD"],
  "source_quality":                "high",
  "publisher_raw":                 "finance.yahoo.com",
  "trust_tier":                    3,
  "body_kind":                     "article_text",
  "ingest_source":                 "newscrawler_local",
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
    "first_seen_at_utc":           "2026-05-13T10:08:30+00:00",
    "publish_to_observe_latency_s": 510,
    "_channel":      "google_broad"
  }
}
```

### Bitemporal time fields ⭐

| Field | Semantics | Use case |
|-------|-----------|----------|
| `timestamp_utc` | Source-claimed publish time (**valid time**) | Display, narrative analysis |
| `first_seen_at_utc` | When NewsCrawler first observed this URL (**transaction time**) | **PIT filtering / backtest** |

**Critical**: Backtest replay MUST filter by `first_seen_at_utc <= simulated_T`,
never by `timestamp_utc`. Sources occasionally backfill old articles with
incorrect publish times — using `timestamp_utc` for time windowing creates
look-ahead bias.

`first_seen_at_utc` is **immutable** — NewsCrawler's `save_article` preserves
the original timestamp on every subsequent UPSERT. Content changes go to the
`article_versions` table.

### Other field semantics

| Field | Values | Notes |
|-------|--------|-------|
| `trust_tier` | 3/2/1/0 | 3=TOP (WSJ/Bloomberg), 2=OK (Yahoo/SA), 1=low, 0=blocked |
| `body_kind` | `article_text` / `summary_snippet` | Snippets get 15% scorer discount in AIStock |
| `ingest_source` | `newscrawler_local` | Fixed; used by AIStock validator and scorer |
| `tickers_hint` | list[str] | All relevant tickers; drives routing + entity linking |
| `source_quality` | `high`/`medium`/`low` | Derived from trust_tier |
| `meta.classifier_version` | e.g. `"tax_v7-lm_v3-disambig_v2"` | Which version produced derived signals; backtest can pin |
| `meta.publish_to_observe_latency_s` | int ≥ 0 | publish → observe lag; AIStock scorer uses for staleness penalty |
| `meta.importance` | float 0.0–1.0 | Rules-only priority hint (2026-07-29): event type + primary source + magnitude + corroboration + relevance. Advisory only — does not gate |

AIStock drops items where `trust_tier < min_trust_tier` (default 2) AND `source_quality == "low"`.

### AIStock-side consumption (2026-05-13)

`AIStock.shared.models.NewsItem` mirrors the top-level PIT fields:
`first_seen_at_utc`, `classifier_version`, `publish_to_observe_latency_s`.

The collector (`data_sources/collector.py::_check_news_freshness`) now filters
by `first_seen_at_utc` (falling back to `timestamp_utc` for non-NewsCrawler
sources). Sentiment scorer (`sentiment_engine/pipeline/scorer.py`) applies a
staleness multiplier based on observed latency:
- `< 30 min`:  no penalty
- `30 min - 3 hr`: 0.95×
- `3 - 24 hr`: 0.80×
- `> 24 hr`: 0.50× (likely backfill)

## Signal Quality Upgrades (2026-07-29)

Three minimally-invasive changes on top of the existing pipeline. All are
additive — the AIStock output contract is unchanged (locked by
`tests/test_crawl_news_output_contract.py`), old DB rows are never mutated, and
each has an off switch or is a pure new field. Branch:
`feat/crawler-importance-url-dedup-2026-07-29`.

1. **URL canonicalization dedup** (`canonicalize_url`, applied in `parse_rss`
   and after Google-News URL decode in `resolve_article_url`). Strips tracking
   / attribution query params (`utm_*`, `fbclid`, `gclid`, …) and normalizes
   scheme/host + query order, so `…?utm_source=rss` and `…?utm_source=twitter`
   variants of the same story collapse to one URL key (the dedup + SQLite store
   key on URL). Opaque `news.google.com` redirectors are left untouched until
   decoded, then canonicalized. Verified on the live DB: merges real Benzinga
   `utm_*` duplicates, **zero** false merges of distinct articles.

2. **Off-loop full-text extraction** (`fetch_article_body`). trafilatura's
   CPU-bound HTML parse now runs via `asyncio.to_thread` instead of inline on
   the event loop, so a slow page no longer stalls the other
   `ARTICLE_FETCH_CONCURRENCY` in-flight body fetches.

3. **Conservative noise filter with retention floor** (`is_noise` +
   `filter_noise_with_floor`, applied in `quality_filter` and
   `load_rolling_window` after dedup, before the quality gate). Drops obvious
   promotional / engagement-bait headlines (`NOISE_TITLE_PATTERNS`:
   "unstoppable stock", "is attracting investor attention", "reasons to buy",
   millionaire-maker, …) that slip past the quality gate on trust-3 sources.
   CONSERVATIVE — real events (acquires / reports / launches / FDA / "upgraded
   to Buy") never match (recall guard in `tests/test_crawl_news_noise.py`).
   **Minimum-retention floor**: a ticker with `<= NOISE_MIN_RETAIN` (default 2)
   items skips the filter, and if filtering would leave fewer than that, the
   best-scoring dropped items are restored — a sparsely-covered ticker is never
   zeroed. Off: `NEWSCRAWLER_NOISE_FILTER=0`; floor: `NEWSCRAWLER_NOISE_MIN_RETAIN`.

**`meta.importance`** (`importance_score`, → `to_news_item` meta) — a rules-only
[0,1] scalar (strongest event type + primary-source bump + structured magnitude
+ corroboration + relevance) for AIStock to prioritise *which* watchlist news
matters most. No LLM, no extra IO; does **not** change any existing scoring or
gating — purely a new advisory field.

**Business Wire source + widened wire gate (2026-07-30).** Added Business Wire's
all-news feed to `_WIRE_TRIPWIRE_FEEDS` (the other major newswire alongside PR
Newswire / GlobeNewswire; verified live). To make the wire tripwire actually
surface Business Wire's watchlist events — and to close a pre-existing gap that
also affected the other two wires — `_TRIPWIRE_EVENT_TYPES` was widened with two
NEW taxonomy categories, `major_contract` (won/awarded/sized contracts & orders)
and `capital_investment` (large capex / plant / fab / campus builds), plus the
existing `partnership`. Both new categories require a size/scope qualifier so
routine consumer PR (National Cheesecake Day, pet-food campaigns) stays out —
recall guards in `tests/test_crawl_news_upgrade.py`.

### Reference projects evaluated (2026-07-29)

The upgrade brief suggested selectively borrowing from Crawlee Python, RSSHub,
Trafilatura, Scrapling, and Crawl4AI. What was actually adopted vs. why not:

- **Trafilatura** — already the primary body extractor (`extract_article_text`).
  The only change was running it off the event loop (`asyncio.to_thread`).
- **URL canonicalization** — reused this repo's own
  `crawler/dedupe/canonical.py::normalise_url` tracking-param list rather than
  pulling a new dependency; `canonicalize_url` in `crawl_news.py` is the
  hot-path copy so the single-file crawler has no cross-package import.
- **Crawlee Python** — **NOT adopted.** Its core value (async scheduling,
  per-domain concurrency, retry/backoff, circuit breaking, request dedup /
  conditional GET) is already implemented inline: `asyncio.Semaphore`
  (`TICKER_CONCURRENCY`) + `_DomainRateLimiter`, `fetch_url` retry/backoff, the
  per-domain circuit breaker, and `fetch_rss_with_conditional_get` (ETag/304).
  Adopting Crawlee would mean a rewrite for no net capability — rejected under
  the "in-place, no rewrite" constraint.
- **RSSHub / Scrapling / Crawl4AI (browser pool, adaptive selectors, Playwright
  fallback)** — **NOT adopted.** All current sources are structured RSS/API/
  socket feeds with **no JS execution needed**, so a headless-browser tier adds
  heavy deps and ops surface for zero coverage gain on today's channels. Left as
  an option if a future source genuinely requires a rendered page.

**Deferred backlog — adaptive concurrency (Crawlee `AutoscaledPool` idea).**
`TICKER_CONCURRENCY` / `ARTICLE_FETCH_CONCURRENCY` are fixed constants. Brief
requirement #8 ("set concurrency from host resources at runtime") is only
partially met. A worthwhile next step is a lightweight CPU/memory-aware pool
that raises concurrency on a fast host and backs off under load — WITHOUT
pulling in Crawlee itself. Not yet implemented.

## Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `build_runtime_paths()` | line ~142 | Resolve all data paths from env/args |
| `configure_runtime_paths()` | line ~183 | Apply resolved paths to module globals |
| `_DomainRateLimiter` | line ~225 | Per-domain async rate limiter |
| `build_sources()` | line ~918 | Build RSS source list for a ticker (10 channels) |
| `get_trust()` | line ~1277 | Look up trust tier for a source |
| `relevance_score()` | line ~1290 | Compute ticker relevance for an article |
| `is_noise()` | line ~1344 | Conservative hype/fluff headline detector (NOISE_TITLE_PATTERNS) |
| `filter_noise_with_floor()` | line ~1359 | Drop is_noise() items with a per-ticker minimum-retention floor |
| `article_quality_score()` | line ~1521 | 7-signal composite quality scorer |
| `classify_events()` | line ~1926 | Regex taxonomy for event types (see categories below) |
| `lexicon_sentiment()` | line ~2354 | Loughran-McDonald financial sentiment scorer |
| `_load_aistock_watchlist()` | line ~2510 | Read ticker list from AIStock config |
| `parse_tickers()` | line ~2612 | Resolve preset / comma-list / default |
| `iter_error_events()` | line ~2700 | Read structured error events from per-run `errors.jsonl` |
| `summarize_errors()` | line ~2740 | Aggregate error audit report (by level/event/domain/status) |
| `export_articles_by_ticker()` | line ~2878 | Write stable per-ticker files |
| `load_rolling_window()` | line ~2943 | PIT-correct rolling-window read from SQLite (backtest `as_of`); applies noise floor + two-layer gate |
| `persist_run_artifacts()` | line ~3152 | Write run dir + stable exports |
| `dedup_articles()` | line ~3252 | SimHash + rapidfuzz fuzzy title clustering |
| `quality_filter()` | line ~3317 | Full quality pipeline + noise floor + score gating |
| `is_high_value_article()` / `importance_score()` | line ~3471 / ~3527 | Fulltext-selection gate / rules-only [0,1] importance (→ `meta.importance`) |
| `extract_article_text()` | line ~3628 | trafilatura-based full-text extraction with regex fallback |
| `canonicalize_url()` | line ~3762 | Strip tracking params + normalize URL for dedup keying |
| `parse_rss()` | line ~3797 | Parse RSS XML into article dicts (handles 10 channel tags) |
| `fetch_url()` | line ~4140 | HTTP fetch: retry + circuit breaker + conditional GET |
| `fetch_article_body()` | line ~4384 | Full-text fetch; trafilatura extraction offloaded via `asyncio.to_thread` |
| `save_article()` | line ~4705 | Bitemporal UPSERT into `news.db` (preserves `first_seen_at`) |
| `crawl()` | line ~4829 | Fetch + filter single ticker |
| `merge_articles_by_url()` | line ~4926 | Merge cross-ticker duplicate articles |
| `_fetch_shared_benzinga_rss()` | line ~4957 | Fetch BZ RSS once, share globally |
| `crawl_watchlist()` | line ~5404 | Parallel tickers, merge, save |
| `to_news_item()` | line ~5703 | Raw article → AIStock NewsItem (event_types + sentiment + importance) |
| `main()` | line ~5927 | CLI entry point |

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
| `NEWSCRAWLER_SEC_EDGAR` | `1` | Set `0` to disable the SEC EDGAR filings channel |
| `NEWSCRAWLER_SEC_USER_AGENT` | `NewsCrawler/1.0 (contact: …)` | Declared UA for SEC fair-access policy |
| `NEWSCRAWLER_IBKR_NEWS` | `1` | Set `0` to disable the IBKR TWS wire-news channel |
| `NEWSCRAWLER_IBKR_PORT` | *(probe 7496/4001/7497/4002)* | Pin the TWS/Gateway socket port |
| `NEWSCRAWLER_IBKR_CLIENT_ID` | `23` | ib_insync clientId (17=AIStock monitor, keep distinct) |
| `NEWSCRAWLER_NOISE_FILTER` | `1` | Set `0` to disable the hype/fluff noise filter (2026-07-29) |
| `NEWSCRAWLER_NOISE_MIN_RETAIN` | `2` | Per-ticker floor the noise filter never drops below |

All relative paths resolve from `PROJECT_ROOT` (script location).
`python /opt/NewsCrawler/crawl_news.py` works correctly from any cwd.

## Performance

500 tickers complete in **~12-15 minutes**. Previous milestones:
- 67 min (original sequential baseline)
- 38 min (expanded sources, naïve rate limiter)
- ~13 min (after 2026-04 optimizations below)

### Speed optimizations

1. **Semaphore-based per-domain rate limiter** — Google News gets 3 concurrent
   slots instead of 1. Effective throughput 1.67 → ~5 req/s.
2. **Query consolidation** — `broad_urls` reduced from 5 → 3 queries, Benzinga
   from 2 → 1. Saves ~2 URLs per ticker.
3. **Hot-ticker gating** — Reuters / Bloomberg / CNBC / Investing dedicated
   proxies only run for the top-150 tickers of the **live** AIStock watchlist
   (`_get_hot_tickers()`, lazily cached). BUGFIX 2026-06-29: was built from the
   stale hardcoded `AISTOCK500_TICKERS`, which wrongly denied 28 live top-150
   momentum names (RKLB, ARM, ASML, ASTS, …) their premium proxies.
4. **Per-run URL caches** — when an article appears in multiple tickers'
   fulltext-candidate lists, the body fetch + trafilatura extraction runs
   once (`_BODY_CACHE`). Same for Google News URL decoding (`_GOOGLE_URL_CACHE`),
   which costs 2-3 HTTP requests per decode. Cleared at each `crawl_watchlist`
   start. End-of-run summary reports cache hit rate.
5. **Skipped dead Benzinga RSS** — `/feed` and `/news/feed` started returning
   403 (Cloudflare). Shared fetch is now a no-op; BZ coverage flows entirely
   through `google_benzinga` proxy.

Combined: ~5,500 → ~2,965 Google requests per 500-ticker run (46% fewer).

| Tuning constant | Default | Purpose |
|-----------------|---------|---------|
| `TICKER_CONCURRENCY` | 5 | Tickers fetched in parallel |
| `DOMAIN_DELAY["news.google.com"]` | 0.6s | Google News RSS rate limit |
| `DOMAIN_DELAY["finance.yahoo.com"]` | 0.5s | Yahoo Finance RSS rate limit |
| `DOMAIN_DELAY["www.nasdaq.com"]` | 1.5s | Nasdaq per-ticker RSS |
| `DOMAIN_DELAY["default"]` | 1.0s | All other domains |
| `FETCH_MAX_RETRIES` | 3 | Retries on 429/502/503/504/timeouts with exp backoff |
| `FETCH_BASE_BACKOFF` | 2.0s | Initial backoff (doubled each retry, capped 30s) |
| `_CB_FAILURE_THRESHOLD` | 5 | Consecutive failures before circuit breaker trips |
| `_CB_PAUSE_SECONDS` | 60 | Domain pause duration when circuit breaker trips |
| `AIOHTTP_MAX_FIELD_SIZE` | 32KB | Accept oversized headers (Yahoo ships huge cookies) |
| `ARTICLE_FETCH_CONCURRENCY` | 6 | Concurrent full-text fetches |

Optimizations:
- **Per-domain rate limiter**: `_DomainRateLimiter` applies different delays per host
- **Parallel ticker processing**: `asyncio.Semaphore(TICKER_CONCURRENCY)` runs N tickers concurrently
- **Shared Benzinga RSS**: fetched once per run, distributed to all tickers via `relevance_score()` attribution
- **Shared HTTP connection pool**: single `aiohttp.ClientSession` with DNS cache for all tickers

### Resilience (Fetch Layer)

`fetch_url()` has three layers of defense against source-side issues:

1. **Exponential-backoff retry** — 5xx / 429 / timeouts / connection resets retry
   up to 3x with 2→4→8s backoff (respects `Retry-After` header on 429/503).
   4xx errors (400/403/404) bail immediately — retrying won't help.

2. **Per-domain circuit breaker** — after 5 consecutive failures on the same
   domain, that domain is paused for 60s. Prevents hammering a down host
   (e.g. Google News returning 503 during rate-limit).

3. **Generous aiohttp header limits** — `max_line_size` / `max_field_size`
   bumped to 32KB (from default 8KB). Eliminates spurious 400 "Header value
   is too long" errors from Yahoo Finance and other sites with huge cookies.

Empty-error bug eliminated: all exceptions now log their type + message
(`ClientError[ClientResponseError]: 400, message='...'`) via typed except clauses.

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
Each article gets a quality score [0.0, 1.0]; articles below `QUALITY_THRESHOLD` (0.45) are dropped.

| Signal | Weight | Source |
|--------|--------|--------|
| Source Authority | 0.30 | trust_tier: TOP=1.0, OK=0.7, low=0.3 |
| Ticker Relevance | 0.25 | Existing relevance_score() |
| Headline Informativeness | 0.15 | Action keywords, length, clickbait/listicle penalty |
| Temporal Freshness | 0.10 | Continuous decay: 4h=1.0, 12h=0.8, 24h=0.6, 48h=0.2 |
| Content Specificity | 0.10 | Multi-ticker roundup penalty + entity density ($, %) |
| Summary Richness | 0.05 | Full text=1.0, summary>=100ch=0.7, none=0.1 |
| Source Diversity | 0.05 | Multi-source confirmation: 3+=1.0, 0=0.2 |

**Threshold**: 0.45 standard, 0.40 adaptive fallback (for tickers with <2 articles).
**Effect**: low-quality articles dropped from 72% to 18% of output; AIStock usable rate ~82%.

The adaptive threshold applies **both** in `quality_filter()` (per single run)
AND in `load_rolling_window()` (per ticker at export time), so mid-cap tickers
with thin coverage still get exported rather than dropping to zero.

### Two-layer per-ticker gate (rolling window export)

For each ticker, the export applies two layers of gating:

**Layer A — Trust-based aggregator suppression:**
- If the ticker has **≥2 articles from trust≥2 sources** → drop tier-1 aggregators.
  Result: clean feed for large-caps (NVDA / AAPL etc.) with ~0% aggregator noise.
- If the ticker has **<2 trust≥2 articles** → keep tier-1 aggregators as fallback.
  Result: mid-caps (TAL / CMI / FTV) get aggregator-only coverage instead of 0.

**Layer B — Quality-score adaptive threshold:**
- Normal gate: `quality_score >= 0.45`
- If a ticker has <2 articles above 0.45 → relax to `>= 0.40` for that ticker.

### Noise aggregators (`_NOISE_AGGREGATOR_TIER1`)

Domains like `gurufocus`, `marketbeat`, `tipranks`, `247wallst`, `stockstory`
return trust=1 instead of trust=0. Combined with Layer A above:
- Large-caps: suppressed by layer A → ~0% pollution
- Mid-caps: survive layer A as fallback, then quality_score gates them

### Per-ticker feed relaxation

Articles from Yahoo Finance RSS and Nasdaq RSS (both `/rss/headline?s=TICKER`
style — already filtered upstream to one ticker) use a much lower relevance
threshold (0.02 vs. 0.10 for Google News) and get a relevance floor of 0.30.
Rationale: the source is authoritative about attribution, so we don't re-check
by counting ticker mentions.

### Measured impact on 500-ticker run

| Metric | Before (v3) | After (v5 two-layer) |
|--------|-------------|---------------------|
| Articles exported | 3,740 | **3,898** |
| Tickers with ≥1 article | 441 (88%) | **461 (91%)** |
| Trust ≥ 2 ratio | 95% | **97%** |
| Tier-1 contamination | 0% | **2%** (only for mid-caps that need fallback) |

The quality score is exported in `meta.quality_score` for AIStock to optionally use.

## Near-Duplicate Clustering

`dedup_articles()` runs two passes over filtered articles:

1. **SimHash (Hamming ≤ 8)** — catches near-identical headlines (structural match)
2. **Fuzzy token-set ratio (rapidfuzz)** — catches paraphrased same-event stories

`FUZZY_TITLE_THRESHOLD = 85` (0-100 scale). Articles sorted by `(trust desc, relevance desc)`
before clustering, so the highest-trust publisher wins each cluster and other sources
become `_alt_sources` — feeding the source_diversity signal in the quality scorer.

Fuzzy pass auto-disables if `rapidfuzz` isn't installed (graceful ImportError check).

## Financial Sentiment (Loughran-McDonald)

`lexicon_sentiment()` scores each article against a curated subset (~300 high-signal
terms) of the Loughran-McDonald 2018 Financial Sentiment Dictionary. Unlike general
lexicons (VADER, AFINN), LM is calibrated to financial text — e.g. "liability" is
neutral, "restated" is strongly negative.

Three wordlists as `frozenset[str]` (heavily expanded 2026-04 for watchlist vocabulary):
- `LM_POSITIVE` — **385** positive terms incl. price action (surges, skyrockets, rallies),
  tech momentum (revolutionary, breakthrough, accelerate, dominate, pioneer),
  analyst (bullish, upgrade, outperform), capital (buyback, dividend)
- `LM_NEGATIVE` — **533** negative terms incl. price action (plunges, tumbles, crashes),
  risk (headwind, overhang, bubble), analyst (bearish, cut, slash), cyber (breach,
  outage, ransomware), macro (tariff, recession)
- `LM_UNCERTAINTY` — **134** hedging terms (may, could, possibly, contingent,
  pending, wait-and-see, reportedly, potentially)

Output shape stored in `meta.sentiment`:
```
{"score": 0.67, "pos": 0.032, "neg": 0.006, "unc": 0.010, "matched": 12}
```

**Score normalization**: `(pos - neg) / matched` — divided by matched (not total tokens)
so short headlines with clear signals stay strongly signed instead of being diluted.
If no dictionary words matched, `meta.sentiment` is set to `null`.

## Event Classification Taxonomy

`classify_events()` applies regex patterns to title + summary + first 500 chars of body.
Each article can match multiple event types (stored in `meta.event_types`).
AIStock can route articles by event type instead of just trust tier.

21 event types (~200 regex patterns total, pre-compiled at import):

**Corporate actions**: `earnings_release`, `earnings_guidance`, `analyst_rating`, `ma_activity`,
`management_change`, `product_launch`, `partnership`, `major_contract`, `capital_investment`,
`capital_action`, `insider_activity`

**Risk / regulatory**: `litigation`, `regulatory`, `trade_policy`, `cyber_risk`, `activist_short`

**Market dynamics**: `price_action` (up/down moves, 52-week highs/lows, breakouts),
`valuation` (fair value, P/E, too-late-to-buy), `technical_signal` (RSI, MACD, options flow),
`market_commentary` (trending stock, laps the market, defensive name),
`macro_sector` (Fed, tariffs, inflation, GDP)

`major_contract` (won/awarded/sized contracts & orders) and `capital_investment`
(large capex / plant / fab / campus builds) were added 2026-07-30 to cover the
watchlist priority list's "重大合同/订单" and "重大投资"; both require a size or
scope qualifier so routine PR stays out, and both feed the wire tripwire gate.

Patterns live in `EVENT_TAXONOMY`. Designed against live watchlist headlines —
regression tests verify real missed titles (SNDK skyrockets, Corning taps Meta,
CocaCola trending, etc.) now classify correctly.

## High-value Event Signals (2026-06-10)

`extract_event_signals()` parses structured magnitudes from the raw headline at
ingest time, stored in `meta.event_signals`. These are the highest-ROI inputs
per the AIStock value analysis (GUIDANCE sev 0.7, REGULATION 0.8, M&A-large 0.8,
EARNINGS_SURPRISE magnitude-scaled, PRODUCT breakthrough 0.75):

| Signal key | Example | AIStock use |
|------------|---------|-------------|
| `deal_size_usd_m` / `deal_size_class` | `$10.9B → 10900, "large"` | M&A severity → 0.80 (large) / 0.35 (small) |
| `earnings_beat_pct` | `beats by 12% → 12.0` | EARNINGS sign + severity (≥8% → 0.70) |
| `earnings_surprise_dir` | `"strong_beat"` | EARNINGS severity floor |
| `guidance_direction` | `"raised"` / `"cut"` | GUIDANCE sign (cut → -1, sev 0.60) |
| `regulatory_outcome` | `"approval"` / `"rejection"` | REGULATION sign-flip (CRL → -1, sev 0.70) |
| `product_breakthrough` | `True` (tape-out, 2nm) | PRODUCT severity → 0.75, horizon 1wk |

**Why upstream extraction**: NewsCrawler parses the ORIGINAL headline before any
title cleaning. AIStock's `impact.py assess_impact(event_signals=...)` prefers
these authoritative magnitudes over re-parsing post-processed text, falling back
to keyword heuristics when absent (backward compatible). Validated end-to-end:
AbbVie/Apogee $10.9B → large, Salesforce/Fin $3.6B → mid, Merck FDA approval,
multiple guidance-raised — all flow NewsCrawler → connector → impact.py.

## Source coverage note (investing.com)

investing.com is tier-2 (trust=2). Added as a hot-ticker-gated Google proxy
(`google_investing`, top-150 only) rather than a dedicated channel — its content
skews toward price_action/analyst (AIStock's lowest-value categories), so the
ROI is in the M&A/insider/guidance items it occasionally surfaces, captured via
the standard relevance gate (no per-ticker relaxation → avoids body-match noise).

## Full-text Extraction

`extract_article_text()` uses [trafilatura](https://trafilatura.readthedocs.io/) when available
(boilerplate-free main-content extractor — handles paywalls, cookie notices, sidebars cleanly)
with a regex-based fallback for environments without trafilatura installed.

Install with `pip install trafilatura` to enable — the module does a graceful `ImportError`
check and falls back silently.

## Architecture Notes

- **Single-file**: all logic in `crawl_news.py`, no submodules
- **No API key**: public RSS only — Benzinga RSS, Google News RSS, Yahoo Finance RSS
- **No JS rendering**: RSS feeds bypass Cloudflare challenge
- **Per-domain rate limiting**: different delays for different hosts (Google 0.3s, Benzinga 1.5s)
- **Parallel tickers**: 5 concurrent tickers via asyncio.Semaphore
- **Shared Benzinga RSS**: fetched once globally, shared across all tickers
- **7-signal quality scoring**: composite score gates articles at 0.40 threshold
- **SQLite dedup**: articles keyed by URL; re-runs are fast
- **Async I/O**: `aiohttp` + `asyncio` with shared connection pool
- **Structured logging**: `structlog` → stderr; stdout = pure JSON
