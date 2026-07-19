"""
Finnhub API source — migrated from AIStock (sibling repo).

Provides per-ticker company news + global general news by calling Finnhub's
REST API. Runs in PARALLEL with the main RSS crawl loop so it doesn't
extend total wall-clock time (RSS crawl on 500 tickers takes 10-15 min,
Finnhub on 500 tickers takes ~500s, fits entirely within the RSS window).

Rate limit: Finnhub free tier = 60 calls/min (global). Uses an asyncio
token-bucket to stay under the limit regardless of concurrency level.

Output: list of dicts matching the article schema produced by
`parse_rss()` in crawl_news.py, so downstream quality_filter / dedup /
save_article / AIStock export work without any additional changes.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import aiohttp

log = logging.getLogger("crawler.sources.finnhub_api")

BASE_URL = "https://finnhub.io/api/v1"

# Finnhub "source" strings → NewsCrawler (source_name, source_domain, trust)
# Trust tiers: 3=top, 2=ok, 1=low, 0=blocked (matches crawl_news.py::get_trust)
_FINNHUB_PUBLISHER_MAP: dict[str, tuple[str, str, int]] = {
    # underlying publisher raw → (display name, canonical domain, trust tier)
    "yahoo": ("Yahoo Finance", "finance.yahoo.com", 3),
    "seekingalpha": ("Seeking Alpha", "seekingalpha.com", 3),
    "benzinga": ("Benzinga", "benzinga.com", 3),
    "reuters": ("Reuters", "reuters.com", 3),
    "bloomberg": ("Bloomberg", "bloomberg.com", 3),
    "cnbc": ("CNBC", "cnbc.com", 3),
    "marketwatch": ("MarketWatch", "marketwatch.com", 3),
    "the_wall_street_journal": ("Wall Street Journal", "wsj.com", 3),
    "wsj": ("Wall Street Journal", "wsj.com", 3),
    "barrons": ("Barron's", "barrons.com", 3),
    "financial_times": ("Financial Times", "ft.com", 3),
    "chartmill": ("ChartMill", "chartmill.com", 2),
    "fintel": ("Fintel", "fintel.io", 2),
    "investorplace": ("InvestorPlace", "investorplace.com", 1),
    "the_motley_fool": ("Motley Fool", "fool.com", 2),
    "motley_fool": ("Motley Fool", "fool.com", 2),
    "247wallst": ("24/7 Wall St.", "247wallst.com", 1),
    "gobankingrates": ("GoBankingRates", "gobankingrates.com", 1),
    "insider_monkey": ("Insider Monkey", "insidermonkey.com", 1),
    "talkmarkets": ("TalkMarkets", "talkmarkets.com", 1),
    # Finnhub-self editorial aggregation — medium trust, unique content
    "finnhub": ("Finnhub", "finnhub.io", 2),
    "": ("Finnhub", "finnhub.io", 2),  # missing publisher
}


def _map_publisher(raw: str) -> tuple[str, str, int]:
    """Canonicalize a Finnhub 'source' string to (name, domain, trust)."""
    key = (raw or "").strip().lower().replace(" ", "_")
    if key in _FINNHUB_PUBLISHER_MAP:
        return _FINNHUB_PUBLISHER_MAP[key]
    # Unknown publishers: tag as Finnhub tier-2 so they still flow through
    return (raw or "Finnhub", "finnhub.io", 2)


class _TokenBucket:
    """Simple async token bucket: max N tokens, refills at R tokens/sec."""

    def __init__(self, max_tokens: int, refill_per_sec: float):
        self.max_tokens = max_tokens
        self.tokens = float(max_tokens)
        self.refill_per_sec = refill_per_sec
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Block until a token is available, then consume it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(
                    float(self.max_tokens), self.tokens + elapsed * self.refill_per_sec,
                )
                self.last_refill = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                # How long until next token available?
                wait = (1.0 - self.tokens) / self.refill_per_sec
            await asyncio.sleep(max(wait, 0.05))


def _article_from_finnhub_item(
    item: dict, ticker: str, is_general: bool = False,
) -> dict | None:
    """Convert a Finnhub API response item to NewsCrawler article dict."""
    title = (item.get("headline") or "").strip()
    if not title:
        return None

    url = (item.get("url") or "").strip()

    # Published timestamp (Finnhub uses epoch seconds)
    ts_unix = item.get("datetime", 0)
    try:
        ts = datetime.fromtimestamp(ts_unix, tz=timezone.utc) if ts_unix else datetime.now(timezone.utc)
    except Exception:
        ts = datetime.now(timezone.utc)
    published = ts.isoformat()

    # Source mapping
    raw_source = item.get("source", "")
    source_name, source_domain, trust = _map_publisher(raw_source)

    # Body (Finnhub "summary" is short abstract)
    summary = (item.get("summary") or "").strip()
    if len(summary) > 500:
        summary = summary[:500] + "..."

    # Related tickers
    related = (item.get("related") or "").strip()
    related_tickers = [t.strip() for t in related.split(",") if t.strip()] if related else [ticker]

    # Stable id (prefer Finnhub's own id, fall back to URL uuid5)
    fh_id = item.get("id")
    if fh_id:
        article_id = f"fh-{fh_id}"
    else:
        article_id = str(uuid.uuid5(uuid.NAMESPACE_URL, url or title))

    # Channel tag — "finnhub_api" so quality_filter + dedup recognize source
    channel = "finnhub_api_general" if is_general else "finnhub_api"

    return {
        "id": article_id,
        "title": title,
        "url": url,
        "_source_name": source_name,
        "_source_domain": source_domain,
        "_trust": trust,
        "_channel": channel,
        "_ticker": ticker,
        "_tickers": related_tickers[:5] or [ticker],
        "published": published,
        "summary": summary,
        # Extra meta — ignored by quality_filter but useful downstream
        "_meta": {
            "finnhub_id": fh_id,
            "finnhub_publisher_raw": raw_source,
            "finnhub_category": item.get("category", ""),
            "finnhub_related": related,
        },
    }


async def _fetch_ticker_news(
    session: aiohttp.ClientSession,
    ticker: str,
    api_key: str,
    days_back: int,
    bucket: _TokenBucket,
    timeout_sec: float = 15.0,
    max_per_ticker: int = 30,
) -> list[dict]:
    """Fetch company-news for one ticker. Returns list of NewsCrawler-schema dicts."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    from_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    params = {
        "symbol": ticker,
        "from": from_date,
        "to": to_date,
        "token": api_key,
    }
    url = f"{BASE_URL}/company-news"

    await bucket.acquire()

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status == 429:
                # Rate limited — back off, requeue token, retry once
                log.warning("finnhub_api rate-limited on %s; backing off 5s", ticker)
                await asyncio.sleep(5.0)
                await bucket.acquire()
                async with session.get(url, params=params, timeout=timeout) as resp2:
                    if resp2.status != 200:
                        log.warning("finnhub_api retry %d for %s", resp2.status, ticker)
                        return []
                    payload = await resp2.json()
            elif resp.status != 200:
                log.warning("finnhub_api HTTP %d for %s", resp.status, ticker)
                return []
            else:
                payload = await resp.json()
    except asyncio.TimeoutError:
        log.warning("finnhub_api timeout for %s", ticker)
        return []
    except Exception as exc:
        log.warning("finnhub_api error for %s: %s", ticker, exc)
        return []

    if not isinstance(payload, list):
        return []

    out: list[dict] = []
    for item in payload[:max_per_ticker]:
        art = _article_from_finnhub_item(item, ticker, is_general=False)
        if art is not None:
            out.append(art)
    return out


async def _fetch_general_news(
    session: aiohttp.ClientSession,
    api_key: str,
    bucket: _TokenBucket,
    timeout_sec: float = 15.0,
    max_items: int = 50,
) -> list[dict]:
    """Fetch global market news (no ticker filter)."""
    params = {"category": "general", "token": api_key}
    url = f"{BASE_URL}/news"

    await bucket.acquire()

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with session.get(url, params=params, timeout=timeout) as resp:
            if resp.status != 200:
                log.warning("finnhub_api general HTTP %d", resp.status)
                return []
            payload = await resp.json()
    except Exception as exc:
        log.warning("finnhub_api general error: %s", exc)
        return []

    if not isinstance(payload, list):
        return []

    # General news has no ticker association — attach an empty ticker_hint
    # but still produce articles. These get filtered out of per-ticker
    # exports naturally since _ticker is "".
    out: list[dict] = []
    for item in payload[:max_items]:
        art = _article_from_finnhub_item(item, ticker="", is_general=True)
        if art is not None:
            out.append(art)
    return out


async def fetch_finnhub_for_tickers(
    tickers: list[str],
    api_key: str | None = None,
    days_back: int = 3,
    max_concurrency: int = 8,
    rate_per_min: int = 55,   # 5 below Finnhub's 60/min ceiling for safety
    include_general: bool = True,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, list[dict]]:
    """
    Fetch Finnhub news for a batch of tickers.

    Returns a dict: {ticker: [article_dict, ...]}. The special key "" holds
    general-news articles (no ticker attribution).

    Uses a global token bucket for rate limiting (the 60 calls/min cap is
    shared across all workers) and a semaphore to bound concurrency.
    """
    api_key = api_key or os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        log.info("FINNHUB_API_KEY not set — skipping Finnhub API source")
        return {}

    if not tickers:
        return {}

    tickers = sorted(set(t.upper() for t in tickers if t))
    bucket = _TokenBucket(max_tokens=rate_per_min, refill_per_sec=rate_per_min / 60.0)
    sem = asyncio.Semaphore(max_concurrency)

    own_session = session is None
    if own_session:
        connector = aiohttp.TCPConnector(limit=max_concurrency * 2, ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    result: dict[str, list[dict]] = {}
    t0 = time.time()

    async def _one(t: str):
        async with sem:
            arts = await _fetch_ticker_news(session, t, api_key, days_back, bucket)
            if arts:
                result[t] = arts

    try:
        tasks = [asyncio.create_task(_one(t)) for t in tickers]
        if include_general:
            async def _gen():
                gen_arts = await _fetch_general_news(session, api_key, bucket)
                if gen_arts:
                    result[""] = gen_arts
            tasks.append(asyncio.create_task(_gen()))

        # Gather; don't let single-task failure crash the batch
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if own_session:
            await session.close()

    elapsed = time.time() - t0
    total_items = sum(len(v) for v in result.values())
    ticker_cov = sum(1 for k in result if k)
    log.info(
        "finnhub_api done: %d items across %d tickers in %.1fs (general=%d)",
        total_items, ticker_cov, elapsed, len(result.get("", [])),
    )
    return result
