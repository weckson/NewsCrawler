#!/usr/bin/env python3
"""One-shot IR RSS feed discovery for the AIStock watchlist.

For each ticker, derives company-domain slugs from AIStock's company_aliases
and probes common investor-relations RSS URL patterns (Q4 Inc platform,
Notified, custom IR sites). Valid feeds (HTTP 200, parses as RSS/Atom with
>= 1 entry) are written to data/ir_feeds.json, which crawl_news.py loads at
import time and merges into OFFICIAL_IR_FEEDS.

Re-run quarterly or after watchlist changes:
    python scripts/probe_ir_feeds.py            # full probe (~5-10 min)
    python scripts/probe_ir_feeds.py --tickers NVDA,AAPL   # subset

The probe is polite: 8s timeout, no retries, 20 concurrent, one hit per
ticker is enough (stops trying further patterns).
"""
import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

import aiohttp
import feedparser

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AISTOCK_CONFIG = PROJECT_ROOT.parent / "AIStock" / "config" / "default.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "ir_feeds.json"

CONCURRENCY = 20
TIMEOUT = aiohttp.ClientTimeout(total=8)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NewsCrawler-IR-Probe/1.0"

# URL patterns observed across S&P 500 IR sites. {slug} = lowercase company
# slug, e.g. "apple". Ordered by observed hit-rate (Q4 Inc platform first —
# it hosts the majority of large-cap IR sites).
PATTERNS = [
    # Q4 Inc platform (most common)
    "https://investors.{slug}.com/rss/pressrelease.aspx",
    "https://investor.{slug}.com/rss/pressrelease.aspx",
    "https://ir.{slug}.com/rss/pressrelease.aspx",
    # Notified / GlobeNewswire-hosted
    "https://investors.{slug}.com/rss/news-releases.xml",
    "https://investor.{slug}.com/rss/news-releases.xml",
    # Custom / Drupal-style (AMD pattern)
    "https://ir.{slug}.com/news-events/press-releases/rss",
    "https://investor.{slug}.com/news-events/press-releases/rss",
    # Generic
    "https://ir.{slug}.com/rss",
    "https://investors.{slug}.com/rss",
]

_SLUG_STRIP = re.compile(r"\b(inc|corp|corporation|company|co|ltd|plc|group|holdings?|technologies|technology|the)\b\.?", re.I)
_NON_ALNUM = re.compile(r"[^a-z0-9]")


def derive_slugs(ticker: str, aliases: list[str]) -> list[str]:
    """Company name -> domain slug candidates. 'Advanced Micro Devices' -> ['amd' via ticker, 'advancedmicrodevices', 'advancedmicro']."""
    slugs: list[str] = []
    seen = set()

    def add(s: str):
        s = _NON_ALNUM.sub("", s.lower())
        if 2 <= len(s) <= 30 and s not in seen:
            seen.add(s)
            slugs.append(s)

    # Ticker itself is often the domain (amd.com, nvidia... no — but ir.aa.com etc.)
    add(ticker)
    for alias in aliases:
        # Full squashed name
        add(alias)
        # Stripped of corporate suffixes
        stripped = _SLUG_STRIP.sub("", alias).strip()
        add(stripped)
        # First word only (apple, microsoft, boeing)
        first = stripped.split()[0] if stripped.split() else ""
        add(first)
    return slugs[:6]  # cap probe budget per ticker


def looks_like_press_feed(body: bytes) -> bool:
    if not body or len(body) < 100:
        return False
    head = body[:300].lstrip()
    if not (head.startswith(b"<?xml") or b"<rss" in head or b"<feed" in head):
        return False
    feed = feedparser.parse(body)
    return len(feed.entries) >= 1


async def probe_ticker(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                       ticker: str, aliases: list[str]) -> tuple[str, str | None]:
    slugs = derive_slugs(ticker, aliases)
    for slug in slugs:
        for pattern in PATTERNS:
            url = pattern.format(slug=slug)
            async with sem:
                try:
                    async with session.get(url, headers={"User-Agent": UA},
                                           timeout=TIMEOUT, allow_redirects=True) as resp:
                        if resp.status != 200:
                            continue
                        ctype = resp.headers.get("Content-Type", "")
                        if "html" in ctype and "xml" not in ctype:
                            continue
                        body = await resp.read()
                except Exception:
                    continue
            if looks_like_press_feed(body):
                return ticker, url
    return ticker, None


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", default=None, help="Comma-separated subset")
    args = parser.parse_args()

    cfg = json.loads(AISTOCK_CONFIG.read_text(encoding="utf-8"))
    aliases_map: dict[str, list[str]] = cfg.get("company_aliases", {})
    watchlist = [t for t in cfg.get("watchlist", [])
                 if isinstance(t, str) and t.strip() and not t.startswith("_")]
    if args.tickers:
        watchlist = [t.strip().upper() for t in args.tickers.split(",")]

    print(f"Probing IR feeds for {len(watchlist)} tickers "
          f"({len(PATTERNS)} patterns x up-to-6 slugs each)...", file=sys.stderr)

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [probe_ticker(session, sem, t, aliases_map.get(t, []))
                 for t in watchlist]
        results = []
        done = 0
        for coro in asyncio.as_completed(tasks):
            ticker, url = await coro
            done += 1
            if url:
                results.append((ticker, url))
                print(f"  [{done}/{len(watchlist)}] HIT  {ticker}: {url}", file=sys.stderr)
            elif done % 50 == 0:
                print(f"  [{done}/{len(watchlist)}] ...", file=sys.stderr)

    feeds = {t: url for t, url in sorted(results)}
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(feeds, indent=2), encoding="utf-8")
    print(f"\nDiscovered {len(feeds)} IR feeds -> {OUTPUT_PATH}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
