"""
IBKR TWS news source (2026-07-19).

Pulls Dow Jones / Briefing.com wire headlines from a RUNNING TWS / IB Gateway
over the socket API (ib_insync, readonly). Validated 2026-07-19 against the
live account: 8 providers (DJ-N, DJ-RT, DJ-RTA/RTE/RTG, DJNL, BRFG, BRFUPDN),
73.8% of headlines absent from all RSS channels, matched stories observed a
median 6.8h earlier than the crawler's first_seen, and reqNewsArticle returns
full Barron's/WSJ paywalled text.

Design mirrors finnhub_api.py: runs as a parallel asyncio task inside
crawl_watchlist's RSS window (zero wall-clock cost), emits article dicts in
the parse_rss() schema, and merges through the same quality_filter / dedup /
save_article pipeline.

MARKET-DATA READ ONLY: connects with readonly=True, never touches orders.
Graceful by design: if TWS isn't running / ib_insync missing / no news
subscription, returns {} and the crawl proceeds without the channel.

Attribution note: articles come from reqHistoricalNews keyed by the ticker's
conId — Dow Jones' own editorial tagging. Treated as authoritative (like
Yahoo/Nasdaq per-ticker RSS): relevance gets a 0.30 floor, never a hard gate,
and the 7-signal quality score does the final gating.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger("crawler.sources.ibkr_news")

# Ports to probe, in order: TWS live, GW live, TWS paper, GW paper.
_DEFAULT_PORTS = (7496, 4001, 7497, 4002)
_DEFAULT_CLIENT_ID = 23   # NOT 17 (AIStock monitor) / 42 (probe scripts)

# providerCode → (display name, canonical domain, trust tier)
# Trust tiers match crawl_news.get_trust: 3=TOP, 2=OK, 1=low.
_PROVIDER_MAP: dict[str, tuple[str, str, int]] = {
    "DJ-N":    ("Dow Jones Global Equity", "dowjones.com", 3),
    "DJ-RT":   ("Dow Jones Trader News", "dowjones.com", 3),
    "DJ-RTA":  ("Dow Jones Asia Pacific", "dowjones.com", 3),
    "DJ-RTE":  ("Dow Jones Europe", "dowjones.com", 3),
    "DJ-RTG":  ("Dow Jones Global", "dowjones.com", 3),
    "DJNL":    ("Dow Jones Newsletters", "dowjones.com", 3),
    "BRFUPDN": ("Briefing.com Analyst Actions", "briefing.com", 3),
    "BRFG":    ("Briefing.com", "briefing.com", 2),
}

# Body fetch (reqNewsArticle) is gated to high-value events to bound cost.
# Superset of crawl_news._TRIPWIRE_EVENT_TYPES plus the two categories the
# 2026-07 evaluation showed IBKR uniquely surfaces (insider Form-4 stories,
# analyst initiations).
_BODY_EVENT_TYPES = frozenset({
    "ma_activity", "earnings_release", "earnings_guidance", "regulatory",
    "insider_activity", "analyst_rating",
})

# Headline hygiene ---------------------------------------------------------

_TAG_PREFIX = re.compile(r"^(?:\{[^}]*\}\s*)+")      # {A:800015:L:en} tags
_CONTINUATION = re.compile(r"\s-\d+-\s*$")            # "... -2-" page fragments
_HTML_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t\r\f\v]+")


def clean_headline(raw: str) -> str:
    """Strip IBKR metadata tags and DJ bullet markers from a wire headline."""
    h = _TAG_PREFIX.sub("", raw or "").strip()
    h = h.lstrip("* ").strip()
    return h


def is_fragment(headline: str) -> bool:
    """True for continuation-page fragments ("Story Title -2-") and stubs
    too short to carry signal on their own."""
    if _CONTINUATION.search(headline):
        return True
    return len(headline) < 12


def html_to_text(body_html: str) -> str:
    """DJ article bodies are simple <p>-tag HTML with entity escapes."""
    text = _HTML_TAG.sub(" ", body_html or "")
    text = html.unescape(text)
    lines = [ln.strip() for ln in text.splitlines()]
    text = "\n".join(ln for ln in lines if ln)
    return _WS.sub(" ", text).strip()


def provider_meta(code: str) -> tuple[str, str, int]:
    if code in _PROVIDER_MAP:
        return _PROVIDER_MAP[code]
    if code.startswith("DJ"):
        return ("Dow Jones", "dowjones.com", 3)
    return (code or "IBKR News", "interactivebrokers.com", 2)


def _parse_news_time(value: Any) -> datetime:
    """HistoricalNews.time is a naive-UTC datetime (or its str form)."""
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.now(timezone.utc)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def article_from_headline(art: Any, ticker: str) -> dict | None:
    """Convert an ib_insync HistoricalNews record to the parse_rss() article
    schema. Returns None for fragments/empties."""
    headline = clean_headline(getattr(art, "headline", "") or "")
    if not headline or is_fragment(headline):
        return None
    provider = getattr(art, "providerCode", "") or ""
    article_id = getattr(art, "articleId", "") or ""
    name, domain, trust = provider_meta(provider)
    published = _parse_news_time(getattr(art, "time", None))
    return {
        "id": f"ibkr-{article_id}" if article_id else f"ibkr-{hash(headline)}",
        "title": headline,
        # No public URL exists for wire items; honest synthetic scheme keyed
        # by IBKR's own articleId (stable across runs → SQLite dedup works).
        "url": f"ibkr-news://{article_id or headline[:80]}",
        "_source_name": name,
        "_source_domain": domain,
        "_trust": trust,
        "_channel": "ibkr_news",
        "_ticker": ticker,
        "_tickers": [ticker],
        "published": published.isoformat(),
        "summary": "",
        "_meta": {"ibkr_provider": provider, "ibkr_article_id": article_id},
    }


# conId cache --------------------------------------------------------------

def _load_conid_cache(path: Path | None) -> dict[str, int]:
    if path and path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return {k.upper(): int(v) for k, v in raw.items() if v}
        except Exception as exc:
            log.warning("ibkr_news conid cache unreadable (%s); refetching", exc)
    return {}


def _save_conid_cache(path: Path | None, cache: dict[str, int]) -> None:
    if not path:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache, indent=0, sort_keys=True), encoding="utf-8")
    except Exception as exc:
        log.warning("ibkr_news conid cache write failed: %s", exc)


# Connection ---------------------------------------------------------------

def _tws_port_alive(host: str, ports: tuple[int, ...], timeout: float = 0.6) -> int | None:
    """Fast TCP probe so a closed TWS costs <1s, not a connect timeout."""
    for port in ports:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return port
        except OSError:
            continue
    return None


async def fetch_ibkr_news_for_tickers(
    tickers: list[str],
    hours: int = 48,
    *,
    classify_fn: Callable[[str, str], list] | None = None,
    conid_cache_path: Path | None = None,
    host: str = "127.0.0.1",
    max_per_ticker: int = 25,
    max_bodies: int = 40,
    req_concurrency: int = 4,
    req_delay: float = 0.15,
) -> dict[str, list[dict]]:
    """Fetch per-ticker wire news from a running TWS/Gateway.

    Returns {ticker: [article_dict, ...]} in parse_rss() schema, or {} when
    the channel is unavailable (TWS closed, ib_insync missing, no providers).

    classify_fn (crawl_news.classify_events, injected to avoid a circular
    import) gates which articles earn a reqNewsArticle body fetch.
    """
    if not tickers:
        return {}
    if os.environ.get("NEWSCRAWLER_IBKR_NEWS", "1") in ("0", "false", "no"):
        log.info("ibkr_news disabled via NEWSCRAWLER_IBKR_NEWS")
        return {}

    env_port = os.environ.get("NEWSCRAWLER_IBKR_PORT")
    ports = (int(env_port),) if env_port else _DEFAULT_PORTS
    client_id = int(os.environ.get("NEWSCRAWLER_IBKR_CLIENT_ID", _DEFAULT_CLIENT_ID))

    port = _tws_port_alive(host, ports)
    if port is None:
        log.info("ibkr_news: no TWS/Gateway listening on %s — channel skipped", list(ports))
        return {}

    try:
        from ib_insync import IB, Stock
    except ImportError:
        log.warning("ibkr_news: ib_insync not installed — channel skipped")
        return {}

    t0 = time.time()
    ib = IB()
    try:
        await ib.connectAsync(host, port, clientId=client_id, timeout=10, readonly=True)
    except Exception as exc:
        log.warning("ibkr_news: connect %s:%s failed (%s) — channel skipped",
                    host, port, type(exc).__name__)
        return {}

    result: dict[str, list[dict]] = {}
    try:
        providers = await ib.reqNewsProvidersAsync()
        codes = "+".join(p.code for p in providers)
        if not codes:
            log.warning("ibkr_news: account has no news provider subscriptions")
            return {}

        # ── conId resolution (cached across runs) ────────────────────────
        tickers = [t.upper() for t in tickers]
        conids = _load_conid_cache(conid_cache_path)
        missing = [t for t in tickers if t not in conids]
        for i in range(0, len(missing), 50):
            batch = missing[i:i + 50]
            contracts = [Stock(t, "SMART", "USD") for t in batch]
            try:
                qualified = await ib.qualifyContractsAsync(*contracts)
            except Exception as exc:
                log.warning("ibkr_news: qualify batch failed: %s", exc)
                continue
            for c in qualified:
                if getattr(c, "conId", 0):
                    conids[c.symbol] = c.conId
        if missing:
            _save_conid_cache(conid_cache_path, conids)

        # ── per-ticker historical news ───────────────────────────────────
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        sem = asyncio.Semaphore(req_concurrency)

        async def _one(ticker: str) -> None:
            conid = conids.get(ticker)
            if not conid:
                return
            async with sem:
                try:
                    arts = await ib.reqHistoricalNewsAsync(
                        conid, codes, "", "", max_per_ticker)
                except Exception as exc:
                    log.warning("ibkr_news: %s news fetch failed: %s", ticker, exc)
                    return
                await asyncio.sleep(req_delay)   # pacing inside the slot
            out = []
            for a in arts or []:
                art = article_from_headline(a, ticker)
                if art is None:
                    continue
                if datetime.fromisoformat(art["published"]) < cutoff:
                    continue
                out.append(art)
            if out:
                result[ticker] = out

        await asyncio.gather(*(_one(t) for t in tickers))

        # ── body fetch for high-value events (bounded) ───────────────────
        if classify_fn is not None and max_bodies > 0:
            candidates: list[dict] = []
            for arts in result.values():
                for art in arts:
                    try:
                        ets = set(classify_fn(art["title"], ""))
                    except Exception:
                        ets = set()
                    if ets & _BODY_EVENT_TYPES:
                        art["_meta"]["ibkr_event_hint"] = sorted(ets & _BODY_EVENT_TYPES)
                        candidates.append(art)
            candidates.sort(key=lambda a: a["published"], reverse=True)
            fetched = 0
            for art in candidates[:max_bodies]:
                prov = art["_meta"]["ibkr_provider"]
                aid = art["_meta"]["ibkr_article_id"]
                if not (prov and aid):
                    continue
                async with sem:
                    try:
                        na = await ib.reqNewsArticleAsync(prov, aid)
                    except Exception:
                        continue
                    await asyncio.sleep(req_delay)
                text = html_to_text(getattr(na, "articleText", "") or "")
                if len(text) >= 200:
                    art["body"] = text
                    fetched += 1
            log.info("ibkr_news bodies: %d/%d high-value fetched",
                     fetched, len(candidates))
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass

    n_items = sum(len(v) for v in result.values())
    log.info("ibkr_news done: %d items across %d/%d tickers in %.1fs (port %s)",
             n_items, len(result), len(tickers), time.time() - t0, port)
    return result
