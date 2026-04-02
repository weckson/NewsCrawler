"""
Crawler scheduler — cron-like planner.

Reads sources.yaml, respects per-source intervals and cooldowns, and
dispatches fetch jobs for each enabled source. Exposes Prometheus metrics
and a CLI entry point.

Usage:
    python -m crawler.scheduler               # run scheduler loop
    python -m crawler.scheduler --once        # single pass then exit
    python -m crawler.scheduler --source KEY  # run one source then exit
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import aiohttp
import click
import structlog
import yaml
from prometheus_client import start_http_server, Gauge

from crawler.alerts import dispatcher, rules as alert_rules
from crawler.compliance import policy
from crawler.dedupe import simhash as deduper
from crawler.dedupe.canonical import normalise_url, make_fingerprint
from crawler.parsers import benzinga as bz_parser, rss as rss_parser, html as html_parser
from crawler.storage import db, object_store

# ── Logging ───────────────────────────────────────────────────────────────────
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
log = structlog.get_logger(__name__)

# ── Prometheus ────────────────────────────────────────────────────────────────
SOURCES_ACTIVE = Gauge("crawler_sources_active", "Number of enabled sources")
ITEMS_INGESTED = Gauge("crawler_items_ingested_total", "Total items ingested", ["source_key"])


# ── Config loading ────────────────────────────────────────────────────────────

def load_config(path: str = "config/sources.yaml") -> dict:
    p = Path(path)
    if not p.exists():
        # Fall back to example config in dev
        p = Path("config/sources.example.yaml")
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Source runner dispatch ────────────────────────────────────────────────────

async def run_source(
    source: dict,
    session: aiohttp.ClientSession,
) -> int:
    """Run a single source crawl. Returns count of items ingested."""
    key = source["source_key"]
    src_type = source.get("type", "rss")
    rate_cfg = source.get("rate_limit", {})
    store_body = policy.should_store_body(key)
    terms_ver = policy.get_terms_version(key)

    # Check cooldown
    state = await db.get_crawl_state(key)
    if state.get("cooldown_until_utc"):
        cooldown = state["cooldown_until_utc"]
        if isinstance(cooldown, str):
            from dateutil import parser as dp
            cooldown = dp.parse(cooldown)
        if cooldown.tzinfo is None:
            cooldown = cooldown.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) < cooldown:
            log.info("source_in_cooldown", source_key=key, until=str(cooldown))
            return 0

    from crawler.fetcher import fetch, FetchResult
    from crawler.compliance.robots import get_snapshot

    items_ingested = 0

    try:
        if src_type == "api" and key.startswith("benzinga"):
            items_ingested = await _run_benzinga(source, session, state, store_body, rate_cfg, terms_ver)
        elif src_type in ("rss", "atom"):
            items_ingested = await _run_rss(source, session, state, store_body, rate_cfg, terms_ver)
        elif src_type == "web":
            items_ingested = await _run_web(source, session, state, store_body, rate_cfg, terms_ver)
        elif src_type == "vendor_feed":
            log.info("vendor_feed_stub", source_key=key)
            return 0
        else:
            log.warning("unknown_source_type", source_key=key, type=src_type)
            return 0

        await db.record_crawl_success(key)
        ITEMS_INGESTED.labels(source_key=key).set(items_ingested)
        log.info("source_done", source_key=key, items=items_ingested)

    except Exception as exc:
        log.error("source_error", source_key=key, error=str(exc))
        from datetime import timedelta
        state_data = await db.get_crawl_state(key)
        failures = (state_data.get("consecutive_failures") or 0) + 1
        cooldown_s = min(900_000, 1_000 * (2 ** failures)) / 1_000
        cooldown_until = datetime.now(timezone.utc).replace(microsecond=0)
        from datetime import timedelta
        cooldown_until = cooldown_until + timedelta(seconds=cooldown_s)
        await db.record_crawl_failure(key, str(type(exc).__name__), cooldown_until)

    return items_ingested


async def _run_benzinga(
    source: dict,
    session: aiohttp.ClientSession,
    state: dict,
    store_body: bool,
    rate_cfg: dict,
    terms_ver: Optional[str],
) -> int:
    from crawler.fetcher import fetch

    key = source["source_key"]
    updated_since = state.get("updated_since")
    page = 0
    total = 0
    latest_ts: Optional[int] = None

    while True:
        url = bz_parser.build_news_url(page=page, updated_since=updated_since)
        result = await fetch(
            session, url, key,
            rate_cfg=rate_cfg,
            extra_headers=bz_parser.auth_headers(),
            payload_format="json",
            source_type="api",
            terms_policy_version=terms_ver,
        )

        if result.not_modified:
            break

        import json as _json
        data = _json.loads(result.body)
        items = bz_parser.parse_news_response(data, source_key=key, store_body=store_body)

        if not items:
            break  # no more pages

        for item in items:
            await _ingest_item(item, key, result.raw_id, session)
            total += 1
            if item.get("published_time_utc"):
                ts = int(item["published_time_utc"].timestamp())
                if latest_ts is None or ts > latest_ts:
                    latest_ts = ts

        page += 1

    # Advance delta cursor to now so next run only fetches new items
    if latest_ts is not None:
        await db.set_crawl_state(key, updated_since=latest_ts)

    return total


async def _run_rss(
    source: dict,
    session: aiohttp.ClientSession,
    state: dict,
    store_body: bool,
    rate_cfg: dict,
    terms_ver: Optional[str],
) -> int:
    from crawler.fetcher import fetch

    key = source["source_key"]
    total = 0

    for feed_url in source.get("feed_urls", []):
        etag = state.get("etag")
        last_modified = None
        if state.get("last_modified"):
            lm = state["last_modified"]
            last_modified = lm.strftime("%a, %d %b %Y %H:%M:%S GMT") if hasattr(lm, "strftime") else str(lm)

        result = await fetch(
            session, feed_url, key,
            rate_cfg=rate_cfg,
            etag=etag,
            last_modified=last_modified,
            extra_headers={"Accept": "application/rss+xml, application/atom+xml, */*"},
            payload_format="rss",
            source_type="rss",
            terms_policy_version=terms_ver,
        )

        if result.not_modified:
            log.info("rss_not_modified", source_key=key, feed_url=feed_url)
            continue

        items = rss_parser.parse(result.body, key, feed_url, store_body=store_body)
        for item in items:
            await _ingest_item(item, key, result.raw_id, session)
            total += 1

        # Update ETag / Last-Modified for next conditional request
        state_updates: dict = {}
        if result.etag:
            state_updates["etag"] = result.etag
        if result.last_modified:
            from email.utils import parsedate_to_datetime
            try:
                state_updates["last_modified"] = parsedate_to_datetime(result.last_modified)
            except Exception:
                pass
        if state_updates:
            await db.set_crawl_state(key, **state_updates)

    return total


async def _run_web(
    source: dict,
    session: aiohttp.ClientSession,
    state: dict,
    store_body: bool,
    rate_cfg: dict,
    terms_ver: Optional[str],
) -> int:
    from crawler.fetcher import fetch

    key = source["source_key"]
    base_url = source.get("base_url", "")
    endpoints = source.get("endpoints", {})
    total = 0

    for endpoint_name, path in endpoints.items():
        url = base_url + path
        result = await fetch(
            session, url, key,
            rate_cfg=rate_cfg,
            extra_headers={"Accept": "text/html"},
            payload_format="html",
            source_type="web",
            terms_policy_version=terms_ver,
        )
        if result.not_modified:
            continue

        item = html_parser.parse(result.body, url, key, store_body=store_body)
        if item:
            await _ingest_item(item, key, result.raw_id, session)
            total += 1

    return total


async def _ingest_item(
    item: dict,
    source_key: str,
    raw_id,
    session: aiohttp.ClientSession,
) -> None:
    """Normalise → dedupe → store → alert a single item."""
    # Normalise URL
    if item.get("canonical_url"):
        item["canonical_url"] = normalise_url(item["canonical_url"])

    # Compliance flags
    has_body = bool(item.get("body"))
    item["compliance_flags"] = policy.get_compliance_flags(source_key, has_body)
    item["rights"] = policy.get_rights_metadata(source_key)

    # Dedupe
    item = deduper.dedupe_item(item)

    # Fingerprint for raw_items_index
    fingerprint = item.get("fingerprint_v1") or make_fingerprint(
        source_key,
        item.get("canonical_external_id"),
        item.get("title", ""),
    )

    # Insert raw item index
    if raw_id:
        await db.insert_raw_item(
            raw_id=raw_id,
            source_item_id=item.get("canonical_external_id"),
            source_item_url=item.get("canonical_url"),
            title_raw=item.get("title"),
            published_time_utc=item.get("published_time_utc"),
            updated_time_utc=item.get("updated_time_utc"),
            content_pointer=None,
            fingerprint_v1=fingerprint,
        )

    # Upsert normalised item
    if item.get("published_time_utc") is None:
        item["published_time_utc"] = datetime.now(timezone.utc)
    await db.upsert_news_item(item)

    # Alerts
    matched = alert_rules.evaluate(item)
    if matched:
        await dispatcher.dispatch(item, matched, session)


# ── Scheduler loop ────────────────────────────────────────────────────────────

async def run_loop(
    config_path: str = "config/sources.yaml",
    run_once: bool = False,
    target_source: Optional[str] = None,
) -> None:
    cfg = load_config(config_path)
    sources = [s for s in cfg.get("sources", []) if s.get("enabled", False)]

    if target_source:
        sources = [s for s in sources if s["source_key"] == target_source]
        if not sources:
            log.error("source_not_found", source_key=target_source)
            return

    # Load policies and rules
    policy.load_policies(cfg.get("sources", []))
    alert_rules.load_rules(cfg.get("alert_rules", []))

    SOURCES_ACTIVE.set(len(sources))
    log.info("scheduler_start", sources=len(sources), run_once=run_once)

    # Start metrics server
    metrics_port = int(os.environ.get("METRICS_PORT", 9090))
    try:
        start_http_server(metrics_port)
        log.info("metrics_server_started", port=metrics_port)
    except Exception as exc:
        log.warning("metrics_server_failed", error=str(exc))

    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            tasks = [run_source(src, session) for src in sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for src, result in zip(sources, results):
                if isinstance(result, Exception):
                    log.error("source_unhandled", source_key=src["source_key"], error=str(result))

            if run_once:
                break

            # Sleep until next poll cycle (1 minute between passes)
            await asyncio.sleep(60)


# ── CLI ───────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--config", default="config/sources.yaml", help="Path to sources.yaml")
@click.option("--once", is_flag=True, default=False, help="Run one pass and exit")
@click.option("--source", default=None, help="Run a single source by source_key")
def cli(config: str, once: bool, source: Optional[str]) -> None:
    """Financial news crawler scheduler."""
    asyncio.run(run_loop(config_path=config, run_once=once, target_source=source))


if __name__ == "__main__":
    cli()
