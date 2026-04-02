"""
PostgreSQL data-access layer using psycopg3 (async).

All write operations are idempotent where possible (ON CONFLICT DO NOTHING /
DO UPDATE) so that re-running a crawl never duplicates rows.
"""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg
import psycopg.rows
import structlog

log = structlog.get_logger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")


@asynccontextmanager
async def get_conn():
    """Async context manager yielding a psycopg3 async connection."""
    async with await psycopg.AsyncConnection.connect(
        _DB_URL, row_factory=psycopg.rows.dict_row
    ) as conn:
        yield conn


# ── raw_documents ─────────────────────────────────────────────────────────────

async def insert_raw_document(
    *,
    source_key: str,
    source_type: str,
    fetch_time_utc: datetime,
    source_event_time_utc: Optional[datetime],
    request_url: str,
    request_method: str,
    request_headers_redacted: dict,
    response_status: int,
    response_headers: dict,
    payload_format: str,
    payload_object_key: str,
    payload_sha256: bytes,
    etag: Optional[str],
    last_modified: Optional[datetime],
    robots_policy_snapshot: Optional[dict],
    terms_policy_version: Optional[str],
    fetch_latency_ms: Optional[int],
    error_class: Optional[str],
    error_detail: Optional[str],
) -> uuid.UUID:
    raw_id = uuid.uuid4()
    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO raw_documents (
                raw_id, source_key, source_type, fetch_time_utc,
                source_event_time_utc, request_url, request_method,
                request_headers_redacted, response_status, response_headers,
                payload_format, payload_object_key, payload_sha256,
                etag, last_modified, robots_policy_snapshot,
                terms_policy_version, fetch_latency_ms,
                error_class, error_detail
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            """,
            (
                raw_id, source_key, source_type, fetch_time_utc,
                source_event_time_utc, request_url, request_method,
                json.dumps(request_headers_redacted), response_status,
                json.dumps(response_headers), payload_format,
                payload_object_key, payload_sha256, etag, last_modified,
                json.dumps(robots_policy_snapshot) if robots_policy_snapshot else None,
                terms_policy_version, fetch_latency_ms, error_class, error_detail,
            ),
        )
    return raw_id


# ── raw_items_index ───────────────────────────────────────────────────────────

async def insert_raw_item(
    *,
    raw_id: uuid.UUID,
    source_item_id: Optional[str],
    source_item_url: Optional[str],
    title_raw: Optional[str],
    published_time_utc: Optional[datetime],
    updated_time_utc: Optional[datetime],
    content_pointer: Optional[dict],
    fingerprint_v1: Optional[bytes],
) -> uuid.UUID:
    raw_item_id = uuid.uuid4()
    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO raw_items_index (
                raw_item_id, raw_id, source_item_id, source_item_url,
                title_raw, published_time_utc, updated_time_utc,
                content_pointer, fingerprint_v1
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                raw_item_id, raw_id, source_item_id, source_item_url,
                title_raw, published_time_utc, updated_time_utc,
                json.dumps(content_pointer) if content_pointer else None,
                fingerprint_v1,
            ),
        )
    return raw_item_id


# ── news_items ────────────────────────────────────────────────────────────────

async def upsert_news_item(item: dict) -> uuid.UUID:
    """
    Insert or update a normalised news item.

    Deduplication key: canonical_url.
    On conflict: update mutable fields and append provenance.
    """
    news_id = item.get("news_id") or uuid.uuid4()
    now = datetime.now(timezone.utc)

    async with get_conn() as conn:
        existing = await conn.fetchrow(
            "SELECT news_id FROM news_items WHERE canonical_url = %s",
            item["canonical_url"],
        )
        if existing:
            news_id = existing["news_id"]
            await conn.execute(
                """
                UPDATE news_items SET
                    title = %s,
                    summary = %s,
                    body = %s,
                    updated_time_utc = %s,
                    tickers = %s,
                    entities = %s,
                    topics = %s,
                    sentiment = %s,
                    last_seen_utc = %s,
                    provenance = provenance || %s::jsonb,
                    compliance_flags = %s,
                    rights = %s
                WHERE news_id = %s
                """,
                (
                    item["title"],
                    item.get("summary"),
                    item.get("body"),
                    item.get("updated_time_utc"),
                    item.get("tickers", []),
                    json.dumps(item.get("entities")) if item.get("entities") else None,
                    item.get("topics", []),
                    json.dumps(item.get("sentiment")) if item.get("sentiment") else None,
                    now,
                    json.dumps([item.get("provenance_entry", {})]),
                    item.get("compliance_flags", []),
                    json.dumps(item.get("rights")) if item.get("rights") else None,
                    news_id,
                ),
            )
            # snapshot version
            await _snapshot_version(conn, news_id, item)
        else:
            await conn.execute(
                """
                INSERT INTO news_items (
                    news_id, canonical_source, canonical_url,
                    canonical_external_id, title, summary, body, language,
                    published_time_utc, updated_time_utc, authors,
                    source_attribution, content_type, tickers, entities,
                    topics, sentiment, dedupe_cluster_id, dedupe_score,
                    rights, compliance_flags, first_seen_utc, last_seen_utc,
                    provenance
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    news_id,
                    item["canonical_source"],
                    item["canonical_url"],
                    item.get("canonical_external_id"),
                    item["title"],
                    item.get("summary"),
                    item.get("body"),
                    item.get("language", "en"),
                    item["published_time_utc"],
                    item.get("updated_time_utc"),
                    item.get("authors"),
                    json.dumps(item.get("source_attribution")) if item.get("source_attribution") else None,
                    item.get("content_type", "news"),
                    item.get("tickers", []),
                    json.dumps(item.get("entities")) if item.get("entities") else None,
                    item.get("topics", []),
                    json.dumps(item.get("sentiment")) if item.get("sentiment") else None,
                    item.get("dedupe_cluster_id") or uuid.uuid4(),
                    item.get("dedupe_score"),
                    json.dumps(item.get("rights")) if item.get("rights") else None,
                    item.get("compliance_flags", []),
                    now,
                    now,
                    json.dumps([item.get("provenance_entry", {})]),
                ),
            )
    return news_id


async def _snapshot_version(conn, news_id: uuid.UUID, item: dict) -> None:
    await conn.execute(
        """
        INSERT INTO news_item_versions (
            news_id, title, summary, body, diff_meta, source_item_id
        ) VALUES (%s,%s,%s,%s,%s,%s)
        """,
        (
            news_id,
            item["title"],
            item.get("summary"),
            item.get("body"),
            json.dumps({"updated": True}),
            item.get("canonical_external_id"),
        ),
    )


# ── crawl_state ───────────────────────────────────────────────────────────────

async def get_crawl_state(source_key: str) -> dict:
    async with get_conn() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM crawl_state WHERE source_key = %s", source_key
        )
        return dict(row) if row else {}


async def set_crawl_state(source_key: str, **kwargs) -> None:
    """Upsert crawl state fields for *source_key*."""
    if not kwargs:
        return
    cols = list(kwargs.keys())
    vals = list(kwargs.values())
    set_clause = ", ".join(f"{c} = %s" for c in cols)
    async with get_conn() as conn:
        await conn.execute(
            f"""
            INSERT INTO crawl_state (source_key, {', '.join(cols)})
            VALUES (%s, {', '.join(['%s'] * len(cols))})
            ON CONFLICT (source_key) DO UPDATE SET {set_clause}
            """,
            [source_key, *vals, *vals],
        )


async def record_crawl_success(source_key: str, updated_since: Optional[int] = None) -> None:
    from datetime import datetime, timezone
    kwargs: dict[str, Any] = {
        "last_success_utc": datetime.now(timezone.utc),
        "consecutive_failures": 0,
        "cooldown_until_utc": None,
        "last_error_utc": None,
    }
    if updated_since is not None:
        kwargs["updated_since"] = updated_since
    await set_crawl_state(source_key, **kwargs)


async def record_crawl_failure(
    source_key: str,
    error_class: str,
    cooldown_until_utc: Optional[datetime] = None,
) -> None:
    from datetime import datetime, timezone
    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO crawl_state (source_key, last_error_utc, consecutive_failures, cooldown_until_utc)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (source_key) DO UPDATE SET
                last_error_utc = EXCLUDED.last_error_utc,
                consecutive_failures = crawl_state.consecutive_failures + 1,
                cooldown_until_utc = EXCLUDED.cooldown_until_utc
            """,
            (source_key, datetime.now(timezone.utc), cooldown_until_utc),
        )
