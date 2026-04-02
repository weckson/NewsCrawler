"""
SQLite data-access layer for local/dev mode.

Drop-in replacement for postgres.py — same function signatures so scheduler
and fetcher can be swapped without code changes.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import structlog

log = structlog.get_logger(__name__)

_DB_PATH = os.environ.get("SQLITE_DB_PATH", "./data/crawler.db")
_initialised = False


def _ensure_db() -> None:
    global _initialised
    if _initialised:
        return
    p = Path(_DB_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with _get_conn() as conn:
        conn.executescript(_SCHEMA)
    _initialised = True


@contextmanager
def _get_conn():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
    _ensure_db()
    raw_id = uuid.uuid4()
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO raw_documents (
                raw_id, source_key, source_type, fetch_time_utc,
                source_event_time_utc, request_url, request_method,
                request_headers_redacted, response_status, response_headers,
                payload_format, payload_object_key, payload_sha256,
                etag, last_modified, robots_policy_snapshot,
                terms_policy_version, fetch_latency_ms,
                error_class, error_detail
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(raw_id), source_key, source_type,
                _dt_str(fetch_time_utc),
                _dt_str(source_event_time_utc),
                request_url, request_method,
                json.dumps(request_headers_redacted),
                response_status,
                json.dumps(response_headers),
                payload_format, payload_object_key,
                payload_sha256.hex() if isinstance(payload_sha256, bytes) else payload_sha256,
                etag,
                _dt_str(last_modified),
                json.dumps(robots_policy_snapshot) if robots_policy_snapshot else None,
                terms_policy_version, fetch_latency_ms,
                error_class, error_detail,
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
    _ensure_db()
    raw_item_id = uuid.uuid4()
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO raw_items_index (
                raw_item_id, raw_id, source_item_id, source_item_url,
                title_raw, published_time_utc, updated_time_utc,
                content_pointer, fingerprint_v1
            ) VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                str(raw_item_id), str(raw_id), source_item_id,
                source_item_url, title_raw,
                _dt_str(published_time_utc),
                _dt_str(updated_time_utc),
                json.dumps(content_pointer) if content_pointer else None,
                fingerprint_v1.hex() if isinstance(fingerprint_v1, bytes) else fingerprint_v1,
            ),
        )
    return raw_item_id


# ── news_items ────────────────────────────────────────────────────────────────

async def upsert_news_item(item: dict) -> uuid.UUID:
    _ensure_db()
    news_id = item.get("news_id") or uuid.uuid4()
    now = datetime.now(timezone.utc)

    with _get_conn() as conn:
        row = conn.execute(
            "SELECT news_id FROM news_items WHERE canonical_url = ?",
            (item["canonical_url"],),
        ).fetchone()

        if row:
            news_id = uuid.UUID(row["news_id"]) if isinstance(row["news_id"], str) else row["news_id"]
            conn.execute(
                """
                UPDATE news_items SET
                    title = ?,
                    summary = ?,
                    body = ?,
                    updated_time_utc = ?,
                    tickers = ?,
                    entities = ?,
                    topics = ?,
                    sentiment = ?,
                    last_seen_utc = ?,
                    compliance_flags = ?,
                    rights = ?
                WHERE news_id = ?
                """,
                (
                    item["title"],
                    item.get("summary"),
                    item.get("body"),
                    _dt_str(item.get("updated_time_utc")),
                    json.dumps(item.get("tickers", [])),
                    json.dumps(item.get("entities")) if item.get("entities") else None,
                    json.dumps(item.get("topics", [])),
                    json.dumps(item.get("sentiment")) if item.get("sentiment") else None,
                    _dt_str(now),
                    json.dumps(item.get("compliance_flags", [])),
                    json.dumps(item.get("rights")) if item.get("rights") else None,
                    str(news_id),
                ),
            )
            # snapshot version
            conn.execute(
                """
                INSERT INTO news_item_versions (
                    version_id, news_id, version_time_utc, title, summary, body,
                    diff_meta, source_item_id
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    str(uuid.uuid4()), str(news_id), _dt_str(now),
                    item["title"], item.get("summary"), item.get("body"),
                    json.dumps({"updated": True}),
                    item.get("canonical_external_id"),
                ),
            )
        else:
            cluster_id = item.get("dedupe_cluster_id") or uuid.uuid4()
            conn.execute(
                """
                INSERT INTO news_items (
                    news_id, canonical_source, canonical_url,
                    canonical_external_id, title, summary, body, language,
                    published_time_utc, updated_time_utc, authors,
                    source_attribution, content_type, tickers, entities,
                    topics, sentiment, dedupe_cluster_id, dedupe_score,
                    rights, compliance_flags, first_seen_utc, last_seen_utc,
                    provenance
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(news_id),
                    item["canonical_source"],
                    item["canonical_url"],
                    item.get("canonical_external_id"),
                    item["title"],
                    item.get("summary"),
                    item.get("body"),
                    item.get("language", "en"),
                    _dt_str(item["published_time_utc"]),
                    _dt_str(item.get("updated_time_utc")),
                    json.dumps(item.get("authors")) if item.get("authors") else None,
                    json.dumps(item.get("source_attribution")) if item.get("source_attribution") else None,
                    item.get("content_type", "news"),
                    json.dumps(item.get("tickers", [])),
                    json.dumps(item.get("entities")) if item.get("entities") else None,
                    json.dumps(item.get("topics", [])),
                    json.dumps(item.get("sentiment")) if item.get("sentiment") else None,
                    str(cluster_id),
                    item.get("dedupe_score"),
                    json.dumps(item.get("rights")) if item.get("rights") else None,
                    json.dumps(item.get("compliance_flags", [])),
                    _dt_str(now),
                    _dt_str(now),
                    json.dumps([item.get("provenance_entry", {})]),
                ),
            )
    return news_id


# ── crawl_state ───────────────────────────────────────────────────────────────

async def get_crawl_state(source_key: str) -> dict:
    _ensure_db()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM crawl_state WHERE source_key = ?", (source_key,)
        ).fetchone()
        return dict(row) if row else {}


async def set_crawl_state(source_key: str, **kwargs) -> None:
    _ensure_db()
    if not kwargs:
        return
    # Serialise datetimes
    for k, v in kwargs.items():
        if isinstance(v, datetime):
            kwargs[k] = _dt_str(v)

    cols = list(kwargs.keys())
    vals = list(kwargs.values())
    placeholders = ", ".join(["?"] * len(cols))
    set_clause = ", ".join(f"{c} = ?" for c in cols)

    with _get_conn() as conn:
        conn.execute(
            f"""
            INSERT INTO crawl_state (source_key, {', '.join(cols)})
            VALUES (?, {placeholders})
            ON CONFLICT (source_key) DO UPDATE SET {set_clause}
            """,
            [source_key, *vals, *vals],
        )


async def record_crawl_success(source_key: str, updated_since: Optional[int] = None) -> None:
    kwargs: dict[str, Any] = {
        "last_success_utc": _dt_str(datetime.now(timezone.utc)),
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
    _ensure_db()
    now_str = _dt_str(datetime.now(timezone.utc))
    cooldown_str = _dt_str(cooldown_until_utc)

    with _get_conn() as conn:
        row = conn.execute(
            "SELECT consecutive_failures FROM crawl_state WHERE source_key = ?",
            (source_key,),
        ).fetchone()

        if row:
            failures = (row["consecutive_failures"] or 0) + 1
            conn.execute(
                """
                UPDATE crawl_state SET
                    last_error_utc = ?,
                    consecutive_failures = ?,
                    cooldown_until_utc = ?
                WHERE source_key = ?
                """,
                (now_str, failures, cooldown_str, source_key),
            )
        else:
            conn.execute(
                """
                INSERT INTO crawl_state (source_key, last_error_utc, consecutive_failures, cooldown_until_utc)
                VALUES (?, ?, 1, ?)
                """,
                (source_key, now_str, cooldown_str),
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dt_str(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


# ── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_documents (
    raw_id                    TEXT PRIMARY KEY,
    source_key                TEXT NOT NULL,
    source_type               TEXT NOT NULL,
    fetch_time_utc            TEXT NOT NULL,
    source_event_time_utc     TEXT,
    request_url               TEXT NOT NULL,
    request_method            TEXT NOT NULL DEFAULT 'GET',
    request_headers_redacted  TEXT,
    response_status           INTEGER NOT NULL,
    response_headers          TEXT,
    payload_format            TEXT NOT NULL,
    payload_object_key        TEXT NOT NULL,
    payload_sha256            TEXT NOT NULL,
    etag                      TEXT,
    last_modified             TEXT,
    robots_policy_snapshot    TEXT,
    terms_policy_version      TEXT,
    fetch_latency_ms          INTEGER,
    error_class               TEXT,
    error_detail              TEXT
);

CREATE TABLE IF NOT EXISTS raw_items_index (
    raw_item_id         TEXT PRIMARY KEY,
    raw_id              TEXT NOT NULL,
    source_item_id      TEXT,
    source_item_url     TEXT,
    title_raw           TEXT,
    published_time_utc  TEXT,
    updated_time_utc    TEXT,
    content_pointer     TEXT,
    fingerprint_v1      TEXT
);

CREATE TABLE IF NOT EXISTS news_items (
    news_id                 TEXT PRIMARY KEY,
    canonical_source        TEXT NOT NULL,
    canonical_url           TEXT NOT NULL,
    canonical_external_id   TEXT,
    title                   TEXT NOT NULL,
    summary                 TEXT,
    body                    TEXT,
    language                TEXT NOT NULL DEFAULT 'en',
    published_time_utc      TEXT NOT NULL,
    updated_time_utc        TEXT,
    authors                 TEXT,
    source_attribution      TEXT,
    content_type            TEXT NOT NULL DEFAULT 'news',
    tickers                 TEXT NOT NULL DEFAULT '[]',
    entities                TEXT,
    topics                  TEXT NOT NULL DEFAULT '[]',
    sentiment               TEXT,
    dedupe_cluster_id       TEXT NOT NULL,
    dedupe_score            REAL,
    rights                  TEXT,
    compliance_flags        TEXT NOT NULL DEFAULT '[]',
    first_seen_utc          TEXT NOT NULL,
    last_seen_utc           TEXT NOT NULL,
    provenance              TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_news_items_canonical_url ON news_items (canonical_url);
CREATE INDEX IF NOT EXISTS idx_news_items_published ON news_items (published_time_utc DESC);

CREATE TABLE IF NOT EXISTS news_item_versions (
    version_id        TEXT PRIMARY KEY,
    news_id           TEXT NOT NULL,
    version_time_utc  TEXT NOT NULL,
    title             TEXT NOT NULL,
    summary           TEXT,
    body              TEXT,
    diff_meta         TEXT,
    source_item_id    TEXT
);

CREATE TABLE IF NOT EXISTS crawl_state (
    source_key            TEXT PRIMARY KEY,
    cursor                TEXT,
    updated_since         INTEGER,
    etag                  TEXT,
    last_modified         TEXT,
    last_success_utc      TEXT,
    last_error_utc        TEXT,
    consecutive_failures  INTEGER NOT NULL DEFAULT 0,
    cooldown_until_utc    TEXT,
    policy_overrides      TEXT
);
"""
