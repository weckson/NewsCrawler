-- Migration 001: initial schema
-- Run order matters: raw_documents first, then raw_items_index, then news_items.

BEGIN;

-- ── Enums ─────────────────────────────────────────────────────────────────────
CREATE TYPE source_type_enum AS ENUM ('api', 'rss', 'atom', 'vendor_feed', 'web');
CREATE TYPE payload_format_enum AS ENUM ('json', 'rss', 'atom', 'html', 'pdf');
CREATE TYPE content_type_enum AS ENUM ('news', 'press_release', 'regulatory', 'transcript');

-- ── raw_documents ─────────────────────────────────────────────────────────────
-- Immutable record of every HTTP response received. Never updated; only appended.
CREATE TABLE raw_documents (
    raw_id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_key                TEXT NOT NULL,
    source_type               source_type_enum NOT NULL,
    fetch_time_utc            TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_event_time_utc     TIMESTAMPTZ,
    request_url               TEXT NOT NULL,
    request_method            TEXT NOT NULL DEFAULT 'GET',
    request_headers_redacted  JSONB,
    response_status           INT NOT NULL,
    response_headers          JSONB,
    payload_format            payload_format_enum NOT NULL,
    payload_object_key        TEXT NOT NULL,        -- pointer into object storage
    payload_sha256            BYTEA NOT NULL,
    etag                      TEXT,
    last_modified             TIMESTAMPTZ,
    robots_policy_snapshot    JSONB,
    terms_policy_version      TEXT,
    fetch_latency_ms          INT,
    error_class               TEXT,                 -- timeout | dns | tls | blocked | captcha
    error_detail              TEXT
);

CREATE INDEX raw_documents_source_key_fetch_time
    ON raw_documents (source_key, fetch_time_utc DESC);
CREATE INDEX raw_documents_sha256
    ON raw_documents (payload_sha256);
CREATE INDEX raw_documents_response_status
    ON raw_documents (response_status)
    WHERE response_status NOT IN (200, 304);

-- ── raw_items_index ───────────────────────────────────────────────────────────
-- One row per item extracted from a raw document (fan-out provenance index).
CREATE TABLE raw_items_index (
    raw_item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_id              UUID NOT NULL REFERENCES raw_documents (raw_id),
    source_item_id      TEXT,          -- Benzinga id / RSS guid / storyId
    source_item_url     TEXT,
    title_raw           TEXT,
    published_time_utc  TIMESTAMPTZ,
    updated_time_utc    TIMESTAMPTZ,
    content_pointer     JSONB,         -- JSONPath / offset into payload blob
    fingerprint_v1      BYTEA          -- stable hash of canonical fields
);

CREATE INDEX raw_items_index_raw_id
    ON raw_items_index (raw_id);
CREATE INDEX raw_items_index_source_item_id
    ON raw_items_index (source_item_id)
    WHERE source_item_id IS NOT NULL;
CREATE INDEX raw_items_index_fingerprint
    ON raw_items_index (fingerprint_v1)
    WHERE fingerprint_v1 IS NOT NULL;

-- ── news_items ────────────────────────────────────────────────────────────────
-- Normalised, deduplicated, query-ready layer.
CREATE TABLE news_items (
    news_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_source        TEXT NOT NULL,
    canonical_url           TEXT NOT NULL,
    canonical_external_id   TEXT,
    title                   TEXT NOT NULL,
    summary                 TEXT,
    body                    TEXT,               -- only if licensed/permitted
    language                TEXT NOT NULL DEFAULT 'en',
    published_time_utc      TIMESTAMPTZ NOT NULL,
    updated_time_utc        TIMESTAMPTZ,
    authors                 TEXT[],
    source_attribution      JSONB,
    content_type            content_type_enum NOT NULL DEFAULT 'news',
    tickers                 TEXT[] NOT NULL DEFAULT '{}',
    entities                JSONB,
    topics                  TEXT[] NOT NULL DEFAULT '{}',
    sentiment               JSONB,
    dedupe_cluster_id       UUID NOT NULL,
    dedupe_score            REAL,
    rights                  JSONB,
    compliance_flags        TEXT[] NOT NULL DEFAULT '{}',
    first_seen_utc          TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_utc           TIMESTAMPTZ NOT NULL DEFAULT now(),
    provenance              JSONB NOT NULL DEFAULT '[]'
);

CREATE INDEX news_items_published_time
    ON news_items (published_time_utc DESC);
CREATE INDEX news_items_tickers
    ON news_items USING GIN (tickers);
CREATE INDEX news_items_topics
    ON news_items USING GIN (topics);
CREATE INDEX news_items_dedupe_cluster
    ON news_items (dedupe_cluster_id);
CREATE INDEX news_items_canonical_url
    ON news_items (canonical_url);
CREATE INDEX news_items_canonical_source_published
    ON news_items (canonical_source, published_time_utc DESC);

-- ── news_item_versions ────────────────────────────────────────────────────────
-- Snapshot every meaningful mutation so we can audit "silent updates."
CREATE TABLE news_item_versions (
    version_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    news_id           UUID NOT NULL REFERENCES news_items (news_id),
    version_time_utc  TIMESTAMPTZ NOT NULL DEFAULT now(),
    title             TEXT NOT NULL,
    summary           TEXT,
    body              TEXT,
    diff_meta         JSONB,
    source_item_id    TEXT
);

CREATE INDEX news_item_versions_news_id
    ON news_item_versions (news_id, version_time_utc DESC);

-- ── crawl_state ───────────────────────────────────────────────────────────────
-- Per-source cursor, ETag, and backoff state for efficient delta pulls.
CREATE TABLE crawl_state (
    source_key            TEXT PRIMARY KEY,
    cursor                TEXT,
    updated_since         BIGINT,          -- Unix timestamp for Benzinga updatedSince
    etag                  TEXT,
    last_modified         TIMESTAMPTZ,
    last_success_utc      TIMESTAMPTZ,
    last_error_utc        TIMESTAMPTZ,
    consecutive_failures  INT NOT NULL DEFAULT 0,
    cooldown_until_utc    TIMESTAMPTZ,
    policy_overrides      JSONB
);

COMMIT;
