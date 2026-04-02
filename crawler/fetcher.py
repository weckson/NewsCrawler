"""
Async HTTP fetcher with:
- Per-domain rate limiting (token-bucket style)
- Exponential backoff with full jitter
- Retry-After header support (RFC 9110)
- Conditional requests (ETag / If-None-Match, Last-Modified / If-Modified-Since)
- robots.txt enforcement
- CAPTCHA detection and stop-and-escalate
- Structured logging + Prometheus metrics
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import aiohttp
import structlog
from prometheus_client import Counter, Histogram

from crawler.compliance import captcha, robots
from crawler.storage import db, object_store

log = structlog.get_logger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────────────────
FETCH_TOTAL = Counter(
    "crawler_fetch_total",
    "Total HTTP fetch attempts",
    ["source_key", "status"],
)
FETCH_LATENCY = Histogram(
    "crawler_fetch_latency_seconds",
    "HTTP fetch latency",
    ["source_key"],
)
RATE_LIMIT_EVENTS = Counter(
    "crawler_rate_limit_total",
    "429/503 rate-limit events",
    ["source_key"],
)
DETECTION_EVENTS = Counter(
    "crawler_detection_total",
    "CAPTCHA/403 detection events",
    ["domain"],
)

# ── Domain rate-limit state ───────────────────────────────────────────────────

@dataclass
class DomainState:
    semaphore: asyncio.Semaphore
    min_delay_ms: int
    max_delay_ms: int
    max_retries: int
    backoff_base_ms: int = 1_000
    backoff_cap_ms: int = 900_000
    _last_request_at: float = field(default=0.0, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def acquire(self) -> None:
        await self.semaphore.acquire()
        async with self._lock:
            now = time.monotonic()
            elapsed_ms = (now - self._last_request_at) * 1_000
            delay_ms = random.uniform(self.min_delay_ms, self.max_delay_ms)
            wait_ms = max(0.0, delay_ms - elapsed_ms)
            if wait_ms > 0:
                await asyncio.sleep(wait_ms / 1_000)
            self._last_request_at = time.monotonic()

    def release(self) -> None:
        self.semaphore.release()

    def jittered_backoff(self, attempt: int) -> float:
        """Full-jitter exponential backoff (seconds)."""
        cap = min(self.backoff_cap_ms, self.backoff_base_ms * (2 ** attempt))
        return random.uniform(0, cap) / 1_000


_domain_states: dict[str, DomainState] = {}
_states_lock = asyncio.Lock()

_DEFAULT_RATE = {
    "max_concurrency": 1,
    "min_delay_ms": 1_000,
    "max_delay_ms": 3_000,
    "max_retries": 3,
}


def _domain(url: str) -> str:
    return urlparse(url).netloc


async def _get_domain_state(url: str, rate_cfg: dict) -> DomainState:
    domain = _domain(url)
    async with _states_lock:
        if domain not in _domain_states:
            cfg = {**_DEFAULT_RATE, **rate_cfg}
            _domain_states[domain] = DomainState(
                semaphore=asyncio.Semaphore(cfg["max_concurrency"]),
                min_delay_ms=cfg["min_delay_ms"],
                max_delay_ms=cfg["max_delay_ms"],
                max_retries=cfg["max_retries"],
            )
        return _domain_states[domain]


# ── FetchResult ───────────────────────────────────────────────────────────────

@dataclass
class FetchResult:
    url: str
    status: int
    headers: dict
    body: bytes
    etag: Optional[str]
    last_modified: Optional[str]
    latency_ms: int
    not_modified: bool = False     # True on 304
    raw_id: Optional[object] = None


# ── Core fetch function ───────────────────────────────────────────────────────

_USER_AGENT = "NewsCrawler/1.0 (+crawler@example.com)"


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    source_key: str,
    rate_cfg: dict | None = None,
    *,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    extra_headers: dict | None = None,
    payload_format: str = "json",
    source_type: str = "api",
    robots_policy_snapshot: dict | None = None,
    terms_policy_version: str | None = None,
) -> FetchResult:
    """
    Fetch *url* with rate-limiting, backoff, conditional requests, and
    CAPTCHA detection. Stores raw payload to object storage and records
    raw_documents entry.
    """
    rate_cfg = rate_cfg or {}
    domain_state = await _get_domain_state(url, rate_cfg)
    max_retries = rate_cfg.get("max_retries", _DEFAULT_RATE["max_retries"])

    # robots.txt check
    if not await robots.is_allowed(session, url):
        log.info("robots_blocked", url=url)
        raise RobotsBlocked(url)

    # CAPTCHA halt check
    if captcha.is_halted(url):
        raise captcha.CaptchaDetected(_domain(url), {})

    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "application/json",
        **(extra_headers or {}),
    }
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        await domain_state.acquire()
        t0 = time.monotonic()
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                latency_ms = int((time.monotonic() - t0) * 1_000)
                status = resp.status
                resp_headers = dict(resp.headers)

                FETCH_TOTAL.labels(source_key=source_key, status=str(status)).inc()
                FETCH_LATENCY.labels(source_key=source_key).observe(latency_ms / 1_000)

                log.info(
                    "fetch",
                    url=url,
                    status=status,
                    latency_ms=latency_ms,
                    attempt=attempt,
                )

                if status == 304:
                    domain_state.release()
                    return FetchResult(
                        url=url, status=304, headers=resp_headers,
                        body=b"", etag=etag, last_modified=last_modified,
                        latency_ms=latency_ms, not_modified=True,
                    )

                if status == 429 or status == 503:
                    domain_state.release()
                    RATE_LIMIT_EVENTS.labels(source_key=source_key).inc()
                    retry_after = _parse_retry_after(resp_headers)
                    wait = retry_after if retry_after else domain_state.jittered_backoff(attempt)
                    log.warning(
                        "rate_limited",
                        url=url,
                        status=status,
                        retry_after=retry_after,
                        waiting_s=wait,
                        attempt=attempt,
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)
                    else:
                        raise RateLimitExceeded(url, status)
                    continue

                if status == 401 or status == 403:
                    body_text = await resp.text(errors="replace")
                    domain_state.release()
                    DETECTION_EVENTS.labels(domain=_domain(url)).inc()
                    captcha.handle_detection(url, body_text, status)  # raises

                body = await resp.read()
                domain_state.release()

                # CAPTCHA check on 200 responses
                if "text" in resp_headers.get("Content-Type", ""):
                    body_text = body.decode(errors="replace")
                    if captcha.detect(body_text, status):
                        DETECTION_EVENTS.labels(domain=_domain(url)).inc()
                        captcha.handle_detection(url, body_text, status)  # raises

                # Store raw payload
                fmt = payload_format
                object_key, sha256 = object_store.put_raw(body, source_key, fmt)

                new_etag = resp_headers.get("ETag")
                new_lm = resp_headers.get("Last-Modified")

                # Record raw_documents row
                raw_id = await db.insert_raw_document(
                    source_key=source_key,
                    source_type=source_type,
                    fetch_time_utc=datetime.now(timezone.utc),
                    source_event_time_utc=None,
                    request_url=url,
                    request_method="GET",
                    request_headers_redacted=_redact_headers(headers),
                    response_status=status,
                    response_headers=resp_headers,
                    payload_format=fmt,
                    payload_object_key=object_key,
                    payload_sha256=sha256,
                    etag=new_etag,
                    last_modified=_parse_http_date(new_lm) if new_lm else None,
                    robots_policy_snapshot=robots_policy_snapshot,
                    terms_policy_version=terms_policy_version,
                    fetch_latency_ms=latency_ms,
                    error_class=None,
                    error_detail=None,
                )

                return FetchResult(
                    url=url,
                    status=status,
                    headers=resp_headers,
                    body=body,
                    etag=new_etag,
                    last_modified=new_lm,
                    latency_ms=latency_ms,
                    raw_id=raw_id,
                )

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            domain_state.release()
            last_exc = exc
            wait = domain_state.jittered_backoff(attempt)
            log.warning(
                "fetch_error",
                url=url,
                error=str(exc),
                attempt=attempt,
                retry_in_s=wait,
            )
            if attempt < max_retries:
                await asyncio.sleep(wait)
            else:
                raise FetchError(url, exc) from exc

    raise FetchError(url, last_exc)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_retry_after(headers: dict) -> float | None:
    """Parse Retry-After header (seconds or HTTP-date) per RFC 9110."""
    val = headers.get("Retry-After")
    if val is None:
        return None
    try:
        return float(val)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime
        retry_dt = parsedate_to_datetime(val)
        delta = (retry_dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


def _parse_http_date(val: str) -> datetime | None:
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(val)
    except Exception:
        return None


def _redact_headers(headers: dict) -> dict:
    """Remove secret values from headers before logging/storing."""
    redacted = {}
    for k, v in headers.items():
        if k.lower() in ("authorization", "x-api-key", "api-key", "token"):
            redacted[k] = "***REDACTED***"
        else:
            redacted[k] = v
    return redacted


# ── Exceptions ────────────────────────────────────────────────────────────────

class RobotsBlocked(Exception):
    def __init__(self, url: str):
        super().__init__(f"robots.txt disallows: {url}")


class RateLimitExceeded(Exception):
    def __init__(self, url: str, status: int):
        super().__init__(f"Rate limit exceeded ({status}) for {url}")


class FetchError(Exception):
    def __init__(self, url: str, cause: Exception | None):
        super().__init__(f"Fetch failed for {url}: {cause}")
        self.cause = cause
