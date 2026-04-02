"""
Integration tests: fetcher
- Acceptance test 2: Retry-After handling (must NOT retry before Retry-After seconds)
- Acceptance test 3: Conditional GET (ETag → If-None-Match → 304)
- Rate limit detection
- CAPTCHA detection + halt
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp
from aioresponses import aioresponses

from crawler.fetcher import fetch, RateLimitExceeded
from crawler.compliance.captcha import CaptchaDetected, clear_halt, _halted_domains


# ── Helpers ───────────────────────────────────────────────────────────────────

DUMMY_URL = "https://api.benzinga.com/api/v2/news?pageSize=1"
DUMMY_SOURCE = "benzinga_news_api"
DUMMY_PAYLOAD = json.dumps([{"id": 1, "title": "Test", "created": 1711411200}]).encode()


def _mock_robots_allow(monkeypatch):
    """Patch robots.is_allowed to always return True."""
    monkeypatch.setattr("crawler.fetcher.robots.is_allowed", AsyncMock(return_value=True))


def _mock_db(monkeypatch):
    """Patch DB insert to a no-op."""
    monkeypatch.setattr("crawler.fetcher.db.insert_raw_document", AsyncMock(return_value="mock-uuid"))


def _mock_object_store(monkeypatch):
    """Patch object store to a no-op."""
    monkeypatch.setattr("crawler.fetcher.object_store.put_raw", lambda p, s, f: ("key/test.json", b"\x00" * 32))


# ── Acceptance test 2: Retry-After ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_retry_after_respected(monkeypatch, freezer=None):
    """
    When a domain returns 429 Retry-After: 5, the fetcher must wait at least
    ~5 seconds before retrying (using time mocking to avoid real sleeps).
    """
    _mock_robots_allow(monkeypatch)
    _mock_db(monkeypatch)
    _mock_object_store(monkeypatch)

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with aioresponses() as m:
        # First response: 429 with Retry-After: 5
        m.get(DUMMY_URL, status=429, headers={"Retry-After": "5"})
        # Second response: 200 with payload
        m.get(DUMMY_URL, status=200, body=DUMMY_PAYLOAD,
              headers={"Content-Type": "application/json"})

        async with aiohttp.ClientSession() as session:
            result = await fetch(
                session, DUMMY_URL, DUMMY_SOURCE,
                rate_cfg={"max_retries": 3, "min_delay_ms": 0, "max_delay_ms": 10},
            )

    # The fetcher must have slept for at least 5 seconds (the Retry-After value)
    assert any(s >= 5.0 for s in sleep_calls), (
        f"Expected sleep >= 5s for Retry-After, got: {sleep_calls}"
    )
    assert result.status == 200


# ── Acceptance test 3: Conditional GET ────────────────────────────────────────

@pytest.mark.asyncio
async def test_conditional_get_etag(monkeypatch):
    """
    First request stores ETag; second request sends If-None-Match and handles 304.
    """
    _mock_robots_allow(monkeypatch)
    _mock_db(monkeypatch)
    _mock_object_store(monkeypatch)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    with aioresponses() as m:
        # First: 200 with ETag
        m.get(
            DUMMY_URL, status=200, body=DUMMY_PAYLOAD,
            headers={"ETag": '"abc123"', "Content-Type": "application/json"},
        )
        # Second: 304 Not Modified
        m.get(DUMMY_URL, status=304, headers={})

        async with aiohttp.ClientSession() as session:
            # First fetch
            result1 = await fetch(session, DUMMY_URL, DUMMY_SOURCE,
                                  rate_cfg={"max_retries": 1, "min_delay_ms": 0, "max_delay_ms": 10})
            assert result1.status == 200
            assert result1.etag == '"abc123"'
            assert not result1.not_modified

            # Second fetch with stored ETag
            result2 = await fetch(
                session, DUMMY_URL, DUMMY_SOURCE,
                etag=result1.etag,
                rate_cfg={"max_retries": 1, "min_delay_ms": 0, "max_delay_ms": 10},
            )
            assert result2.status == 304
            assert result2.not_modified
            assert result2.body == b""


# ── CAPTCHA detection ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_captcha_403_halts_domain(monkeypatch):
    """403 response triggers CAPTCHA halt and raises CaptchaDetected."""
    _mock_robots_allow(monkeypatch)
    _mock_db(monkeypatch)
    _mock_object_store(monkeypatch)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    # Clear any prior halt state
    _halted_domains.clear()

    with aioresponses() as m:
        m.get(DUMMY_URL, status=403, body=b"Access Denied",
              headers={"Content-Type": "text/html"})

        async with aiohttp.ClientSession() as session:
            with pytest.raises(CaptchaDetected) as exc_info:
                await fetch(session, DUMMY_URL, DUMMY_SOURCE,
                            rate_cfg={"max_retries": 0, "min_delay_ms": 0, "max_delay_ms": 10})

    assert "api.benzinga.com" in exc_info.value.domain
    assert "api.benzinga.com" in _halted_domains

    # Cleanup
    clear_halt("api.benzinga.com")


# ── Rate limit exhaustion ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rate_limit_exceeded_after_retries(monkeypatch):
    """After max_retries 429s with no success, raises RateLimitExceeded."""
    _mock_robots_allow(monkeypatch)
    _mock_db(monkeypatch)
    _mock_object_store(monkeypatch)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())

    with aioresponses() as m:
        for _ in range(4):  # max_retries=3 → 4 attempts total
            m.get(DUMMY_URL, status=429, headers={"Retry-After": "1"})

        async with aiohttp.ClientSession() as session:
            with pytest.raises(RateLimitExceeded):
                await fetch(session, DUMMY_URL, DUMMY_SOURCE,
                            rate_cfg={"max_retries": 3, "min_delay_ms": 0, "max_delay_ms": 10})
