"""
robots.txt fetching, caching, and policy evaluation (RFC 9309).

Uses the `protego` library which implements RFC 9309 correctly including:
- crawl-delay directives
- allow/disallow precedence rules
- error-handling (treat 4xx as allow-all; treat 5xx as deny-all temporarily)
"""

from __future__ import annotations

import asyncio
import time
from urllib.parse import urlparse

import aiohttp
import protego
import structlog

log = structlog.get_logger(__name__)

# Cache TTL in seconds (RFC 9309 recommends at least 24h)
_CACHE_TTL = 86_400
_USER_AGENT = "NewsCrawler/1.0 (+crawler@example.com)"

# In-memory cache: domain -> (parsed_robots, fetched_at)
_cache: dict[str, tuple[protego.Protego, float]] = {}
_lock = asyncio.Lock()


def _origin(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


async def _fetch_robots(session: aiohttp.ClientSession, origin: str) -> protego.Protego:
    robots_url = f"{origin}/robots.txt"
    try:
        async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                text = await resp.text(errors="replace")
                return protego.Protego.parse(text)
            elif resp.status == 404 or (400 <= resp.status < 500):
                # RFC 9309 §2.3.1: treat 4xx as no restrictions
                log.debug("robots_4xx_allow_all", url=robots_url, status=resp.status)
                return protego.Protego.parse("")
            else:
                # RFC 9309 §2.3.1: treat 5xx as temporarily deny-all
                log.warning("robots_5xx_deny_all", url=robots_url, status=resp.status)
                return protego.Protego.parse(f"User-agent: *\nDisallow: /")
    except Exception as exc:
        log.warning("robots_fetch_error", url=robots_url, error=str(exc))
        # Conservative: allow-all on network errors (we can't confirm denial)
        return protego.Protego.parse("")


async def is_allowed(
    session: aiohttp.ClientSession, url: str, user_agent: str = _USER_AGENT
) -> bool:
    """Return True if *url* is allowed per the relevant robots.txt."""
    origin = _origin(url)
    async with _lock:
        cached = _cache.get(origin)
        now = time.time()
        if cached is None or (now - cached[1]) > _CACHE_TTL:
            robots = await _fetch_robots(session, origin)
            _cache[origin] = (robots, now)
        else:
            robots = cached[0]

    allowed = robots.can_fetch(url, user_agent)
    if not allowed:
        log.info("robots_disallowed", url=url)
    return allowed


async def crawl_delay(session: aiohttp.ClientSession, url: str) -> float:
    """Return the crawl-delay in seconds specified for this origin (0 if none)."""
    origin = _origin(url)
    async with _lock:
        cached = _cache.get(origin)
    if cached:
        delay = cached[0].crawl_delay(_USER_AGENT)
        return float(delay) if delay else 0.0
    return 0.0


def get_snapshot(url: str) -> dict | None:
    """Return a snapshot of the cached robots policy for audit logging."""
    origin = _origin(url)
    cached = _cache.get(origin)
    if cached is None:
        return None
    robots, fetched_at = cached
    return {"origin": origin, "fetched_at": fetched_at, "source": "cache"}
