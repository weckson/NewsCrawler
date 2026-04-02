"""
Alert dispatcher: sends matched alerts to configured channels.

Supported channels:
- slack     (SLACK_WEBHOOK_URL env var)
- email     (stub — extend with SMTP/SES as needed)
- webhook   (generic HTTP POST)
- log       (always enabled; useful for dev/testing)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import structlog

log = structlog.get_logger(__name__)


async def dispatch(item: dict, matched_rules: list[dict], session: aiohttp.ClientSession) -> None:
    """Fire alerts for *item* to channels specified in each matched rule."""
    if not matched_rules:
        return

    # Always log
    log.info(
        "alert_fired",
        rules=[r.get("name") for r in matched_rules],
        title=item.get("title", "")[:120],
        url=item.get("canonical_url"),
        tickers=item.get("tickers"),
        published=str(item.get("published_time_utc")),
    )

    notify_channels: set[str] = set()
    for rule in matched_rules:
        notify_channels.update(rule.get("notify", []))

    payload = _build_payload(item, matched_rules)

    for channel in notify_channels:
        if channel == "slack":
            await _send_slack(session, payload)
        elif channel == "email":
            _send_email_stub(payload)
        elif channel == "webhook":
            webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
            if webhook_url:
                await _send_webhook(session, webhook_url, payload)


def _build_payload(item: dict, rules: list[dict]) -> dict:
    return {
        "alert_time_utc": datetime.now(timezone.utc).isoformat(),
        "rules": [r.get("name") for r in rules],
        "title": item.get("title"),
        "url": item.get("canonical_url"),
        "source": item.get("canonical_source"),
        "published": str(item.get("published_time_utc")),
        "tickers": item.get("tickers", []),
        "topics": item.get("topics", []),
        "summary": item.get("summary", "")[:300] if item.get("summary") else None,
    }


async def _send_slack(session: aiohttp.ClientSession, payload: dict) -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        log.debug("slack_webhook_not_configured")
        return

    tickers_str = ", ".join(payload.get("tickers", [])) or "—"
    text = (
        f"*{payload['title']}*\n"
        f"Source: `{payload['source']}`  |  Tickers: `{tickers_str}`\n"
        f"Rules: {', '.join(payload['rules'])}\n"
        f"<{payload['url']}|Read article>"
    )
    body = json.dumps({"text": text}).encode()
    try:
        async with session.post(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                log.warning("slack_send_failed", status=resp.status)
    except Exception as exc:
        log.warning("slack_send_error", error=str(exc))


def _send_email_stub(payload: dict) -> None:
    """Stub — extend with smtplib / boto3 SES / etc."""
    email = os.environ.get("ALERT_EMAIL")
    if email:
        log.info("email_alert_stub", to=email, title=payload.get("title"))


async def _send_webhook(
    session: aiohttp.ClientSession, url: str, payload: dict
) -> None:
    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status not in (200, 201, 202, 204):
                log.warning("webhook_send_failed", status=resp.status, url=url)
    except Exception as exc:
        log.warning("webhook_send_error", error=str(exc))
