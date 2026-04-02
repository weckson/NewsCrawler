"""
CAPTCHA detection and stop-and-escalate policy.

Policy (from design doc):
  1. Stop automated fetching for the domain immediately.
  2. Record a detection incident.
  3. Escalate to a human operator — NO solving, NO proxy rotation.

Detection heuristics are conservative; false positives are preferred over
attempting to bypass challenges (high legal/contractual risk).
"""

from __future__ import annotations

import hashlib
import time
from urllib.parse import urlparse

import structlog

log = structlog.get_logger(__name__)

# Domains currently halted due to CAPTCHA detection
_halted_domains: dict[str, dict] = {}

_CAPTCHA_SIGNALS = [
    "captcha",
    "challenge",
    "are you a human",
    "verify you are human",
    "cloudflare",
    "just a moment",
    "access denied",
    "bot detection",
    "ddos-guard",
    "checking your browser",
]


def detect(response_text: str, status_code: int) -> bool:
    """
    Return True if the response looks like a CAPTCHA / bot challenge.
    Only checks text responses; never attempts to solve.
    """
    if status_code == 403:
        return True
    lower = response_text.lower()
    return any(signal in lower for signal in _CAPTCHA_SIGNALS)


def handle_detection(url: str, response_text: str, status_code: int) -> None:
    """
    Record the incident and halt the domain.
    Raises CaptchaDetected so the fetcher can abort cleanly.
    """
    domain = urlparse(url).netloc
    sample_hash = hashlib.sha256(response_text[:2048].encode()).hexdigest()
    incident = {
        "domain": domain,
        "url": url,
        "status_code": status_code,
        "sample_html_hash": sample_hash,
        "detected_at": time.time(),
    }
    _halted_domains[domain] = incident
    log.error(
        "captcha_detected",
        domain=domain,
        url=url,
        status=status_code,
        sample_hash=sample_hash,
        action="domain_halted_escalate_to_operator",
    )
    raise CaptchaDetected(domain, incident)


def is_halted(url: str) -> bool:
    """Return True if the domain has been halted due to a prior CAPTCHA incident."""
    domain = urlparse(url).netloc
    return domain in _halted_domains


def get_incidents() -> list[dict]:
    return list(_halted_domains.values())


def clear_halt(domain: str) -> None:
    """Called by a human operator once the situation has been resolved."""
    _halted_domains.pop(domain, None)
    log.info("captcha_halt_cleared", domain=domain)


class CaptchaDetected(Exception):
    def __init__(self, domain: str, incident: dict):
        self.domain = domain
        self.incident = incident
        super().__init__(f"CAPTCHA detected on {domain} — domain halted, operator escalation required")
