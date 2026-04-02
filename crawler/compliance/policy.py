"""
Per-source licensing and redistribution policy engine.

Reads policy from sources.yaml and enforces flags like:
- store_full_text
- redistribution_allowed
- terms_policy_version
"""

from __future__ import annotations

from typing import Optional

import structlog

log = structlog.get_logger(__name__)

# Loaded at startup from sources config
_policies: dict[str, dict] = {}


def load_policies(sources: list[dict]) -> None:
    """Populate the in-memory policy registry from parsed sources.yaml."""
    global _policies
    _policies = {}
    for src in sources:
        key = src["source_key"]
        _policies[key] = src.get("licensing", {})
    log.info("policies_loaded", count=len(_policies))


def get_compliance_flags(source_key: str, has_full_text: bool = False) -> list[str]:
    """
    Return a list of compliance flags for a given source + content combination.

    Flags:
      no_redistribution   — content must not be redistributed
      no_full_text        — full text must not be stored
    """
    policy = _policies.get(source_key, {})
    flags: list[str] = []

    if not policy.get("redistribution_allowed", False):
        flags.append("no_redistribution")

    if has_full_text and not policy.get("store_full_text", False):
        flags.append("no_full_text")

    return flags


def should_store_body(source_key: str) -> bool:
    """True if the source's licensing allows storing full article body."""
    return _policies.get(source_key, {}).get("store_full_text", False)


def get_rights_metadata(source_key: str) -> dict:
    policy = _policies.get(source_key, {})
    return {
        "source_key": source_key,
        "store_full_text": policy.get("store_full_text", False),
        "redistribution_allowed": policy.get("redistribution_allowed", False),
    }


def get_terms_version(source_key: str) -> Optional[str]:
    return _policies.get(source_key, {}).get("terms_version")
