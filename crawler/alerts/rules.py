"""
Alert rule evaluation engine.

Evaluates each normalised news item against configured alert rules and
returns matched rule names. Dedup-aware: only fires once per dedupe cluster.
"""

from __future__ import annotations

import re
import uuid
from typing import Optional

import structlog

log = structlog.get_logger(__name__)

# Loaded from sources.yaml alert_rules section
_rules: list[dict] = []

# Tracks clusters already alerted (in-memory; use Redis for multi-process)
_alerted_clusters: set[uuid.UUID] = set()


def load_rules(rules_config: list[dict]) -> None:
    global _rules
    _rules = rules_config or []
    log.info("alert_rules_loaded", count=len(_rules))


def evaluate(item: dict) -> list[dict]:
    """
    Evaluate *item* against all configured rules.

    Returns list of matched rule dicts (name + notify channels).
    Skips items whose dedupe_cluster_id has already been alerted.
    """
    cluster_id = item.get("dedupe_cluster_id")
    if cluster_id and cluster_id in _alerted_clusters:
        log.debug("alert_cluster_deduped", cluster_id=str(cluster_id))
        return []

    matched = []
    for rule in _rules:
        if _matches(item, rule):
            matched.append(rule)
            log.info(
                "alert_match",
                rule=rule.get("name"),
                title=item.get("title", "")[:80],
                url=item.get("canonical_url"),
            )

    if matched and cluster_id:
        _alerted_clusters.add(cluster_id)

    return matched


def _matches(item: dict, rule: dict) -> bool:
    """Return True if *item* satisfies any condition in *rule*."""
    # Keyword match against title + summary
    keywords = rule.get("keywords", [])
    if keywords:
        text = " ".join(filter(None, [item.get("title"), item.get("summary")])).lower()
        if any(k.lower() in text for k in keywords):
            return True

    # Ticker match
    rule_tickers = {t.upper() for t in rule.get("tickers", [])}
    item_tickers = {t.upper() for t in item.get("tickers", [])}
    if rule_tickers and rule_tickers & item_tickers:
        return True

    # Source match
    rule_sources = set(rule.get("sources", []))
    if rule_sources and item.get("canonical_source") in rule_sources:
        return True

    # Topic match
    rule_topics = {t.lower() for t in rule.get("topics", [])}
    item_topics = {t.lower() for t in item.get("topics", [])}
    if rule_topics and rule_topics & item_topics:
        return True

    return False
