"""
Unit tests: compliance modules
- CAPTCHA detection + stop-and-escalate
- Policy flags (no_redistribution, no_full_text)
- robots.txt policy
"""

from __future__ import annotations

import pytest

from crawler.compliance import captcha, policy
from crawler.compliance.captcha import (
    detect,
    handle_detection,
    is_halted,
    clear_halt,
    CaptchaDetected,
    _halted_domains,
)


# ── captcha.py ────────────────────────────────────────────────────────────────

def setup_function():
    _halted_domains.clear()


def test_detect_403_always_true():
    assert detect("Normal page content", 403) is True


def test_detect_cloudflare_challenge():
    html = "<html><body>Checking your browser before accessing the site.</body></html>"
    assert detect(html, 200) is True


def test_detect_captcha_keyword():
    html = "<html><title>CAPTCHA required</title></html>"
    assert detect(html, 200) is True


def test_detect_normal_page():
    html = "<html><body><h1>Latest Financial News</h1></body></html>"
    assert detect(html, 200) is False


def test_handle_detection_raises_and_halts():
    url = "https://example.com/news"
    with pytest.raises(CaptchaDetected) as exc_info:
        handle_detection(url, "<html>just a moment</html>", 200)

    assert exc_info.value.domain == "example.com"
    assert is_halted(url)


def test_is_halted_false_before_detection():
    assert not is_halted("https://clean-site.com/news")


def test_clear_halt_removes_domain():
    url = "https://example.com/news"
    _halted_domains["example.com"] = {"domain": "example.com"}
    assert is_halted(url)
    clear_halt("example.com")
    assert not is_halted(url)


def test_captcha_incident_records_sample_hash():
    url = "https://blocked.com/page"
    try:
        handle_detection(url, "captcha challenge page", 403)
    except CaptchaDetected as exc:
        assert "sample_html_hash" in exc.incident
        assert exc.incident["status_code"] == 403
    finally:
        clear_halt("blocked.com")


# ── policy.py ─────────────────────────────────────────────────────────────────

def test_policy_no_redistribution_flag():
    policy.load_policies([
        {"source_key": "test_rss", "licensing": {"redistribution_allowed": False, "store_full_text": False}}
    ])
    flags = policy.get_compliance_flags("test_rss", has_full_text=False)
    assert "no_redistribution" in flags


def test_policy_no_full_text_flag():
    policy.load_policies([
        {"source_key": "test_rss", "licensing": {"redistribution_allowed": False, "store_full_text": False}}
    ])
    flags = policy.get_compliance_flags("test_rss", has_full_text=True)
    assert "no_full_text" in flags


def test_policy_no_flag_when_licensed():
    policy.load_policies([
        {"source_key": "benzinga_api", "licensing": {"redistribution_allowed": False, "store_full_text": True}}
    ])
    flags = policy.get_compliance_flags("benzinga_api", has_full_text=True)
    assert "no_full_text" not in flags


def test_should_store_body_respects_config():
    policy.load_policies([
        {"source_key": "licensed_src", "licensing": {"store_full_text": True}},
        {"source_key": "unlicensed_src", "licensing": {"store_full_text": False}},
    ])
    assert policy.should_store_body("licensed_src") is True
    assert policy.should_store_body("unlicensed_src") is False


# ── Acceptance test 5: forbidden sources audit ────────────────────────────────

def test_no_forbidden_sources_in_config():
    """
    The example config must NOT contain modules or entries for excluded sources:
    Polygon, SEC, FINRA, Finnhub, Reddit, FRED, yfinance.
    """
    import yaml
    from pathlib import Path

    config_path = Path("config/sources.example.yaml")
    if not config_path.exists():
        pytest.skip("sources.example.yaml not found")

    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    source_keys = [s["source_key"] for s in cfg.get("sources", [])]
    combined = " ".join(source_keys).lower()

    forbidden = ["polygon", "edgar", "finra", "finnhub", "reddit", "fred", "yfinance", "yahoo"]
    for term in forbidden:
        assert term not in combined, (
            f"Forbidden source '{term}' found in sources.example.yaml: {source_keys}"
        )
