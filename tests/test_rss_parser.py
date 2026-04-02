"""
Unit tests: RSS/Atom parser
- Correct field extraction (GUID, link, published date, content)
- URL canonicalisation (tracking param removal)
- Near-empty / malformed feed handling
"""

from pathlib import Path
from datetime import timezone

import pytest

from crawler.parsers.rss import parse, _canonical_url, _clean_text, _infer_content_type

FIXTURE = Path(__file__).parent / "fixtures" / "prnewswire.rss"


@pytest.fixture
def rss_bytes():
    return FIXTURE.read_bytes()


def test_parse_count(rss_bytes):
    items = parse(rss_bytes, "prnewswire_rss_financial", "https://www.prnewswire.com/rss")
    assert len(items) == 2


def test_parse_fields(rss_bytes):
    items = parse(rss_bytes, "prnewswire_rss_financial", "https://www.prnewswire.com/rss")
    item = items[0]
    assert "Acme Corp" in item["title"]
    assert item["canonical_url"].startswith("https://")
    assert item["published_time_utc"] is not None
    assert item["published_time_utc"].tzinfo == timezone.utc
    assert item["content_type"] == "press_release"
    assert item["fingerprint_v1"] is not None


def test_parse_strips_tracking_params(rss_bytes):
    items = parse(rss_bytes, "prnewswire_rss_financial", "https://www.prnewswire.com/rss")
    # Second item has utm_source and utm_medium in its URL
    item = items[1]
    assert "utm_source" not in item["canonical_url"]
    assert "utm_medium" not in item["canonical_url"]


def test_parse_no_body_when_not_licensed(rss_bytes):
    items = parse(rss_bytes, "prnewswire_rss_financial", "https://x", store_body=False)
    for item in items:
        assert item["body"] is None


def test_parse_empty_feed():
    empty_rss = b"""<?xml version="1.0"?><rss version="2.0"><channel><title>Empty</title></channel></rss>"""
    items = parse(empty_rss, "test_source", "https://example.com/rss")
    assert items == []


def test_parse_malformed_feed():
    items = parse(b"NOT XML AT ALL >>>", "test_source", "https://example.com/rss")
    # Should not raise; returns empty list or best-effort items
    assert isinstance(items, list)


def test_canonical_url_removes_fragment():
    url = _canonical_url("https://example.com/news/123?utm_source=rss#section")
    assert "#section" not in url
    assert "utm_source" not in url


def test_canonical_url_preserves_path():
    url = _canonical_url("https://example.com/news/article-title-123")
    assert "article-title-123" in url


def test_clean_text_strips_html():
    text = _clean_text("<p>Hello <strong>World</strong></p>")
    assert "<" not in text
    assert "Hello World" in text


def test_infer_content_type_press_release():
    assert _infer_content_type("prnewswire_rss", "Company Announces New CEO") == "press_release"


def test_infer_content_type_regulatory():
    assert _infer_content_type("asic_newsroom", "ASIC bans broker") == "regulatory"


def test_infer_content_type_default():
    assert _infer_content_type("some_api", "Markets rise on positive data") == "news"
