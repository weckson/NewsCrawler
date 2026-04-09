from crawl_news import (
    _extract_google_decode_attrs,
    _extract_google_news_base64,
    build_aistock_payload,
    extract_article_text,
    to_news_item,
    to_news_items,
)


def test_to_news_item_maps_current_article_to_newsitem_schema():
    article = {
        "id": "article-1",
        "title": "AMD wins major AI server deal",
        "url": "https://example.com/amd-deal",
        "published": "2026-03-29T10:30:45+00:00",
        "summary": "AMD secured a new hyperscaler contract.",
        "_source_name": "Reuters",
        "_source_domain": "www.reuters.com",
        "_trust": 3,
        "_relevance": 0.92,
        "_quality_score": 0.85,
        "_channel": "google_broad",
        "_ticker": "AMD",
        "_alt_sources": ["CNBC"],
    }

    item = to_news_item(article)

    assert item["id"] == "article-1"
    assert item["timestamp_utc"] == "2026-03-29T10:30:45+00:00"
    assert item["source"] == "reuters"
    assert item["url"] == "https://example.com/amd-deal"
    assert item["title"] == "AMD wins major AI server deal"
    assert item["body"] == "AMD secured a new hyperscaler contract."
    assert item["author"] is None
    assert item["language"] == "en"
    assert item["ticker"] == "AMD"
    assert item["tickers_hint"] == ["AMD"]
    assert item["source_quality"] == "high"
    assert item["publisher_raw"] == "reuters.com"
    assert item["trust_tier"] == 3
    assert item["body_kind"] == "summary_snippet"
    assert item["ingest_source"] == "newscrawler_local"
    # Meta fields
    assert item["meta"]["source_name"] == "Reuters"
    assert item["meta"]["source_domain"] == "www.reuters.com"
    assert item["meta"]["event_origin"] == "news"
    assert item["meta"]["relevance"] == 0.92
    assert item["meta"]["quality_score"] == 0.85
    assert item["meta"]["_channel"] == "google_broad"
    assert item["meta"]["_alt_sources"] == ["CNBC"]


def test_to_news_items_preserves_array_shape():
    items = to_news_items(
        [
            {
                "id": "article-1",
                "title": "AMD headline",
                "url": "https://example.com/amd",
            }
        ]
    )

    assert isinstance(items, list)
    assert len(items) == 1
    assert items[0]["id"] == "article-1"
    assert items[0]["source"] == "unknown"
    assert items[0]["source_quality"] == "low"
    assert items[0]["ingest_source"] == "newscrawler_local"


def test_to_news_item_prefers_article_body_when_present():
    article = {
        "id": "article-2",
        "title": "AMD detailed story",
        "url": "https://example.com/amd-detailed",
        "summary": "Short summary.",
        "body": "Paragraph one.\n\nParagraph two.",
        "_ticker": "AMD",
    }

    item = to_news_item(article)

    assert item["body"] == "Paragraph one.\n\nParagraph two."
    assert item["body_kind"] == "article_text"


def test_extract_article_text_reads_paragraphs_from_article_html():
    html = """
    <html>
      <body>
        <article class="story-body">
          <p>Advanced Micro Devices announced a major expansion with a hyperscale partner that will deploy new GPU clusters across multiple regions.</p>
          <p>The agreement includes long-term supply commitments, updated capacity planning, and new software optimization work.</p>
        </article>
      </body>
    </html>
    """

    body = extract_article_text(html)

    assert "Advanced Micro Devices announced a major expansion" in body
    assert "long-term supply commitments" in body


def test_extract_google_news_base64_and_decode_attrs():
    url = "https://news.google.com/rss/articles/CBMiabc123?oc=5"
    html = '<div data-n-a-sg="sig-value" data-n-a-ts="1743379200"></div>'

    assert _extract_google_news_base64(url) == "CBMiabc123"
    assert _extract_google_decode_attrs(html) == ("sig-value", "1743379200")


def test_build_aistock_payload_keeps_soft_news_stream():
    news_items = [{"id": "n1", "source": "reuters", "title": "Test"}]

    payload = build_aistock_payload(news_items)

    assert payload["hard_event_news"] == []
    assert payload["soft_event_news"] == news_items
    assert payload["alt_sentiment_news"] == []
    assert payload["news"] == news_items
