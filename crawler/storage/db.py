"""
Database facade — auto-selects SQLite (local) or PostgreSQL based on DATABASE_URL.

If DATABASE_URL is empty or starts with "sqlite", uses the built-in SQLite backend.
Otherwise, uses the PostgreSQL backend (requires psycopg).
"""

import os

_db_url = os.environ.get("DATABASE_URL", "")

if _db_url and not _db_url.startswith("sqlite"):
    from crawler.storage.postgres import (  # noqa: F401
        insert_raw_document,
        insert_raw_item,
        upsert_news_item,
        get_crawl_state,
        set_crawl_state,
        record_crawl_success,
        record_crawl_failure,
    )
else:
    from crawler.storage.sqlite_db import (  # noqa: F401
        insert_raw_document,
        insert_raw_item,
        upsert_news_item,
        get_crawl_state,
        set_crawl_state,
        record_crawl_success,
        record_crawl_failure,
    )
