#!/usr/bin/env python3
"""
Compatibility wrapper for the legacy entry point.

Use `python crawl_news.py` for the generic multi-ticker crawler.
"""

from crawl_news import *  # noqa: F401,F403


if __name__ == "__main__":
    import asyncio

    from crawl_news import main

    asyncio.run(main())
