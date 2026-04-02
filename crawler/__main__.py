"""
Entry point: python -m crawler [--once] [--source KEY]

Automatically loads .env before starting the scheduler.
"""

from dotenv import load_dotenv
load_dotenv()  # noqa: E402 — must run before any other import reads env vars

from crawler.scheduler import cli  # noqa: E402

if __name__ == "__main__":
    cli()
else:
    # `python -m crawler` calls this module as __main__
    cli()
