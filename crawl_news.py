#!/usr/bin/env python3
"""
Financial News Crawler — quality-filtered, no API key.

Benzinga coverage strategy (3 channels, 0 API key):
  ① Benzinga RSS   /feed  + /news/feed — official RSS, 10-15 articles, no CF block
  ② Google->BZ     site:benzinga.com   — Google News RSS filtered to Benzinga only, ~100 articles
  ③ Google broad   AMD stock news      — all sources, quality-scored

Anti-scrape approach:
  - NO direct Benzinga HTML scraping (blocked by Cloudflare challenge)
  - RSS feeds = structured data, no JS rendering needed, no CF challenge
  - Google News RSS = public RSS, no auth, no rate limit issues
  - Polite User-Agent, 2s delay between requests, conditional GET

Quality pipeline:
  Raw articles -> Relevance filter -> Source trust tier -> Junk title filter ->
  Blocked domain filter -> Time recency -> Near-dup dedup (SimHash) -> Sort by quality

Usage:
    python crawl_news.py
    python crawl_news.py --ticker NVDA
    python crawl_news.py --hours 24
"""

import argparse
import asyncio
import hashlib
import html as html_lib
import json
import logging
import os
import re
import sqlite3
import struct
import sys
import uuid
from collections import defaultdict
from contextlib import redirect_stdout
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path
from time import mktime
from urllib.parse import urlparse, quote, quote_plus

import aiohttp
import feedparser
import structlog

try:
    import trafilatura
    _TRAFILATURA_AVAILABLE = True
except ImportError:
    trafilatura = None  # type: ignore
    _TRAFILATURA_AVAILABLE = False

try:
    from rapidfuzz import fuzz as _rf_fuzz
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _rf_fuzz = None  # type: ignore
    _RAPIDFUZZ_AVAILABLE = False

# Load .env early so FINNHUB_API_KEY (and other secrets) are available before
# crawl_watchlist reads os.environ.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass

# ── Error-event JSONL sink ───────────────────────────────────────────────
# Every WARNING / ERROR / CRITICAL structlog event is appended as a JSON line
# to the current run's errors.jsonl (set by setup_run_logging). This gives
# a structured, grep-able audit trail that is much easier to scan than
# scrolling through the full run.log.

_ERROR_JSONL_PATH: Path | None = None
# Levels worth preserving for audit
_ERROR_JSONL_LEVELS = {"warning", "error", "critical", "exception"}


def _error_jsonl_processor(_logger, _method_name, event_dict):
    """structlog processor — sideloads WARNING+ events to errors.jsonl.

    Must be ordered AFTER `add_log_level` so `event_dict["level"]` is set.
    Returns event_dict unchanged so the normal ConsoleRenderer downstream
    still gets its input.
    """
    level = event_dict.get("level")
    if level not in _ERROR_JSONL_LEVELS or _ERROR_JSONL_PATH is None:
        return event_dict
    try:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "event": event_dict.get("event"),
            **{k: v for k, v in event_dict.items()
               if k not in ("level", "timestamp", "event")},
        }
        with open(_ERROR_JSONL_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, default=str, ensure_ascii=False) + "\n")
    except Exception:
        # Never let audit logging crash the run
        pass
    return event_dict


structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        _error_jsonl_processor,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)
log = structlog.get_logger(__name__)
_RUN_FILE_HANDLER: logging.Handler | None = None

# Route all log output to stderr so that JSON stdout output stays clean
# for piping / redirection. This handler is set up once at module load.
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setLevel(logging.DEBUG)
_stderr_handler.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_stderr_handler)
logging.getLogger().setLevel(logging.INFO)

# ── Config ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent


def _resolve_runtime_path(value: str | Path, *, relative_to: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = relative_to / path
    return path.resolve()


def build_runtime_paths(*, data_dir: str | Path | None = None) -> dict[str, Path]:
    data_root_input = data_dir or os.environ.get("NEWSCRAWLER_DATA_DIR", "data")
    data_root = _resolve_runtime_path(data_root_input, relative_to=PROJECT_ROOT)

    db_value = os.environ.get("NEWSCRAWLER_DB_PATH")
    legacy_db_value = os.environ.get("NEWSCRAWLER_LEGACY_DB_PATH")
    raw_dir_value = os.environ.get("NEWSCRAWLER_RAW_DIR")
    runs_dir_value = os.environ.get("NEWSCRAWLER_RUNS_DIR")
    aistock_export_value = os.environ.get("NEWSCRAWLER_AISTOCK_EXPORT_DIR")
    ticker_exports_value = os.environ.get("NEWSCRAWLER_TICKER_EXPORTS_DIR")

    db_path = _resolve_runtime_path(db_value, relative_to=data_root) if db_value else data_root / "news.db"
    legacy_db_path = (
        _resolve_runtime_path(legacy_db_value, relative_to=data_root)
        if legacy_db_value
        else data_root / "amd_news.db"
    )
    raw_dir = _resolve_runtime_path(raw_dir_value, relative_to=data_root) if raw_dir_value else data_root / "raw"
    runs_dir = _resolve_runtime_path(runs_dir_value, relative_to=data_root) if runs_dir_value else data_root / "runs"
    aistock_export_dir = (
        _resolve_runtime_path(aistock_export_value, relative_to=data_root)
        if aistock_export_value
        else data_root / "aistock"
    )
    ticker_exports_dir = (
        _resolve_runtime_path(ticker_exports_value, relative_to=aistock_export_dir)
        if ticker_exports_value
        else aistock_export_dir / "by_ticker"
    )

    return {
        "DATA_DIR": data_root,
        "DB_PATH": db_path,
        "LEGACY_DB_PATH": legacy_db_path,
        "RAW_DIR": raw_dir,
        "RUNS_DIR": runs_dir,
        "AISTOCK_EXPORT_DIR": aistock_export_dir,
        "TICKER_EXPORTS_DIR": ticker_exports_dir,
    }


def configure_runtime_paths(*, data_dir: str | Path | None = None) -> dict[str, Path]:
    paths = build_runtime_paths(data_dir=data_dir)
    globals().update(paths)
    return paths


configure_runtime_paths()
DEFAULT_TICKER = "AMD"
DEFAULT_HOURS = 48
DEFAULT_TICKER_SET = "aistock"  # reads from sibling AIStock repo; falls back to aistock500
ARTICLE_FETCH_CONCURRENCY = 6
ARTICLE_MAX_BODY_CHARS = 20000
DEFAULT_FULLTEXT_MODE = "high-value"
DEFAULT_FULLTEXT_MAX_ARTICLES = 20

# ── Concurrency & rate-limit tuning ──────────────────────────────────────────
TICKER_CONCURRENCY = 5          # tickers fetched in parallel
DOMAIN_DELAY: dict[str, float] = {
    # Per-request floor between consecutive fetches to the same domain. Google
    # News tolerates ~1.67 req/s on a single lock; the semaphore below lets us
    # issue up to DOMAIN_CONCURRENCY parallel fetches within that floor.
    "news.google.com": 0.6,
    "finance.yahoo.com": 0.5,
    "www.nasdaq.com": 1.5,
    # SEC fair-access policy caps at 10 req/s; 0.15s floor keeps us well under
    # even with concurrent slots (we make ~5 EDGAR requests per run anyway).
    "www.sec.gov": 0.15,
    "default": 1.0,
}

# How many concurrent in-flight fetches a domain allows. For per-domain rate
# limiters that serialize at 0.6s between STARTS, a concurrency of 3 gives
# ~3× effective throughput while each slot still respects its 0.6s floor.
# Google News in particular can sustain 3 parallel streams without 503s.
DOMAIN_CONCURRENCY: dict[str, int] = {
    "news.google.com": 3,
    "finance.yahoo.com": 2,
    "www.nasdaq.com": 1,
    "default": 1,
}


class _DomainRateLimiter:
    """Per-domain rate limiter with per-domain concurrency.

    Each domain has:
      - A semaphore with DOMAIN_CONCURRENCY[dom] slots
      - A per-slot lock enforcing DOMAIN_DELAY[dom] between consecutive starts

    With 3 slots and 0.6s delay, Google News can sustain ~5 req/s
    (3 × 1.67 req/s/slot) vs 1.67 req/s on a single lock. Saves ~65%
    of total wall-clock time on a 500-ticker run.
    """

    def __init__(self) -> None:
        self._slot_last: dict[str, list[float]] = {}   # per-domain, per-slot last timestamp
        self._semas: dict[str, asyncio.Semaphore] = {}
        self._slot_locks: dict[str, asyncio.Lock] = {}  # guards index selection

    def _sema_for(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._semas:
            n = DOMAIN_CONCURRENCY.get(domain, DOMAIN_CONCURRENCY["default"])
            self._semas[domain] = asyncio.Semaphore(n)
            self._slot_last[domain] = [0.0] * n
            self._slot_locks[domain] = asyncio.Lock()
        return self._semas[domain]

    async def wait(self, url: str) -> None:
        from urllib.parse import urlparse as _urlparse
        import time as _time
        domain = _urlparse(url).netloc
        delay = DOMAIN_DELAY.get(domain, DOMAIN_DELAY["default"])
        sema = self._sema_for(domain)
        await sema.acquire()
        try:
            # Pick the least-recently-used slot
            async with self._slot_locks[domain]:
                slot_times = self._slot_last[domain]
                slot = min(range(len(slot_times)), key=lambda i: slot_times[i])
                now = _time.monotonic()
                elapsed = now - slot_times[slot]
                if elapsed < delay:
                    slot_times[slot] = now + (delay - elapsed)
                    sleep_for = delay - elapsed
                else:
                    slot_times[slot] = now
                    sleep_for = 0
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        finally:
            # Release sema immediately — our timestamp-based serialization
            # continues to enforce min delay per slot even without holding sema.
            sema.release()


_rate_limiter = _DomainRateLimiter()

SEMICONDUCTOR_TICKERS = [
    "AMD", "NVDA", "INTC", "AVGO", "TSM",
    "QCOM", "MU", "MRVL", "ARM", "ADI",
    "NXPI", "ON", "MPWR", "MCHP", "ASML",
    "AMAT", "LRCX", "KLAC", "TER", "TXN",
]

AISTOCK500_TICKERS = [
    "NVDA", "AVGO", "MU", "AMD", "AMAT", "LRCX", "KLAC", "TXN", "ADI", "ANET",
    "QCOM", "WDC", "SNDK", "STX", "CDNS", "SNPS", "NXPI", "MPWR", "TER", "MCHP",
    "ON", "AAPL", "BRK.B", "WMT", "JPM", "JNJ", "COST", "ABBV", "NFLX", "PG",
    "HD", "BAC", "KO", "CSCO", "MRK", "PM", "UNH", "MS", "GS", "WFC",
    "MCD", "TMUS", "LIN", "PEP", "INTC", "VZ", "AMGN", "ABT", "T", "C",
    "DIS", "GILD", "APH", "TJX", "SCHW", "PFE", "HON", "LOW", "WELL", "NEM",
    "CB", "PLD", "GLW", "ACN", "BMY", "MDT", "PGR", "COF", "MCK", "HCA",
    "MO", "SBUX", "CMCSA", "CVS", "UPS", "EQIX", "NKE", "WMB", "FDX", "MRSH",
    "SHW", "AMT", "BX", "JCI", "MMM", "ECL", "ADP", "PNC", "USB", "EMR",
    "ITW", "MNST", "CRH", "CL", "ORLY", "MDLZ", "CI", "TDG", "COR", "AON",
    "GM", "ELV", "WBD", "TEL", "TRV", "ROST", "SPG", "SRE", "TFC", "AZO",
    "O", "APD", "DLR", "AJG", "AFL", "F", "ALL", "ZTS", "AME", "GWW",
    "CAH", "PSA", "CTVA", "CARR", "KEYS", "ADSK", "TGT", "BDX", "EA", "EW",
    "CIEN", "GRMN", "HSY", "CVNA", "MET", "YUM", "DHI", "FITB", "SYY", "CBRE",
    "AIG", "KR", "AMP", "DAL", "KDP", "VTR", "EBAY", "ED", "EL", "TTWO",
    "CCI", "HIG", "GEHC", "LVS", "LYV", "RMD", "ROP", "KMB", "CPRT", "IR",
    "KVUE", "TPL", "OTIS", "ACGL", "STT", "WDAY", "DG", "UAL", "A", "PRU",
    "HBAN", "PAYX", "FICO", "ADM", "MTB", "VICI", "IRM", "EXR", "XYL", "TDY",
    "TPR", "WAT", "CTSH", "DTE", "DOV", "ULTA", "IQV", "RJF", "HAL", "CHTR",
    "FE", "ROL", "PPL", "KHC", "WTW", "HPE", "LEN", "BIIB", "JBL", "MTD",
    "PPG", "TSCO", "STZ", "HUBB", "WRB", "DVN", "NTRS", "Q", "OMC", "EXPE",
    "FIS", "PHM", "CFG", "CINF", "DLTR", "EFX", "AVB", "CHD", "STE", "ARES",
    "DRI", "SW", "WSM", "BRO", "LUV", "VLTO", "GIS", "RF", "SYF", "EQR",
    "LH", "DGX", "BG", "CTRA", "IP", "HUM", "TSN", "KEY", "L", "NI",
    "AMCR", "JBHT", "CNC", "DOW", "CHRW", "RL", "LULU", "BR", "SBAC", "GPN",
    "FSLR", "IFF", "ALB", "NVR", "MRNA", "VRSN", "PKG", "PFG", "TROW", "DD",
    "INCY", "SNA", "LII", "NTAP", "ZBH", "EXPD", "SMCI", "EVRG", "MKC", "CSGP",
    "PTC", "LNT", "FTV", "LYB", "WST", "BALL", "WY", "TKO", "HPQ", "VTRS",
    "HOLX", "DECK", "ESS", "GPC", "COO", "NDSN", "PNR", "J", "INVH", "CDW",
    "TRMB", "KIM", "IEX", "MAA", "APTV", "CF", "CLX", "TYL", "AVY", "PSKY",
    "MAS", "REG", "ERIE", "HRL", "HAS", "ALLE", "BEN", "EG", "ALGN", "DPZ",
    "HST", "SWK", "BF.B", "GNRC", "BBY", "UHS", "SOLV", "SJM", "UDR", "AES",
    "DOC", "GDDY", "JKHY", "IVZ", "GL", "BLDR", "TTD", "AIZ", "FOXA", "NCLH",
    "WYNN", "CPT", "IT", "ZBRA", "RVTY", "AOS", "APA", "BAX", "DVA", "HSIC",
    "MGM", "FRT", "ARE", "TECH", "CAG", "TAP", "BXP", "NWSA", "SWKS", "MOS",
    "CRL", "POOL", "FDS", "CPB", "EPAM", "SATS", "MSFT", "AMZN", "GOOGL", "META",
    "ORCL", "PLTR", "IBM", "CRM", "APP", "INTU", "NOW", "ADBE", "DELL", "DDOG",
    "TSLA", "UBER", "BKNG", "MAR", "RCL", "ABNB", "DASH", "HLT", "CMG", "CCL",
    "LLY", "TMO", "ISRG", "DHR", "SYK", "VRTX", "BSX", "REGN", "IDXX", "DXCM",
    "PODD", "XOM", "CVX", "COP", "FCX", "SLB", "KMI", "EOG", "BKR", "VLO",
    "PSX", "MPC", "OXY", "OKE", "TRGP", "FANG", "VMC", "MLM", "NUE", "EQT",
    "STLD", "EXE", "V", "MA", "AXP", "BLK", "SPGI", "CME", "ICE", "MCO",
    "BK", "KKR", "HOOD", "APO", "NDAQ", "COIN", "PYPL", "MSCI", "XYZ", "FISV",
    "IBKR", "CBOE", "CPAY", "GE", "RTX", "BA", "LMT", "HWM", "NOC", "GD",
    "LHX", "AXON", "LDOS", "HII", "TXT", "CAT", "DE", "UNP", "PH", "TT",
    "WM", "CMI", "CTAS", "CSX", "RSG", "NSC", "PCAR", "URI", "FAST", "ROK",
    "WAB", "ODFL", "EME", "GEV", "NEE", "ETN", "CEG", "SO", "DUK", "PWR",
    "AEP", "VST", "D", "EXC", "FIX", "XEL", "ETR", "PEG", "PCG", "NRG",
    "WEC", "AEE", "ATO", "EIX", "ES", "CNP", "AWK", "CMS", "PNW", "LITE",
    "VRT", "COHR", "PANW", "CRWD", "MSI", "FTNT", "VRSK", "FFIV", "AKAM", "GEN",
]

LEGACY_203_TICKERS = AISTOCK500_TICKERS[:203]
AISTOCK200_TICKERS = [ticker for ticker in LEGACY_203_TICKERS if ticker not in {"A", "C", "T"}]

# Backward-compatible alias for earlier naming.
MEGA_WATCHLIST_TICKERS = AISTOCK500_TICKERS

BUILTIN_TICKER_SETS: dict[str, list[str] | None] = {
    "aistock": None,           # dynamically loaded from sibling AIStock repo (see _load_aistock_watchlist)
    "aistock500": AISTOCK500_TICKERS,
    "aistock200": AISTOCK200_TICKERS,
    "core200": AISTOCK200_TICKERS,
    "semis20": SEMICONDUCTOR_TICKERS,
    "mega_watchlist": AISTOCK500_TICKERS,
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

# ── Source trust tiers (3=best, 0=blocked) ────────────────────────────────────

SOURCE_TRUST: dict[str, int] = {
    # Tier 3 — Top-tier financial media
    "benzinga.com": 3, "reuters.com": 3, "bloomberg.com": 3,
    "wsj.com": 3, "cnbc.com": 3, "ft.com": 3,
    "marketwatch.com": 3, "barrons.com": 3, "investopedia.com": 3,
    "fool.com": 3, "seekingalpha.com": 3, "thestreet.com": 3,
    "prnewswire.com": 3, "businesswire.com": 3, "globenewswire.com": 3,
    "sec.gov": 3, "yahoo.com": 3, "finance.yahoo.com": 3,
    "barchart.com": 3, "zacks.com": 3,
    "ir.amd.com": 3,
    # Tier 2 — Good quality
    "investors.com": 2, "nasdaq.com": 2, "morningstar.com": 2,
    "investing.com": 2, "tradingview.com": 2,
    "thefly.com": 2, "invezz.com": 2, "insidermonkey.com": 2,
    "wccftech.com": 2, "tomshardware.com": 2, "anandtech.com": 2,
    "arstechnica.com": 2, "theverge.com": 2, "kiplinger.com": 2,
    "theglobeandmail.com": 2,
    # Tier 2 — Aggregators / trading platforms with editorial curation
    "msn.com": 2,               # Republishes Reuters/AP/CNBC articles
    "aol.com": 2,               # Republishes Motley Fool / Yahoo Finance
    "moomoo.com": 2,            # Trading platform with curated news feed
    "marketscreener.com": 2,    # Financial data / screening platform
    "finbold.com": 2,           # Financial news aggregator
    "mint.com": 2,              # Indian business news (part of HT Media)
    # Tier 1 — Acceptable
    "stockanalysis.com": 1,
}

BLOCKED_DOMAINS = {
    # Truly spammy / low-signal sources — hard-blocked always
    "talkmarkets.com", "stocktitan.net", "stocktitan.com", "accesswire.com",
    "wallstreetzen.com", "smarteranalyst.com", "analystratings.net",
    "tickeron.com", "macroaxis.com", "wisesheets.io",
    "fxleadnews.com", "fxlead.com", "meyka.com", "direxion.com",
    "quiverquant.com", "quiverquantitative.com", "tradingkey.com",
    "simplywall.st",
    # Crypto exchanges (irrelevant for stock news)
    "bitget.com", "coincentral.com", "blockonomi.com", "mexc.com",
    "coingape.com", "cryptonews.com", "beincrypto.com",
    # Non-English / low-value aggregators
    "adhocnews.com", "adhocnewsde.com",
    # Auto-generated press release spam
    "stocktitannet.com", "marketbeatcom.com",
    # Noise wrappers that heavily re-serve other sources without value
    "national-today.com", "stocktwits.com", "fxleaders.com", "intellectiaai.com",
    "chartmill.com",
}

# Noise aggregators — previously in BLOCKED_DOMAINS, now demoted to tier 1.
# For LARGE caps with plenty of tier-2/3 coverage, these get outcompeted by
# the quality_score gate and effectively still filtered out.
# For MID/SMALL caps where these are often the ONLY fresh source, the
# adaptive QUALITY_THRESHOLD_RELAXED (0.40) lets a few through so that
# AIStock still sees *something* rather than nothing.
# Trust=1 → _signal_source_authority = 0.15 (very low weight).
_NOISE_AGGREGATOR_TIER1 = {
    "gurufocus.com", "marketbeat.com", "tipranks.com", "247wallst.com",
    "stockstory.org", "defenseworld.net", "trefis.com", "tradersunion.com",
    "stock-traders-daily.com",
}

JUNK_TITLE_PATTERNS = [
    r"^\d+ stocks? to (?:buy|watch|avoid)",
    r"^best \d+ stocks?",
    r"^top \d+ (?:stocks?|picks?|etfs?)",
    r"insider (?:buying|selling|trading) (?:alert|report)",
    r"^etf\b",
    r"price prediction \d{4}",
    r"(?:should you|is it time to) (?:buy|sell)",
    r"^\$\d+.*to \$\d+",
    r"make .*\d+%.*per (?:day|week)",
    r"intraday.*watch liquidity",
    r"^VTI is ", r"^SPY is ", r"^QQQ is ",
    r"\bshares sold by\b",
    r"\bhas \$[\d\.]+ million holdings in\b",
    r"\braises stock holdings in\b",
    r"\bcompany profile, stock price, news, rankings\b",
    r"\bstock price, news, quote & history\b",
    r"\bquote & history\b",
    r"\bhow investors may respond to\b",
    r"\bthe \$\d+ trillion opportunity\b",
    # Landing/section pages that RSS sometimes surfaces (esp. Bloomberg paywall)
    r"^(?:stocks|bonds|currencies|commodities|markets|rates?\s*&?\s*bonds?)\s*[-–—]\s*[a-z]+\.(?:com|org|net)\s*$",
    r"^(?:american|european|asian|global)\s+stocks?\s*[-–—]\s*[a-z]+\.(?:com|org|net)\s*$",
    # Generic "Latest ... News" titles that are topic/tag aggregation pages
    r"^latest\s+.*\s+news\s*$",
    r"^.*\blatest\s+stock\s+analysis\s*$",
]

WEAK_SIGNAL_PATTERNS = [
    r"\bshares sold by\b",
    r"\bshares purchased by\b",
    r"\bshares acquired by\b",
    r"\bshares in .* purchased by\b",
    r"\bstake in\b",
    r"\bholding history\b",
    r"\bholdings in\b",
    r"\blargest position\b",
    r"\bstock position\b",
    r"\braises stock holdings in\b",
    r"\blowers position in\b",
    r"\breduces position in\b",
    r"\bposition (?:raised|lifted|cut|trimmed|lessened|reduced)\b",
    r"\bacquires \d[\d,]* shares of\b",
    r"\bbuys(?: \d[\d,]*)? shares of\b",
    r"\bpurchases shares of(?: \d[\d,]*)?\b",
    r"\bpurchases \d[\d,]* shares of\b",
    r"\bsells \d[\d,]* shares of\b",
    r"\bcompany profile\b",
    r"\brankings\b",
    r"\bprice prediction\b",
    r"\bhow investors may respond\b",
    r"\bgenerational buying opportunity\b",
]

NEWS_CONTENT_KEYWORDS = [
    "earnings", "revenue", "profit", "loss", "guidance",
    "acquisition", "merger", "deal", "partnership",
    "layoff", "restructur", "downgrade", "upgrade",
    "analyst", "rating", "price target",
    "ceo", "cfo", "appoint", "resign",
    "dividend", "buyback", "share repurchase",
    "lawsuit", "settlement", "patent",
    "product launch", "chip", "gpu", "cpu", "ai ",
    "data center", "server", "instinct", "epyc", "ryzen",
    "mi300", "mi400", "mi450", "rdna", "zen",
    "beat", "miss", "exceed", "surge", "plunge", "rally",
    "quarter", "q1", "q2", "q3", "q4", "annual",
    "why is", "trading lower", "trading higher",
]

# ── Quality scoring system ───────────────────────────────────────────────────
# Composite score modeled on RavenPack / Bloomberg / GDELT evaluation methods.
# 7 orthogonal signals, weighted sum → [0.0, 1.0].  Threshold gates entry.

QUALITY_WEIGHTS = {
    "source_authority":          0.30,
    "ticker_relevance":          0.25,
    "headline_informativeness":  0.15,
    "temporal_freshness":        0.10,
    "content_specificity":       0.10,
    "summary_richness":          0.05,
    "source_diversity":          0.05,
}
QUALITY_THRESHOLD = 0.45       # articles below this are dropped
QUALITY_THRESHOLD_RELAXED = 0.40  # fallback for tickers with < 2 articles

# ── Classifier versioning for PIT-correct backtesting ──────────────────────
# Bump this string whenever event taxonomy / LM lexicon / disambiguation /
# quality scoring is changed in a way that would alter derived signals.
# Older articles in SQLite retain the version under which they were classified;
# backtests can pin to a specific version for reproducible re-runs.
#
# Format: <taxonomy_ver>-<lexicon_ver>-<disambig_ver>
CLASSIFIER_VERSION = "tax_v7-lm_v3-disambig_v2"

CLICKBAIT_PATTERNS = [
    r"you won'?t believe",
    r"this stock will",
    r"one stock to",
    r"secret.{0,20}(invest|stock|buy)",
    r"what (?:no one|nobody) (?:is telling|tells) you",
    r"the next (?:tesla|nvidia|amazon|apple)",
    r"become a millionaire",
    r"get rich",
    r"don'?t miss (?:this|out)",
    r"explosive growth",
    r"retire (?:early|rich)",
    r"\bhidden gem\b",
    r"once.in.a.lifetime",
    r"\d+x (?:return|gain|potential)",
]

# Financial action words that indicate actionable news (used by headline signal)
_ACTION_KEYWORDS = [
    "earnings", "revenue", "profit", "loss", "guidance", "forecast",
    "acquisition", "acquire", "merger", "merge", "deal", "buyout",
    "layoff", "restructur", "downgrade", "upgrade", "outperform", "underperform",
    "analyst", "rating", "price target", "initiate", "reiterate",
    "ceo", "cfo", "appoint", "resign", "hire", "fire", "step down",
    "dividend", "buyback", "share repurchase", "split",
    "lawsuit", "settlement", "patent", "fda", "approval", "reject",
    "beat", "miss", "exceed", "surge", "plunge", "rally", "crash", "soar",
    "q1", "q2", "q3", "q4", "quarter", "annual", "fiscal",
    "ipo", "sec filing", "insider", "bankruptcy", "default",
    "tariff", "sanction", "regulation", "antitrust",
    "contract", "partnership", "joint venture", "spinoff",
    "recall", "investigation", "probe", "subpoena",
]

COMPANY_NAMES = {
    "a": ["a", "agilent", "agilent technologies"],
    "amd": ["amd", "advanced micro devices", "advanced micro"],
    "apd": ["apd", "air products", "air products and chemicals"],
    "all": ["all", "allstate", "allstate corporation"],
    "bdx": ["bdx", "becton dickinson", "becton, dickinson", "becton dickinson and company"],
    "cost": ["cost", "costco", "costco wholesale"],
    "dlr": ["dlr", "digital realty", "digital realty trust"],
    "nvda": ["nvda", "nvidia"],
    "intc": ["intc", "intel"],
    "avgo": ["avgo", "broadcom"],
    "tsm": ["tsm", "tsmc", "taiwan semiconductor"],
    "c": ["c", "citigroup", "citi"],
    "ci": ["ci", "cigna", "the cigna group"],
    "cl": ["cl", "colgate", "colgate-palmolive", "colgate palmolive"],
    "d": ["d", "dominion energy"],
    "dg": ["dg", "dollar general"],
    "doc": ["doc", "healthpeak", "healthpeak properties"],
    "ed": ["ed", "consolidated edison", "con ed"],
    "el": ["el", "estee lauder", "estée lauder"],
    "f": ["f", "ford", "ford motor", "ford motor company"],
    "fe": ["fe", "firstenergy", "first energy"],
    "fast": ["fast", "fastenal"],
    "ir": ["ir", "ingersoll rand"],
    "j": ["j", "jacobs", "jacobs solutions"],
    "key": ["key", "keycorp"],
    "keys": ["keys", "keysight", "keysight technologies"],
    "l": ["l", "loews", "loews corporation"],
    "low": ["low", "lowe's", "lowes", "lowe's companies", "lowes companies"],
    "pm": ["pm", "philip morris", "philip morris international"],
    "ppl": ["ppl", "ppl corporation"],
    "qcom": ["qcom", "qualcomm"],
    "mu": ["mu", "micron"],
    "mrvl": ["mrvl", "marvell"],
    "arm": ["arm", "arm holdings"],
    "adi": ["adi", "analog devices"],
    "nxpi": ["nxpi", "nxp"],
    "on": ["on", "on semiconductor", "onsemi"],
    "mpwr": ["mpwr", "monolithic power"],
    "mchp": ["mchp", "microchip technology", "microchip"],
    "asml": ["asml"],
    "amat": ["amat", "applied materials"],
    "lrcx": ["lrcx", "lam research"],
    "klac": ["klac", "kla"],
    "snps": ["snps", "synopsys"],
    "ter": ["ter", "teradyne"],
    "tgt": ["tgt", "target corporation", "target stores"],
    "tmus": ["tmus", "t-mobile", "t mobile", "t-mobile us", "t mobile us"],
    "txn": ["txn", "texas instruments"],
    "now": ["now", "servicenow", "service now"],
    "o": ["o", "realty income", "realty income corporation"],
    "app": ["app", "applovin", "app lovin"],
    "so": ["so", "southern company"],
    "sw": ["sw", "smurfit westrock", "westrock"],
    "t": ["t", "at&t", "att"],
    "tsla": ["tsla", "tesla"],
    "aapl": ["aapl", "apple"],
    "msft": ["msft", "microsoft"],
    "googl": ["googl", "google", "alphabet"],
    "amzn": ["amzn", "amazon"],
    "meta": ["meta", "facebook"],
    "v": ["v", "visa"],
    # 2026-07-16 watchlist top-up batch
    "as": ["as", "amer sports"],
    "pl": ["pl", "planet labs"],
    "fig": ["fig", "figma"],
    "elf": ["elf", "e.l.f. beauty", "elf beauty"],
    "iot": ["iot", "samsara"],
    "bros": ["bros", "dutch bros"],
    "leu": ["leu", "centrus energy", "centrus"],
    "tem": ["tem", "tempus ai"],
    "zeta": ["zeta", "zeta global"],
    "alk": ["alk", "alaska air group", "alaska airlines", "alaska air"],
    "wing": ["wing", "wingstop"],
    "cart": ["cart", "instacart", "maplebear"],
    "skhy": ["skhy", "sk hynix"],
    "spcx": ["spcx", "spacex", "space exploration technologies"],
    "crwv": ["crwv", "coreweave"],
    "rddt": ["rddt", "reddit"],
    "apld": ["apld", "applied digital"],
    "smtc": ["smtc", "semtech"],
    "ntnx": ["ntnx", "nutanix"],
    "tost": ["tost", "toast inc"],
    "crcl": ["crcl", "circle internet group", "circle internet financial"],
    "glxy": ["glxy", "galaxy digital"],
    "oscr": ["oscr", "oscar health"],
    "ccj": ["ccj", "cameco"],
    "hims": ["hims", "hims & hers", "hims and hers"],
    "avav": ["avav", "aerovironment"],
    "aal": ["aal", "american airlines"],
    "lunr": ["lunr", "intuitive machines"],
    "achr": ["achr", "archer aviation"],
    "joby": ["joby", "joby aviation"],
    "vktx": ["vktx", "viking therapeutics"],
    "nbis": ["nbis", "nebius"],
    "celh": ["celh", "celsius holdings"],
    # 2026-07-16 high-beta curation adds
    "gme": ["gme", "gamestop"],
    "djt": ["djt", "trump media", "truth social"],
    "open": ["open", "opendoor"],
    "qs": ["qs", "quantumscape"],
    "uec": ["uec", "uranium energy corp", "uranium energy"],
    "cls": ["cls", "celestica"],
    "bmnr": ["bmnr", "bitmine immersion", "bitmine"],
}

# Tickers that collide with common English words / names / places — require
# strong contextual markers ($TICKER, (TICKER), NASDAQ:TICKER, or ticker+
# finance keyword) to count as a mention. Otherwise Google News returns false
# positives like "Ben Stokes cricket", "pool accidents", "Lake Erie", etc.
CONTEXT_ONLY_TICKERS = {ticker for ticker in AISTOCK500_TICKERS if len(ticker) <= 2} | {
    # Short dictionary words / pronouns / common verbs
    "ALL", "ANY", "APP", "ARE", "BIG", "BIT", "CAN", "COST", "DOC", "DOW", "FAST",
    "FIT", "FOR", "FUN", "GEN", "GOT", "HAS", "KEY", "LOW", "NEW", "NOT", "NOW",
    "OLD", "ONE", "OUR", "OUT", "PAY", "SEE", "SET", "SO", "SW", "TECH", "TOP",
    "TRY", "TWO", "USE", "WAY", "WHO", "WHY", "YES",
    # Common names / places that are 3-4 letter tickers
    "BEN",   # Franklin Resources — collides with Ben Stokes / Ben Stiller
    "POOL",  # Pool Corp — collides with "pool" (swimming, patent, ABS pool)
    "WOLF",  # Wolfspeed — collides with animals / proper names (Wolf Popper)
    "DASH",  # DoorDash — collides with "dash", "Mad Dash", "dash cam"
    "ERIE",  # Erie Indemnity — collides with Lake Erie, city of Erie
    "PSA",   # Public Storage — collides with "PSA" (public service announcement)
    "COO",   # Cooper Companies — collides with the COO executive title
    "CAVA",  # CAVA Group — ok but "cava" also = Spanish sparkling wine
    "PSKY",  # Paramount Skydance — ok
    "HAS",   # Hasbro — auxiliary verb
    "MRNA",  # Moderna — also means messenger RNA in biology articles
    "ALB",   # Albemarle — also a town name
    "PEP",   # Pepsi — also "pep talk", "PEP 8"
    "LIFE",  # LIFE etf — common word
    "CUP",   # Would be a ticker? — common word
    "AI",    # C3.ai — extremely overloaded word
    "IT",    # Gartner IT — pronoun
    "BIG",   # Big Lots
    "HOG",   # Harley-Davidson — collides with animal "hog"
    "CASH",  # Pathward Financial — common word
    "FAT",   # FAT Brands
    "GOOD",  # Gladstone Commercial — common word
    "HOPE",  # Hope Bancorp — common word
    "LOVE",  # Lovesac — common word
    "PLAY",  # Dave & Buster's — common verb
    "ROAD",  # ARS Pharmaceuticals? — common word
    "SAVE",  # Spirit Airlines — common verb
    "WISH",  # ContextLogic — common word
    "WORK",  # Slack (pre-acquisition) — common word
    "YES",   # YES Network? — common word
    "ZION",  # Zions Bancorp — biblical name
    "LUV",   # Southwest Airlines — "love"
    "SHIP",  # Seanergy — common word
    "FOUR",  # Shift4 — common word
    "FIVE",  # Five Below — common word
    "STAR",  # Istar — common word
    "ROLL",  # SPDR trust — common verb
    "SUPER", # Superior Industries — common word
    "TROW",  # T. Rowe Price — name fragment
    # 2026-06-10 audit batch — live-watchlist word collisions found by scan
    "LITE",  # Lumentum — collides with "Miller Lite", "lite version"
    "MET",   # MetLife — past tense of "meet"
    "BALL",  # Ball Corp — sports ball
    "TAP",   # Molson Coors — "tap into", "tap water", "on tap"
    "MOS",   # Mosaic — "MOS" tech jargon (metal-oxide-semiconductor)
    "SPOT",  # Spotify — "spot price", "sweet spot", "spot on"
    "SNAP",  # Snap Inc — "snap decision", "cold snap", SNAP benefits program
    "SHOP",  # Shopify — "shop", "coffee shop"
    "PATH",  # UiPath — "path", "career path", "warpath"
    "CART",  # Instacart (Maplebear) — "shopping cart", "cart"
    "EXE",   # Expand Energy — ".exe" file extension
    "HOOD",  # Robinhood — "neighborhood", "hood"
    "BILL",  # BILL Holdings — "bill", common male name Bill
    "TXT",   # Textron — ".txt", "text" abbreviation
    "FIX",   # Comfort Systems — verb "fix"
    "NET",   # Cloudflare — "net income", ".net", "internet"
    "RIOT",  # Riot Platforms — civil unrest "riot"
    "HUT",   # Hut 8 — "hut", "Pizza Hut"
    "ATO",   # Atmos Energy — common Latin/Spanish root
    "IR",    # Ingersoll Rand — abbreviation for "infrared" / "Iran"
    "UA",    # Under Armour — "u a"
    "X",     # US Steel — letter
    "A",     # Agilent — letter
    "J",     # Jacobs — letter
    "O",     # Realty Income — letter
    "D",     # Dominion — letter
    "Q",     # Quest Diagnostics — letter
    "T",     # AT&T — letter
    "V",     # Visa — letter
    "U",     # Unity — letter
    "C",     # Citigroup — letter
    "F",     # Ford — letter
    "L",     # Loews — letter
    "K",     # Kellanova — letter
    "M",     # Macy's — letter
    # Financial terms / sector generics
    "REIT",  # REIT ETF
    "GOLD",  # Barrick — also commodity
    "COIN",  # Coinbase — generic "coin"
    "GAME",  # Gamestop — generic "game"
    "LIVE",  # Live Nation — common word
    "NICE",  # Nice Ltd — common word
    "FREE",  # Whole Foods? — common word
    # 2-letter tickers (strict match required)
    "ON",    # ON Semiconductor — preposition collision
    "BA",    # Boeing — country code collision
    "GM",    # GM / genetic modification / good morning
    "AI",    # already
    # 2026-07-16 watchlist top-up batch — new word/acronym collisions
    "FIG",   # Figma — the fruit/word "fig"
    "ELF",   # e.l.f. Beauty — the word "elf"
    "IOT",   # Samsara — "IoT" (Internet of Things) acronym
    "BROS",  # Dutch Bros — the word "bros"
    "LEU",   # Centrus Energy — "Leu" amino acid / Romanian currency
    "TEM",   # Tempus AI — transmission electron microscopy acronym
    "ZETA",  # Zeta Global — greek letter
    "ALK",   # Alaska Air — ALK gene in oncology news
    "WING",  # Wingstop — the word "wing"
    "OPEN",  # Opendoor — the word "open" (2026-07-16 add)
}

def _merge_aistock_ticker_registry() -> None:
    """Merge the sibling AIStock repo's config/ticker_registry.json into
    CONTEXT_ONLY_TICKERS (ambiguous=true) and COMPANY_NAMES (aliases).

    That registry is the SINGLE SOURCE for per-ticker metadata on the AIStock
    side (managed by AIStock's scripts/watchlist_tool.py) — merging here means
    a watchlist addition over there propagates to crawler query terms and
    word-collision handling without editing this file. The hardcoded dicts
    above remain the baseline; registry entries only ADD (never remove).
    """
    registry_path = PROJECT_ROOT.parent / "AIStock" / "config" / "ticker_registry.json"
    if not registry_path.exists():
        return
    try:
        entries = json.loads(registry_path.read_text(encoding="utf-8")).get("tickers", {})
    except Exception:
        return
    for ticker, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        tk_upper = ticker.upper()
        tk_lower = ticker.lower()
        if entry.get("ambiguous"):
            CONTEXT_ONLY_TICKERS.add(tk_upper)
        aliases = entry.get("aliases") or []
        if aliases and tk_lower not in COMPANY_NAMES:
            COMPANY_NAMES[tk_lower] = [tk_lower] + [a.strip().lower() for a in aliases if a.strip()]


_merge_aistock_ticker_registry()


US_PRIMARY_EXCHANGES = {
    "NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM",
    "NYSE", "NYSEARCA", "NYSEAMERICAN",
}

def _load_ir_feeds() -> dict[str, list[dict]]:
    """Load discovered IR RSS feeds from data/ir_feeds.json.

    The file is produced by `python scripts/probe_ir_feeds.py` (one-shot
    discovery; re-run quarterly or after watchlist changes). Each entry
    becomes a per-ticker source channel with tag `ir_feed`.

    IR feeds are PRIMARY sources — the company's own press releases with
    zero media-re-reporting latency. trust=3, authoritative attribution
    (the feed belongs to exactly one company).
    """
    feeds: dict[str, list[dict]] = {
        # Hardcoded fallback — always present even without ir_feeds.json
        "AMD": [
            {
                "name": "AMD IR",
                "type": "rss",
                "tag": "ir_feed",
                "urls": ["https://ir.amd.com/news-events/press-releases/rss"],
            }
        ]
    }
    ir_json = PROJECT_ROOT / "data" / "ir_feeds.json"
    if not ir_json.exists():
        return feeds
    try:
        discovered = json.loads(ir_json.read_text(encoding="utf-8"))
        for ticker, url in discovered.items():
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            t = ticker.upper()
            feeds[t] = [{
                "name": f"{t} IR",
                "type": "rss",
                "tag": "ir_feed",
                "urls": [url],
            }]
        log.info("ir_feeds_loaded", count=len(feeds), path=str(ir_json))
    except Exception as exc:
        log.warning("ir_feeds_load_failed", error=str(exc)[:100])
    return feeds


OFFICIAL_IR_FEEDS = _load_ir_feeds()


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE DEFINITIONS — 3 Benzinga channels + broad coverage
# ══════════════════════════════════════════════════════════════════════════════

_HOT_TICKERS_CACHE: frozenset[str] | None = None


def _get_hot_tickers() -> frozenset[str]:
    """Top-150 tickers that get premium proxy channels (Reuters/Bloomberg/
    CNBC/Investing). Lazily computed + cached (the watchlist loader is defined
    later in the module, so this can't run at import time).

    BUGFIX 2026-06-29: previously a module constant built from the hardcoded
    AISTOCK500_TICKERS, but production runs use the LIVE AIStock watchlist
    (514 tickers, different ordering). That misaligned hot-gating: 28 live
    top-150 names (RKLB, ARM, ASML, ASTS, CRDO, IREN, LUNR, …) — exactly the
    high-momentum tickers most likely to have market-moving news — were
    treated as cold and denied the premium proxies. Now derived from the live
    watchlist, falling back to the hardcoded list only when AIStock config is
    unavailable.
    """
    global _HOT_TICKERS_CACHE
    if _HOT_TICKERS_CACHE is None:
        live = _load_aistock_watchlist()
        base = live if live else AISTOCK500_TICKERS
        _HOT_TICKERS_CACHE = frozenset(base[:150])
    return _HOT_TICKERS_CACHE


def build_sources(ticker: str, *, include_global_feeds: bool = True) -> list[dict]:
    t = ticker.upper()
    query = _build_query_term(t)
    secondary_query = _build_secondary_query_term(t)
    # Hot tickers get dedicated Reuters/Bloomberg/CNBC proxy channels.
    # Cold tickers (beyond top-150) skip these — Reuters/Bloomberg rarely
    # cover them, so 400 tickers × 3 proxy URLs = 1200 Google requests that
    # return near-zero value. Skipping saves ~12 min on a 500-ticker run.
    is_hot = t in _get_hot_tickers()

    # Google News `when:Xd` restricts results to the last N days.
    # Two-tier freshness strategy:
    #   - when:1d batch → catches the freshest 24h articles (28% → target 50%+ ≤24h)
    #   - when:2d batch → fills the 24-48h window for broader coverage
    # One query with both ticker symbol AND company name for best recall in a
    # single Google request (previously 2 queries).
    benzinga_query = query if query == secondary_query else f"{query} OR {secondary_query}"
    benzinga_urls = [
        f"https://news.google.com/rss/search?q={quote_plus(benzinga_query)}+site:benzinga.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]
    # Google → SeekingAlpha (proxy route — SA's own RSS is 98% stale)
    seekingalpha_urls = list(dict.fromkeys([
        f"https://news.google.com/rss/search?q={quote_plus(query)}+site:seekingalpha.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))
    # Google → Reuters (Reuters shut down public RSS; Google News is the only
    # viable route for per-ticker Reuters coverage)
    reuters_urls = list(dict.fromkeys([
        f"https://news.google.com/rss/search?q={quote_plus(query)}+site:reuters.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))
    # Google → Bloomberg (paywalled but Google often surfaces headline + lede)
    bloomberg_urls = list(dict.fromkeys([
        f"https://news.google.com/rss/search?q={quote_plus(query)}+site:bloomberg.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))
    # Google → CNBC (their own RSS is mostly editorial/top-stories, per-ticker
    # is better via proxy)
    cnbc_urls = list(dict.fromkeys([
        f"https://news.google.com/rss/search?q={quote_plus(query)}+site:cnbc.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))
    # Google → Investing.com (tier-2 source; carries M&A/insider/analyst per
    # ticker). Hot-ticker gated like the other site-proxies to limit request
    # budget; standard relevance gate (no per-ticker relaxation) to avoid
    # Google body-match noise.
    investing_urls = list(dict.fromkeys([
        f"https://news.google.com/rss/search?q={quote_plus(query)}+site:investing.com+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))
    # Merged broad queries — each query returns 100 results, so we no longer
    # need 5 variants. Three well-chosen queries cover the same article space
    # with 2/5 the request count, cutting Google rate-limit pressure 40%.
    broad_urls = list(dict.fromkeys([
        # 24h-fresh: highest timeliness priority
        f"https://news.google.com/rss/search?q={quote_plus(query)}+stock+news+when:1d&hl=en-US&gl=US&ceid=US:en",
        # 48h broader window + action keywords (earnings, upgrade, dividend)
        f"https://news.google.com/rss/search?q={quote_plus(query)}+stock+earnings+OR+upgrade+OR+dividend+when:2d&hl=en-US&gl=US&ceid=US:en",
        # Company-name search for tickers with descriptive aliases (only if secondary differs)
        f"https://news.google.com/rss/search?q={quote_plus(secondary_query)}+stock+when:2d&hl=en-US&gl=US&ceid=US:en",
    ]))

    sources = []

    # NOTE: benzinga_rss (/feed, /news/feed) removed — as of 2026-04 it returns
    # "Best X Stocks" listicle pages, not news. BZ coverage comes via google_benzinga.
    # NOTE: Global wire feeds (PR Newswire, GlobeNewswire, MarketWatch, CNBC)
    # removed — 40,000 raw → 9 filtered (0.02% ROI). These general wires almost
    # never mention specific S&P 500 tickers. Saves ~6 min per 500-ticker run.

    sources.extend(
        [
            # ── Channel 1: Google News -> Benzinga only ──
            # Uses Google as a proxy to get Benzinga articles
            # Bypasses Cloudflare entirely (Google already crawled it)
            {
                "name": "Google->Benzinga",
                "type": "rss",
                "tag": "google_benzinga",
                "urls": benzinga_urls,
            },
            # ── Channel 2: Google News -> SeekingAlpha ──
            # SA's own RSS is 98% stale (>48h). Google proxy with when:2d
            # catches only fresh SA articles — much higher ROI.
            {
                "name": "Google->SeekingAlpha",
                "type": "rss",
                "tag": "google_seekingalpha",
                "urls": seekingalpha_urls,
            },
            # ── Channels 2b-2e: Reuters/Bloomberg/CNBC/Investing proxies ──
            # Only for hot tickers (top-150). Cold tickers get near-zero yield
            # from these and eat Google rate-limit budget.
            *([
                {"name": "Google->Reuters", "type": "rss",
                 "tag": "google_reuters", "urls": reuters_urls},
                {"name": "Google->Bloomberg", "type": "rss",
                 "tag": "google_bloomberg", "urls": bloomberg_urls},
                {"name": "Google->CNBC", "type": "rss",
                 "tag": "google_cnbc", "urls": cnbc_urls},
                {"name": "Google->Investing", "type": "rss",
                 "tag": "google_investing", "urls": investing_urls},
            ] if is_hot else []),
            # ── Channel 3: Google News broad (all sources) ──
            # 5 query variants: 2x when:1d (fresh) + 3x when:2d (coverage)
            {
                "name": "Google News (all)",
                "type": "rss",
                "tag": "google_broad",
                "urls": broad_urls,
            },
            # ── Channel 4: Yahoo Finance RSS ──
            # Per-ticker headlines, high trust (tier 3), mostly fresh
            {
                "name": "Yahoo Finance RSS",
                "type": "rss",
                "tag": "yahoo_finance_rss",
                "urls": [
                    f"https://finance.yahoo.com/rss/headline?s={t}",
                ],
            },
            # ── Channel 5: Nasdaq per-ticker RSS ──
            # Curated, highest fulltext rate (63%)
            {
                "name": "Nasdaq RSS",
                "type": "rss",
                "tag": "nasdaq_rss",
                "urls": [
                    f"https://www.nasdaq.com/feed/rssoutbound?symbol={t}",
                ],
            },
        ]
    )

    return [*OFFICIAL_IR_FEEDS.get(t, []), *sources]


def _company_aliases(ticker: str) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for alias in COMPANY_NAMES.get(ticker.lower(), []):
        normalized = alias.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        aliases.append(normalized)
    if ticker.lower() not in seen:
        aliases.insert(0, ticker.lower())
    return aliases


def _build_query_term(ticker: str) -> str:
    aliases = _company_aliases(ticker)
    descriptive_aliases = [
        alias for alias in aliases
        if alias != ticker.lower() and len(re.sub(r"[^a-z0-9]+", "", alias)) > max(2, len(ticker))
    ]
    if descriptive_aliases:
        return f"{ticker} {max(descriptive_aliases, key=len)}"
    return ticker


def _build_secondary_query_term(ticker: str) -> str:
    aliases = _company_aliases(ticker)
    descriptive_aliases = [
        alias for alias in aliases
        if alias != ticker.lower() and len(re.sub(r"[^a-z0-9]+", "", alias)) > max(2, len(ticker))
    ]
    if descriptive_aliases:
        return max(descriptive_aliases, key=len)
    return ticker


def _count_phrase_mentions(text: str, phrase: str) -> int:
    if not text or not phrase:
        return 0
    pattern = rf"(?<![a-z0-9]){re.escape(phrase.lower())}(?![a-z0-9])"
    return len(re.findall(pattern, text.lower()))


def _count_contextual_ticker_mentions(text: str, ticker: str) -> int:
    if not text or not ticker:
        return 0
    patterns = [
        rf"\${re.escape(ticker)}\b",
        rf"\(\s*{re.escape(ticker)}\s*\)",
        rf"\b(?:NYSE|NASDAQ|NYSEARCA|NYSEAMERICAN|NASDAQGS|NASDAQGM)\s*:\s*{re.escape(ticker)}\b",
        (
            rf"\b{re.escape(ticker)}\b(?=\s+(?:stock|shares?|corp(?:oration)?|inc\.?|company|earnings|"
            rf"guidance|forecast|analyst|price target|results?|reports?|news))"
        ),
    ]
    return sum(len(re.findall(pattern, text)) for pattern in patterns)


def _count_ticker_mentions(text: str, ticker: str) -> int:
    aliases = _company_aliases(ticker)
    symbol_count = 0
    # Context-only matching applies to:
    #   1. ALL tickers <= 2 chars (computed dynamically — the static set was
    #      built from the hardcoded AISTOCK500 list and silently missed
    #      short tickers added later to the live AIStock watchlist, e.g. Z/W/S)
    #   2. Explicit CONTEXT_ONLY_TICKERS entries (word/name collisions)
    if len(ticker) <= 2 or ticker.upper() in CONTEXT_ONLY_TICKERS:
        symbol_count = _count_contextual_ticker_mentions(text, ticker.upper())
    else:
        symbol_count = _count_phrase_mentions(text, ticker.lower())
    alias_count = sum(
        _count_phrase_mentions(text, alias)
        for alias in aliases
        if alias != ticker.lower()
    )
    return symbol_count + alias_count


def _looks_like_quote_or_instrument_page(title: str, summary: str, ticker: str) -> bool:
    text = f"{title} {summary}".lower()
    if "quote & history" in text:
        return True
    if re.search(r"\b[A-Z]{1,6}\d{6}[CP]\d{8}\b", f"{title} {summary}"):
        return True
    if re.search(rf"\b{re.escape(ticker.upper())}\b.*\b(?:put|call)\b", f"{title} {summary}", re.IGNORECASE):
        return True
    return False


def _has_conflicting_exchange_reference(title: str, summary: str, ticker: str) -> bool:
    text = f"{title} {summary}"
    pattern = re.compile(r"\b([A-Z]{2,12})\s*:\s*([A-Z][A-Z0-9\.-]{0,9})\b")
    target = ticker.upper()
    for exchange, symbol in pattern.findall(text.upper()):
        if symbol != target:
            continue
        if exchange not in US_PRIMARY_EXCHANGES:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# QUALITY ENGINE
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_NAME_TO_DOMAIN = {
    "benzinga": "benzinga.com", "the motley fool": "fool.com",
    "motley fool": "fool.com", "reuters": "reuters.com",
    "cnbc": "cnbc.com", "bloomberg": "bloomberg.com",
    "marketwatch": "marketwatch.com", "investopedia": "investopedia.com",
    "seeking alpha": "seekingalpha.com", "barron's": "barrons.com",
    "yahoo finance": "finance.yahoo.com", "the street": "thestreet.com",
    "tipranks": "tipranks.com", "barchart": "barchart.com",
    "24/7 wall st.": "247wallst.com", "24/7 wall st": "247wallst.com",
    "zacks": "zacks.com", "zacks investment research": "zacks.com",
    "marketbeat": "marketbeat.com", "invezz": "invezz.com",
    "investing.com": "investing.com", "tradingview": "tradingview.com",
    "tom's hardware": "tomshardware.com", "wccftech": "wccftech.com",
    "the verge": "theverge.com", "ars technica": "arstechnica.com",
    "insider monkey": "insidermonkey.com",
    "the globe and mail": "theglobeandmail.com",
    "fxlead": "fxlead.com", "meyka": "meyka.com",
    "quiver quantitative": "quiverquantitative.com",
    "pr newswire": "prnewswire.com", "business wire": "businesswire.com",
    "nasdaq": "nasdaq.com", "morningstar": "morningstar.com",
    "simply wall st": "simplywall.st", "simplywall.st": "simplywall.st",
    "tradingkey": "tradingkey.com",
    # Yahoo Finance international variants → canonical domain
    "yahoo finance canada": "finance.yahoo.com",
    "yahoo finance uk": "finance.yahoo.com",
    "yahoo finance singapore": "finance.yahoo.com",
    "yahoo finance australia": "finance.yahoo.com",
    "yahoo! finance": "finance.yahoo.com",
    # Other common misses from Google News source names
    "investors business daily": "investors.com",
    "investor's business daily": "investors.com",
    "barchart.com": "barchart.com",
    "thestreet.com": "thestreet.com",
    "the wall street journal": "wsj.com",
    "wall street journal": "wsj.com",
    "financial times": "ft.com",
    "the financial times": "ft.com",
    "stocktitan": "stocktitan.com",
    "stock titan": "stocktitan.com",
    "gurufocus": "gurufocus.com",
    "intellectia ai": "intellectiaai.com",
    "intellectia.ai": "intellectiaai.com",
    "msn": "msn.com",
    "msn money": "msn.com",
    "aol": "aol.com",
    "aol.com": "aol.com",
    "moomoo": "moomoo.com",
    "moomoo.com": "moomoo.com",
    "market screener": "marketscreener.com",
    "marketscreener": "marketscreener.com",
    "marketscreener.com": "marketscreener.com",
    "finbold": "finbold.com",
}

DOMAIN_TO_AISTOCK_SOURCE = {
    "benzinga.com": "benzinga",
    "reuters.com": "reuters",
    "bloomberg.com": "bloomberg",
    "wsj.com": "wsj",
    "cnbc.com": "cnbc",
    "ft.com": "financial_times",
    "marketwatch.com": "marketwatch",
    "barrons.com": "barrons",
    "investopedia.com": "investopedia",
    "seekingalpha.com": "seeking_alpha",
    "fool.com": "motley_fool",
    "thestreet.com": "the_street",
    "prnewswire.com": "prnewswire",
    "businesswire.com": "businesswire",
    "globenewswire.com": "globenewswire",
    "finance.yahoo.com": "yahoo_finance",
    "yahoo.com": "yahoo_finance",
    "barchart.com": "barchart",
    "zacks.com": "zacks",
    "tipranks.com": "tipranks",
    "investors.com": "ibd",
    "nasdaq.com": "nasdaq",
    "morningstar.com": "morningstar",
    "investing.com": "investing.com",
    "tradingview.com": "tradingview",
    "247wallst.com": "247_wall_st",
    "thefly.com": "thefly",
    "insidermonkey.com": "insider_monkey",
    "tomshardware.com": "toms_hardware",
    "anandtech.com": "anandtech",
    "arstechnica.com": "ars_technica",
    "theverge.com": "the_verge",
    "kiplinger.com": "kiplinger",
    "theglobeandmail.com": "the_globe_and_mail",
    "simplywall.st": "simply_wall_st",
    "gurufocus.com": "gurufocus",
    "stockanalysis.com": "stockanalysis",
    "marketbeat.com": "marketbeat",
    "ir.amd.com": "ir",
    "msn.com": "msn",
    "aol.com": "aol",
    "moomoo.com": "moomoo",
    "marketscreener.com": "marketscreener",
    "finbold.com": "finbold",
    "sec.gov": "sec_edgar",
}


def _extract_real_source(title: str, url: str) -> tuple[str, str]:
    """
    Returns (source_name, source_domain).
    For Google News RSS, extracts the real source from title suffix.
    """
    if "news.google.com" in url:
        parts = title.rsplit(" - ", 1)
        if len(parts) == 2:
            name = parts[1].strip()
            domain = SOURCE_NAME_TO_DOMAIN.get(name.lower(), "")
            if not domain:
                domain = re.sub(r"[^a-z0-9]", "", name.lower()) + ".com"
            return name, domain
    domain = urlparse(url).netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain, domain


def get_trust(source_name: str, source_domain: str) -> int:
    for blocked in BLOCKED_DOMAINS:
        if blocked in source_domain:
            return 0
    for aggregator in _NOISE_AGGREGATOR_TIER1:
        if aggregator in source_domain:
            return 1  # noise aggregator — low weight, but not blocked
    for trusted, tier in SOURCE_TRUST.items():
        if trusted in source_domain:
            return tier
    return 1  # unknown = tier 1


def relevance_score(title: str, summary: str, ticker: str) -> float:
    text = f"{title} {summary}"
    text_lower = text.lower()

    mention_count = _count_ticker_mentions(text, ticker)
    if mention_count == 0:
        return 0.0

    score = 0.0

    # Ticker in title = strong signal
    if _count_ticker_mentions(title, ticker) > 0:
        score += 0.4
    else:
        score += 0.1

    # Mention density
    score += min(0.2, mention_count * 0.05)

    # Financial content keywords
    keyword_hits = sum(1 for kw in NEWS_CONTENT_KEYWORDS if kw in text_lower)
    score += min(0.3, keyword_hits * 0.05)

    # Penalize market roundups (many tickers mentioned)
    all_tickers = ["nvda", "intc", "tsla", "aapl", "msft", "googl", "amzn", "meta", "tsm", "avgo", "qcom", "arm"]
    other_tickers = [t for t in all_tickers if t != ticker.lower()]
    other_count = sum(1 for t in other_tickers if _count_phrase_mentions(text, t) > 0)
    if other_count >= 4:
        score -= 0.2

    return max(0.0, min(1.0, score))


def is_junk(title: str) -> bool:
    t = title.lower().strip()
    for pat in JUNK_TITLE_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return True
    return False


def is_weak_signal(title: str, source_domain: str) -> bool:
    t = title.lower().strip()
    if "marketbeat.com" in source_domain and re.search(
        r"\b(holdings in|shares sold by|shares purchased by|shares acquired by|shares in .* purchased by|buys(?: \d[\d,]*)? shares of|purchases shares of(?: \d[\d,]*)?|raises stock holdings in|stake in|largest position|stock position|holding history)\b",
        t,
    ):
        return True
    for pat in WEAK_SIGNAL_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return True
    return False


# ── Quality scoring signals ──────────────────────────────────────────────────

def _signal_source_authority(trust: int) -> float:
    """Map trust tier to [0, 1]. Dominant signal — source reputation.

    Tier 1 mapping tightened from 0.30 → 0.15 after 2026-04-11 analysis showed
    tier-1 aggregators (gurufocus/marketbeat/tipranks/…) were passing the 0.40
    gate with median score 0.455. The tighter mapping + raised threshold
    (QUALITY_THRESHOLD 0.40 → 0.45) drops tier-1 aggregator median below gate.
    """
    return {3: 1.0, 2: 0.7, 1: 0.15, 0: 0.0}.get(trust, 0.15)


def _signal_ticker_relevance(relevance: float) -> float:
    """Pass through existing relevance score (already 0.0-1.0)."""
    return max(0.0, min(1.0, relevance))


def _signal_headline_informativeness(title: str) -> float:
    """Score title quality: action keywords, length, clickbait/listicle penalty."""
    t = title.lower()

    # Action keyword density
    hits = sum(1 for kw in _ACTION_KEYWORDS if kw in t)
    action_score = min(1.0, hits * 0.20)

    # Title length — very short = likely auto-generated
    tlen = len(title)
    if tlen >= 50:
        length_score = 1.0
    elif tlen >= 35:
        length_score = 0.7
    elif tlen >= 25:
        length_score = 0.4
    else:
        length_score = 0.0

    # Clickbait penalty
    clickbait_score = 1.0
    for pat in CLICKBAIT_PATTERNS:
        if re.search(pat, t):
            clickbait_score = 0.0
            break

    # Listicle penalty (softer than is_junk hard filter)
    listicle_score = 1.0
    if re.search(r"^\d+ (?:best|top|stocks?|picks?|ways?)", t):
        listicle_score = 0.0

    return (action_score * 0.50
            + length_score * 0.20
            + clickbait_score * 0.15
            + listicle_score * 0.15)


def _signal_temporal_freshness(published: str | None, now: datetime | None = None) -> float:
    """Continuous decay — newer is better. Not a hard cutoff."""
    if not published:
        return 0.3  # unknown age gets middling score
    try:
        pub = datetime.fromisoformat(published)
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
    except Exception:
        return 0.3
    if now is None:
        now = datetime.now(timezone.utc)
    age_hours = max(0.0, (now - pub).total_seconds() / 3600)
    if age_hours <= 4:
        return 1.0
    if age_hours <= 12:
        return 0.8
    if age_hours <= 24:
        return 0.6
    if age_hours <= 36:
        return 0.4
    if age_hours <= 48:
        return 0.2
    return 0.0


def _signal_content_specificity(title: str, summary: str, ticker: str) -> float:
    """Penalize roundups (many tickers); reward specific entities ($, %, M/B)."""
    text = f"{title} {summary}".lower()

    # Multi-ticker penalty: if many other major tickers appear, likely a roundup
    major_tickers = [
        "nvda", "intc", "tsla", "aapl", "msft", "googl", "amzn", "meta",
        "tsm", "avgo", "qcom", "arm", "amd", "jpm", "bac", "wmt",
    ]
    other_tickers = [t for t in major_tickers if t != ticker.lower()]
    other_count = sum(1 for t in other_tickers if re.search(rf"\b{re.escape(t)}\b", text))
    multi_ticker_score = max(0.0, 1.0 - other_count * 0.15)

    # Named entity density: $amounts, percentages, millions/billions
    entity_patterns = [
        r"\$[\d,.]+\s*(?:million|billion|m\b|b\b|k\b)?",  # $123, $1.2M, $3B
        r"\d+(?:\.\d+)?%",                                   # 15%, 3.2%
        r"\d+(?:\.\d+)?\s*(?:million|billion)",              # 100 million
    ]
    entity_count = sum(len(re.findall(pat, text, re.IGNORECASE)) for pat in entity_patterns)
    entity_score = min(1.0, entity_count * 0.3)

    return multi_ticker_score * 0.50 + entity_score * 0.50


def _signal_summary_richness(summary: str | None, body: str | None) -> float:
    """Reward articles with substantial text content."""
    if body and len(body) > 300:
        return 1.0
    text = summary or ""
    if len(text) >= 100:
        return 0.7
    if len(text) >= 40:
        return 0.4
    return 0.1


def _signal_source_diversity(alt_sources_count: int) -> float:
    """Multi-source confirmation = more likely genuinely newsworthy."""
    if alt_sources_count >= 3:
        return 1.0
    if alt_sources_count == 2:
        return 0.7
    if alt_sources_count == 1:
        return 0.4
    return 0.2


def article_quality_score(article: dict, ticker: str, *, now: datetime | None = None) -> float:
    """Composite quality score [0.0, 1.0] from 7 orthogonal signals.

    Based on evaluation methods used by RavenPack, Bloomberg Terminal, GDELT,
    and Refinitiv News Analytics. No ML training needed — rule-based signals
    combined via weighted sum.
    """
    title = (article.get("title") or "").rsplit(" - ", 1)[0]
    summary = article.get("summary") or ""
    body = article.get("body") or None
    trust = article.get("_trust", 1)
    relevance = article.get("_relevance", 0.0)
    published = article.get("published")
    alt_count = len(article.get("_alt_sources", []))

    signals = {
        "source_authority":          _signal_source_authority(trust),
        "ticker_relevance":          _signal_ticker_relevance(relevance),
        "headline_informativeness":  _signal_headline_informativeness(title),
        "temporal_freshness":        _signal_temporal_freshness(published, now),
        "content_specificity":       _signal_content_specificity(title, summary, ticker),
        "summary_richness":          _signal_summary_richness(summary, body),
        "source_diversity":          _signal_source_diversity(alt_count),
    }

    score = sum(QUALITY_WEIGHTS[k] * v for k, v in signals.items())
    return round(max(0.0, min(1.0, score)), 4)


# ── Event classification taxonomy ────────────────────────────────────────────
# Regex-based event detection. Each pattern is matched against "title + summary".
# Articles can match multiple event types. Stored in meta.event_types for
# AIStock to route by event type instead of just trust tier.
EVENT_TAXONOMY: dict[str, list[str]] = {
    # ─── Earnings ─────────────────────────────────────────────────────────
    "earnings_release": [
        r"\bQ[1-4]\s+(?:earnings|results|revenue|report|sales|numbers)\b",
        r"\breports?\s+Q[1-4]\b",
        r"\b(?:beats?|misses?|surpasses?|tops?|exceeds?|falls?\s+short|crush(?:es|ed)?|smash(?:es|ed)?|trounc(?:es|ed)?|blow(?:s|n)?\s+(?:past|by)|whiff(?:s|ed)?)(?:\s+\w+){0,3}\s+(?:estimates?|expectations?|consensus|forecasts?|views?)\b",
        r"\bearnings?\s+(?:beat|miss|preview|recap|call|result|summary|highlights?|roundup)\b",
        r"\bfiscal\s+Q[1-4]\b",
        r"\b(?:quarterly|annual|full[- ]year)\s+(?:earnings|results|revenue|numbers)\b",
        r"\b(?:top|bottom)[- ]?line\s+(?:beats?|misses?|growth|results?)\b",
        r"\bEPS\s+(?:of|at|beats?|misses?|comes)\b",
        r"\brevenue\s+(?:of|beats?|misses?|comes\s+in|grew|rose|fell)\b",
        r"\bQ[1-4]\s+(?:FY)?(?:20)?\d{2}\b",
        r"\bpre[- ]earnings\b", r"\bpost[- ]earnings\b",
        r"\bwhat\s+to\s+(?:watch|expect|know)\s+(?:for|ahead\s+of)\s+(?:Q[1-4]|earnings)\b",
        r"\banalysts?\s+estimate.*\bto\s+report\b",
        # "Wall Street estimates for key metrics" / "Q1 outlook"
        r"\bwall\s+street\s+estimates?\b",
        r"\bQ[1-4]\s+outlook\b",
    ],
    "earnings_guidance": [
        r"\braises?\s+(?:full[- ]year\s+)?(?:guidance|outlook|forecast|target|estimates?)\b",
        r"\b(?:cuts?|lowers?|trims?|slashes?)\s+(?:guidance|outlook|forecast|target|estimates?)\b",
        r"\bguides?\s+(?:above|below|higher|lower|to)\b",
        r"\bforecasts?\s+(?:Q[1-4]|FY|revenue|earnings|growth)\b",
        r"\bupdates?\s+guidance\b",
        r"\breaffirms?\s+(?:guidance|outlook)\b",
        r"\bmaintains?\s+(?:full[- ]year\s+)?(?:guidance|outlook)\b",
        r"\b(?:issues?|provides?)\s+(?:guidance|outlook)\b",
        # "Chipotle Likely to Maintain Full-Year Comparable Sales Outlook"
        r"\b(?:likely\s+to\s+)?maintain\s+(?:full[- ]year|guidance|outlook|forecast)\b",
    ],
    "analyst_rating": [
        r"\b(?:upgrades?|downgrades?|reiterates?|initiates?|initiated?)\s+(?:to|at|with|coverage|rating)\b",
        r"\b(?:price\s+target|PT|target\s+price)\b",
        r"\banalyst\s+(?:upgrade|downgrade|rating|note|call|action|estimates?|reassess|take)\b",
        r"\banalysts?\s+(?:reassess|bullish|bearish|cautious|positive|negative|optimistic)\b",
        r"\b(?:raises?|lowers?|cuts?|lifts?|boosts?|trims?)\s+(?:price\s+target|PT)\b",
        r"\b(?:buy|sell|hold|overweight|underweight|neutral|outperform|underperform)\s+rating\b",
        r"\bmaintains?\s+(?:buy|sell|hold|outperform|underperform|neutral|overweight|underweight)\b",
        r"\b(?:strong|soft)\s+(?:buy|sell)\b",
        r"\bnew\s+(?:buy|sell|hold)\s+rating\b",
        r"\b(?:bull|bear)\s+(?:case|call|signal)\b",
        # "brokers suggest investing", "Wall Street think", "new Strong Buy Stocks"
        r"\bbrokers?\s+(?:suggest|recommend|think|rate)\b",
        r"\bwall\s+street\s+(?:analysts?|think|rates?)\b",
        r"\b(?:new\s+)?strong\s+(?:buy|sell)\s+stocks?\b",
        r"\bupgrades?/downgrades?\b",
        r"\b(?:lifts?|raises?|hikes?)\s+PT\b",
        r"\btarget\s+cuts?\b",
    ],
    "ma_activity": [
        # 2026-06-10 precision fix: the old `to\s+(?:buy|acquire)` matched
        # retail listicle phrasing everywhere — "Best Stocks to Buy Now",
        # "perfect time to buy Nvidia stock", "to Buy and Hold for 3 Years".
        # Measured 22% agreement vs LLM classification. Now:
        #   - "acquires/acquisition" kept (unambiguous)
        #   - "to acquire" kept (corporate phrasing, retail says "to buy")
        #   - "to buy" requires a deal-context anchor nearby and must NOT be
        #     followed by retail-listicle continuations.
        # 2026-06-29 precision fix #2: bare present-tense "acquires" matched
        # 13F institutional holding reports — "Fund Acquires Shares of X",
        # "Acquires New Position in Y", "acquires 46,059 shares". The trailing
        # \b is REQUIRED: without it "acquires?" backtracks to match "Acquire"
        # and the lookahead (seeing "s Shares") wrongly passes. With \b the s
        # is forced, so the lookahead correctly rejects holding-report phrasing.
        r"\bacquires\b(?!\s+(?:(?:additional\s+|a\s+)?(?:new\s+)?(?:position|stake|shares?)\b|\d[\d,]*\s+shares\b))",
        r"\bacquisitions?\b", r"\bto\s+acquire\b", r"\bacquisition\s+of\b",
        r"\bcompletes?\s+(?:its\s+)?acquisition\b",
        # "to buy" ONLY with an explicit deal-context anchor before it —
        # bare "to buy" is retail-listicle language, not M&A.
        # (Patterns compile with IGNORECASE, so a capital-letter anchor
        # after "to buy" can't be used to detect company names.)
        r"\b(?:in\s+talks?|talks?|deal|agreement|offers?|plans?|set|moves?|agrees?)\s+to\s+buy\b",
        # "buying <Company> in a $N billion deal" — present-progressive M&A
        r"\bbuying\b(?=.{0,45}\b(?:deal|billion|million|\$\d))",
        r"\bmerger\b", r"\btakeover\b", r"\bbid\s+(?:for|to)\b",
        r"\bspin[- ]?offs?\b", r"\bdivest(?:s|ment|iture|ed|ing)?\b",
        r"\b(?:all[- ]stock|all[- ]cash|cash\s+and\s+stock)\s+deal\b",
        r"\b(?:hostile|unsolicited)\s+bid\b",
        r"\bstrategic\s+(?:review|alternatives?)\b",
        r"\b(?:tender|exchange)\s+offer\b",
        r"\b(?:agrees?\s+to|agreement\s+to)\s+(?:acquire|sell|merge)\b",
        # "buyout" is common shorthand
        r"\b(?:buyout|LBO|leveraged\s+buyout)\b",
        r"\bgoing\s+private\b",
        # WBD PSKY deal shows this pattern
        r"\b(?:accepts?|rejects?)\s+.{0,20}\bdeal\b",
        # SEC beneficial-ownership filings (tax_v7, sec_edgar channel):
        # Schedule 13D = active/activist >5% stake, 13G = passive >5% stake.
        # Both are stake-accumulation events — the momentum-relevant precursor
        # to M&A/activist campaigns. Also matches media headlines that
        # reference the filing ("files Schedule 13D on ...").
        r"\bschedule\s+13[dg]\b",
        r"\bsc\s+13[dg](?:/a)?\b",
    ],
    "management_change": [
        r"\bnew\s+(?:CEO|CFO|COO|CTO|CIO|president|chairman|chairwoman|chief)\b",
        r"\b(?:appoints?|names?|hires?|elects?|promotes?)\s+(?:new\s+)?(?:CEO|CFO|COO|CTO|president|chair(?:man|woman)?)\b",
        r"\b(?:CEO|CFO|COO|CTO|president|chair(?:man|woman)?)\s+(?:resigns?|steps?\s+down|departs?|retires?|exits?|leaves?|fired)\b",
        r"\bexecutive\s+(?:departure|shakeup|shuffle|transition|changes?)\b",
        r"\bboard\s+(?:of\s+directors?|changes?|reshuffle)\b",
        r"\b(?:successor|succession)\s+(?:plan|named|announced)\b",
        r"\b(?:replaces?|replacing)\s+(?:CEO|CFO|chair)\b",
        r"\b(?:is\s+expected\s+to\s+become|named\s+as)\s+(?:the\s+)?(?:new\s+)?(?:CEO|CFO|president)\b",
    ],
    "product_launch": [
        r"\blaunch(?:es|ed|ing)?\s+(?:new|first|latest|flagship|AI|next[- ]gen)\b",
        r"\bunveils?\b", r"\bdebut(?:s|ed)?\b",
        r"\bannounces?\s+(?:new|launch|partnership|deal|contract)\b",
        r"\b(?:rolls?\s+out|introduces?|releases?)\s+(?:new|first|latest)\b",
        r"\b(?:debut|release|launch)\s+of\s+(?:new|first|latest)\b",
        r"\bnext[- ]gen(?:eration)?\s+(?:product|chip|platform|device|service)\b",
        # AI/Tech launches (watchlist-specific)
        r"\b(?:launches?|unveils?|announces?)\s+(?:AI|LLM|GenAI|machine\s+learning|quantum)\b",
        r"\bAI\s+(?:model|chip|platform|product|tool|service|infrastructure)\s+(?:launch|rollout|debut)\b",
        r"\b(?:new|latest)\s+(?:AI|machine\s+learning|LLM)\s+(?:model|product|tool)\b",
        # Pharma/biotech
        r"\b(?:drug|therapy|treatment)\s+(?:launch|rollout)\b",
        # Semiconductor / hardware milestones (high-value PRODUCT for AIStock —
        # severity bumps to 0.75 on breakthrough keywords)
        r"\btape[\s-]?out\b",
        r"\b(?:mass\s+production|volume\s+production)\b",
        r"\b\d+\s?nm\s+(?:chip|node|process|tape)\b",
        r"\bannounces?\s+\w+(?:\s+\w+){0,3}\s+(?:chip|processor|GPU|model|platform)\b",
    ],
    "litigation": [
        r"\blawsuit\b", r"\bsued?\s+(?:by|over|for)\b",
        r"\bSEC\s+(?:probe|investigation|charges?|lawsuit|complaint)\b",
        r"\b(?:settles?|settlement|settled)\s+(?:with|for|lawsuit)\b",
        r"\bclass[- ]action\b", r"\bclass\s+suit\b",
        r"\b(?:fraud|securities)\s+(?:charge|allegation|case)\b",
        r"\bantitrust\b", r"\bDOJ\s+(?:probe|investigation|charges?)\b",
        r"\b(?:investor|shareholder)\s+investigation\b",
        r"\b(?:law\s+firm|attorneys?)\s+(?:announces?|launches?)\s+investigation\b",
        r"\bwhistleblower\b",
        r"\binjunction\b",
        # "Wolf Popper LLP Announces Investigation on Behalf of ... Investors"
        r"\binvestigation\s+on\s+behalf\s+of\b",
    ],
    "regulatory": [
        r"\bFDA\s+(?:approv\w*|reject\w*|clear\w*|warning|letter|decision|grants?|authoriz\w*)\b",
        # "FDA approves X", "FDA grants ... approval" — allow intervening words
        r"\bFDA\s+\w+(?:\s+\w+){0,4}\s+(?:approval|clearance|vaccine|drug|therapy|device)\b",
        r"\b(?:FTC|FCC|EPA|CFPB|OCC|OSHA)\s+(?:approval|probe|investigation|ruling|action|fine)\b",
        r"\b(?:approves?|rejects?|clears?|authoriz(?:es|ed))(?:\s+\w+){0,4}\s+(?:drug|device|therapy|vaccine|treatment|acquisition|merger|deal)\b",
        r"\bcomplete\s+response\s+letter\b",
        r"\bphase\s+[123]\s+(?:trial|results?|data|study)\b",
        r"\b(?:regulatory|compliance)\s+(?:approval|review|action|hurdle)\b",
        r"\b(?:fines?|penalties|sanctions?)\s+(?:of\s+\$|imposed|levied)\b",
        r"\b(?:export\s+controls?|sanctions?|blacklist(?:ed|ing)?)\b",
        # Pentagon/NS
        r"\b(?:Pentagon|national\s+security|Department\s+of\s+Defense)\s+(?:blacklist|ban|restrict)\b",
        # SEC 8-K current report (tax_v7, sec_edgar channel): the mandated
        # material-event disclosure. Matches both our synthetic EDGAR titles
        # ("SEC Form 8-K filing: ...") and media references ("reveals in 8-K").
        r"\bform\s+8-k\b",
        r"\b8-k\s+filing\b",
    ],
    "capital_action": [
        r"\b(?:declares?|increases?|raises?|cuts?|suspends?|initiates?)\s+dividend\b",
        r"\bdividend\s+(?:hike|cut|raise|boost|payable|yield)\b",
        r"\bshare\s+(?:repurchase|buyback)\b",
        r"\b(?:announces?|approves?|completes?)\s+(?:share\s+)?(?:buyback|repurchase)(?:\s+program)?\b",
        r"\bstock\s+split\b", r"\breverse\s+split\b",
        r"\b(?:issues?|offering|raises?)\s+(?:shares?|stock|debt|notes?|bonds?)\b",
        r"\b(?:secondary|follow[- ]on|public)\s+offering\b",
        r"\bconvertible\s+notes?\b",
        r"\b(?:new|issues?)\s+\$?\d+.*(?:million|billion)\s+(?:notes?|bonds?|debt)\b",
        r"\bcredit\s+(?:facility|line|agreement)\b",
        r"\b(?:refinanc|restructur)(?:es?|ed|ing)\s+debt\b",
        r"\bprivate\s+placement\b",
    ],
    "insider_activity": [
        r"\binsider\s+(?:buying|selling|trade|purchase|sales?|ownership)\b",
        r"\b10b5-1\b",
        r"\bSEC\s+Form\s+4\b",
        # Executive stock transactions — common Investing.com / benzinga pattern
        # Pattern 1: "{Title} sells/buys $NNN"  (no requirement for trailing 'stock')
        r"\b(?:CEO|CFO|COO|CTO|CRO|director|officer|insider|executive)\s+(?:sells?|sold|buys?|bought|purchases?|acquires?)\s+\$?[\d.,]+",
        # Pattern 2: "sells $NNN in [anything] stock/shares"
        r"\b(?:sells?|sold|buys?|bought)\s+\$?[\d.,]+(?:k|m|K|M|\s+(?:thousand|million|billion))?\s+in\s+(?:\w+\s+){0,3}(?:stock|shares)\b",
        # Pattern 3: "shares sold by {title}"
        r"\bshares?\s+(?:sold|bought|acquired|purchased)\s+by\s+(?:CEO|CFO|COO|CTO|director|officer|insider)\b",
        # Pattern 4: "Form 4 filing"
        r"\bForm\s+4\s+(?:filing|disclosure)\b",
    ],
    "macro_sector": [
        r"\binterest\s+rates?\b", r"\bFed\s+(?:meeting|decision|rate|cut|hike|pause)\b",
        r"\bFOMC\b", r"\bJerome\s+Powell\b", r"\bPowell\s+(?:says?|testimony|speech)\b",
        r"\b(?:inflation|CPI|PPI|PCE)\s+(?:data|report|reading|print)\b",
        r"\btariffs?\b", r"\btrade\s+(?:war|deal|tension|dispute)\b",
        r"\brecession\b", r"\bGDP\b", r"\bjobs\s+report\b", r"\bnon[- ]?farm\s+payrolls?\b",
        r"\bunemployment\s+(?:rate|claims)\b",
        r"\bgeopolitical\b", r"\bsupply\s+chain\b",
        r"\boil\s+(?:prices?|shock|supply|demand)\b",
        r"\bdollar\s+(?:weakens|strengthens|index)\b",
        r"\b(?:treasury|bond)\s+yield\b",
    ],

    # ─── NEW CATEGORIES ──────────────────────────────────────────────────

    # Price action — up/down moves, 52-week highs/lows (most-requested missing pattern)
    "price_action": [
        # Up
        r"\b(?:skyrockets?|skyrocketed|soars?|soared|surges?|surged|rallies|rallied|jumps?|jumped|spikes?|spiked|climbs?|climbed|leaps?|leaped|rises?\s+higher|moves?\s+(?:\d+(?:\.\d+)?%\s+)?higher|gains?\s+\d+(?:\.\d+)?%|extends?\s+(?:rally|gains?)|bounces?\s+back|rebounds?)\b",
        # Down
        r"\b(?:plunges?|plunged|plummets?|plummeted|tumbles?|tumbled|crashes?|crashed|sinks?|sank|slides?|slid|slips?|slipped|drops?\s+\d+(?:\.\d+)?%|falls?\s+\d+(?:\.\d+)?%|dives?|dived|slumps?|slumped|slips?\s+below|breaks?\s+down)\b",
        # 52-week / all-time highs / lows (allow plurals: "52-week highs")
        r"\b(?:52[- ]?week|all[- ]?time|record|fresh|new|multi[- ]year)\s+(?:highs?|lows?|tops?|peaks?|bottoms?)\b",
        r"\bhits?\s+(?:52[- ]?week|all[- ]?time|record|new|fresh)\s+(?:highs?|lows?)\b",
        # Percentage moves in titles
        r"\bmoves?\s+\d+(?:\.\d+)?%\s+(?:higher|lower)\b",
        r"\bup\s+\d+(?:\.\d+)?%\s+(?:since|today|this\s+(?:week|month|year))\b",
        r"\bdown\s+\d+(?:\.\d+)?%\s+(?:since|today|this\s+(?:week|month|year))\b",
        # "Why X stock is up/down today"
        r"\bwhy\s+.*\bstock\s+is\s+(?:up|down|falling|rising|surging|plunging)\b",
        # "X stock jumps/drops"
        r"\b(?:stock|shares?)\s+(?:jumps?|drops?|spikes?|tumbles?|soars?|plummets?|surges?|plunges?)\b",
        # Momentum/breakout
        r"\b(?:breaks?\s+out|breakout|breakdown|momentum\s+(?:building|fades?))\b",
    ],

    # Valuation / analyst commentary — very common in RSS
    "valuation": [
        r"\bvaluation\s+(?:check|reflect|analysis|concern|support)\b",
        r"\b(?:fair|intrinsic)\s+value\b",
        r"\b(?:overvalued|undervalued|fairly\s+valued|fully\s+valued)\b",
        r"\b(?:P/E|P/B|P/S|PEG|EV/EBITDA)\s+(?:ratio|multiple)\b",
        r"\b(?:trading|priced)\s+at\s+(?:a\s+)?(?:premium|discount)\b",
        r"\b(?:stretched|expensive|cheap|attractive)\s+(?:valuation|pricing)\b",
        r"\btoo\s+(?:late|early)\s+to\s+(?:buy|sell|consider)\b",
        r"\bvaluation\s+check\s+after\b",
        r"\bassess(?:es|ing)?\s+(?:.*\s+)?valuation\b",
        # "Is X pricing look stretched" / "Does X valuation reflect"
        r"\bpricing\s+(?:look|reflect|stretched|compress)\b",
        r"\bis\s+it\s+time\s+to\s+(?:buy|sell)\b",
    ],

    # Partnership / deal — non-M&A business deals (AI partnerships, contracts, alliances)
    "partnership": [
        r"\bpartners?(?:hip)?\s+with\b",
        r"\b(?:strategic|multi[- ]year|long[- ]term)\s+(?:partnership|alliance|agreement|deal|collaboration|contract)\b",
        r"\b(?:inks?|signs?|lands?|wins?|secures?)\s+(?:a\s+)?(?:deal|contract|agreement|partnership)\b",
        r"\bjoint\s+venture\b",
        r"\bcollaborat(?:es?|ed|ing|ion)\s+with\b",
        r"\b(?:taps?|selects?|picks?|chooses?)\s+(?:[A-Z]\w+\s+){1,3}(?:for|to\s+power|to\s+help|as\s+)\b",
        # AI/Cloud deals — watchlist-heavy pattern
        r"\b(?:AI|cloud|quantum|semiconductor|chip)\s+(?:deal|partnership|alliance|contract)\b",
        r"\b(?:to\s+power|to\s+help\s+develop|to\s+build|to\s+deploy)\b",
        r"\b(?:multi[- ]year|multi[- ]billion)(?:\s+\w+){0,4}\s+(?:contracts?|deals?|commitments?|agreements?|partnerships?)\b",
        r"\b(?:wins?|awards?|secures?|lands?)(?:\s+\w+){0,3}\s+(?:contracts?|deals?|commitments?)\b",
    ],

    # Technical trading signals (momentum trader vocabulary)
    "technical_signal": [
        r"\bRSI\b", r"\bMACD\b", r"\bBollinger\s+bands?\b",
        r"\b(?:support|resistance)\s+(?:level|zone|break|holds?|broken)\b",
        r"\b(?:moving\s+average|MA|SMA|EMA)\s+(?:cross|support|resistance)\b",
        r"\b(?:golden|death)\s+cross\b",
        r"\b(?:oversold|overbought)\b",
        r"\bFibonacci\s+(?:retracement|level)\b",
        r"\b(?:head\s+and\s+shoulders|double\s+(?:top|bottom)|cup\s+and\s+handle)\b",
        r"\b(?:breaks?\s+out|breakout|breakdown)\s+(?:above|below|from)\b",
        r"\b(?:bull|bear)\s+(?:flag|pennant)\b",
        r"\boptions?\s+(?:data|activity|flow|volume|unusual)\b",
        r"\b(?:notable|unusual)\s+options?\s+activity\b",
        r"\bcall/put\s+ratio\b",
    ],

    # Trade policy / sanctions / tariffs — relevant to many watchlist stocks
    "trade_policy": [
        r"\btariffs?\s+(?:on|against|imposed|announced)\b",
        r"\btrade\s+(?:war|dispute|tension|deal|agreement)\b",
        r"\bsanctions?\s+(?:on|against|imposed|lifted)\b",
        r"\b(?:export|import)\s+(?:controls?|ban|restrict(?:ion|ed)?)\b",
        r"\bnational\s+security\s+(?:concern|review|risk)\b",
        r"\bentity\s+list\b",
        r"\bCFIUS\s+review\b",
        r"\b(?:Pentagon|DoD)\s+(?:blacklist|ban|restrict)\b",
        r"\b(?:US|China|EU)\s+(?:trade|tariff|sanction)\s+(?:policy|announcement|action)\b",
        r"\bsemiconductor\s+(?:export|sanction|restriction)\b",
    ],

    # Cyber / operational risk — major events for tech watchlist
    "cyber_risk": [
        r"\b(?:data\s+)?breach(?:ed)?\b",
        r"\b(?:cyber)?attack(?:ed|s)?\b",
        r"\bhack(?:ed|ers?|ing)\b",
        r"\bransomware\b",
        r"\b(?:security|privacy)\s+(?:flaw|vulnerability|concern|incident|risk)\b",
        r"\b(?:system|service|platform|cloud|network|major)\s+(?:outage|downtime|disruption|incident)\b",
        r"\boutages?\b",
        r"\bdowntimes?\b",
        r"\bglitch(?:es)?\b",
        r"\bdata\s+(?:leak|exposure|loss|compromise)\b",
        r"\b(?:sensitive|customer|personal)\s+data\s+(?:stolen|leaked|exposed)\b",
        r"\bDDoS\b",
        r"\bzero[- ]day\b",
    ],

    # Market sentiment / coverage — "trending", "market lap", "defensive"
    "market_commentary": [
        r"\btrending\s+stock\b",
        r"\bheavily\s+search(?:ed)?\b",
        r"\bmost[- ](?:watched|searched|active)\b",
        r"\b(?:laps?|laps?\s+the|beats?\s+the|outpaces?|outperforms?)\s+(?:stock\s+)?market\b",
        r"\bmarket\s+(?:gains?|returns?|movers?)\b",
        r"\b(?:defensive|cyclical|safe[- ]haven|growth|value|momentum)\s+(?:stock|name|play)\b",
        r"\bsurpasses?\s+market\s+returns?\b",
        r"\bbeats?\s+stock\s+market\s+(?:gains?|upswing|returns?)\b",
        r"\b(?:underperforms?|lags?)\s+(?:the\s+)?(?:market|sector|peers)\b",
        r"\bbiggest\s+(?:gainers?|losers?|movers?)\b",
        r"\bpre[- ]market\s+movers?\b",
        r"\bafter[- ]hours?\s+(?:movers?|trading)\b",
        # Zacks/MSN common title templates
        r"\bsurpasses?\s+market\s+returns?\b",
        r"\b(?:key\s+)?facts?\s+(?:worth\s+knowing|to\s+(?:know|consider))\b",
        r"\bjim\s+cramer\s+(?:says?|likes?|calls?|picks?)\b",
        r"\b(?:stocks?\s+to\s+watch|stocks?\s+on\s+(?:the\s+)?(?:move|radar))\b",
        # Strength/weakness commentary
        r"\b(?:will\s+this\s+)?(?:strength|weakness)\s+(?:last|continue|hold)\b",
    ],

    # Short-seller / activist
    "activist_short": [
        r"\bshort[- ]seller\b",
        r"\b(?:activist|short)\s+(?:position|report|campaign)\b",
        r"\bshort\s+interest\b",
        r"\b(?:activist\s+investor|hedge\s+fund)\s+(?:takes?|builds?|discloses?)\s+(?:stake|position)\b",
        r"\bhindenburg\b",
        r"\bproxy\s+(?:fight|battle|contest)\b",
    ],
}

# Compile once for performance
_EVENT_TAXONOMY_COMPILED: dict[str, list[re.Pattern]] = {
    event_type: [re.compile(p, re.IGNORECASE) for p in patterns]
    for event_type, patterns in EVENT_TAXONOMY.items()
}


def classify_events(title: str, summary: str = "", body: str | None = None) -> list[str]:
    """Return list of event types this article matches.

    Uses title + summary (and first 500 chars of body if available) to avoid
    false positives from irrelevant later paragraphs.
    """
    text_parts = [title, summary or ""]
    if body:
        text_parts.append(body[:500])
    text = " ".join(text_parts)

    matched = []
    for event_type, patterns in _EVENT_TAXONOMY_COMPILED.items():
        if any(p.search(text) for p in patterns):
            matched.append(event_type)
    return matched


# ── Financial sentiment lexicon (Loughran-McDonald subset) ───────────────────
# Loughran-McDonald Financial Sentiment Dictionary (2018 version) — curated
# high-signal subset. The full dictionary has ~2,300 negative and ~350 positive
# words; we include the most common ~15-20% that capture most financial-context
# polarity signal. Unlike general-purpose lexicons (VADER, AFINN), LM is
# calibrated to financial text — e.g. "liability" is neutral here, "restated"
# is negative.
#
# Full dictionary: https://sraf.nd.edu/loughranmcdonald-master-dictionary/

LM_POSITIVE: frozenset[str] = frozenset({
    # ── Earnings & performance ──
    "achieve", "achieved", "achievement", "achievements", "advancement", "advancements",
    "beat", "beats", "beaten", "beating",
    "benefit", "benefited", "benefits", "benefitting", "beneficial", "beneficiary",
    "boost", "boosted", "boosting", "boosts",
    "breakthrough", "breakthroughs",
    "collaborate", "collaborated", "collaborating", "collaboration", "collaborations",
    "constructive", "constructively",
    "delight", "delighted", "delightful",
    "effective", "effectively", "efficient", "efficiently", "efficiency", "efficiencies",
    "enhance", "enhanced", "enhancement", "enhancements", "enhances", "enhancing",
    "excellence", "excellent", "excellently",
    "exceptional", "exceptionally",
    "exclusive", "exclusively",
    "favorable", "favorably", "favored",
    "gain", "gained", "gaining", "gains",
    "good", "great", "greater", "greatest",
    "growth", "grow", "growing", "grew", "grows", "grown",
    "highest", "impressive", "impressively",
    "improve", "improved", "improvement", "improvements", "improves", "improving",
    "innovate", "innovated", "innovating", "innovation", "innovations", "innovative", "innovator",
    "leading", "leadership", "leader", "leaders",
    "opportunities", "opportunity", "optimistic", "optimism",
    "outperform", "outperformed", "outperforming", "outperforms", "outperformance",
    "positive", "positively",
    "profitability", "profitable", "profitably", "profit", "profits",
    "progress", "progressed", "progresses", "progressing",
    "record", "records",
    "robust", "robustly",
    "solid", "solidly",
    "stability", "stable",
    "strength", "strengthen", "strengthened", "strengthening", "strengthens",
    "strong", "stronger", "strongest", "strongly",
    "succeed", "succeeded", "success", "successful", "successfully",
    "surpass", "surpassed", "surpasses", "surpassing",
    "top-line", "topline", "tremendous", "unprecedented",
    "upbeat", "upgrade", "upgraded", "upgrades", "upgrading", "upside",
    "valuable", "versatile", "winner", "winners", "winning",

    # ── Price action (momentum up) — watchlist-critical ──
    "surge", "surged", "surges", "surging",
    "soar", "soared", "soars", "soaring",
    "skyrocket", "skyrockets", "skyrocketed", "skyrocketing",
    "rally", "rallied", "rallies", "rallying",
    "jump", "jumped", "jumps", "jumping",
    "spike", "spiked", "spikes", "spiking",
    "climb", "climbed", "climbs", "climbing",
    "rise", "rose", "risen", "rises", "rising",
    "leap", "leaped", "leaps", "leaping",
    "bounce", "bounced", "bounces", "bouncing",
    "rebound", "rebounded", "rebounding", "rebounds",
    "recover", "recovered", "recovering", "recovers", "recovery",
    "accelerate", "accelerated", "accelerating", "accelerates", "acceleration",
    "advance", "advanced", "advances", "advancing",
    "gain", "gained", "gains", "gaining",  # already listed, ok for frozenset dedup
    "uptick", "upturn", "upswing", "upward", "upswings",
    "surging", "surges",

    # ── Analyst / rating positive ──
    "bullish", "bull",
    "buy", "buys",
    "outperform", "overweight",
    "uplift", "uplifted", "uplifting",
    "raised", "raises", "raising",
    "reiterates", "reiterated", "reiterate",
    "initiates", "initiated", "initiate",
    "reaffirms", "reaffirmed", "reaffirm",
    "maintains", "maintained",  # context-sensitive but mostly positive
    "endorse", "endorsed", "endorses",

    # ── Growth / expansion / tech ──
    "expand", "expanded", "expands", "expanding", "expansion", "expansions",
    "scale", "scaled", "scales", "scaling",
    "accelerate", "acceleration", "accelerating",  # dup
    "dominate", "dominated", "dominates", "dominating", "dominance", "dominant",
    "lead", "leads", "led", "leading",  # dup
    "win", "wins", "won", "winning",  # dup
    "launch", "launched", "launches", "launching",
    "unveil", "unveiled", "unveils", "unveiling",
    "debut", "debuted", "debuts", "debuting",
    "introduce", "introduced", "introduces", "introducing",
    "flagship", "premier", "elite",
    "cutting-edge", "leading-edge", "state-of-the-art",
    "revolutionize", "revolutionized", "revolutionizes", "revolutionizing", "revolutionary",
    "transform", "transformed", "transforms", "transforming", "transformation", "transformative",
    "pioneer", "pioneered", "pioneers", "pioneering",
    "thrive", "thrived", "thrives", "thriving",
    "triumph", "triumphs", "triumphant",
    "momentum", "momentous",
    "resilient", "resilience", "resiliency",

    # ── Deal / partnership positive ──
    "partner", "partners", "partnership", "partnerships", "partnered", "partnering",
    "alliance", "alliances",
    "secure", "secured", "secures", "securing",
    "award", "awarded", "awards",
    "approve", "approved", "approves", "approving", "approval", "approvals",
    "clear", "cleared", "clears", "clearance",
    "certify", "certified", "certifies", "certification",
    "endorse", "endorsed", "endorses",  # dup

    # ── Capital action positive ──
    "buyback", "buybacks", "repurchase", "repurchased", "repurchases", "repurchasing",
    "dividend", "dividends",  # neutral but often paired with positive context
    "distribution", "distributions",
    "yield",  # can be ambiguous, but often positive in news

    # ── Market commentary positive ──
    "strength",  # dup
    "resilient", "resilience",  # dup
    "reliable", "reliably",
    "confident", "confidently", "confidence",
    "ahead",  # "ahead of estimates"
    "aboveboard", "above-consensus",
    "outpace", "outpaced", "outpaces", "outpacing",

    # ── High/best superlatives often in headlines ──
    "best", "bestselling", "top-ranked", "top-rated",
    "highest-rated", "top-performing",
    "milestone", "milestones",
    "record-high", "all-time",

    # ── Tech / AI positive (watchlist-specific) ──
    "scalable", "scalability",
    "secure", "secured", "secures",  # dup
    "trusted", "trust",
    "smart", "smarter", "smartest",
    "intelligent",

    # ── Labor / operational positive ──
    "hiring", "hires",
    "expansion",  # dup
    "construction",  # building new facilities, often positive
    "reinvest", "reinvested", "reinvesting",
})

LM_NEGATIVE: frozenset[str] = frozenset({
    # ── Earnings & performance losses ──
    "abandon", "abandoned", "abandoning", "abandonment",
    "adverse", "adversely",
    "allegation", "allegations", "alleged", "allegedly",
    "anomalies", "anomaly",
    "bankruptcy", "bankrupt", "bankrupted",
    "bottleneck", "bottlenecks",
    "breach", "breached", "breaches", "breaching",
    "challenging", "challenges", "challenged",
    "claim", "claims",
    "close", "closed", "closing", "closure", "closures",
    "collapse", "collapsed", "collapses", "collapsing",
    "concern", "concerned", "concerning", "concerns",
    "corrupt", "corrupted", "corruption",
    "crisis", "critical", "criticize", "criticized", "critiques",
    "damage", "damaged", "damages", "damaging",
    "decline", "declined", "declines", "declining", "declination",
    "default", "defaulted", "defaulting", "defaults",
    "deficit", "deficits",
    "delay", "delayed", "delaying", "delays",
    "deteriorate", "deteriorated", "deteriorating", "deterioration",
    "difficult", "difficulty", "difficulties",
    "disappoint", "disappointed", "disappointing", "disappointment", "disappoints",
    "dismiss", "dismissed", "dismissing", "dismissal",
    "disruption", "disruptions", "disrupted", "disrupt", "disrupts",
    "divest", "divested", "divesting", "divestiture",
    "doubt", "doubted", "doubtful", "doubts",
    "downgrade", "downgraded", "downgrades", "downgrading",
    "downturn", "downturns",
    "drop", "dropped", "drops", "dropping",
    "erode", "eroded", "erosion", "erodes", "eroding",
    "fail", "failed", "failing", "fails", "failure", "failures",
    "fall", "fallen", "falling", "falls", "fell",
    "fraud", "fraudulent", "fraudulently",
    "halt", "halted", "halting", "halts",
    "hurt", "hurts", "hurting",
    "impair", "impaired", "impairment", "impairments",
    "inability", "inadequate", "incorrect", "incorrectly",
    "inefficiency", "inefficient",
    "injunction", "insolvent", "insolvency",
    "investigation", "investigations", "investigating", "investigate", "investigated",
    "lack", "lacked", "lacking", "lacks",
    "lag", "lagged", "lagging", "lags",
    "lawsuit", "lawsuits", "layoff", "layoffs",
    "litigation", "lose", "loses", "losing", "loss", "losses", "lost",
    "mislead", "misleading", "misled", "misrepresent", "misrepresented",
    "miss", "missed", "misses", "missing",
    "negative", "negatively",
    "obstacle", "obstacles", "omit", "omitted",
    "penalty", "penalties",
    "poor", "poorly",
    "probe", "probed", "probes", "probing",
    "problem", "problems",
    "recall", "recalled", "recalling", "recalls",
    "recession", "recessionary",
    "reject", "rejected", "rejecting", "rejects", "rejection",
    "restate", "restated", "restatement", "restatements",
    "restructure", "restructured", "restructuring",
    "risk", "risks", "risky", "risked",
    "shortage", "shortages", "shortfall", "shortfalls",
    "shutdown", "slow", "slowed", "slowing", "slowdown", "slows",
    "stolen", "struggle", "struggled", "struggles", "struggling",
    "sue", "sued", "sues", "suing",
    "suffer", "suffered", "suffering", "suffers",
    "suspend", "suspended", "suspending", "suspends", "suspension",
    "terminate", "terminated", "terminates", "terminating", "termination",
    "threat", "threaten", "threatened", "threatening", "threats",
    "uncertain", "uncertainty", "unexpected", "unfavorable",
    "violate", "violated", "violates", "violating", "violation", "violations",
    "warn", "warned", "warning", "warnings", "warns",
    "weak", "weaken", "weakened", "weakening", "weakens", "weaker", "weakness",
    "worse", "worsen", "worsened", "worsening", "worst",

    # ── Price action (momentum down) — watchlist-critical ──
    "plunge", "plunged", "plunges", "plunging",
    "plummet", "plummeted", "plummets", "plummeting",
    "tumble", "tumbled", "tumbles", "tumbling",
    "crash", "crashed", "crashes", "crashing",
    "sink", "sinks", "sinking", "sank", "sunk",
    "slide", "slides", "sliding", "slid",
    "slip", "slipped", "slips", "slipping",
    "dip", "dipped", "dips", "dipping",
    "slump", "slumped", "slumps", "slumping",
    "dive", "dived", "dives", "diving",
    "swoon", "swooned", "swoons",
    "retreat", "retreated", "retreats", "retreating",

    # ── Analyst / rating negative ──
    "bearish", "bear", "bears",
    "sell", "sells", "sold",  # context: sell-off, sold off
    "underperform", "underperformed", "underperforms", "underperformance",
    "underweight",
    "cut", "cuts", "cutting",  # price target cut, guidance cut
    "slash", "slashed", "slashes", "slashing",
    "trim", "trimmed", "trims", "trimming",
    "cautious", "caution",
    "downbeat",

    # ── Financial distress / risk ──
    "headwind", "headwinds",
    "pressure", "pressures", "pressured", "pressuring",
    "overhang", "overhangs",
    "weakness", "weaknesses",  # dup
    "squeeze", "squeezed", "squeezes", "squeezing",
    "strain", "strained", "strains", "straining",
    "stress", "stressed", "stresses", "stressing",
    "stagnate", "stagnated", "stagnating", "stagnation", "stagnant",
    "overvalued", "overheated", "overheating", "overheat",
    "bubble", "bubbles",
    "collapse",  # dup
    "bear-market", "selloff", "sell-off",
    "drawdown", "drawdowns",

    # ── Legal / regulatory risk ──
    "sanctioned", "sanctions",
    "blacklist", "blacklisted", "blacklisting",
    "banned", "ban", "bans",
    "fine", "fined", "fines", "fining",
    "penalty", "penalties",  # dup
    "noncompliance", "non-compliance",
    "illegal", "illegally",
    "felony", "felonies",
    "misconduct",
    "antitrust",
    "subpoena", "subpoenaed",

    # ── Operational / cyber risk ──
    "outage", "outages",
    "downtime",
    "glitch", "glitches",
    "hack", "hacked", "hacker", "hackers", "hacking",
    "breach", "breached", "breaches",  # dup
    "cyberattack", "cyberattacks",
    "ransomware",
    "vulnerability", "vulnerabilities", "vulnerable",
    "exposed", "exposure", "exposures",
    "leak", "leaked", "leaks", "leaking",
    "defect", "defective", "defects",
    "flaw", "flawed", "flaws",
    "error", "errors", "erroneous",
    "malfunction", "malfunctioned", "malfunctions",
    "recall",  # dup

    # ── Macro / geopolitical risk ──
    "tariff", "tariffs",
    "trade-war",
    "geopolitical",
    "conflict", "conflicts",
    "tension", "tensions",
    "instability", "unstable", "unsettled",
    "turmoil",
    "chaos", "chaotic",
    "volatile", "volatility",  # dup
    "unrest",
    "crisis", "crises",  # dup

    # ── Management / reputation negative ──
    "scandal", "scandals", "scandalous",
    "ousted", "ousting",
    "fired", "firing", "fires",  # context: firing employees
    "resign", "resigned", "resigns", "resigning", "resignation",
    "depart", "departed", "departing", "departure", "departures",
    "step-down", "stepped-down",
    "turnover",  # management turnover
    "controversy", "controversial",
    "backlash",

    # ── Market commentary negative ──
    "trouble", "troubled", "troubles", "troubling",
    "ugly", "bad", "badly", "worse", "worst",
    "tough", "toughest",
    "soft", "softer", "soft-demand", "softening",
    "sluggish", "sluggishly",
    "lackluster",
    "mediocre",
    "subdued",
    "muted",
    "diluted", "dilutive", "dilution",
    "missed", "misses",  # dup
    "skeptic", "skeptical", "skepticism",
    "disappointing",  # dup

    # ── Litigation specific ──
    "charged", "charges", "charging",
    "accuse", "accused", "accuses", "accusing", "accusation", "accusations",
    "class-action",
    "settlement",  # neutral but usually implies wrongdoing
    "liable", "liability",  # "liabilities" already listed
    "whistleblower",

    # ── Earnings specific ──
    "shortfall", "shortfalls",  # dup
    "cut-guidance", "guided-down",
    "below-consensus", "below-expectations",
    "guided-lower",
})

# Uncertainty / hedging words — a high share indicates management hedging,
# which in finance correlates with downside surprises.
LM_UNCERTAINTY: frozenset[str] = frozenset({
    "approximate", "approximately", "approximation",
    "assume", "assumed", "assumes", "assuming", "assumption", "assumptions",
    "believe", "believed", "believes",
    "conditional", "contingent", "contingency", "contingencies",
    "could",
    "depend", "depended", "depending", "depends", "dependent",
    "estimate", "estimated", "estimates", "estimating",
    "expose", "exposed", "exposes", "exposure", "exposures",
    "indefinite", "indefinitely",
    "likelihood", "likely",
    "may", "maybe", "might",
    "possibility", "possible", "possibly", "possibilities",
    "predict", "predicted", "predicting", "prediction", "predictions", "predicts",
    "probable", "probably",
    "seem", "seemed", "seems",
    "speculate", "speculated", "speculation", "speculative", "speculating",
    "tentative", "tentatively",
    "uncertain", "uncertainty", "uncertainties",
    "unclear", "unknown", "unpredictable",
    "volatile", "volatility",

    # ── Additional hedging language commonly in financial analysis ──
    "if", "whether",  # conditional markers
    "suggest", "suggested", "suggests", "suggesting",
    "indicate", "indicated", "indicates", "indicating",
    "appear", "appears", "appeared",
    "pending", "pendings",
    "tentative", "tentatively",  # dup
    "presumably", "presumed",
    "apparently", "apparent",
    "ostensibly",
    "seemingly",
    "reportedly", "reported",
    "allegedly",
    "purportedly", "purported",
    "questionable",
    "ambiguous", "ambiguity",
    "undetermined", "undecided",
    "unresolved",
    "wait-and-see",
    "watch-and-wait",
    "uncertainty", "uncertainties",  # dup
    "wariness", "wary",
    "hesitant", "hesitantly", "hesitation",
    "reluctant", "reluctantly", "reluctance",
    "overhang", "overhangs",
    "somewhat",
    "potentially", "potential",
    "projected", "projection", "projections",
    "anticipated", "anticipates", "anticipate", "anticipating", "anticipation",

    # ── Guidance hedging ──
    "subject-to",
    "provided-that",
    "expectation", "expectations", "expect", "expects", "expected",
    "forecast", "forecasts", "forecasted",
    "outlook",
})

_SENTIMENT_TOKEN_RE = re.compile(r"[a-z]+(?:-[a-z]+)?")


def lexicon_sentiment(title: str, summary: str = "", body: str | None = None) -> dict:
    """Compute Loughran-McDonald financial sentiment from title + summary + body head.

    Returns a dict with:
      - score:  net sentiment in [-1, 1] = (pos - neg) / max(1, total_matched)
      - pos:    positive word frequency (fraction of total tokens)
      - neg:    negative word frequency
      - unc:    uncertainty/hedging word frequency
      - matched: total count of LM-dictionary hits

    The score is normalized by **matched** words (not total tokens) so that
    a short headline with 2 positive words and 0 negative is solidly positive,
    not diluted by neutral filler.
    """
    text_parts = [title or "", summary or ""]
    if body:
        # Cap at 2000 chars: enough for lede + 2-3 paragraphs, avoids
        # long-tail filler dominating the signal.
        text_parts.append(body[:2000])
    text = " ".join(text_parts).lower()
    tokens = _SENTIMENT_TOKEN_RE.findall(text)
    total = len(tokens)
    if total == 0:
        return {"score": 0.0, "pos": 0.0, "neg": 0.0, "unc": 0.0, "matched": 0}

    pos = sum(1 for t in tokens if t in LM_POSITIVE)
    neg = sum(1 for t in tokens if t in LM_NEGATIVE)
    unc = sum(1 for t in tokens if t in LM_UNCERTAINTY)

    matched = pos + neg
    score = (pos - neg) / matched if matched else 0.0

    return {
        "score": round(score, 4),
        "pos": round(pos / total, 4),
        "neg": round(neg / total, 4),
        "unc": round(unc / total, 4),
        "matched": matched,
    }


# ── High-value event signal extraction ────────────────────────────────────
# AIStock's impact.py assigns severity/sign per event type, then re-parses the
# headline for magnitude modifiers (deal size, beat %, guidance direction,
# approval/rejection). We pre-extract those structured magnitudes here so the
# downstream severity calc has a clean numeric input instead of re-running
# fragile text heuristics. These are the highest-ROI signals per the AIStock
# value analysis: GUIDANCE (sev 0.7), REGULATION (0.8), M&A large (0.8),
# EARNINGS_SURPRISE (magnitude-scaled), PRODUCT breakthrough (0.75).

# Deal size: "$8.5 billion", "$68B", "$1.2 trillion"
_RE_DEAL_SIZE = re.compile(
    r"\$\s?(\d+(?:\.\d+)?)\s?(billion|bn|b|trillion|tn|t|million|mn|m)\b", re.I)
# Earnings beat/miss magnitude: "beats by 12%", "misses by $0.05", "tops estimates by 8%"
_RE_BEAT_PCT = re.compile(
    r"\b(beats?|tops?|misses?|trails?|surpass(?:es)?|falls?\s+short)\b"
    r"(?:[^.%]{0,40}?)\b(\d+(?:\.\d+)?)\s?%", re.I)
_RE_BEAT_WORD = re.compile(
    r"\b(crush(?:es|ed)?|blowout|smash(?:es|ed)?|trounc(?:es|ed)?|"
    r"obliterat(?:es|ed)?|handily\s+beat)\b", re.I)
_RE_MISS_WORD = re.compile(
    r"\b(badly\s+miss(?:es|ed)?|disappoint(?:s|ing|ed)?|whiff(?:s|ed)?|"
    r"falls?\s+well\s+short)\b", re.I)
# Guidance direction
_RE_GUIDANCE_UP = re.compile(
    r"\b(rais(?:es|ed|ing)|hik(?:es|ed)|boost(?:s|ed)?|lift(?:s|ed)?|"
    r"upgrad(?:es|ed))\b[^.]{0,30}\b(guidance|outlook|forecast|target|view)\b", re.I)
_RE_GUIDANCE_DOWN = re.compile(
    r"\b(cuts?|lower(?:s|ed)?|slash(?:es|ed)?|trim(?:s|med)?|reduc(?:es|ed)|"
    r"warns?|downgrad(?:es|ed))\b[^.]{0,30}\b(guidance|outlook|forecast|target|view)\b", re.I)
# Regulatory approval / rejection (sign-flipping for AIStock REGULATION)
_RE_APPROVAL = re.compile(
    r"\b(approv(?:es|ed|al)|clear(?:s|ed|ance)|grant(?:s|ed)?|authoriz(?:es|ed)|"
    r"green[\s-]?light(?:s|ed)?|wins?\s+(?:fda|approval|clearance))\b", re.I)
_RE_REJECTION = re.compile(
    r"\b(reject(?:s|ed|ion)|denies|denied|declin(?:es|ed)|complete\s+response\s+letter|"
    r"crl\b|fails?\s+to\s+(?:win|gain)|halts?|suspend(?:s|ed)?)\b", re.I)
# Product breakthrough (semiconductor/tech — AIStock bumps severity to 0.75)
_RE_BREAKTHROUGH = re.compile(
    r"\b(tape[\s-]?out|mass\s+production|breakthrough|first\s+(?:chip|of\s+its\s+kind)|"
    r"next[\s-]?gen(?:eration)?|record[\s-]?breaking|world['’]?s\s+(?:first|fastest|largest))\b", re.I)

_DEAL_UNIT_MULT = {
    "trillion": 1_000_000, "tn": 1_000_000, "t": 1_000_000,
    "billion": 1_000, "bn": 1_000, "b": 1_000,
    "million": 1, "mn": 1, "m": 1,
}


def extract_event_signals(title: str, summary: str = "", body: str | None = None,
                          event_types: list[str] | None = None) -> dict:
    """Extract structured magnitude signals from a headline for AIStock's
    severity/sign calculation. Returns only the keys that were detected;
    empty dict if nothing high-value found.

    Keys (all optional):
      deal_size_usd_m       : M&A deal size in USD millions (int)
      deal_size_class       : "large" (>=$10bn) | "mid" ($1-10bn) | "small" (<$1bn)
      earnings_beat_pct     : signed % beat(+)/miss(-) (float)
      earnings_surprise_dir : "beat" | "miss" | "strong_beat" | "strong_miss"
      guidance_direction    : "raised" | "cut"
      regulatory_outcome    : "approval" | "rejection"
      product_breakthrough  : True
    """
    text = " ".join([title or "", summary or "", (body or "")[:300]])
    et = set(event_types or [])
    out: dict = {}

    # ── M&A deal size (only when ma_activity tagged, to avoid generic $ amounts) ──
    if "ma_activity" in et:
        m = _RE_DEAL_SIZE.search(text)
        if m:
            val = float(m.group(1))
            unit = m.group(2).lower()
            usd_m = int(val * _DEAL_UNIT_MULT.get(unit, 1))
            out["deal_size_usd_m"] = usd_m
            out["deal_size_class"] = (
                "large" if usd_m >= 10_000 else "mid" if usd_m >= 1_000 else "small")

    # ── Earnings beat/miss magnitude ──
    if "earnings_release" in et or "earnings_guidance" in et:
        bm = _RE_BEAT_PCT.search(text)
        if bm:
            verb = bm.group(1).lower()
            pct = float(bm.group(2))
            is_miss = verb.startswith(("miss", "trail", "fall"))
            out["earnings_beat_pct"] = round(-pct if is_miss else pct, 2)
        # Strong qualitative signals override / supplement
        if _RE_BEAT_WORD.search(text):
            out["earnings_surprise_dir"] = "strong_beat"
        elif _RE_MISS_WORD.search(text):
            out["earnings_surprise_dir"] = "strong_miss"
        elif "earnings_beat_pct" in out:
            out["earnings_surprise_dir"] = "beat" if out["earnings_beat_pct"] > 0 else "miss"

    # ── Guidance direction ──
    if "earnings_guidance" in et or "earnings_release" in et:
        if _RE_GUIDANCE_UP.search(text):
            out["guidance_direction"] = "raised"
        elif _RE_GUIDANCE_DOWN.search(text):
            out["guidance_direction"] = "cut"

    # ── Regulatory outcome (sign-flipping) ──
    if "regulatory" in et:
        if _RE_APPROVAL.search(text):
            out["regulatory_outcome"] = "approval"
        elif _RE_REJECTION.search(text):
            out["regulatory_outcome"] = "rejection"

    # ── Product breakthrough ──
    if "product_launch" in et and _RE_BREAKTHROUGH.search(text):
        out["product_breakthrough"] = True

    return out


def _load_aistock_watchlist() -> list[str]:
    """Load the ticker watchlist from the sibling AIStock repo's config/default.json.

    Returns the same list that AIStock uses so crawls stay in sync.
    Falls back to an empty list if the repo or config is not found.
    """
    aistock_config = PROJECT_ROOT.parent / "AIStock" / "config" / "default.json"
    if not aistock_config.exists():
        log.warning("aistock_watchlist_not_found", path=str(aistock_config))
        return []
    try:
        data = json.loads(aistock_config.read_text(encoding="utf-8"))
        raw = [t for t in data.get("watchlist", []) if isinstance(t, str) and t.strip() and not t.startswith("_")]
        tickers = list(dict.fromkeys(raw))  # dedup, preserve order
        max_t = int(data.get("max_tickers", 500))
        result = tickers[:max_t]
        log.info("aistock_watchlist_loaded", path=str(aistock_config), count=len(result))
        return result
    except Exception as exc:
        log.warning("aistock_watchlist_load_error", error=str(exc))
        return []


def parse_tickers(value: str | None, preset: str | None = None) -> list[str]:
    if preset:
        if preset == "aistock":
            tickers = _load_aistock_watchlist()
            if tickers:
                return tickers
            log.warning("aistock_watchlist_unavailable", fallback="aistock500")
            return AISTOCK500_TICKERS[:]
        preset_tickers = BUILTIN_TICKER_SETS.get(preset)
        if preset_tickers:
            return preset_tickers[:]

    if not value:
        return [DEFAULT_TICKER]

    seen: set[str] = set()
    tickers: list[str] = []
    for raw in value.split(","):
        ticker = raw.strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers or [DEFAULT_TICKER]


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def make_run_dir(label: str) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_label = re.sub(r"[^a-z0-9_-]+", "_", label.lower()).strip("_")
    run_dir = RUNS_DIR / f"{stamp}_{safe_label}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def setup_run_logging(run_dir: Path) -> Path:
    global _RUN_FILE_HANDLER, _ERROR_JSONL_PATH
    log_path = run_dir / "run.log"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if _RUN_FILE_HANDLER is not None:
        root.removeHandler(_RUN_FILE_HANDLER)
        _RUN_FILE_HANDLER.close()
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)
    _RUN_FILE_HANDLER = handler

    # Enable structured error/warning sideload for this run
    _ERROR_JSONL_PATH = run_dir / "errors.jsonl"

    return log_path


def teardown_run_logging() -> None:
    global _RUN_FILE_HANDLER, _ERROR_JSONL_PATH
    if _RUN_FILE_HANDLER is None:
        return
    root = logging.getLogger()
    root.removeHandler(_RUN_FILE_HANDLER)
    _RUN_FILE_HANDLER.close()
    _RUN_FILE_HANDLER = None
    _ERROR_JSONL_PATH = None


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


# ── Error audit: scan past runs ────────────────────────────────────────────

def iter_error_events(*, hours: int | None = None) -> list[dict]:
    """Walk `data/runs/*/errors.jsonl` and yield all structured error events
    within the last `hours` (or all if None).

    Each event is a dict with keys: `ts`, `level`, `event`, plus any
    key/value pairs that were logged (e.g. `url`, `error`, `domain`).
    """
    cutoff = None
    if hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    events: list[dict] = []
    for errors_file in sorted(RUNS_DIR.glob("*/errors.jsonl")):
        try:
            for line in errors_file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if cutoff is not None:
                    ts = evt.get("ts")
                    if not ts:
                        continue
                    try:
                        dt = datetime.fromisoformat(ts)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt < cutoff:
                            continue
                    except Exception:
                        continue
                evt["_run_dir"] = errors_file.parent.name
                events.append(evt)
        except Exception:
            continue
    return events


def summarize_errors(*, hours: int = 24, top: int = 10) -> dict:
    """Aggregate error events from the last `hours` into a grouped summary.

    Returns:
      {
        "window_hours": 24,
        "total_events": 42,
        "by_level":  {"error": 30, "warning": 12},
        "by_event":  {"fetch_error": 18, "circuit_breaker_tripped": 2, ...},
        "by_domain": {"news.google.com": 15, "finance.yahoo.com": 8, ...},
        "by_status": {"503": 10, "400": 8},
        "top_errors": [(count, msg_prefix), ...],
        "recent":    [<last 10 events>],
      }
    """
    events = iter_error_events(hours=hours)
    from collections import Counter
    by_level = Counter(e.get("level", "unknown") for e in events)
    by_event = Counter(e.get("event", "unknown") for e in events)
    by_domain = Counter()
    by_status = Counter()
    by_error_prefix: Counter = Counter()
    for e in events:
        # Extract domain from url if present
        url = e.get("url", "")
        if url:
            try:
                dom = urlparse(url).netloc
                if dom:
                    by_domain[dom] += 1
            except Exception:
                pass
        status = e.get("status")
        if status is not None:
            by_status[str(status)] += 1
        err_msg = e.get("error", "")
        if err_msg:
            # Group by first 60 chars of error message
            by_error_prefix[str(err_msg)[:60]] += 1

    recent = events[-10:]
    return {
        "window_hours": hours,
        "total_events": len(events),
        "by_level": dict(by_level.most_common()),
        "by_event": dict(by_event.most_common(top)),
        "by_domain": dict(by_domain.most_common(top)),
        "by_status": dict(by_status.most_common()),
        "top_errors": by_error_prefix.most_common(top),
        "recent": recent,
    }


def print_error_summary(*, hours: int = 24) -> None:
    """Human-readable error summary for CLI output (to stderr)."""
    summary = summarize_errors(hours=hours)
    out = sys.stderr
    print(f"=== Error audit: last {summary['window_hours']}h ===", file=out)
    print(f"Total events: {summary['total_events']}", file=out)
    if summary["total_events"] == 0:
        print("No errors/warnings recorded. ✓", file=out)
        return
    print(f"\nBy level:   {summary['by_level']}", file=out)
    print(f"\nBy event type (top {len(summary['by_event'])}):", file=out)
    for k, v in summary["by_event"].items():
        print(f"  {v:5d}  {k}", file=out)
    if summary["by_domain"]:
        print(f"\nBy domain:", file=out)
        for k, v in summary["by_domain"].items():
            print(f"  {v:5d}  {k}", file=out)
    if summary["by_status"]:
        print(f"\nBy HTTP status: {summary['by_status']}", file=out)
    if summary["top_errors"]:
        print(f"\nTop error messages:", file=out)
        for msg, cnt in summary["top_errors"]:
            print(f"  {cnt:5d}  {msg}", file=out)
    print(f"\nMost recent {len(summary['recent'])} events:", file=out)
    for e in summary["recent"]:
        ts = e.get("ts", "")
        lvl = e.get("level", "")
        evt = e.get("event", "")
        detail = " ".join(f"{k}={v}" for k, v in e.items()
                         if k not in ("ts", "level", "event", "_run_dir"))
        print(f"  [{ts}] [{lvl}] {evt}  {detail[:150]}", file=out)


def _progress(msg: str, **kw) -> None:
    """Print a human-readable progress line to stderr (separate from JSON stdout)."""
    ts = datetime.now().strftime("%H:%M:%S")
    extras = "  " + "  ".join(f"{k}={v}" for k, v in kw.items()) if kw else ""
    print(f"[{ts}] {msg}{extras}", file=sys.stderr, flush=True)


def _canonical_news_source(article: dict) -> tuple[str, str]:
    source_domain = (article.get("_source_domain") or "").strip().lower()
    if source_domain.startswith("www."):
        source_domain = source_domain[4:]
    if source_domain in DOMAIN_TO_AISTOCK_SOURCE:
        return DOMAIN_TO_AISTOCK_SOURCE[source_domain], source_domain

    source_name = (article.get("_source_name") or "").strip().lower()
    if source_name:
        normalized_name = source_name.replace(" ", "_")
        if normalized_name == "amd_investor_relations":
            return "ir", source_name
        return normalized_name, source_name
    if source_domain:
        return source_domain.replace(".", "_"), source_domain
    return "unknown", "unknown"


def _source_quality(article: dict) -> str:
    trust = article.get("_trust", 1)
    if trust >= 3:
        return "high"
    if trust == 2:
        return "medium"
    return "low"


def build_aistock_payload(news_items: list[dict]) -> dict:
    return {
        "hard_event_news": [],
        "soft_event_news": news_items,
        "alt_sentiment_news": [],
        "news": news_items,
    }


def group_articles_by_ticker(articles: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for article in articles:
        tickers = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
        for ticker in tickers:
            grouped[ticker].append(article)
    return dict(grouped)


def export_articles_by_ticker(
    *,
    run_dir: Path,
    filtered_articles: list[dict],
    news_items: list[dict],
) -> None:
    articles_by_ticker = group_articles_by_ticker(filtered_articles)
    news_items_by_ticker: dict[str, list[dict]] = defaultdict(list)
    for item in news_items:
        for ticker in item.get("tickers_hint", []):
            news_items_by_ticker[ticker].append(item)

    run_by_ticker_dir = run_dir / "by_ticker"
    run_by_ticker_dir.mkdir(parents=True, exist_ok=True)
    stable_root_dir = AISTOCK_EXPORT_DIR
    stable_root_dir.mkdir(parents=True, exist_ok=True)
    stable_by_ticker_dir = TICKER_EXPORTS_DIR
    stable_by_ticker_dir.mkdir(parents=True, exist_ok=True)

    write_json(run_dir / "aistock_payload.json", build_aistock_payload(news_items))
    write_json(stable_root_dir / "latest_news_items.json", news_items)
    write_json(stable_root_dir / "latest_payload.json", build_aistock_payload(news_items))

    for ticker in sorted(set(articles_by_ticker) | set(news_items_by_ticker)):
        run_ticker_dir = run_by_ticker_dir / ticker
        run_ticker_dir.mkdir(parents=True, exist_ok=True)
        stable_ticker_dir = stable_by_ticker_dir / ticker
        stable_ticker_dir.mkdir(parents=True, exist_ok=True)

        ticker_articles = articles_by_ticker.get(ticker, [])
        ticker_news_items = news_items_by_ticker.get(ticker, [])
        ticker_summary = {
            "ticker": ticker,
            "article_count": len(ticker_articles),
            "news_item_count": len(ticker_news_items),
        }
        ticker_payload = build_aistock_payload(ticker_news_items)

        write_json(run_ticker_dir / "articles.json", ticker_articles)
        write_json(run_ticker_dir / "news_items.json", ticker_news_items)
        write_json(run_ticker_dir / "aistock_payload.json", ticker_payload)
        write_json(run_ticker_dir / "summary.json", ticker_summary)

        write_json(stable_ticker_dir / "latest_articles.json", ticker_articles)
        write_json(stable_ticker_dir / "latest_news_items.json", ticker_news_items)
        write_json(stable_ticker_dir / "latest_payload.json", ticker_payload)
        write_json(stable_ticker_dir / "latest_summary.json", ticker_summary)


def render_report(
    articles,
    raw_articles,
    channel_stats,
    new_count,
    bz_count,
    ticker_label,
    hours,
    per_ticker_stats: dict[str, int] | None = None,
) -> str:
    buf = StringIO()
    with redirect_stdout(buf):
        print_results(articles, raw_articles, channel_stats, new_count, bz_count, ticker_label, hours, per_ticker_stats)
    return buf.getvalue()


def load_rolling_window(
    tickers: list[str],
    hours: int,
    *,
    as_of: datetime | None = None,
    classifier_version: str | None = None,
) -> list[dict]:
    """Read SQLite articles within the rolling time window — PIT-correct.

    PIT (point-in-time) semantics:
      - `as_of=None` (default, live mode):  cutoff = now - hours, ceiling = now
      - `as_of=T` (backtest mode):           cutoff = T - hours,   ceiling = T
        — only articles with `first_seen_at <= T` are returned (no look-ahead).

    Filter axis:
      - `first_seen_at` (when we observed the article) — guarantees PIT correctness.
      - We deliberately do NOT filter by `published` because source-claimed time
        can be in the future or backfilled; using it would create look-ahead bias.

    Used at export time so that `latest_news_items.json` and the per-ticker
    files always reflect the full N-hour view, not just what was fetched in
    the current run.

    Quality score and sentiment/events are re-computed with the current
    taxonomy/lexicon, so any dictionary/regex updates take effect on the
    next run without needing to re-crawl. Pass `classifier_version` to pin
    backtests to a specific historical classifier.
    """
    now = as_of or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat()
    now_iso = now.isoformat()
    ticker_set = {t.upper() for t in tickers}

    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Bitemporal PIT query: only return articles that existed in our system
        # at or before `as_of`. first_seen_at is the immutable transaction time.
        rows = conn.execute(
            """
            SELECT id, title, url, source_name, source_domain, trust_tier,
                   relevance, channel, published, summary, body,
                   primary_ticker, tickers, alt_sources, first_seen_at
            FROM news
            WHERE first_seen_at >= ? AND first_seen_at <= ?
            ORDER BY first_seen_at DESC
            """,
            (cutoff_iso, now_iso),
        ).fetchall()
    finally:
        conn.close()

    articles: list[dict] = []
    stale_relevance = 0
    stale_junk = 0
    for row in rows:
        (id_, title, url, src_name, src_dom, trust, rel, ch, pub,
         summary, body, prim_tkr, tickers_str, alt, first_seen) = row
        ticker_list = [t for t in (tickers_str or "").split(",") if t]
        # Filter to articles for one of the requested tickers
        if not any(t.upper() in ticker_set for t in ticker_list):
            continue
        # Re-apply current relevance + junk rules to filter out stale
        # records that passed under older/looser disambiguation rules.
        primary = prim_tkr or (ticker_list[0] if ticker_list else "")
        if primary:
            clean_title = title.rsplit(" - ", 1)[0] if " - " in title else title
            # wire_tripwire attribution is authoritative (curated alias match) —
            # bypass relevance recompute (NewsCrawler's relevance_score would
            # score 0 for company names absent from its smaller alias table).
            if ch == "wire_tripwire":
                current_rel = max(rel or 0.0, 0.55)
            elif ch == "sec_edgar":
                # Issuer-CIK attribution from the filing itself — authoritative,
                # bypass relevance recompute (same rationale as wire_tripwire).
                current_rel = max(rel or 0.0, 0.55)
            elif ch == "ibkr_news":
                # DJ conid attribution is authoritative; headlines often omit
                # the company name → relevance recompute would wrongly prune.
                current_rel = max(rel or 0.0, 0.30)
            else:
                current_rel = relevance_score(clean_title, summary or "", primary)
                # Per-ticker RSS feeds (Yahoo/Nasdaq) get a much lower gate since
                # the ticker is in the URL path. Google proxy channels (reuters/
                # bloomberg/cnbc) use standard gate — Google body-match is too loose.
                per_ticker_feed = ch in ("yahoo_finance_rss", "nasdaq_rss", "ir_feed", "amd_ir")
                rel_gate = 0.02 if per_ticker_feed else 0.10
                if current_rel < rel_gate:
                    stale_relevance += 1
                    continue
                if per_ticker_feed and current_rel < 0.30:
                    current_rel = 0.30
            if is_junk(clean_title):
                stale_junk += 1
                continue
            # ibkr_news / sec_edgar exempt from weak-signal (insider/stake
            # stories from the DJ wire or the regulator itself are signal,
            # not aggregator 13F churn — see quality_filter step ⑤).
            if ch not in ("ibkr_news", "sec_edgar") and is_weak_signal(clean_title, src_dom or ""):
                continue
            if _looks_like_quote_or_instrument_page(clean_title, summary or "", primary):
                continue
            if _has_conflicting_exchange_reference(clean_title, summary or "", primary):
                continue
            rel = current_rel  # Use fresh relevance score
        article: dict = {
            "id": id_,
            "title": title,
            "url": url,
            "_source_name": src_name,
            "_source_domain": src_dom,
            "_trust": trust,
            "_relevance": rel or 0.0,
            "_channel": ch,
            "published": pub,
            "_first_seen_at": first_seen,   # PIT timestamp, propagated to export
            "summary": summary,
            "body": body,
            "_ticker": prim_tkr,
            "_tickers": ticker_list,
            "_alt_sources": [a for a in (alt or "").split(",") if a],
        }
        # Recompute quality score with current taxonomy
        if primary:
            try:
                article["_quality_score"] = article_quality_score(article, primary, now=now)
            except Exception:
                article["_quality_score"] = 0.0
        articles.append(article)

    # Per-ticker adaptive gate with two layers:
    #   Layer A (trust): if a ticker has >=2 articles from trust>=2 sources,
    #                    drop all tier-1 (aggregator) articles — keeps large-cap
    #                    feeds clean. Otherwise keep tier-1 as fallback.
    #   Layer B (quality): if primary_gate (Q>=0.45) has <2, relax to 0.40
    #                      for that ticker so mid-caps still get export.
    by_ticker: dict[str, list[dict]] = {}
    for a in articles:
        tkr = (a.get("_ticker") or "").upper()
        by_ticker.setdefault(tkr, []).append(a)

    gated: list[dict] = []
    relaxed_tickers = 0
    aggregator_fallback_tickers = 0
    for tkr, arts in by_ticker.items():
        # Layer A: trust-based aggregator suppression
        high_trust = [a for a in arts if (a.get("_trust") or 0) >= 2]
        tier1_agg = [a for a in arts if (a.get("_trust") or 0) == 1]
        if len(high_trust) >= 2:
            # Enough tier-2/3 coverage — drop tier-1 aggregators for cleanliness
            candidate_pool = high_trust
        else:
            # Mid-cap fallback: include tier-1 aggregators too
            candidate_pool = high_trust + tier1_agg
            if tier1_agg:
                aggregator_fallback_tickers += 1

        # Layer B: quality-score adaptive gate on this ticker's pool
        primary_gate = [a for a in candidate_pool
                        if a.get("_quality_score", 0.0) >= QUALITY_THRESHOLD]
        if len(primary_gate) >= 2 or len(primary_gate) == len(candidate_pool):
            gated.extend(primary_gate)
            continue
        relaxed = [a for a in candidate_pool
                   if a.get("_quality_score", 0.0) >= QUALITY_THRESHOLD_RELAXED]
        if len(relaxed) > len(primary_gate):
            relaxed_tickers += 1
        gated.extend(relaxed)

    # Sort: quality desc, then newest first
    gated.sort(
        key=lambda a: (
            -a.get("_quality_score", 0.0),
            -(datetime.fromisoformat(a["published"]).timestamp()
              if a.get("published") else 0),
        )
    )
    log.info(
        "rolling_window_loaded",
        total=len(rows),
        stale_relevance=stale_relevance,
        stale_junk=stale_junk,
        passed_relevance=len(articles),
        after_quality_gate=len(gated),
        relaxed_tickers=relaxed_tickers,
        aggregator_fallback_tickers=aggregator_fallback_tickers,
        tickers=len(ticker_set),
        hours=hours,
        cutoff=cutoff_iso,
    )
    return gated


def persist_run_artifacts(
    run_dir: Path,
    *,
    run_label: str,
    tickers: list[str],
    hours: int,
    raw_articles: list[dict],
    filtered_articles: list[dict],
    news_items: list[dict],
    channel_stats: dict[str, int],
    new_count: int,
    bz_count: int,
    per_ticker_stats: dict[str, int] | None,
    report_text: str,
    rolling_articles: list[dict] | None = None,
    rolling_news_items: list[dict] | None = None,
) -> None:
    # Run artifacts always reflect THIS run's results (for audit/debug).
    # Stable exports (latest_news_items.json + by_ticker/) use rolling window
    # data when available, so repeated runs accumulate rather than overwrite.
    export_articles = rolling_articles if rolling_articles is not None else filtered_articles
    export_news_items = rolling_news_items if rolling_news_items is not None else news_items

    summary = {
        "run_label": run_label,
        "ticker_codes": tickers,
        "hours": hours,
        "raw_count": len(raw_articles),
        "filtered_count": len(filtered_articles),
        "news_item_count": len(news_items),
        "db_write_count": new_count,
        "benzinga_count": bz_count,
        "channel_stats": dict(channel_stats),
        "per_ticker_stats": per_ticker_stats or {},
        "rolling_window_count": len(export_articles) if rolling_articles is not None else None,
        "artifacts": {
            "raw_articles": "raw_articles.json",
            "filtered_articles": "filtered_articles.json",
            "news_items": "news_items.json",
            "aistock_payload": "aistock_payload.json",
            "by_ticker": "by_ticker/",
            "report": "report.txt",
            "log": "run.log",
        },
    }
    write_json(run_dir / "raw_articles.json", raw_articles)
    write_json(run_dir / "filtered_articles.json", filtered_articles)
    write_json(run_dir / "news_items.json", news_items)
    write_json(run_dir / "summary.json", summary)
    export_articles_by_ticker(
        run_dir=run_dir,
        filtered_articles=export_articles,
        news_items=export_news_items,
    )
    (run_dir / "report.txt").write_text(report_text, encoding="utf-8")


# ── SimHash dedup ─────────────────────────────────────────────────────────────

def _simhash(text: str) -> int:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    stop = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "of", "with", "is", "are", "was", "were", "it", "its",
            "this", "that", "from", "by", "be", "has", "have", "had",
            "not", "no", "stock", "stocks", "shares", "today", "why"}
    tokens = [t for t in tokens if t not in stop and len(t) > 1]
    if not tokens:
        return 0
    v = [0] * 64
    for tok in tokens:
        h = struct.unpack("<Q", hashlib.md5(tok.encode()).digest()[:8])[0]
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    return sum((1 << i) for i in range(64) if v[i] > 0)


def _hamming(a: int, b: int) -> int:
    x = a ^ b
    c = 0
    while x:
        c += x & 1
        x >>= 1
    return c


def _clean_title_for_match(title: str) -> str:
    """Strip source suffix + normalize for fuzzy matching."""
    if " - " in title:
        title = title.rsplit(" - ", 1)[0]
    title = re.sub(r"[^\w\s]", " ", title.lower())
    return re.sub(r"\s+", " ", title).strip()


# Fuzzy title similarity threshold (0-100).
# 85 = tight cluster (same story, different wording).
# Empirically: "Nvidia beats Q3 estimates" vs "NVIDIA crushes earnings" → ~88
# but "Nvidia drops" vs "Nvidia jumps" → ~55 (kept separate).
FUZZY_TITLE_THRESHOLD = 85


def dedup_articles(articles: list[dict]) -> list[dict]:
    """Deduplicate near-identical articles and cluster same-event stories.

    Two-pass approach:
    1. SimHash (Hamming ≤ 8) — catches near-identical titles (structural)
    2. Fuzzy token-set ratio (rapidfuzz) — catches rewrites of same event

    Keeps highest-trust version; other sources become alt_sources, driving
    the source_diversity signal in the quality scorer.
    """
    articles.sort(key=lambda a: (-a.get("_trust", 0), -a.get("_relevance", 0)))
    kept = []
    hashes: list[tuple[int, int]] = []  # (simhash, index_in_kept)
    clean_titles: list[str] = []  # parallel to kept — normalized titles for fuzzy pass

    for article in articles:
        raw_title = article["title"]
        plain_title = raw_title.rsplit(" - ", 1)[0] if " - " in raw_title else raw_title
        clean = _clean_title_for_match(raw_title)

        # Pass 1: SimHash (structural near-duplicate)
        sh = _simhash(plain_title)
        dup_idx: int | None = None
        for eh, idx in hashes:
            if _hamming(sh, eh) <= 8:
                dup_idx = idx
                break

        # Pass 2: Fuzzy title clustering (semantic same-event)
        # Only run if SimHash didn't already match, and only if rapidfuzz is available.
        if dup_idx is None and _RAPIDFUZZ_AVAILABLE and clean:
            for idx, existing_clean in enumerate(clean_titles):
                if not existing_clean:
                    continue
                # token_set_ratio ignores word order and duplication,
                # good for paraphrased headlines.
                similarity = _rf_fuzz.token_set_ratio(clean, existing_clean)
                if similarity >= FUZZY_TITLE_THRESHOLD:
                    dup_idx = idx
                    break

        if dup_idx is not None:
            alt = article.get("_source_name", "")
            if alt and alt not in kept[dup_idx].get("_alt_sources", []):
                kept[dup_idx].setdefault("_alt_sources", []).append(alt)
            continue

        kept.append(article)
        hashes.append((sh, len(kept) - 1))
        clean_titles.append(clean)

    return kept


# ── Full quality pipeline ─────────────────────────────────────────────────────

def quality_filter(articles: list[dict], ticker: str, hours: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    now = datetime.now(timezone.utc)
    passed = []

    for a in articles:
        title = a["title"]
        summary = a.get("summary", "") or ""
        clean_title = title.rsplit(" - ", 1)[0] if " - " in title else title

        # ── Hard pre-filters (fast, binary) ──────────────────────────────

        # ① Time recency (hard cutoff)
        if a.get("published"):
            try:
                pub = datetime.fromisoformat(a["published"])
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
                if pub < cutoff:
                    continue
            except Exception:
                pass

        # ② Source trust (0 = blocked)
        trust = a.get("_trust", 1)
        if trust == 0:
            continue

        if _looks_like_quote_or_instrument_page(clean_title, summary, ticker):
            continue
        if _has_conflicting_exchange_reference(clean_title, summary, ticker):
            continue

        # ③ Relevance (must be about our ticker)
        # Per-ticker RSS feeds (Yahoo/Nasdaq /rss/headline?s=TICKER) use a
        # much lower relevance gate because the upstream attribution is
        # authoritative — the ticker is in the URL path, so the article is
        # guaranteed to be about this ticker even if the title doesn't
        # repeat the name 5 times.
        #
        # Google News proxy channels (google_reuters, google_bloomberg,
        # google_cnbc) are NOT in this category — investigation shows Google
        # often returns body-match tangential articles (e.g. "Apple's AI age"
        # appears for NVDA query because Nvidia is mentioned in the body
        # about chips). These need standard relevance filtering.
        channel = a.get("_channel", "")
        # finnhub_api_general has no ticker attribution — skip ticker-
        # relevance check but still enforce junk/length/quality gates below.
        if channel == "finnhub_api_general":
            a["_relevance"] = 0.30
        elif channel == "wire_tripwire":
            # Attribution is authoritative (matched against the AIStock company-
            # alias index from the press-release headline) AND the item already
            # passed the high-value event gate. NewsCrawler's own relevance_score
            # uses a smaller alias table and would wrongly score 0 (e.g. "Rocket
            # Lab" not in COMPANY_NAMES). Bypass the relevance gate; assign a
            # strong floor so it competes in the quality score.
            a["_relevance"] = max(a.get("_relevance", 0.0), 0.55)
        elif channel == "ibkr_news":
            # Dow Jones' own editorial conid tagging — authoritative attribution
            # like the per-ticker RSS feeds, but DJ wire headlines often don't
            # repeat the company name ("VP Papermaster Sells 6,000 Of ..."),
            # so relevance_score would wrongly zero them. No hard gate; floor
            # 0.30 (weaker than wire_tripwire's 0.55 — DJ also peer-tags
            # sector stories) and let the 7-signal quality score gate.
            a["_relevance"] = max(relevance_score(clean_title, summary, ticker), 0.30)
        elif channel == "sec_edgar":
            # Attribution comes from the filing's own issuer/subject CIK mapped
            # through SEC's company_tickers.json — the strongest attribution in
            # the system (the regulator's primary record, not a media mention).
            # Bypass the relevance gate like wire_tripwire; same 0.55 floor.
            a["_relevance"] = max(a.get("_relevance", 0.0), 0.55)
        else:
            rel = relevance_score(clean_title, summary, ticker)
            # finnhub_api is per-ticker queried (ticker in API URL param), so
            # authoritative ticker attribution — same treatment as yahoo/nasdaq
            # per-ticker RSS feeds.
            per_ticker_feed = channel in ("yahoo_finance_rss", "nasdaq_rss", "finnhub_api", "ir_feed", "amd_ir", "wire_tripwire")
            relevance_gate = 0.02 if per_ticker_feed else 0.10
            if rel < relevance_gate:
                continue
            if channel == "benzinga_rss" and rel < 0.55:
                continue
            # Boost: per-ticker feeds get a floor of 0.30 on relevance so they
            # compete fairly in the quality_score computation.
            if per_ticker_feed and rel < 0.30:
                rel = 0.30
            a["_relevance"] = rel

        # ④ Junk title
        if is_junk(clean_title):
            continue

        # ⑤ Weak-signal investor/holdings/profile content.
        # ibkr_news is exempt: the patterns target tier-1 aggregator 13F churn
        # ("XYZ Capital sells 6,000 shares of…"), but the same phrasing on the
        # DJ wire is a curated insider/stake story (Form 4, activist) — the
        # exact content the 2026-07 evaluation showed this channel uniquely
        # surfaces. DJ's editorial selection is the noise filter here.
        # sec_edgar is exempt for the same reason: a Form 4 / 13D straight from
        # the regulator IS the insider/stake signal, not aggregator churn.
        if channel not in ("ibkr_news", "sec_edgar") and is_weak_signal(clean_title, a.get("_source_domain", "")):
            continue

        # ⑥ Title too short
        if len(clean_title) < 25:
            continue

        # ── Composite quality score ──────────────────────────────────────
        # ⑦ Multi-signal quality scoring (7 signals, weighted)
        qscore = article_quality_score(a, ticker, now=now)
        a["_quality_score"] = qscore

        passed.append(a)

    log.info("quality_filter", input=len(articles), pre_filter=len(passed))

    # ⑧ Dedup (preserves alt_sources for source_diversity signal)
    deduped = dedup_articles(passed)
    log.info("dedup", before=len(passed), after=len(deduped))

    # ⑨ Post-dedup: recompute quality score with source_diversity signal
    for a in deduped:
        a["_quality_score"] = article_quality_score(a, ticker, now=now)

    # ⑩ Quality gate — drop below threshold
    threshold = QUALITY_THRESHOLD
    gated = [a for a in deduped if a["_quality_score"] >= threshold]

    # Adaptive: if too few pass, relax threshold for this ticker
    if len(gated) < 2 and len(deduped) > len(gated):
        gated = [a for a in deduped if a["_quality_score"] >= QUALITY_THRESHOLD_RELAXED]
        if gated:
            log.info("quality_gate_relaxed", ticker=ticker, threshold=QUALITY_THRESHOLD_RELAXED, passed=len(gated))

    log.info("quality_gate", ticker=ticker, before_gate=len(deduped), after_gate=len(gated), threshold=threshold)

    # ⑪ Sort: quality_score desc (primary), then newest first (tiebreak)
    gated.sort(key=lambda a: (
        -a.get("_quality_score", 0),
        -(datetime.fromisoformat(a["published"]).timestamp()
          if a.get("published") else 0),
    ))

    return gated


def is_high_value_article(article: dict) -> bool:
    title = article.get("title", "")
    summary = article.get("summary", "") or ""
    text = f"{title} {summary}".lower()
    trust = article.get("_trust", 1)
    relevance = article.get("_relevance", 0.0)
    channel = article.get("_channel", "")
    source_domain = article.get("_source_domain", "")

    if trust >= 3 and relevance >= 0.55:
        return True
    if channel in ("ir_feed", "amd_ir"):  # company IR press releases — always high-value
        return True
    if any(keyword in text for keyword in NEWS_CONTENT_KEYWORDS):
        return True
    if source_domain in {"reuters.com", "bloomberg.com", "wsj.com", "cnbc.com", "marketwatch.com"} and relevance >= 0.35:
        return True
    return False


def select_articles_for_fulltext(
    articles: list[dict],
    *,
    mode: str = DEFAULT_FULLTEXT_MODE,
    max_articles: int = DEFAULT_FULLTEXT_MAX_ARTICLES,
) -> list[dict]:
    if mode == "off":
        return []
    if mode == "all":
        return articles[:]

    selected: list[dict] = []
    per_ticker_count: dict[str, int] = defaultdict(int)
    for article in articles:
        # sec_edgar URLs point at EDGAR filing *index* pages — trafilatura
        # would extract the document-list table, not a story body. The
        # synthetic title + summary already carry the signal; skip fulltext.
        if article.get("_channel") == "sec_edgar":
            continue
        if not is_high_value_article(article):
            continue
        tickers = article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])
        primary = tickers[0] if tickers else "_untagged"
        if per_ticker_count[primary] >= max_articles:
            continue
        per_ticker_count[primary] += 1
        selected.append(article)
    return selected


# ══════════════════════════════════════════════════════════════════════════════
# PARSING
# ══════════════════════════════════════════════════════════════════════════════

def clean_text(text: str) -> str:
    text = html_lib.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_article_text(html: str) -> str:
    """Extract main article body from raw HTML.

    Uses trafilatura (industry-standard main-content extractor) when available,
    falling back to a regex-based heuristic that selects the largest
    article/main/section/div containing paragraph tags.
    """
    # Primary path: trafilatura — handles paywalls, boilerplate, sidebars,
    # cookie notices, related-articles widgets much better than regex.
    if _TRAFILATURA_AVAILABLE:
        try:
            text = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=False,
                include_images=False,
                include_links=False,
                deduplicate=True,
                favor_precision=True,  # prefer clean extraction over recall
                no_fallback=False,
            )
            if text and len(text) >= 200:
                if len(text) > ARTICLE_MAX_BODY_CHARS:
                    text = text[:ARTICLE_MAX_BODY_CHARS].rsplit(" ", 1)[0].rstrip() + "..."
                return text
        except Exception as exc:
            log.debug("trafilatura_failed", error=str(exc)[:100])
            # Fall through to legacy extractor

    # Fallback: legacy regex-based extractor
    html = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", html)
    html = re.sub(r"(?is)<noscript\b[^>]*>.*?</noscript>", " ", html)

    candidates = re.findall(
        r"(?is)<(article|main|section|div)\b[^>]*(?:class|id)\s*=\s*[\"'][^\"']*"
        r"(?:article|content|story|post|entry|body|main)[^\"']*[\"'][^>]*>(.*?)</\1>",
        html,
    )
    candidate_html = max((fragment for _, fragment in candidates), key=len, default=html)

    paragraphs = re.findall(r"(?is)<p\b[^>]*>(.*?)</p>", candidate_html)
    parts: list[str] = []
    for paragraph in paragraphs:
        text = clean_text(paragraph)
        if len(text) < 40:
            continue
        lowered = text.lower()
        if lowered.startswith(("advertisement", "subscribe", "read more", "click here")):
            continue
        parts.append(text)

    if not parts:
        text = clean_text(candidate_html)
        sentences = re.split(r"(?<=[.!?])\s+", text)
        parts = [sentence.strip() for sentence in sentences if len(sentence.strip()) >= 60]

    body = "\n\n".join(parts)
    if len(body) > ARTICLE_MAX_BODY_CHARS:
        body = body[:ARTICLE_MAX_BODY_CHARS].rsplit(" ", 1)[0].rstrip() + "..."
    return body


def _extract_google_news_base64(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if parsed.netloc != "news.google.com" or len(parts) < 2:
        return None
    if parts[-2] not in {"articles", "read"}:
        return None
    return parts[-1]


def _extract_google_decode_attrs(html: str) -> tuple[str | None, str | None]:
    match = re.search(r'data-n-a-sg="([^"]+)"[^>]*data-n-a-ts="([^"]+)"', html)
    if match:
        return match.group(1), match.group(2)
    match = re.search(r'data-n-a-ts="([^"]+)"[^>]*data-n-a-sg="([^"]+)"', html)
    if match:
        return match.group(2), match.group(1)
    return None, None


def parse_rss(body: bytes, source_tag: str, ticker: str) -> list[dict]:
    feed = feedparser.parse(body)
    articles = []

    for entry in feed.entries:
        title = clean_text(entry.get("title", ""))
        link = entry.get("link", "")
        summary = clean_text(entry.get("summary", "") or entry.get("description", ""))
        if len(summary) > 500:
            summary = summary[:500] + "..."

        published = None
        if entry.get("published_parsed"):
            try:
                published = datetime.fromtimestamp(
                    mktime(entry.published_parsed), tz=timezone.utc
                ).isoformat()
            except Exception:
                published = entry.get("published")

        source_name, source_domain = _extract_real_source(title, link)
        trust = get_trust(source_name, source_domain)

        # Channel-specific overrides — structured feeds have a known publisher,
        # so we don't need to infer source from the title suffix.
        if source_tag == "benzinga_rss":
            source_name = "Benzinga"
            source_domain = "benzinga.com"
            trust = 3
        elif source_tag in ("ir_feed", "amd_ir"):
            # Company IR press release feed — primary source, authoritative
            # attribution (feed belongs to exactly one company). Derive the
            # domain from the article link so DOMAIN_TO_AISTOCK_SOURCE and
            # per-domain rate limiting still work per company.
            ir_domain = urlparse(link).netloc.lower() if link else "investor-relations"
            source_name = f"{ticker} Investor Relations" if ticker else "Investor Relations"
            source_domain = ir_domain or "investor-relations"
            trust = 3
        elif source_tag == "yahoo_finance_rss":
            source_name = "Yahoo Finance"
            source_domain = "finance.yahoo.com"
            trust = 3
        elif source_tag == "nasdaq_rss":
            source_name = "Nasdaq"
            source_domain = "nasdaq.com"
            trust = 2
        elif source_tag in ("seeking_alpha_rss", "google_seekingalpha"):
            source_name = "Seeking Alpha"
            source_domain = "seekingalpha.com"
            trust = 3
        elif source_tag == "google_reuters":
            source_name = "Reuters"
            source_domain = "reuters.com"
            trust = 3
        elif source_tag == "google_bloomberg":
            source_name = "Bloomberg"
            source_domain = "bloomberg.com"
            trust = 3
        elif source_tag == "google_cnbc":
            source_name = "CNBC"
            source_domain = "cnbc.com"
            trust = 3
        elif source_tag == "google_investing":
            source_name = "Investing.com"
            source_domain = "investing.com"
            trust = 2
        elif source_tag == "wire_tripwire":
            # Primary press-release wire (PR Newswire / GlobeNewswire). Derive
            # the real publisher domain from the link; trust=3 (company's own
            # authoritative announcement, hard-source tier in AIStock).
            wdom = urlparse(link).netloc.lower().lstrip("www.") if link else "prnewswire.com"
            source_domain = wdom or "prnewswire.com"
            source_name = "GlobeNewswire" if "globenewswire" in wdom else "PR Newswire"
            trust = 3
        elif source_tag == "pr_newswire":
            source_name = "PR Newswire"
            source_domain = "prnewswire.com"
            trust = 3
        elif source_tag == "globenewswire":
            source_name = "GlobeNewswire"
            source_domain = "globenewswire.com"
            trust = 3
        elif source_tag == "marketwatch_top":
            source_name = "MarketWatch"
            source_domain = "marketwatch.com"
            trust = 3
        elif source_tag == "cnbc_finance":
            source_name = "CNBC"
            source_domain = "cnbc.com"
            trust = 3

        articles.append({
            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, link or title)),
            "title": title,
            "url": link,
            "_source_name": source_name,
            "_source_domain": source_domain,
            "_trust": trust,
            "_channel": source_tag,
            "_ticker": ticker,
            "_tickers": [ticker],
            "published": published,
            "summary": summary,
        })

    return articles


# ══════════════════════════════════════════════════════════════════════════════
# FETCHER
# ══════════════════════════════════════════════════════════════════════════════

# ── Per-domain circuit breaker ────────────────────────────────────────────
# When a domain returns too many consecutive failures, pause it for a while
# instead of burning time on every subsequent request. This is important when
# Google News starts returning 503s — continuing to hammer it makes the
# rate-limit worse and produces cascading empty results.

_DOMAIN_FAILURES: dict[str, int] = {}
_DOMAIN_PAUSED_UNTIL: dict[str, float] = {}
_CB_FAILURE_THRESHOLD = 5     # consecutive failures before tripping
_CB_PAUSE_SECONDS = 60        # pause duration when tripped


def _circuit_breaker_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def _circuit_breaker_is_open(url: str) -> bool:
    import time as _t
    domain = _circuit_breaker_domain(url)
    pause_until = _DOMAIN_PAUSED_UNTIL.get(domain, 0)
    if pause_until and _t.monotonic() < pause_until:
        return True
    if pause_until:
        # pause expired, reset
        _DOMAIN_PAUSED_UNTIL.pop(domain, None)
        _DOMAIN_FAILURES[domain] = 0
    return False


def _circuit_breaker_record_failure(url: str) -> None:
    import time as _t
    domain = _circuit_breaker_domain(url)
    _DOMAIN_FAILURES[domain] = _DOMAIN_FAILURES.get(domain, 0) + 1
    if _DOMAIN_FAILURES[domain] >= _CB_FAILURE_THRESHOLD:
        _DOMAIN_PAUSED_UNTIL[domain] = _t.monotonic() + _CB_PAUSE_SECONDS
        log.warning(
            "circuit_breaker_tripped",
            domain=domain,
            consecutive_failures=_DOMAIN_FAILURES[domain],
            pause_seconds=_CB_PAUSE_SECONDS,
        )


def _circuit_breaker_record_success(url: str) -> None:
    domain = _circuit_breaker_domain(url)
    if _DOMAIN_FAILURES.get(domain, 0) > 0:
        _DOMAIN_FAILURES[domain] = 0


# Default aiohttp's max_line_size = max_field_size = 8190 bytes. Yahoo Finance
# article pages ship >8KB Set-Cookie headers, causing 400 "Header value is too
# long". Bump to 32KB — low memory cost, eliminates a class of spurious errors.
AIOHTTP_MAX_LINE_SIZE = 32 * 1024
AIOHTTP_MAX_FIELD_SIZE = 32 * 1024


def _make_client_session(
    *, connector_limit: int = 20, ttl_dns_cache: int = 300
) -> aiohttp.ClientSession:
    """Construct an aiohttp ClientSession with generous header-size limits
    and a sensible connector, used throughout the crawler."""
    connector = aiohttp.TCPConnector(
        limit=connector_limit,
        ttl_dns_cache=ttl_dns_cache,
    )
    return aiohttp.ClientSession(
        connector=connector,
        # Accept longer response headers (Yahoo Finance ships huge Set-Cookie)
        max_line_size=AIOHTTP_MAX_LINE_SIZE,
        max_field_size=AIOHTTP_MAX_FIELD_SIZE,
    )


# ── HTTP fetch with retry / backoff ────────────────────────────────────────
FETCH_MAX_RETRIES = 3
FETCH_BASE_BACKOFF = 2.0      # seconds; doubled each retry


# Per-run counters for HTTP conditional-GET cache effectiveness.
_CONDITIONAL_GET_HITS_304 = 0
_CONDITIONAL_GET_REQUESTS = 0


def _reset_http_cache_counters() -> None:
    global _CONDITIONAL_GET_HITS_304, _CONDITIONAL_GET_REQUESTS
    _CONDITIONAL_GET_HITS_304 = 0
    _CONDITIONAL_GET_REQUESTS = 0


def _feed_state_get(url: str) -> dict | None:
    """Return last-seen ETag / Last-Modified / sha256 for a feed URL."""
    if not url:
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            row = conn.execute(
                "SELECT etag, last_modified, sha256 FROM feed_state WHERE url = ?",
                (url,),
            ).fetchone()
            if not row:
                return None
            return {"etag": row[0], "last_modified": row[1], "sha256": row[2]}
        finally:
            conn.close()
    except Exception:
        return None


def _feed_state_store(url: str, etag: str | None, last_modified: str | None,
                      sha256: str | None, status: int) -> None:
    if not url:
        return
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """INSERT INTO feed_state (url, etag, last_modified, sha256,
                                          last_fetched_at, last_status, cache_hits_304)
                   VALUES (?, ?, ?, ?, ?, ?, 0)
                   ON CONFLICT(url) DO UPDATE SET
                     etag = excluded.etag,
                     last_modified = excluded.last_modified,
                     sha256 = excluded.sha256,
                     last_fetched_at = excluded.last_fetched_at,
                     last_status = excluded.last_status""",
                (url, etag, last_modified, sha256, now_iso, status),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def _feed_state_increment_304(url: str) -> None:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            conn.execute(
                "UPDATE feed_state SET cache_hits_304 = cache_hits_304 + 1 WHERE url = ?",
                (url,),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


async def fetch_rss_with_conditional_get(
    session: aiohttp.ClientSession, url: str,
) -> tuple[bytes | None, bool]:
    """Fetch RSS feed with HTTP conditional GET (If-None-Match / If-Modified-Since).

    Returns (body, unchanged):
      - (bytes, False): new content, parse it
      - (None, True):   server returned 304, OR content hash matches last fetch
                        → skip parse, rely on rolling window for existing articles
      - (None, False):  network error, treat as miss
    """
    global _CONDITIONAL_GET_HITS_304, _CONDITIONAL_GET_REQUESTS
    _CONDITIONAL_GET_REQUESTS += 1

    state = _feed_state_get(url) or {}
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.5",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }
    if state.get("etag"):
        headers["If-None-Match"] = state["etag"]
    if state.get("last_modified"):
        headers["If-Modified-Since"] = state["last_modified"]

    if _circuit_breaker_is_open(url):
        return None, False

    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            status = resp.status
            if status == 304:
                _CONDITIONAL_GET_HITS_304 += 1
                _feed_state_increment_304(url)
                _circuit_breaker_record_success(url)
                log.info("fetch_304", url=url[:90])
                return None, True

            if status == 200:
                body = await resp.read()
                # Compare sha256 with last-fetched — if identical, content
                # didn't actually change even though no ETag was provided.
                body_sha = hashlib.sha256(body).hexdigest()
                _circuit_breaker_record_success(url)
                if state.get("sha256") == body_sha:
                    _CONDITIONAL_GET_HITS_304 += 1
                    _feed_state_increment_304(url)
                    log.info("fetch_unchanged_sha", url=url[:90])
                    _feed_state_store(url, resp.headers.get("ETag"),
                                      resp.headers.get("Last-Modified"),
                                      body_sha, 200)
                    return None, True
                # New content — store fresh state
                _feed_state_store(url, resp.headers.get("ETag"),
                                  resp.headers.get("Last-Modified"),
                                  body_sha, 200)
                log.info("fetch", url=url[:90], status=200)
                return body, False

            # Non-success — fall back to the regular fetch_url retry path
            log.info("fetch_rss_non200_fallback", url=url[:90], status=status)
    except Exception as exc:
        log.debug("fetch_rss_conditional_failed", url=url[:90], error=str(exc)[:80])

    # Fallback: use the full-featured fetch_url with retry
    body = await fetch_url(session, url)
    if body:
        try:
            body_sha = hashlib.sha256(body).hexdigest()
            _feed_state_store(url, None, None, body_sha, 200)
        except Exception:
            pass
    return body, False


async def fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    extra_headers: dict[str, str] | None = None,
) -> bytes | None:
    """Fetch a URL with exponential-backoff retry on transient errors.

    Retries on:
      - 429 (rate limited) — respects Retry-After if provided
      - 503 (service unavailable) — common Google News rate-limit response
      - 502/504 (bad gateway / gateway timeout)
      - asyncio.TimeoutError / connection reset

    Gives up immediately on 200 (success), 4xx non-429 (client error,
    e.g. 403 paywall), and anything else deterministic.

    Trips a per-domain circuit breaker after `_CB_FAILURE_THRESHOLD` consecutive
    failures so we stop hammering a domain that's down.
    """
    if _circuit_breaker_is_open(url):
        log.debug("circuit_breaker_skip", url=url[:90])
        return None

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }
    if extra_headers:
        # e.g. SEC EDGAR requires a declared UA with contact info instead of
        # the browser-imitating default (unidentified bots get blocked).
        headers.update(extra_headers)

    last_err = None
    for attempt in range(FETCH_MAX_RETRIES):
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                status = resp.status
                if status == 200:
                    _circuit_breaker_record_success(url)
                    log.info("fetch", url=url[:90], status=status)
                    return await resp.read()

                # Retryable statuses
                if status in (429, 502, 503, 504):
                    # Respect Retry-After header if present (common for 429/503)
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after else None
                    except (TypeError, ValueError):
                        wait_s = None
                    if wait_s is None:
                        wait_s = FETCH_BASE_BACKOFF * (2 ** attempt)
                    wait_s = min(wait_s, 30.0)  # cap backoff
                    log.info(
                        "fetch_retry",
                        url=url[:90],
                        status=status,
                        attempt=attempt + 1,
                        wait=wait_s,
                    )
                    if attempt + 1 < FETCH_MAX_RETRIES:
                        await asyncio.sleep(wait_s)
                        continue
                    # Final attempt still failed
                    _circuit_breaker_record_failure(url)
                    log.warning("fetch_rate_limited", url=url[:90], status=status)
                    return None

                # Non-retryable (4xx except 429, unknown)
                log.info("fetch", url=url[:90], status=status)
                return None

        except asyncio.TimeoutError:
            last_err = "TimeoutError"
            retryable = True
        except aiohttp.ClientConnectorError as exc:
            last_err = f"ClientConnectorError: {exc}"
            retryable = True
        except aiohttp.ClientPayloadError as exc:
            last_err = f"ClientPayloadError: {exc}"
            retryable = True
        except aiohttp.ServerDisconnectedError as exc:
            last_err = f"ServerDisconnectedError: {exc}"
            retryable = True
        except aiohttp.ClientResponseError as exc:
            # 4xx from response parsing (e.g. Yahoo's oversized cookies →
            # 400 "Header value is too long"). These never succeed on retry.
            last_err = f"ClientResponseError[{exc.status}]: {str(exc)[:120]}"
            retryable = exc.status in (429, 502, 503, 504)
        except aiohttp.ClientError as exc:
            last_err = f"ClientError[{type(exc).__name__}]: {exc}"
            retryable = True
        except Exception as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            retryable = False

        # Only retry transient errors
        if retryable and attempt + 1 < FETCH_MAX_RETRIES:
            wait_s = FETCH_BASE_BACKOFF * (2 ** attempt)
            log.info(
                "fetch_retry",
                url=url[:90],
                error=(last_err or "unknown")[:120],
                attempt=attempt + 1,
                wait=wait_s,
            )
            await asyncio.sleep(wait_s)
        elif not retryable:
            # Non-retryable — bail out immediately
            log.warning(
                "fetch_giveup",
                url=url[:90],
                error=(last_err or "unknown")[:160],
                reason="not_retryable",
            )
            return None

    # All retries exhausted
    _circuit_breaker_record_failure(url)
    log.error(
        "fetch_error",
        url=url[:90],
        error=(last_err or "unknown")[:160],
        attempts=FETCH_MAX_RETRIES,
    )
    return None


# Per-process URL → extracted body cache. Reset at each crawl_watchlist run.
# Prevents fetching the same article body multiple times when the article
# is shared across many tickers (e.g. "Top analyst upgrades: BAC, BX, COP,
# CORR, FB, GM" — one article × 6 tickers = 6 redundant fetches without cache).
_BODY_CACHE: dict[str, str | None] = {}
_BODY_CACHE_HITS = 0
_BODY_CACHE_MISSES = 0
_BODY_CACHE_SQLITE_HITS = 0   # cross-run SQLite cache hits

# Per-process Google News URL → resolved URL cache. Each Google News URL
# decode involves 2-3 extra HTTP requests (fetch decode page + POST to
# batchexecute). Caching saves that overhead when the same Google URL
# appears in multiple tickers' candidate lists.
_GOOGLE_URL_CACHE: dict[str, str | None] = {}
_GOOGLE_URL_HITS = 0
_GOOGLE_URL_SQLITE_HITS = 0   # cross-run SQLite cache hits

# How long to trust a cached body before re-fetching. Articles rarely change
# after publication so 7 days is conservative; bump to 30 if storage is cheap.
FULLTEXT_CACHE_MAX_AGE_DAYS = 7


def _reset_body_cache() -> None:
    global _BODY_CACHE_HITS, _BODY_CACHE_MISSES, _GOOGLE_URL_HITS
    global _BODY_CACHE_SQLITE_HITS, _GOOGLE_URL_SQLITE_HITS
    _BODY_CACHE.clear()
    _GOOGLE_URL_CACHE.clear()
    _BODY_CACHE_HITS = 0
    _BODY_CACHE_MISSES = 0
    _BODY_CACHE_SQLITE_HITS = 0
    _GOOGLE_URL_HITS = 0
    _GOOGLE_URL_SQLITE_HITS = 0


# ── Cross-run SQLite caches ───────────────────────────────────────────────

def _fulltext_cache_lookup(url: str) -> str | None:
    """Return cached body if URL was successfully extracted within max_age days."""
    if not url:
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            cutoff = (datetime.now(timezone.utc)
                      - timedelta(days=FULLTEXT_CACHE_MAX_AGE_DAYS)).isoformat()
            row = conn.execute(
                "SELECT body FROM fulltext_cache WHERE url = ? AND cached_at >= ?",
                (url, cutoff),
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _fulltext_cache_store(url: str, body: str) -> None:
    if not url or not body:
        return
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            h = hashlib.sha256(body.encode("utf-8")).hexdigest()
            conn.execute(
                """INSERT OR REPLACE INTO fulltext_cache
                   (url, body, extractor, content_hash, cached_at)
                   VALUES (?, ?, 'trafilatura', ?, ?)""",
                (url, body, h, now_iso),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        log.debug("fulltext_cache_store_failed", error=str(exc)[:80])


def _url_resolution_lookup(encoded_url: str) -> str | None:
    if not encoded_url:
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            row = conn.execute(
                "SELECT decoded_url FROM url_resolution WHERE encoded_url = ?",
                (encoded_url,),
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()
    except Exception:
        return None


def _url_resolution_store(encoded_url: str, decoded_url: str) -> None:
    if not encoded_url or not decoded_url:
        return
    try:
        conn = sqlite3.connect(str(DB_PATH))
        try:
            conn.execute(
                """INSERT OR REPLACE INTO url_resolution
                   (encoded_url, decoded_url, decoded_at) VALUES (?, ?, ?)""",
                (encoded_url, decoded_url, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


async def fetch_article_body(session: aiohttp.ClientSession, article: dict) -> None:
    """Fetch full article body with three layers of caching:

      1. In-run dict cache (_BODY_CACHE) — same article seen across tickers
         in this run is only fetched once.
      2. Cross-run SQLite cache (fulltext_cache table) — body extracted in
         previous runs within FULLTEXT_CACHE_MAX_AGE_DAYS is reused without
         hitting network or running trafilatura.
      3. Network fetch + trafilatura extraction — fallback for new URLs.
    """
    global _BODY_CACHE_HITS, _BODY_CACHE_MISSES, _BODY_CACHE_SQLITE_HITS
    url = article.get("url") or ""
    if not url:
        return
    if "benzinga.com" in (article.get("_source_domain") or ""):
        return

    # Layer 1: in-run cache
    if url in _BODY_CACHE:
        _BODY_CACHE_HITS += 1
        cached = _BODY_CACHE[url]
        if cached is not None:
            article["body"] = cached
        return

    # Layer 2: cross-run SQLite cache
    sqlite_body = _fulltext_cache_lookup(url)
    if sqlite_body:
        _BODY_CACHE_SQLITE_HITS += 1
        article["body"] = sqlite_body
        _BODY_CACHE[url] = sqlite_body  # populate in-run cache too
        return

    # Layer 3: network fetch + extraction
    _BODY_CACHE_MISSES += 1
    body_bytes = await fetch_url(session, url)
    if not body_bytes:
        _BODY_CACHE[url] = None
        return

    try:
        html = body_bytes.decode("utf-8", errors="ignore")
    except Exception:
        _BODY_CACHE[url] = None
        return

    body = extract_article_text(html)
    if len(body) < 200:
        _BODY_CACHE[url] = None
        return
    article["body"] = body
    _BODY_CACHE[url] = body
    # Persist to cross-run cache for future runs
    _fulltext_cache_store(url, body)


async def decode_google_news_url(session: aiohttp.ClientSession, url: str) -> str | None:
    base64_str = _extract_google_news_base64(url)
    if not base64_str:
        return None

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.5",
    }
    signature = None
    timestamp = None

    for candidate in (
        f"https://news.google.com/articles/{base64_str}",
        f"https://news.google.com/rss/articles/{base64_str}",
    ):
        try:
            async with session.get(candidate, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    continue
                html = await resp.text(errors="ignore")
        except Exception:
            continue

        signature, timestamp = _extract_google_decode_attrs(html)
        if signature and timestamp:
            break

    if not signature or not timestamp:
        return None

    payload = [
        "Fbv4je",
        (
            '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,'
            f'null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"{base64_str}",{timestamp},"{signature}"]'
        ),
    ]
    post_headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "User-Agent": USER_AGENT,
    }

    try:
        async with session.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            headers=post_headers,
            data=f"f.req={quote(json.dumps([[payload]]))}",
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                return None
            text = await resp.text(errors="ignore")
    except Exception:
        return None

    try:
        parsed = json.loads(text.split("\n\n", 1)[1])[:-2]
        return json.loads(parsed[0][2])[1]
    except Exception:
        return None


async def resolve_article_url(session: aiohttp.ClientSession, article: dict) -> None:
    """Resolve `news.google.com/...` URLs to their real target.

    Three layers (analogous to fetch_article_body):
      1. In-run dict (_GOOGLE_URL_CACHE)
      2. Cross-run SQLite (url_resolution table) — deterministic mapping,
         no TTL needed.
      3. Network: 2-3 HTTP requests to news.google.com to decode the base64.
    """
    global _GOOGLE_URL_HITS, _GOOGLE_URL_SQLITE_HITS
    url = article.get("url") or ""
    if "news.google.com" not in url:
        return

    # Layer 1: in-run cache
    if url in _GOOGLE_URL_CACHE:
        _GOOGLE_URL_HITS += 1
        decoded = _GOOGLE_URL_CACHE[url]
    else:
        # Layer 2: cross-run SQLite cache
        decoded = _url_resolution_lookup(url)
        if decoded:
            _GOOGLE_URL_SQLITE_HITS += 1
            _GOOGLE_URL_CACHE[url] = decoded
        else:
            # Layer 3: network decode
            decoded = await decode_google_news_url(session, url)
            _GOOGLE_URL_CACHE[url] = decoded
            if decoded:
                _url_resolution_store(url, decoded)

    if decoded:
        article["url"] = decoded
        article["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, decoded or article.get("title", "")))


async def enrich_articles_with_bodies(articles: list[dict]) -> None:
    if not articles:
        return

    semaphore = asyncio.Semaphore(ARTICLE_FETCH_CONCURRENCY)

    async with _make_client_session(connector_limit=ARTICLE_FETCH_CONCURRENCY) as session:
        async def enrich(article: dict) -> None:
            async with semaphore:
                await resolve_article_url(session, article)
                url = article.get("url") or ""
                if url:
                    await _rate_limiter.wait(url)
                await fetch_article_body(session, article)

        await asyncio.gather(*(enrich(article) for article in articles))


# ══════════════════════════════════════════════════════════════════════════════
# STORAGE
# ══════════════════════════════════════════════════════════════════════════════

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    legacy_path = LEGACY_DB_PATH
    current_path = DB_PATH
    if not current_path.exists() and legacy_path.exists():
        legacy_path.replace(current_path)
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript("""
        -- Core news table — main store. `first_seen_at` is the PIT-correct
        -- timestamp (when we first ingested this URL); `published` is the
        -- source's claimed publication time and may be unreliable / backfilled.
        --
        -- BITEMPORAL MODEL:
        --   published      = valid_time (source claim, may be inaccurate)
        --   first_seen_at  = transaction_time (when we first observed it; immutable)
        --   fetched_at     = last refresh time (updates on each crawl)
        --
        -- All backtest queries MUST filter by first_seen_at, NEVER by published,
        -- otherwise look-ahead bias.
        CREATE TABLE IF NOT EXISTS news (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_domain TEXT,
            trust_tier INTEGER DEFAULT 1,
            relevance REAL DEFAULT 0.0,
            channel TEXT,
            published TEXT,
            summary TEXT,
            body TEXT,
            primary_ticker TEXT,
            tickers TEXT,
            alt_sources TEXT,
            fetched_at TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,        -- PIT transaction time
            content_hash TEXT,                  -- sha256(title + summary + body)
            classifier_version TEXT,            -- last classifier that touched this row
            UNIQUE(url)
        );

        -- Content version history: when an article URL's content changes
        -- (title or body), keep the old version. Most rows never have a v2.
        CREATE TABLE IF NOT EXISTS article_versions (
            article_id   TEXT NOT NULL,
            version      INTEGER NOT NULL,
            title        TEXT,
            summary      TEXT,
            body         TEXT,
            content_hash TEXT NOT NULL,
            observed_at  TEXT NOT NULL,
            PRIMARY KEY (article_id, version)
        );

        -- Cross-run cache: full-article body extraction (trafilatura).
        -- Keyed by URL because article_id may change (e.g. Google URL → resolved).
        CREATE TABLE IF NOT EXISTS fulltext_cache (
            url           TEXT PRIMARY KEY,
            body          TEXT NOT NULL,
            extractor     TEXT DEFAULT 'trafilatura',
            content_hash  TEXT,
            cached_at     TEXT NOT NULL
        );

        -- Cross-run cache: Google News URL → resolved canonical URL.
        -- Deterministic (no TTL needed in normal operation).
        CREATE TABLE IF NOT EXISTS url_resolution (
            encoded_url   TEXT PRIMARY KEY,
            decoded_url   TEXT NOT NULL,
            decoded_at    TEXT NOT NULL
        );

        -- HTTP conditional GET state per RSS feed URL
        CREATE TABLE IF NOT EXISTS feed_state (
            url              TEXT PRIMARY KEY,
            etag             TEXT,
            last_modified    TEXT,
            sha256           TEXT,
            last_fetched_at  TEXT NOT NULL,
            last_status      INTEGER,
            cache_hits_304   INTEGER DEFAULT 0
        );
    """)
    _migrate_news_table(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_published ON news(published DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_first_seen ON news(first_seen_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tickers ON news(tickers)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_channel ON news(channel)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fulltext_cached ON fulltext_cache(cached_at)")
    conn.commit()
    return conn


def _migrate_news_table(conn) -> None:
    """Idempotent migrations for existing databases.

    Backfill rules:
      - first_seen_at:     defaults to fetched_at (best approximation)
      - content_hash:      NULL (next save_article computes it)
      - classifier_version: NULL (next save_article tags it)
    """
    cols = {
        row[1]: row for row in conn.execute("PRAGMA table_info(news)").fetchall()
    }
    if not cols:
        return
    if "ticker" in cols and "tickers" not in cols:
        conn.execute("ALTER TABLE news RENAME COLUMN ticker TO tickers")
    if "body" not in cols:
        conn.execute("ALTER TABLE news ADD COLUMN body TEXT")
    if "primary_ticker" not in cols:
        conn.execute("ALTER TABLE news ADD COLUMN primary_ticker TEXT")
    if "first_seen_at" not in cols:
        # Add column without NOT NULL so existing rows are accepted, then backfill.
        conn.execute("ALTER TABLE news ADD COLUMN first_seen_at TEXT")
        conn.execute("UPDATE news SET first_seen_at = fetched_at WHERE first_seen_at IS NULL")
        log.info("schema_migration", action="added_first_seen_at", backfilled_from="fetched_at")
    if "content_hash" not in cols:
        conn.execute("ALTER TABLE news ADD COLUMN content_hash TEXT")
    if "classifier_version" not in cols:
        conn.execute("ALTER TABLE news ADD COLUMN classifier_version TEXT")
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(news)").fetchall()}
    if "idx_ticker" in indexes:
        conn.execute("DROP INDEX IF EXISTS idx_ticker")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tickers ON news(tickers)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_primary_ticker ON news(primary_ticker)")


def _content_hash(title: str, summary: str | None, body: str | None) -> str:
    """SHA256 of normalized content — used to detect article revisions."""
    src = "|".join([
        (title or "").strip(),
        (summary or "").strip(),
        (body or "").strip(),
    ])
    return hashlib.sha256(src.encode("utf-8")).hexdigest()


def save_article(conn, article: dict) -> bool:
    """Save (or update) an article in the bitemporal news table.

    Critical PIT-correctness behavior on UPSERT:
      - `first_seen_at` is NEVER overwritten — preserved from initial insert
      - `published` is NEVER overwritten — source's claim is set once
      - `fetched_at` IS overwritten — reflects last refresh time
      - If content changed (new hash), the OLD version is archived to
        article_versions before update, so we have a full revision history.

    Returns True on NEW row insertion, False on update (existing URL).
    """
    tickers = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
    primary_ticker = tickers[0] if tickers else None
    now_iso = datetime.now(timezone.utc).isoformat()
    title = article["title"]
    summary = article.get("summary")
    body = article.get("body")
    new_hash = _content_hash(title, summary, body)
    url = article["url"]

    # Check if URL exists and whether content has changed
    existing = conn.execute(
        "SELECT id, content_hash, first_seen_at FROM news WHERE url = ?",
        (url,),
    ).fetchone()

    if existing is not None:
        existing_id, old_hash, existing_first_seen = existing
        # Archive old version if content actually changed
        if old_hash and old_hash != new_hash:
            # Pull the old row to snapshot it
            old = conn.execute(
                "SELECT title, summary, body FROM news WHERE id = ?",
                (existing_id,),
            ).fetchone()
            if old:
                old_title, old_summary, old_body = old
                # Determine next version number
                max_v_row = conn.execute(
                    "SELECT MAX(version) FROM article_versions WHERE article_id = ?",
                    (existing_id,),
                ).fetchone()
                next_v = (max_v_row[0] or 1) + 1
                # version=1 was the initial state; we're now creating version=next_v
                # which represents the OLD content before this update. Ensure v1 exists.
                if max_v_row[0] is None:
                    # Backfill v1 from current state before update
                    conn.execute(
                        """INSERT OR IGNORE INTO article_versions
                           (article_id, version, title, summary, body, content_hash, observed_at)
                           VALUES (?, 1, ?, ?, ?, ?, ?)""",
                        (existing_id, old_title, old_summary, old_body, old_hash, existing_first_seen),
                    )
                    next_v = 2
                conn.execute(
                    """INSERT OR REPLACE INTO article_versions
                       (article_id, version, title, summary, body, content_hash, observed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (existing_id, next_v, title, summary, body, new_hash, now_iso),
                )
                log.info("article_version_appended",
                         article_id=existing_id[:16], version=next_v, url=url[:80])

    # UPSERT — preserves first_seen_at and published; updates everything else
    cur = conn.execute(
        """INSERT INTO news
        (id, title, url, source_name, source_domain, trust_tier,
         relevance, channel, published, summary, body, primary_ticker,
         tickers, alt_sources, fetched_at, first_seen_at,
         content_hash, classifier_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            id = excluded.id,
            title = excluded.title,
            source_name = excluded.source_name,
            source_domain = excluded.source_domain,
            trust_tier = excluded.trust_tier,
            relevance = excluded.relevance,
            channel = excluded.channel,
            -- published intentionally NOT updated — source claim is set once
            summary = excluded.summary,
            body = excluded.body,
            primary_ticker = excluded.primary_ticker,
            tickers = excluded.tickers,
            alt_sources = excluded.alt_sources,
            fetched_at = excluded.fetched_at,
            -- first_seen_at intentionally NOT updated — PIT immutability
            content_hash = excluded.content_hash,
            classifier_version = excluded.classifier_version
        """,
        (
            article["id"], title, url,
            article.get("_source_name", ""),
            article.get("_source_domain", ""),
            article.get("_trust", 1),
            article.get("_relevance", 0.0),
            article.get("_channel", ""),
            article.get("published"),
            summary, body,
            primary_ticker,
            ",".join(tickers),
            ",".join(article.get("_alt_sources", [])),
            now_iso,
            now_iso,  # first_seen_at — only used on INSERT, never on UPDATE
            new_hash,
            CLASSIFIER_VERSION,
        ),
    )
    conn.commit()
    return cur.rowcount > 0 and existing is None


def save_raw(payload: bytes, name: str, fmt: str = "xml"):
    raw_dir = RAW_DIR / re.sub(r"[^a-z0-9_]", "_", name.lower())
    raw_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(payload).hexdigest()[:16]
    (raw_dir / f"{digest}.{fmt}").write_bytes(payload)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def crawl(
    ticker: str,
    hours: int,
    *,
    save_results: bool = True,
    include_global_feeds: bool = True,
    fulltext_mode: str = DEFAULT_FULLTEXT_MODE,
    fulltext_max_articles: int = DEFAULT_FULLTEXT_MAX_ARTICLES,
    _session: aiohttp.ClientSession | None = None,
    _shared_bz_articles: list[dict] | None = None,
):
    conn = init_db()
    sources = build_sources(ticker, include_global_feeds=include_global_feeds)
    all_raw: list[dict] = []
    channel_stats: dict[str, int] = defaultdict(int)

    # Inject shared Benzinga RSS articles (already fetched once globally)
    if _shared_bz_articles is not None:
        bz_for_ticker = [dict(a, _ticker=ticker) for a in _shared_bz_articles]
        channel_stats["benzinga_rss"] += len(bz_for_ticker)
        all_raw.extend(bz_for_ticker)
        sources = [s for s in sources if s["tag"] != "benzinga_rss"]

    n_sources = len(sources)
    if n_sources or all_raw:
        extras_str = f" (+{len(_shared_bz_articles)} shared BZ)" if _shared_bz_articles else ""
        _progress(f"  [{ticker}] fetching {n_sources} channel(s){extras_str}")

    owns_session = _session is None
    if owns_session:
        _session = _make_client_session(connector_limit=5)

    try:
        for i, source in enumerate(sources, 1):
            tag = source["tag"]
            log.info("crawling", channel=source["name"], tag=tag)
            for url in source["urls"]:
                await _rate_limiter.wait(url)
                body, unchanged = await fetch_rss_with_conditional_get(_session, url)
                if unchanged:
                    # 304 / sha-match — feed contents not changed since last run.
                    # Existing articles still in SQLite → rolling window will export them.
                    _progress(f"  [{ticker}] {i}/{n_sources} {source['name']} — unchanged (304)")
                    log.info("parsed", channel=tag, raw=0, cache_hit=True)
                    continue
                if body is None:
                    _progress(f"  [{ticker}] {i}/{n_sources} {source['name']} — no response")
                    continue
                save_raw(body, tag)
                articles = parse_rss(body, tag, ticker)
                channel_stats[tag] += len(articles)
                all_raw.extend(articles)
                _progress(f"  [{ticker}] {i}/{n_sources} {source['name']}", raw=len(articles))
                log.info("parsed", channel=tag, raw=len(articles))
    finally:
        if owns_session:
            await _session.close()

    # Quality pipeline
    _progress(f"  [{ticker}] quality filter", raw=len(all_raw))
    filtered = quality_filter(all_raw, ticker, hours)
    _progress(f"  [{ticker}] quality filter done", passed=len(filtered))

    # Count Benzinga articles
    bz_count = sum(1 for a in filtered if "benzinga" in a.get("_source_domain", ""))

    new_count = 0
    if save_results:
        for a in filtered:
            if save_article(conn, a):
                new_count += 1
        _progress(f"  [{ticker}] saved to DB", new=new_count, total=len(filtered))

    fulltext_candidates = select_articles_for_fulltext(
        filtered,
        mode=fulltext_mode,
        max_articles=fulltext_max_articles,
    )
    log.info(
        "fulltext_selection",
        ticker=ticker,
        mode=fulltext_mode,
        candidates=len(fulltext_candidates),
        filtered=len(filtered),
    )
    if fulltext_candidates:
        _progress(f"  [{ticker}] fetching full text", articles=len(fulltext_candidates))
        await enrich_articles_with_bodies(fulltext_candidates)
        filtered = merge_articles_by_url(filtered)
        if save_results:
            for a in filtered:
                save_article(conn, a)

    conn.close()
    return filtered, all_raw, channel_stats, new_count, bz_count


def merge_articles_by_url(articles: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for article in articles:
        key = article.get("url") or article.get("id") or article.get("title")
        if key not in merged:
            copy = dict(article)
            copy["_tickers"] = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
            copy["_alt_sources"] = list(article.get("_alt_sources", []))
            merged[key] = copy
            continue

        current = merged[key]
        current["_tickers"] = sorted(set(current.get("_tickers", [])) | set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
        current["_relevance"] = max(current.get("_relevance", 0.0), article.get("_relevance", 0.0))
        current["_trust"] = max(current.get("_trust", 0), article.get("_trust", 0))
        current["_alt_sources"] = sorted(set(current.get("_alt_sources", [])) | set(article.get("_alt_sources", [])))
        if not current.get("summary") and article.get("summary"):
            current["summary"] = article["summary"]
        if current.get("published") and article.get("published"):
            if article["published"] > current["published"]:
                current["published"] = article["published"]
        elif article.get("published"):
            current["published"] = article["published"]
        if len(current.get("_tickers", [])) == 1:
            current["_ticker"] = current["_tickers"][0]
        else:
            current["_ticker"] = None

    return list(merged.values())


async def _fetch_shared_benzinga_rss(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch Benzinga RSS once globally — shared across all tickers.

    NOTE: As of 2026-04, both /feed and /news/feed return 403 (Cloudflare
    bot protection). Kept as a no-op so the call site (and tests) don't
    break, but skipping the network call entirely. BZ coverage now comes
    exclusively via the google_benzinga channel (`site:benzinga.com when:2d`).
    """
    return []


# ── M&A wire tripwire (2026-06-29) ──────────────────────────────────────────
# Breaking M&A/8-K events first appear on the primary PR wires at announcement
# (~minutes), while the free secondary aggregators NewsCrawler relies on
# (Yahoo/Benzinga/Google) surface them 6-12h later. Root case: Rocket Lab→
# Iridium $8B broke 01:00 UTC; the 07:08 crawl found nothing (all sources
# 6h behind); only caught at 12:19.
#
# This is a FILTERED tripwire, NOT the old volume firehose (removed for 0.02%
# ROI). Two-stage cheap→expensive filter keeps cost bounded:
#   1. event-type gate (cheap regex)  — discard ~99% with no high-value event
#   2. company-name → watchlist match — discard items about non-watchlist cos
# Only items passing BOTH are emitted, pre-attributed to the matched ticker.

_WIRE_TRIPWIRE_FEEDS = [
    # PR Newswire's dedicated M&A feed — purpose-built, highest signal density
    "https://www.prnewswire.com/rss/financial-services-latest-news/acquisitions-mergers-and-takeovers-list.rss",
    # GlobeNewswire public companies — broad press-release wire
    "https://www.globenewswire.com/rssfeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
    # PR Newswire general (filtered hard downstream)
    "https://www.prnewswire.com/rss/news-releases-list.rss",
]

# Only these event types justify a wire tripwire hit (first-publication value).
_TRIPWIRE_EVENT_TYPES = frozenset({
    "ma_activity", "earnings_release", "earnings_guidance", "regulatory",
})

_NAME_TO_TICKER_CACHE: dict[str, str] | None = None


def _load_name_to_ticker() -> dict[str, str]:
    """Reverse index: lowercased company alias → ticker, from AIStock's
    company_aliases config. Used to attribute a wire press-release headline
    ("Rocket Lab to Acquire …") to its watchlist ticker (RKLB).

    Ambiguity guards: aliases < 5 chars or shared by multiple tickers are
    dropped (a press release names the full company, so long aliases are safe
    and precise; short ones invite false matches)."""
    global _NAME_TO_TICKER_CACHE
    if _NAME_TO_TICKER_CACHE is not None:
        return _NAME_TO_TICKER_CACHE
    index: dict[str, str] = {}
    ambiguous: set[str] = set()
    try:
        cfg_path = PROJECT_ROOT.parent / "AIStock" / "config" / "default.json"
        if cfg_path.exists():
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            watch = {t for t in cfg.get("watchlist", []) if isinstance(t, str)}
            for ticker, aliases in (cfg.get("company_aliases", {}) or {}).items():
                if ticker not in watch:
                    continue
                for alias in (aliases or []):
                    a = alias.strip().lower()
                    if len(a) < 5:
                        continue
                    if a in index and index[a] != ticker:
                        ambiguous.add(a)
                    else:
                        index[a] = ticker
            for a in ambiguous:
                index.pop(a, None)
    except Exception as exc:
        log.warning("name_to_ticker_load_failed", error=str(exc)[:100])
    _NAME_TO_TICKER_CACHE = index
    log.info("name_to_ticker_loaded", aliases=len(index))
    return index


def _match_watchlist_ticker(title: str, summary: str = "") -> str | None:
    """Return the watchlist ticker named in a wire headline, or None.
    Longest-alias-wins to prefer the most specific company name."""
    index = _load_name_to_ticker()
    if not index:
        return None
    text = f"{title} {summary}".lower()
    best: str | None = None
    best_len = 0
    for alias, ticker in index.items():
        if len(alias) <= best_len:
            continue
        # word-boundary containment
        i = text.find(alias)
        if i < 0:
            continue
        before = text[i - 1] if i > 0 else " "
        after = text[i + len(alias)] if i + len(alias) < len(text) else " "
        if not before.isalnum() and not after.isalnum():
            best, best_len = ticker, len(alias)
    return best


async def _fetch_wire_tripwire(session: aiohttp.ClientSession) -> dict[str, list[dict]]:
    """Fetch PR-wire feeds once; return {ticker: [articles]} for items that
    pass BOTH the high-value event-type gate AND the watchlist-name match."""
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    raw_seen = 0
    ev_pass = 0
    for url in _WIRE_TRIPWIRE_FEEDS:
        try:
            await _rate_limiter.wait(url)
            body = await fetch_url(session, url)
            if not body:
                continue
            articles = parse_rss(body, "wire_tripwire", "")
        except Exception as exc:
            log.warning("wire_tripwire_fetch_failed", url=url[:60], error=str(exc)[:80])
            continue
        for a in articles:
            raw_seen += 1
            title = a.get("title", "")
            summ = a.get("summary", "") or ""
            ets = classify_events(title, summ)
            if not (_TRIPWIRE_EVENT_TYPES & set(ets)):
                continue  # cheap gate: no high-value event → drop
            ev_pass += 1
            ticker = _match_watchlist_ticker(title, summ)
            if not ticker:
                continue  # not about a watchlist company → drop
            a["_ticker"] = ticker
            a["_tickers"] = [ticker]
            a["_channel"] = "wire_tripwire"
            by_ticker[ticker].append(a)
    n_hits = sum(len(v) for v in by_ticker.values())
    _progress(f"  [global] wire tripwire  raw={raw_seen} event_pass={ev_pass} "
              f"watchlist_hits={n_hits} tickers={len(by_ticker)}")
    log.info("wire_tripwire", raw=raw_seen, event_pass=ev_pass,
             watchlist_hits=n_hits, tickers=len(by_ticker))
    return dict(by_ticker)


# ── SEC EDGAR filings tripwire (2026-07-19) ─────────────────────────────────
# The 2026-07 moomoo OpenAPI evaluation showed the highest-signal items in its
# news search were SEC filing re-posts (Form 4 insider trades, 8-K material
# events) — but with date-only timestamps, unusable under this project's PIT
# rules. Direct EDGAR is strictly better: free, no key, and every entry in the
# "latest filings" Atom feed carries the exact acceptance timestamp.
#
# Same shape as the wire tripwire: a handful of GLOBAL requests per run (one
# per form family, plus a weekly-cached company_tickers.json), then a cheap
# two-stage filter (exact form-type whitelist → issuer/subject CIK ∈
# watchlist). 宁缺毋滥: only event-class forms pass — Form 4 (insider
# transactions), 8-K (material events), SC 13D/G (>5% stakes). Fund-holdings
# forms (NPORT-P, 13F-HR) and offering paperwork (424B*, S-*) never enter the
# pipeline, even though the prefix-matching `type=` query may return them.
#
# SEC fair-access policy: ≤10 req/s and a declared User-Agent with contact
# info (browser-imitating UAs get bot-blocked). See _sec_headers() and the
# "www.sec.gov" entry in DOMAIN_DELAY.

_EDGAR_LATEST_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
    "&type={form}&company=&dateb=&owner=include&count=100&output=atom"
)
_EDGAR_FORM_QUERIES = ("4", "8-K", "SC 13D", "SC 13G")

# Exact form-type whitelist → which filer role carries the watchlist
# attribution. Everything else (NPORT-P, 425, 424B5, 4XX offering docs that
# the prefix query returns) is dropped here.
#   Form 4:  "Issuer" = the company whose insider traded ("Reporting" = person)
#   8-K:     "Filer"  = the company reporting the material event
#   13D/G:   "Subject" = the company whose shares are being accumulated
#            ("Filed by" = the acquiring fund — kept only as title context)
_EDGAR_FORMS: dict[str, str] = {
    "4": "Issuer",
    "4/A": "Issuer",
    "8-K": "Filer",
    "8-K/A": "Filer",
    "SC 13D": "Subject",
    "SC 13D/A": "Subject",
    "SC 13G": "Subject",
    "SC 13G/A": "Subject",
}

# getcurrent entry titles look like:
#   "4 - Kress Colette (0001178579) (Reporting)"
#   "8-K - NVIDIA CORP (0001045810) (Filer)"
#   "SC 13D/A - Semler Scientific, Inc. (0001554859) (Subject)"
_EDGAR_TITLE_RE = re.compile(r"\((?P<cik>\d{7,10})\)\s*\((?P<role>[^()]+)\)\s*$")
_EDGAR_ACCESSION_RE = re.compile(r"accession-number=([0-9-]+)")

_SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_SEC_CIK_CACHE_MAX_AGE_S = 7 * 86400   # ticker↔CIK assignments churn slowly


def _sec_headers() -> dict[str, str]:
    """SEC fair-access headers: declared tool UA with contact info."""
    ua = os.environ.get(
        "NEWSCRAWLER_SEC_USER_AGENT",
        "NewsCrawler/1.0 (contact: a290191715@gmail.com)",
    )
    return {
        "User-Agent": ua,
        "Accept": "application/atom+xml, application/json, */*;q=0.5",
    }


def _build_cik_to_ticker(company_tickers: dict, tickers: list[str]) -> dict[int, str]:
    """Build {cik: watchlist_ticker} from SEC's company_tickers.json payload
    ({"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}).

    SEC spells share classes with '-' (BRK-B) where AIStock uses '.' (BRK.B) —
    compare on the dot-normalized form, emit the watchlist spelling. On CIK
    collisions (GOOGL/GOOG share one CIK) the first file entry wins, so one
    filing attributes to exactly one ticker."""
    norm_watch: dict[str, str] = {}
    for t in tickers:
        norm_watch.setdefault(t.upper().replace("-", "."), t.upper())
    cik_map: dict[int, str] = {}
    rows = company_tickers.values() if isinstance(company_tickers, dict) else company_tickers
    for row in rows:
        try:
            cik = int(row["cik_str"])
            sec_ticker = str(row["ticker"]).upper().replace("-", ".")
        except (KeyError, TypeError, ValueError):
            continue
        watch_ticker = norm_watch.get(sec_ticker)
        if watch_ticker and cik not in cik_map:
            cik_map[cik] = watch_ticker
    return cik_map


async def _load_cik_to_ticker(
    session: aiohttp.ClientSession, tickers: list[str]
) -> dict[int, str]:
    """Load the ticker→CIK universe from SEC (weekly disk cache in DATA_DIR),
    filtered to the watchlist. Empty dict on total failure → channel silently
    absent for this run (same failure posture as TWS-closed for ibkr_news)."""
    import time as _time
    cache_path = DATA_DIR / "sec_company_tickers.json"
    data = None
    try:
        if cache_path.exists() and _time.time() - cache_path.stat().st_mtime < _SEC_CIK_CACHE_MAX_AGE_S:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        data = None
    if data is None:
        try:
            await _rate_limiter.wait(_SEC_COMPANY_TICKERS_URL)
            body = await fetch_url(session, _SEC_COMPANY_TICKERS_URL,
                                   extra_headers=_sec_headers())
            if body:
                data = json.loads(body)
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_text(json.dumps(data), encoding="utf-8")
                except OSError as exc:
                    log.warning("sec_cik_cache_write_failed", error=str(exc)[:80])
        except Exception as exc:
            log.warning("sec_cik_map_fetch_failed", error=str(exc)[:100])
        if data is None and cache_path.exists():
            # Stale cache beats no channel — CIK assignments are stable.
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                log.warning("sec_cik_map_stale_cache_used")
            except Exception:
                data = None
    if not data:
        return {}
    return _build_cik_to_ticker(data, tickers)


def _edgar_entry_time(entry) -> str | None:
    """Acceptance timestamp of a getcurrent entry as UTC ISO.

    NOTE: uses fromisoformat on the raw string (offset-aware, e.g. -04:00)
    with calendar.timegm on the feedparser UTC struct as fallback — NOT the
    mktime idiom used in parse_rss, which assumes the local zone is UTC.
    This channel exists for PIT correctness; the timestamp must be exact."""
    raw = entry.get("updated") or entry.get("published")
    if raw:
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        except ValueError:
            pass
    parsed = entry.get("updated_parsed") or entry.get("published_parsed")
    if parsed:
        try:
            import calendar
            return datetime.fromtimestamp(calendar.timegm(parsed), tz=timezone.utc).isoformat()
        except Exception:
            pass
    return None


def _edgar_title(form: str, company: str, ticker: str, counterparty: str) -> str:
    """Synthesize a descriptive headline from filing metadata.

    Raw EDGAR titles ("4 - NVIDIA CORP (0001045810) (Issuer)") carry no signal
    for downstream display or classification. The synthetic titles are phrased
    to hit the tax_v7 taxonomy exactly: "SEC Form 4" → insider_activity,
    "Form 8-K" → regulatory, "Schedule 13D/G" → ma_activity — so event_types
    survive rolling-window re-classification from the DB. The counterparty
    (insider name / acquiring fund) also keeps same-company same-day filings
    distinct through SimHash/fuzzy dedup."""
    amended = " (amended)" if form.endswith("/A") else ""
    if form in ("4", "4/A"):
        who = f" by {counterparty}" if counterparty else ""
        return f"SEC Form 4 filing{amended}: insider transaction at {company} ({ticker}){who}"
    if form in ("8-K", "8-K/A"):
        return f"SEC Form 8-K filing{amended}: material event reported by {company} ({ticker})"
    sched = "13D" if "13D" in form else "13G"
    stake = "active stake" if sched == "13D" else "passive stake"
    who = f" by {counterparty}" if counterparty else ""
    return f"SEC Schedule {sched} filing{amended}: {stake} in {company} ({ticker}) disclosed{who}"


def _parse_edgar_atom(body: bytes, cik_to_ticker: dict[int, str]) -> tuple[list[dict], int]:
    """Parse a getcurrent Atom feed → (articles for watchlist CIKs, raw entry
    count). Applies the exact form whitelist, the per-form role filter, and
    accession-level dedup (a Form 4 emits Issuer + Reporting entries that
    share one accession — we keep the Issuer row, enriched with the person's
    name from the Reporting row)."""
    feed = feedparser.parse(body)

    # Pass 1: structural parse + counterparty names keyed by accession.
    parsed: list[tuple] = []
    names_by_acc: dict[str, str] = {}
    for entry in feed.entries:
        raw_title = clean_text(entry.get("title", ""))
        m = _EDGAR_TITLE_RE.search(raw_title)
        if not m:
            continue
        cik = int(m.group("cik"))
        role = m.group("role").strip()
        name = raw_title[: m.start()].strip()
        if " - " in name:
            name = name.split(" - ", 1)[1].strip()
        form = ""
        for tag in (entry.get("tags") or []):
            term = (tag.get("term") or "").strip()
            if term:
                form = term
                break
        if not form:
            form = raw_title.split(" - ", 1)[0].strip()
        acc = None
        m_acc = _EDGAR_ACCESSION_RE.search(entry.get("id", "") or "")
        if m_acc:
            acc = m_acc.group(1)
        if acc and role in ("Reporting", "Filed by") and acc not in names_by_acc:
            names_by_acc[acc] = name
        parsed.append((entry, raw_title, form, name, cik, role, acc))

    # Pass 2: filter + build articles.
    articles: list[dict] = []
    seen_acc: set[str] = set()
    for entry, raw_title, form, name, cik, role, acc in parsed:
        want_role = _EDGAR_FORMS.get(form)
        if want_role is None:
            continue   # NPORT-P / 424B5 / 425 / … — not event-class, drop
        if role != want_role:
            continue
        ticker = cik_to_ticker.get(cik)
        if not ticker:
            continue   # not a watchlist company
        if acc:
            if acc in seen_acc:
                continue
            seen_acc.add(acc)
        link = entry.get("link", "")
        published = _edgar_entry_time(entry)
        counterparty = names_by_acc.get(acc or "", "")
        articles.append({
            "id": str(uuid.uuid5(uuid.NAMESPACE_URL, link or f"edgar:{acc}")),
            "title": _edgar_title(form, name, ticker, counterparty),
            "url": link,
            "_source_name": "SEC EDGAR",
            "_source_domain": "sec.gov",
            "_trust": 3,
            "_channel": "sec_edgar",
            "_ticker": ticker,
            "_tickers": [ticker],
            "published": published,
            "summary": (
                f"{form} accepted {published or 'n/a'}, accession {acc or 'n/a'}. "
                f"EDGAR: {raw_title}"
            ),
            "_meta": {"edgar_form": form, "edgar_accession": acc, "edgar_cik": cik},
        })
    return articles, len(feed.entries)


async def _fetch_edgar_filings(
    session: aiohttp.ClientSession, tickers: list[str]
) -> dict[str, list[dict]]:
    """Fetch EDGAR's latest-filings Atom feed once per form family; return
    {ticker: [articles]} for whitelisted forms whose issuer/subject CIK maps
    to a watchlist ticker. `NEWSCRAWLER_SEC_EDGAR=0` disables."""
    if os.environ.get("NEWSCRAWLER_SEC_EDGAR", "1").strip().lower() in ("0", "false", "no"):
        log.info("sec_edgar disabled via NEWSCRAWLER_SEC_EDGAR")
        return {}
    cik_map = await _load_cik_to_ticker(session, tickers)
    if not cik_map:
        log.warning("sec_edgar_no_cik_map")
        return {}
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    raw_seen = 0
    for form in _EDGAR_FORM_QUERIES:
        url = _EDGAR_LATEST_URL.format(form=quote(form))
        try:
            await _rate_limiter.wait(url)
            body = await fetch_url(session, url, extra_headers=_sec_headers())
            if not body:
                log.warning("sec_edgar_feed_empty", form=form)
                continue
            articles, n_entries = _parse_edgar_atom(body, cik_map)
        except Exception as exc:
            log.warning("sec_edgar_fetch_failed", form=form, error=str(exc)[:80])
            continue
        raw_seen += n_entries
        for a in articles:
            by_ticker[a["_ticker"]].append(a)
    n_hits = sum(len(v) for v in by_ticker.values())
    _progress(f"  [global] sec edgar  raw={raw_seen} watchlist_hits={n_hits} "
              f"tickers={len(by_ticker)} ciks_mapped={len(cik_map)}")
    log.info("sec_edgar", raw=raw_seen, watchlist_hits=n_hits, tickers=len(by_ticker))
    return dict(by_ticker)


async def crawl_watchlist(
    tickers: list[str],
    hours: int,
    *,
    fulltext_mode: str = DEFAULT_FULLTEXT_MODE,
    fulltext_max_articles: int = DEFAULT_FULLTEXT_MAX_ARTICLES,
    ibkr_news: bool = True,
    sec_edgar: bool = True,
):
    import time as _time
    t0 = _time.monotonic()

    # Reset per-run cache: avoid redundant fetches when the same article URL
    # appears in multiple tickers' fulltext candidate lists.
    _reset_body_cache()
    _reset_http_cache_counters()

    conn = init_db()
    all_filtered: list[dict] = []
    all_raw: list[dict] = []
    channel_stats: dict[str, int] = defaultdict(int)
    per_ticker_stats: dict[str, int] = {}
    total_new_count = 0

    # Shared HTTP session for all tickers — connection pooling + generous header limits
    async with _make_client_session(connector_limit=20) as session:
        # Step 1: fetch Benzinga RSS once, share with all tickers
        shared_bz = await _fetch_shared_benzinga_rss(session)

        # Step 1c: M&A wire tripwire — fetch primary PR wires once, keep only
        # high-value events that match a watchlist company (catches breaking
        # deals hours before secondary aggregators surface them).
        try:
            wire_by_ticker = await _fetch_wire_tripwire(session)
        except Exception as exc:
            log.warning("wire_tripwire_failed", error=str(exc)[:100])
            wire_by_ticker = {}

        # Step 1e (2026-07-19): SEC EDGAR filings tripwire — Form 4 / 8-K /
        # 13D/G with exact acceptance timestamps, straight from the regulator
        # (primary source, zero media latency). Global fetch, CIK-filtered.
        edgar_by_ticker: dict[str, list[dict]] = {}
        if sec_edgar:
            try:
                edgar_by_ticker = await _fetch_edgar_filings(session, tickers)
            except Exception as exc:
                log.warning("sec_edgar_failed", error=str(exc)[:100])
                edgar_by_ticker = {}

        # Step 1b (NEW): kick off Finnhub API batch in parallel with the
        # per-ticker RSS crawl. Finnhub has its own rate limit (60 calls/min)
        # and typically takes ~500s for 500 tickers — entirely within the
        # RSS crawl window, so zero wall-clock cost.
        finnhub_task = None
        if os.environ.get("FINNHUB_API_KEY"):
            try:
                from crawler.sources.finnhub_api import fetch_finnhub_for_tickers
                # Finnhub uses days_back, not hours. Round up from hours window.
                days_back = max(1, (hours + 23) // 24)
                finnhub_task = asyncio.create_task(
                    fetch_finnhub_for_tickers(
                        tickers, days_back=days_back, session=session,
                    ),
                    name="finnhub_api_batch",
                )
                _progress(f"-- finnhub_api task started (days_back={days_back})")
            except Exception as exc:
                log.warning("finnhub_api start failed", error=str(exc))
                finnhub_task = None

        # Step 1d (2026-07-19): IBKR TWS wire news (Dow Jones / Briefing) in
        # parallel with the RSS crawl. Reads a running TWS/Gateway readonly;
        # silently absent when TWS is closed. Hot-ticker gated on big runs
        # (same policy as the premium Google proxies).
        ibkr_task = None
        if ibkr_news:
            try:
                from crawler.sources.ibkr_news import fetch_ibkr_news_for_tickers
                if len(tickers) > 150:
                    hot = _get_hot_tickers()
                    ibkr_tickers = [t for t in tickers if t in hot]
                else:
                    ibkr_tickers = list(tickers)
                if ibkr_tickers:
                    ibkr_task = asyncio.create_task(
                        fetch_ibkr_news_for_tickers(
                            ibkr_tickers,
                            hours=hours,
                            classify_fn=classify_events,
                            conid_cache_path=DATA_DIR / "ibkr_conids.json",
                        ),
                        name="ibkr_news_batch",
                    )
                    _progress(f"-- ibkr_news task started ({len(ibkr_tickers)} tickers)")
            except Exception as exc:
                log.warning("ibkr_news start failed", error=str(exc)[:100])
                ibkr_task = None

        # Step 2: crawl tickers in parallel batches
        sem = asyncio.Semaphore(TICKER_CONCURRENCY)
        completed = 0

        async def _crawl_one(ticker: str) -> tuple[str, list, list, dict]:
            nonlocal completed
            async with sem:
                filtered, raw, stats, _, _ = await crawl(
                    ticker,
                    hours,
                    save_results=False,
                    include_global_feeds=False,
                    fulltext_mode=fulltext_mode,
                    fulltext_max_articles=fulltext_max_articles,
                    _session=session,
                    _shared_bz_articles=shared_bz,
                )
                completed += 1
                if completed % 10 == 0 or completed == len(tickers):
                    elapsed = _time.monotonic() - t0
                    _progress(f"-- progress {completed}/{len(tickers)} tickers  ({elapsed:.0f}s)")
                return ticker, filtered, raw, stats

        results = await asyncio.gather(*(_crawl_one(t) for t in tickers))

        # Step 2b (NEW): await Finnhub task inside the session context so the
        # HTTP session is still valid if it needs any pending IO.
        finnhub_by_ticker: dict[str, list[dict]] = {}
        if finnhub_task is not None:
            try:
                finnhub_by_ticker = await finnhub_task
            except Exception as exc:
                log.warning("finnhub_api task failed", error=str(exc))
                finnhub_by_ticker = {}

        ibkr_by_ticker: dict[str, list[dict]] = {}
        if ibkr_task is not None:
            try:
                ibkr_by_ticker = await ibkr_task
            except Exception as exc:
                log.warning("ibkr_news task failed", error=str(exc)[:100])
                ibkr_by_ticker = {}

    for ticker, filtered, raw, stats in results:
        all_filtered.extend(filtered)
        all_raw.extend(raw)
        per_ticker_stats[ticker] = len(filtered)
        for channel, count in stats.items():
            channel_stats[channel] += count

    # Run Finnhub articles through the same quality_filter so they land in
    # the same downstream pipeline as RSS articles (dedup, save_article,
    # aistock export).
    if finnhub_by_ticker:
        fh_raw_total = 0
        fh_filtered_total = 0
        for fh_ticker, fh_articles in finnhub_by_ticker.items():
            if not fh_articles:
                continue
            fh_raw_total += len(fh_articles)
            all_raw.extend(fh_articles)
            # General news (ticker="") skips per-ticker quality filter: attach
            # to every ticker's pool would bloat the dedup; instead route to
            # a dedicated synthetic ticker "_GENERAL_" so it appears in the
            # consolidated export but not in per-ticker JSONs.
            if not fh_ticker:
                synthetic = "_GENERAL_"
                filtered = quality_filter(fh_articles, synthetic, hours)
                # Stamp _ticker so group_articles_by_ticker groups correctly
                for a in filtered:
                    a["_ticker"] = synthetic
                    a["_tickers"] = a.get("_tickers") or [synthetic]
                all_filtered.extend(filtered)
                fh_filtered_total += len(filtered)
                channel_stats["finnhub_api_general"] += len(filtered)
                continue
            filtered = quality_filter(fh_articles, fh_ticker, hours)
            all_filtered.extend(filtered)
            per_ticker_stats[fh_ticker] = per_ticker_stats.get(fh_ticker, 0) + len(filtered)
            fh_filtered_total += len(filtered)
            channel_stats["finnhub_api"] += len(filtered)
        _progress(
            f"-- finnhub_api merged: raw={fh_raw_total} filtered={fh_filtered_total}",
        )

    # Merge M&A wire tripwire hits through the same per-ticker quality_filter.
    if wire_by_ticker:
        w_raw = w_filt = 0
        for w_ticker, w_articles in wire_by_ticker.items():
            if not w_articles:
                continue
            w_raw += len(w_articles)
            all_raw.extend(w_articles)
            filtered = quality_filter(w_articles, w_ticker, hours)
            all_filtered.extend(filtered)
            per_ticker_stats[w_ticker] = per_ticker_stats.get(w_ticker, 0) + len(filtered)
            w_filt += len(filtered)
            channel_stats["wire_tripwire"] += len(filtered)
        _progress(f"-- wire_tripwire merged: raw={w_raw} filtered={w_filt}")

    # Merge SEC EDGAR filings through the same per-ticker quality_filter so
    # they flow into dedup / save_article (bitemporal first_seen_at) / export.
    if edgar_by_ticker:
        e_raw = e_filt = 0
        for e_ticker, e_articles in edgar_by_ticker.items():
            if not e_articles:
                continue
            e_raw += len(e_articles)
            all_raw.extend(e_articles)
            filtered = quality_filter(e_articles, e_ticker, hours)
            all_filtered.extend(filtered)
            per_ticker_stats[e_ticker] = per_ticker_stats.get(e_ticker, 0) + len(filtered)
            e_filt += len(filtered)
            channel_stats["sec_edgar"] += len(filtered)
        _progress(f"-- sec_edgar merged: raw={e_raw} filtered={e_filt}")

    # Merge IBKR TWS wire news (DJ/Briefing) through the same per-ticker
    # quality_filter so it flows into dedup / save_article / export.
    if ibkr_by_ticker:
        ib_raw = ib_filt = 0
        for ib_ticker, ib_articles in ibkr_by_ticker.items():
            if not ib_articles:
                continue
            ib_raw += len(ib_articles)
            all_raw.extend(ib_articles)
            filtered = quality_filter(ib_articles, ib_ticker, hours)
            all_filtered.extend(filtered)
            per_ticker_stats[ib_ticker] = per_ticker_stats.get(ib_ticker, 0) + len(filtered)
            ib_filt += len(filtered)
            channel_stats["ibkr_news"] += len(filtered)
        _progress(f"-- ibkr_news merged: raw={ib_raw} filtered={ib_filt}")

    _progress(f"-- merging & dedup across {len(tickers)} tickers", total_raw=len(all_filtered))
    merged_articles = merge_articles_by_url(all_filtered)
    bz_count = sum(1 for article in merged_articles if "benzinga" in article.get("_source_domain", ""))
    for article in merged_articles:
        if save_article(conn, article):
            total_new_count += 1

    elapsed = _time.monotonic() - t0
    _progress(f"-- saved to DB", new=total_new_count, merged=len(merged_articles))
    _progress(f"-- total crawl time: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    conn.close()
    return merged_articles, all_raw, channel_stats, total_new_count, bz_count, per_ticker_stats


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY
# ══════════════════════════════════════════════════════════════════════════════

TRUST_LABEL = {3: "TOP", 2: "OK ", 1: "low", 0: "BLK"}
CHANNEL_LABEL = {
    "amd_ir": "AMD-IR",
    "benzinga_rss": "BZ-RSS",
    "google_benzinga": "BZ-via-Google",
    "google_broad": "Google-broad",
    "ibkr_news": "IBKR-DJ",
    "sec_edgar": "SEC-EDGAR",
}


def _to_timestamp_utc(value: str | None) -> str:
    if value:
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.replace(microsecond=0).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def to_news_item(article: dict) -> dict:
    """Build an AIStock-compatible news item from a raw crawled article.

    Top-level schema (matches AIStock shared/models.py NewsItem):
      id, timestamp_utc, first_seen_at_utc, source, url, title, body, author,
      language, ticker (primary), tickers_hint (all), source_quality,
      publisher_raw, trust_tier, body_kind, ingest_source

    Bitemporal fields:
      timestamp_utc      = source's claimed publish time (valid time)
      first_seen_at_utc  = when NewsCrawler first observed this URL (PIT
                           transaction time) — for backtesting / look-ahead
                           avoidance. AIStock should use this for time filtering.

    meta contains supplementary info plus PIT/lineage metadata:
      classifier_version — which taxonomy + lexicon version produced
                           event_types / sentiment / quality_score, so
                           backtests can pin to a specific version.
    """
    summary = article.get("summary") or None
    body = article.get("body") or summary
    tickers = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
    source, publisher_raw = _canonical_news_source(article)
    source_quality = _source_quality(article)
    primary_ticker = tickers[0] if tickers else None
    trust_tier = article.get("_trust")
    body_kind = ("article_text" if article.get("body") else "summary_snippet") if body else None

    # Event classification — regex taxonomy matched against title + summary + body head
    event_types = classify_events(
        article.get("title", ""),
        article.get("summary", "") or "",
        article.get("body"),
    )

    # Loughran-McDonald financial sentiment — pre-computed so AIStock
    # scorer can use it without re-running the lexicon pass.
    sentiment = lexicon_sentiment(
        article.get("title", ""),
        article.get("summary", "") or "",
        article.get("body"),
    )

    # High-value structured event signals (deal size, beat %, guidance dir,
    # regulatory outcome, product breakthrough) — feed AIStock's severity calc.
    event_signals = extract_event_signals(
        article.get("title", ""),
        article.get("summary", "") or "",
        article.get("body"),
        event_types,
    )

    # Bitemporal: first_seen_at is the immutable PIT transaction time.
    # Fall back to published if missing (only happens for items not yet in SQLite).
    first_seen_iso = _to_timestamp_utc(
        article.get("_first_seen_at") or article.get("published")
    )
    published_iso = _to_timestamp_utc(article.get("published"))

    # Latency: source-claimed publish → first observed by us. Useful for AIStock
    # to weight stale news lower and monitor source lag.
    latency_seconds = None
    try:
        if first_seen_iso and published_iso:
            fs_dt = datetime.fromisoformat(first_seen_iso)
            pub_dt = datetime.fromisoformat(published_iso)
            latency_seconds = max(0, int((fs_dt - pub_dt).total_seconds()))
    except Exception:
        pass

    meta: dict = {
        "source_name": article.get("_source_name"),
        "source_domain": article.get("_source_domain"),
        "event_origin": "news",
        "relevance": article.get("_relevance"),
        "quality_score": article.get("_quality_score"),
        "event_types": event_types or None,
        "event_signals": event_signals or None,
        "sentiment": sentiment if sentiment.get("matched", 0) > 0 else None,
        # PIT / lineage
        "classifier_version": CLASSIFIER_VERSION,
        "first_seen_at_utc": first_seen_iso,    # also in meta for legacy consumers
        "publish_to_observe_latency_s": latency_seconds,
        "_channel": article.get("_channel"),
    }
    alt_sources = article.get("_alt_sources") or []
    if alt_sources:
        meta["_alt_sources"] = alt_sources

    return {
        "id": article.get("id") or str(uuid.uuid5(uuid.NAMESPACE_URL, article.get("url") or article.get("title", ""))),
        # Bitemporal timestamps — both top-level so AIStock can read either.
        "timestamp_utc": published_iso,           # valid time (source claim)
        "first_seen_at_utc": first_seen_iso,      # transaction time (PIT) ⭐
        "source": source,
        "url": article.get("url"),
        "title": article.get("title", ""),
        "body": body,
        "author": None,
        "language": "en",
        "ticker": primary_ticker,
        "tickers_hint": tickers,
        "source_quality": source_quality,
        "publisher_raw": publisher_raw,
        "trust_tier": trust_tier,
        "body_kind": body_kind,
        "ingest_source": "newscrawler_local",
        "meta": {k: v for k, v in meta.items() if v is not None},
    }


def to_news_items(articles: list[dict]) -> list[dict]:
    return [to_news_item(article) for article in articles]


def print_results(articles, raw_articles, channel_stats, new_count, bz_count,
                  ticker_label, hours, per_ticker_stats: dict[str, int] | None = None):
    print()
    print("=" * 92)
    print(f"  {ticker_label} NEWS REPORT — Quality Filtered, {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 92)

    # Stats
    print()
    print("  CRAWL STATS:")
    print(f"  {'Channel':<25s} {'Raw':>6s} {'Description'}")
    print(f"  {'-'*25} {'-'*6} {'-'*40}")
    for tag, count in channel_stats.items():
        label = CHANNEL_LABEL.get(tag, tag)
        desc = {
            "amd_ir": "Official AMD Investor Relations press releases RSS",
            "benzinga_rss": "Official Benzinga RSS /feed + /news/feed",
            "google_benzinga": "Google News filtered to site:benzinga.com",
            "google_broad": "Google News all financial sources",
        }.get(tag, "")
        print(f"  {label:<25s} {count:>6d} {desc}")
    print(f"  {'TOTAL':<25s} {len(raw_articles):>6d}")
    print()
    print(f"  QUALITY PIPELINE:")
    print(f"    Raw articles:       {len(raw_articles):>4d}")
    print(f"    After quality filter: {len(articles):>4d}  (removed {len(raw_articles)-len(articles)} junk/irrelevant/dupes)")
    print(f"    New saved to DB:    {new_count:>4d}")
    print(f"    Benzinga articles:  {bz_count:>4d}  (direct Benzinga coverage)")
    if per_ticker_stats:
        print(f"    Watchlist tickers:  {len(per_ticker_stats):>4d}")
    print()

    if per_ticker_stats:
        print("  TICKER COVERAGE:")
        for symbol, count in sorted(per_ticker_stats.items(), key=lambda item: (-item[1], item[0])):
            print(f"    {symbol:<6s} {count:>4d}")
        print()

    # Anti-scrape summary
    print("  ANTI-SCRAPE STRATEGY:")
    print("    - Benzinga HTML: SKIPPED (Cloudflare JS challenge blocks simple HTTP)")
    print("    - Benzinga RSS:  /feed + /news/feed (no CF, structured XML)")
    print("    - Google->BZ:    site:benzinga.com (Google already crawled, no CF)")
    print(f"    - Polite crawl:  per-domain rate limit {DOMAIN_DELAY}, no JS execution needed")
    print()

    if not articles:
        print(f"  No quality {ticker_label} news in last {hours}h.")
        print()
        return

    # Separate Benzinga vs other
    bz_articles = [a for a in articles if "benzinga" in a.get("_source_domain", "")]
    other_articles = [a for a in articles if "benzinga" not in a.get("_source_domain", "")]

    if bz_articles:
        print(f"  ── BENZINGA ARTICLES ({len(bz_articles)}) ──")
        print()
        _print_articles(bz_articles)

    if other_articles:
        print(f"  ── OTHER SOURCES ({len(other_articles)}) ──")
        print()
        _print_articles(other_articles)

    print("=" * 92)


def _print_articles(articles):
    for i, a in enumerate(articles, 1):
        trust = a.get("_trust", 1)
        rel = a.get("_relevance", 0)
        trust_str = TRUST_LABEL.get(trust, "???")
        channel = CHANNEL_LABEL.get(a.get("_channel", ""), a.get("_channel", ""))

        pub_str = "          "
        if a.get("published"):
            try:
                pub_str = datetime.fromisoformat(a["published"]).strftime("%m-%d %H:%M")
            except Exception:
                pass

        # Clean title
        title = a["title"]
        source_suffix = ""
        if " - " in title:
            parts = title.rsplit(" - ", 1)
            title, source_suffix = parts[0], parts[1]

        source = source_suffix or a.get("_source_name", "")
        alt = a.get("_alt_sources", [])
        alt_str = f"  (also: {', '.join(alt[:3])})" if alt else ""

        qs = a.get("_quality_score", 0)
        print(f"  {i:3d}. [{trust_str}] [{pub_str}] [Q:{qs:.2f}] [rel:{rel:.2f}] [{channel}]")
        print(f"       {title}")
        print(f"       Source: {source}{alt_str}")
        if a.get("summary"):
            s = a["summary"][:160]
            if len(a.get("summary", "")) > 160:
                s += "..."
            print(f"       > {s}")
        print(f"       {a['url'][:120]}")
        print()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Quality financial news crawler (no API key)")
    parser.add_argument("--ticker", default=None, help="Single stock ticker")
    parser.add_argument("--tickers", default=None, help="Comma-separated ticker list")
    parser.add_argument(
        "--ticker-set",
        choices=tuple(BUILTIN_TICKER_SETS),
        default=None,
        help=(
            "Watchlist preset. 'aistock' (default) reads the live watchlist from the sibling "
            "AIStock repo (config/default.json) so crawl tickers always match AIStock exactly. "
            f"Other options: {', '.join(k for k in BUILTIN_TICKER_SETS if k != 'aistock')}."
        ),
    )
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Look-back hours (default: 48)")
    parser.add_argument(
        "--fulltext-mode",
        choices=("off", "high-value", "all"),
        default=DEFAULT_FULLTEXT_MODE,
        help="Full-text fetch strategy: off, high-value, or all (default: high-value)",
    )
    parser.add_argument(
        "--fulltext-max-articles",
        type=int,
        default=DEFAULT_FULLTEXT_MAX_ARTICLES,
        help="Per-ticker cap for full-text fetch in high-value mode (default: 20)",
    )
    parser.add_argument(
        "--output",
        choices=("newsitem-json", "pretty"),
        default="newsitem-json",
        help="Output mode: standard NewsItem JSON array or legacy pretty report",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Base directory for SQLite, raw payloads, run artifacts, and AIStock exports",
    )
    parser.add_argument(
        "--no-rolling",
        action="store_true",
        help=(
            "Disable rolling-window export. By default, latest_news_items.json "
            "contains ALL articles from the last --hours window (across runs). "
            "Use this flag to export only this run's results."
        ),
    )
    parser.add_argument(
        "--summarize-errors",
        type=int,
        metavar="HOURS",
        default=None,
        help=(
            "Print an aggregated error/warning report from the last N hours of runs "
            "(reads data/runs/*/errors.jsonl) and exit. Does not crawl."
        ),
    )
    parser.add_argument(
        "--no-ibkr-news",
        action="store_true",
        help=(
            "Disable the IBKR TWS wire-news channel (Dow Jones/Briefing via a "
            "running TWS/IB Gateway). The channel is skipped automatically when "
            "TWS is not running; watchlist runs only."
        ),
    )
    parser.add_argument(
        "--no-sec-edgar",
        action="store_true",
        help=(
            "Disable the SEC EDGAR filings channel (Form 4 / 8-K / 13D/G "
            "with exact acceptance timestamps for watchlist CIKs). "
            "Watchlist runs only."
        ),
    )
    args = parser.parse_args()
    runtime_paths = configure_runtime_paths(data_dir=args.data_dir)

    # Audit-only mode — no crawl, just print and exit
    if args.summarize_errors is not None:
        print_error_summary(hours=args.summarize_errors)
        return

    hours = args.hours

    if args.ticker_set:
        tickers = parse_tickers(None, preset=args.ticker_set)
    elif args.tickers:
        tickers = parse_tickers(args.tickers)
    elif args.ticker:
        tickers = parse_tickers(args.ticker)
    else:
        tickers = parse_tickers(None, preset=DEFAULT_TICKER_SET)

    run_dir = make_run_dir(f"{'_'.join(tickers[:3])}_{len(tickers)}")
    log_path = setup_run_logging(run_dir)

    import time as _time
    _progress("=" * 60)
    _progress(f"NewsCrawler  tickers={len(tickers)}  hours={hours}h  fulltext={args.fulltext_mode}")
    _progress(f"concurrency = {TICKER_CONCURRENCY} tickers  delays = {DOMAIN_DELAY}")
    _progress(f"data_dir  = {runtime_paths['DATA_DIR']}")
    _progress(f"run_dir   = {run_dir}")
    _progress(f"log       = {log_path}")
    _progress("=" * 60)
    log.info("run_start", run_dir=str(run_dir), data_dir=str(runtime_paths["DATA_DIR"]), tickers=tickers, hours=hours)

    t0 = _time.monotonic()

    if len(tickers) == 1:
        run_label = tickers[0]
        articles, raw, stats, new_count, bz_count = await crawl(
            tickers[0],
            hours,
            fulltext_mode=args.fulltext_mode,
            fulltext_max_articles=args.fulltext_max_articles,
        )
        per_ticker_stats = None
    else:
        run_label = f"{len(tickers)}-ticker watchlist"
        articles, raw, stats, new_count, bz_count, per_ticker_stats = await crawl_watchlist(
            tickers,
            hours,
            fulltext_mode=args.fulltext_mode,
            fulltext_max_articles=args.fulltext_max_articles,
            ibkr_news=not args.no_ibkr_news,
            sec_edgar=not args.no_sec_edgar,
        )
    news_items = to_news_items(articles)
    report_text = render_report(
        articles,
        raw,
        stats,
        new_count,
        bz_count,
        run_label,
        hours,
        per_ticker_stats,
    )

    # Rolling window: load all articles within the last `hours` from SQLite,
    # so the stable export accumulates across repeated runs. Disabled with --no-rolling.
    rolling_articles = None
    rolling_news_items = None
    if not args.no_rolling:
        rolling_articles = load_rolling_window(tickers, hours)
        rolling_news_items = to_news_items(rolling_articles)
        _progress(
            f"rolling window  this_run={len(articles):4d}  "
            f"last_{hours}h_total={len(rolling_articles):4d}"
        )

    persist_run_artifacts(
        run_dir,
        run_label=run_label,
        tickers=tickers,
        hours=hours,
        raw_articles=raw,
        filtered_articles=articles,
        news_items=news_items,
        channel_stats=stats,
        new_count=new_count,
        bz_count=bz_count,
        per_ticker_stats=per_ticker_stats,
        report_text=report_text,
        rolling_articles=rolling_articles,
        rolling_news_items=rolling_news_items,
    )
    log.info(
        "run_complete",
        run_dir=str(run_dir),
        log_path=str(log_path),
        raw=len(raw),
        filtered=len(articles),
        news_items=len(news_items),
    )

    # ── Summary banner ──────────────────────────────────────────────────────
    trust_counts: dict[str, int] = {}
    for a in articles:
        label = TRUST_LABEL.get(a.get("_trust", 1), "???")
        trust_counts[label] = trust_counts.get(label, 0) + 1
    trust_str = "  ".join(f"{k}:{v}" for k, v in sorted(trust_counts.items()))
    elapsed = _time.monotonic() - t0
    _progress("=" * 60)
    _progress(f"DONE  {run_label}  in {elapsed:.1f}s ({elapsed/60:.1f}min)")
    _progress(f"  raw={len(raw)}  filtered={len(articles)}  news_items={len(news_items)}  new_to_db={new_count}")
    _progress(f"  trust  {trust_str}")
    _progress(f"  bz={bz_count}  log={log_path}")
    # Cache statistics — visibility for incremental mode effectiveness
    if (_BODY_CACHE_HITS or _BODY_CACHE_MISSES or _GOOGLE_URL_HITS
            or _BODY_CACHE_SQLITE_HITS or _CONDITIONAL_GET_HITS_304):
        total_body = _BODY_CACHE_HITS + _BODY_CACHE_SQLITE_HITS + _BODY_CACHE_MISSES
        body_hit_pct = (_BODY_CACHE_HITS + _BODY_CACHE_SQLITE_HITS) * 100 // max(total_body, 1)
        _progress(
            f"  body_cache  in_run={_BODY_CACHE_HITS}  sqlite={_BODY_CACHE_SQLITE_HITS}  "
            f"misses={_BODY_CACHE_MISSES}  hit_rate={body_hit_pct}%"
        )
        _progress(
            f"  google_url  in_run={_GOOGLE_URL_HITS}  sqlite={_GOOGLE_URL_SQLITE_HITS}"
        )
        if _CONDITIONAL_GET_REQUESTS:
            cg_pct = _CONDITIONAL_GET_HITS_304 * 100 // max(_CONDITIONAL_GET_REQUESTS, 1)
            _progress(
                f"  http_304    hits={_CONDITIONAL_GET_HITS_304}/"
                f"{_CONDITIONAL_GET_REQUESTS}  ({cg_pct}%)"
            )
    _progress("=" * 60)

    if args.output == "pretty":
        print(f"\n[*] Crawling {run_label} news — quality-filtered, no API key\n")
        print(report_text, end="")
        print(f"\nArtifacts: {run_dir}")
        teardown_run_logging()
        return

    print(json.dumps(news_items, ensure_ascii=False, indent=2))
    teardown_run_logging()


if __name__ == "__main__":
    asyncio.run(main())
