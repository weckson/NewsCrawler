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

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
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
DEFAULT_FULLTEXT_MAX_ARTICLES = 8

# ── Concurrency & rate-limit tuning ──────────────────────────────────────────
TICKER_CONCURRENCY = 5          # tickers fetched in parallel
DOMAIN_DELAY: dict[str, float] = {
    "news.google.com": 0.3,     # Google RSS is very tolerant
    "www.benzinga.com": 1.5,    # Benzinga RSS — be polite
    "default": 1.0,             # everything else
}


class _DomainRateLimiter:
    """Per-domain async rate limiter — different delays for different hosts."""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, domain: str) -> asyncio.Lock:
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()
        return self._locks[domain]

    async def wait(self, url: str) -> None:
        from urllib.parse import urlparse as _urlparse
        import time as _time
        domain = _urlparse(url).netloc
        delay = DOMAIN_DELAY.get(domain, DOMAIN_DELAY["default"])
        lock = self._lock_for(domain)
        async with lock:
            now = _time.monotonic()
            elapsed = now - self._last.get(domain, 0.0)
            if elapsed < delay:
                await asyncio.sleep(delay - elapsed)
            self._last[domain] = _time.monotonic()


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
    "barchart.com": 3, "zacks.com": 3, "tipranks.com": 3,
    "ir.amd.com": 3,
    # Tier 2 — Good quality
    "investors.com": 2, "nasdaq.com": 2, "morningstar.com": 2,
    "investing.com": 2, "tradingview.com": 2, "247wallst.com": 2,
    "thefly.com": 2, "invezz.com": 2, "insidermonkey.com": 2,
    "wccftech.com": 2, "tomshardware.com": 2, "anandtech.com": 2,
    "arstechnica.com": 2, "theverge.com": 2, "kiplinger.com": 2,
    "theglobeandmail.com": 2,
    # Tier 1 — Acceptable
    "simplywall.st": 1, "gurufocus.com": 1, "stockanalysis.com": 1,
}

BLOCKED_DOMAINS = {
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
    r"\bhow investors may respond to\b",
    r"\bthe \$\d+ trillion opportunity\b",
]

WEAK_SIGNAL_PATTERNS = [
    r"\bshares sold by\b",
    r"\bholdings in\b",
    r"\braises stock holdings in\b",
    r"\blowers position in\b",
    r"\breduces position in\b",
    r"\bacquires \d[\d,]* shares of\b",
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
QUALITY_THRESHOLD = 0.40       # articles below this are dropped
QUALITY_THRESHOLD_RELAXED = 0.35  # fallback for tickers with < 2 articles

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
    "all": ["all", "allstate", "allstate corporation"],
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
    "ir": ["ir", "ingersoll rand"],
    "j": ["j", "jacobs", "jacobs solutions"],
    "key": ["key", "keycorp"],
    "l": ["l", "loews", "loews corporation"],
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
    "ter": ["ter", "teradyne"],
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
}

CONTEXT_ONLY_TICKERS = {ticker for ticker in AISTOCK500_TICKERS if len(ticker) <= 2} | {
    "ALL", "APP", "ARE", "DOC", "GEN", "KEY", "NOW", "SO", "SW", "TECH",
}

OFFICIAL_IR_FEEDS = {
    "AMD": [
        {
            "name": "AMD IR",
            "type": "rss",
            "tag": "amd_ir",
            "urls": [
                "https://ir.amd.com/news-events/press-releases/rss",
            ],
        }
    ]
}


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE DEFINITIONS — 3 Benzinga channels + broad coverage
# ══════════════════════════════════════════════════════════════════════════════

def build_sources(ticker: str, *, include_global_feeds: bool = True) -> list[dict]:
    t = ticker.upper()
    query = _build_query_term(t)
    secondary_query = _build_secondary_query_term(t)

    sources = []
    if include_global_feeds:
        sources.extend(
            [
                # ── Channel 1: Benzinga official RSS feeds ──
                # No Cloudflare challenge, structured XML, always works
                {
                    "name": "Benzinga RSS",
                    "type": "rss",
                    "tag": "benzinga_rss",
                    "urls": [
                        "https://www.benzinga.com/feed",
                        "https://www.benzinga.com/news/feed",
                    ],
                },
            ]
        )

    sources.extend(
        [
            # ── Channel 2: Google News -> Benzinga only ──
        # Uses Google as a proxy to get Benzinga articles
        # Bypasses Cloudflare entirely (Google already crawled it)
        # ~100 results per query
        {
            "name": "Google->Benzinga",
            "type": "rss",
            "tag": "google_benzinga",
            "urls": [
                f"https://news.google.com/rss/search?q={quote_plus(query)}+site:benzinga.com&hl=en-US&gl=US&ceid=US:en",
                f"https://news.google.com/rss/search?q={quote_plus(secondary_query)}+site:benzinga.com&hl=en-US&gl=US&ceid=US:en",
            ],
        },
        # ── Channel 3: Google News broad (all sources) ──
        # For comparison and cross-source coverage
        {
            "name": "Google News (all)",
            "type": "rss",
            "tag": "google_broad",
            "urls": [
                f"https://news.google.com/rss/search?q={quote_plus(query)}+stock+news&hl=en-US&gl=US&ceid=US:en",
                f"https://news.google.com/rss/search?q={quote_plus(query)}+earnings+analysis&hl=en-US&gl=US&ceid=US:en",
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
    if ticker.upper() in CONTEXT_ONLY_TICKERS:
        symbol_count = _count_contextual_ticker_mentions(text, ticker.upper())
    else:
        symbol_count = _count_phrase_mentions(text, ticker.lower())
    alias_count = sum(
        _count_phrase_mentions(text, alias)
        for alias in aliases
        if alias != ticker.lower()
    )
    return symbol_count + alias_count


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
    if "marketbeat.com" in source_domain and re.search(r"\b(holdings in|shares sold by|raises stock holdings in)\b", t):
        return True
    for pat in WEAK_SIGNAL_PATTERNS:
        if re.search(pat, t, re.IGNORECASE):
            return True
    return False


# ── Quality scoring signals ──────────────────────────────────────────────────

def _signal_source_authority(trust: int) -> float:
    """Map trust tier to [0, 1]. Dominant signal — source reputation."""
    return {3: 1.0, 2: 0.7, 1: 0.3, 0: 0.0}.get(trust, 0.3)


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
    global _RUN_FILE_HANDLER
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
    return log_path


def teardown_run_logging() -> None:
    global _RUN_FILE_HANDLER
    if _RUN_FILE_HANDLER is None:
        return
    root = logging.getLogger()
    root.removeHandler(_RUN_FILE_HANDLER)
    _RUN_FILE_HANDLER.close()
    _RUN_FILE_HANDLER = None


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")


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
) -> None:
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
        filtered_articles=filtered_articles,
        news_items=news_items,
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


def dedup_articles(articles: list[dict]) -> list[dict]:
    """Keep highest-trust version of near-duplicate stories."""
    articles.sort(key=lambda a: (-a.get("_trust", 0), -a.get("_relevance", 0)))
    kept = []
    hashes: list[tuple[int, int]] = []  # (simhash, index_in_kept)

    for article in articles:
        title = article["title"].rsplit(" - ", 1)[0] if " - " in article["title"] else article["title"]
        sh = _simhash(title)
        is_dup = False
        for eh, idx in hashes:
            if _hamming(sh, eh) <= 8:
                is_dup = True
                alt = article.get("_source_name", "")
                if alt and alt not in kept[idx].get("_alt_sources", []):
                    kept[idx].setdefault("_alt_sources", []).append(alt)
                break
        if not is_dup:
            kept.append(article)
            hashes.append((sh, len(kept) - 1))

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

        # ③ Relevance (must be about our ticker)
        rel = relevance_score(clean_title, summary, ticker)
        if rel < 0.10:
            continue
        if a.get("_channel") == "benzinga_rss" and rel < 0.55:
            continue
        a["_relevance"] = rel

        # ④ Junk title
        if is_junk(clean_title):
            continue

        # ⑤ Weak-signal investor/holdings/profile content
        if is_weak_signal(clean_title, a.get("_source_domain", "")):
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
    if channel == "amd_ir":
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

        # For Benzinga RSS channel: articles are from benzinga.com
        if source_tag == "benzinga_rss":
            source_name = "Benzinga"
            source_domain = "benzinga.com"
            trust = 3
        elif source_tag == "amd_ir":
            source_name = "AMD Investor Relations"
            source_domain = "ir.amd.com"
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

async def fetch_url(session: aiohttp.ClientSession, url: str) -> bytes | None:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            log.info("fetch", url=url[:90], status=resp.status)
            if resp.status == 200:
                return await resp.read()
            return None
    except Exception as exc:
        log.error("fetch_error", url=url[:70], error=str(exc)[:80])
        return None


async def fetch_article_body(session: aiohttp.ClientSession, article: dict) -> None:
    url = article.get("url") or ""
    if not url:
        return
    if "benzinga.com" in (article.get("_source_domain") or ""):
        return

    body_bytes = await fetch_url(session, url)
    if not body_bytes:
        return

    try:
        html = body_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return

    body = extract_article_text(html)
    if len(body) < 200:
        return
    article["body"] = body


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
    url = article.get("url") or ""
    if "news.google.com" not in url:
        return
    decoded = await decode_google_news_url(session, url)
    if decoded:
        article["url"] = decoded
        article["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, decoded or article.get("title", "")))


async def enrich_articles_with_bodies(articles: list[dict]) -> None:
    if not articles:
        return

    connector = aiohttp.TCPConnector(limit=ARTICLE_FETCH_CONCURRENCY, ttl_dns_cache=300)
    semaphore = asyncio.Semaphore(ARTICLE_FETCH_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
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
            UNIQUE(url)
        );
    """)
    _migrate_news_table(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_published ON news(published DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tickers ON news(tickers)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_channel ON news(channel)")
    conn.commit()
    return conn


def _migrate_news_table(conn) -> None:
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
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(news)").fetchall()}
    if "idx_ticker" in indexes:
        conn.execute("DROP INDEX IF EXISTS idx_ticker")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tickers ON news(tickers)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_primary_ticker ON news(primary_ticker)")


def save_article(conn, article: dict) -> bool:
    tickers = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
    primary_ticker = tickers[0] if tickers else None
    payload = (
        article["id"],
        article["title"],
        article["url"],
        article.get("_source_name", ""),
        article.get("_source_domain", ""),
        article.get("_trust", 1),
        article.get("_relevance", 0.0),
        article.get("_channel", ""),
        article.get("published"),
        article.get("summary"),
        article.get("body"),
        primary_ticker,
        ",".join(tickers),
        ",".join(article.get("_alt_sources", [])),
        datetime.now(timezone.utc).isoformat(),
    )
    cur = conn.execute(
        """INSERT INTO news
        (id, title, url, source_name, source_domain, trust_tier,
         relevance, channel, published, summary, body, primary_ticker, tickers, alt_sources, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            id = excluded.id,
            title = excluded.title,
            source_name = excluded.source_name,
            source_domain = excluded.source_domain,
            trust_tier = excluded.trust_tier,
            relevance = excluded.relevance,
            channel = excluded.channel,
            published = excluded.published,
            summary = excluded.summary,
            body = excluded.body,
            primary_ticker = excluded.primary_ticker,
            tickers = excluded.tickers,
            alt_sources = excluded.alt_sources,
            fetched_at = excluded.fetched_at
        """,
        payload,
    )
    conn.commit()
    return cur.rowcount > 0


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
        _progress(f"  [{ticker}] fetching {n_sources} channel(s)" + (f" (+{len(all_raw)} shared BZ)" if _shared_bz_articles else ""))

    owns_session = _session is None
    if owns_session:
        connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
        _session = aiohttp.ClientSession(connector=connector)

    try:
        for i, source in enumerate(sources, 1):
            tag = source["tag"]
            log.info("crawling", channel=source["name"], tag=tag)
            for url in source["urls"]:
                await _rate_limiter.wait(url)
                body = await fetch_url(_session, url)
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
    """Fetch Benzinga RSS once globally — shared across all tickers."""
    bz_urls = [
        "https://www.benzinga.com/feed",
        "https://www.benzinga.com/news/feed",
    ]
    all_articles: list[dict] = []
    for url in bz_urls:
        await _rate_limiter.wait(url)
        body = await fetch_url(session, url)
        if body is None:
            continue
        save_raw(body, "benzinga_rss")
        articles = parse_rss(body, "benzinga_rss", "")
        all_articles.extend(articles)
    _progress(f"  [global] Benzinga RSS fetched once", articles=len(all_articles))
    return all_articles


async def crawl_watchlist(
    tickers: list[str],
    hours: int,
    *,
    fulltext_mode: str = DEFAULT_FULLTEXT_MODE,
    fulltext_max_articles: int = DEFAULT_FULLTEXT_MAX_ARTICLES,
):
    import time as _time
    t0 = _time.monotonic()

    conn = init_db()
    all_filtered: list[dict] = []
    all_raw: list[dict] = []
    channel_stats: dict[str, int] = defaultdict(int)
    per_ticker_stats: dict[str, int] = {}
    total_new_count = 0

    # Shared HTTP session for all tickers — connection pooling
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Step 1: fetch Benzinga RSS once, share with all tickers
        shared_bz = await _fetch_shared_benzinga_rss(session)

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

    for ticker, filtered, raw, stats in results:
        all_filtered.extend(filtered)
        all_raw.extend(raw)
        per_ticker_stats[ticker] = len(filtered)
        for channel, count in stats.items():
            channel_stats[channel] += count

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
      id, timestamp_utc, source, url, title, body, author, language,
      ticker (primary), tickers_hint (all), source_quality, publisher_raw,
      trust_tier, body_kind, ingest_source
    meta contains supplementary info (source_name, source_domain, relevance,
      event_origin) plus internal crawl fields prefixed with "_".
    """
    summary = article.get("summary") or None
    body = article.get("body") or summary
    tickers = sorted(set(article.get("_tickers") or ([article.get("_ticker")] if article.get("_ticker") else [])))
    source, publisher_raw = _canonical_news_source(article)
    source_quality = _source_quality(article)
    primary_ticker = tickers[0] if tickers else None
    trust_tier = article.get("_trust")
    body_kind = ("article_text" if article.get("body") else "summary_snippet") if body else None

    meta: dict = {
        "source_name": article.get("_source_name"),
        "source_domain": article.get("_source_domain"),
        "event_origin": "news",
        "relevance": article.get("_relevance"),
        "quality_score": article.get("_quality_score"),
        "_channel": article.get("_channel"),
    }
    alt_sources = article.get("_alt_sources") or []
    if alt_sources:
        meta["_alt_sources"] = alt_sources

    return {
        "id": article.get("id") or str(uuid.uuid5(uuid.NAMESPACE_URL, article.get("url") or article.get("title", ""))),
        "timestamp_utc": _to_timestamp_utc(article.get("published")),
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
        help="Per-ticker cap for full-text fetch in high-value mode (default: 8)",
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
    args = parser.parse_args()
    runtime_paths = configure_runtime_paths(data_dir=args.data_dir)

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
