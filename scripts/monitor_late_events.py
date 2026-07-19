#!/usr/bin/env python3
"""scripts/monitor_late_events.py — daily watchdog for late/missed high-value news.

The Rocket Lab→Iridium $8B deal (broke 01:00 UTC, NewsCrawler caught it 11h
later via a secondary aggregator) showed that breaking M&A can slip through
for hours when it first publishes on a wire/source we don't poll fast enough.

This monitor scans the rolling SQLite DB and flags HIGH-VALUE events
(M&A / regulatory / guidance / strong earnings surprise) that were:
  - caught ONLY via slow secondary channels (no wire_tripwire / ir_feed), AND
  - first observed with high latency vs the article's claimed publish time.

Each flag is a concrete "we were slow here" case + a source-coverage gap hint
(e.g. a deal that broke on BusinessWire, which our tripwire doesn't poll).
Designed to run daily inside daily_run; it is INFORMATIONAL (always exit 0) —
a flag is a lead to investigate, not a pipeline failure.

  python scripts/monitor_late_events.py            # last 24h
  python scripts/monitor_late_events.py --hours 48

Writes data/validation/late_events_latest.md + late_events_latest.json.
"""
from __future__ import annotations

import argparse
import io
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if sys.platform == "win32":
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from crawl_news import classify_events, DB_PATH  # noqa: E402

REPORT_MD = PROJECT_ROOT / "data" / "validation" / "late_events_latest.md"
REPORT_JSON = PROJECT_ROOT / "data" / "validation" / "late_events_latest.json"

# Channels that deliver an event at/near announcement (authoritative + fast).
FAST_CHANNELS = {"wire_tripwire", "ir_feed", "amd_ir"}
# Event types worth monitoring — breaking, market-moving, first-publication.
HIGH_VALUE_EVENTS = {"ma_activity", "regulatory", "earnings_guidance"}
# Latency (hours) above which a slow-only catch is considered "late".
LATE_THRESHOLD_H = 6.0


def _latency_h(pub: str | None, first_seen: str | None) -> float | None:
    if not pub or not first_seen:
        return None
    try:
        p = datetime.fromisoformat(pub)
        f = datetime.fromisoformat(first_seen)
        if p.tzinfo is None:
            p = p.replace(tzinfo=timezone.utc)
        if f.tzinfo is None:
            f = f.replace(tzinfo=timezone.utc)
        h = (f - p).total_seconds() / 3600
        return h if h >= 0 else None
    except Exception:
        return None


def scan(hours: int) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """SELECT published, first_seen_at, source_name, channel,
                      primary_ticker, title
               FROM news
               WHERE first_seen_at >= ? AND primary_ticker IS NOT NULL
               ORDER BY first_seen_at""",
            (cutoff,),
        ).fetchall()
    finally:
        conn.close()

    # Cluster by (ticker, high-value event type)
    clusters: dict[tuple, list] = defaultdict(list)
    for pub, fs, src, ch, tk, title in rows:
        ev = set(classify_events(title or "", "")) & HIGH_VALUE_EVENTS
        for e in ev:
            clusters[(tk, e)].append({
                "published": pub, "first_seen": fs, "source": src,
                "channel": ch, "title": title,
            })

    flagged = []
    total_hv = len(clusters)
    fast_covered = 0
    for (tk, ev), arts in clusters.items():
        arts.sort(key=lambda a: a["first_seen"] or "")
        had_fast = any(a["channel"] in FAST_CHANNELS for a in arts)
        if had_fast:
            fast_covered += 1
            continue
        earliest = arts[0]
        lat = _latency_h(earliest["published"], earliest["first_seen"])
        # Flag only when BOTH slow-only AND late (conservative → low noise)
        if lat is not None and lat >= LATE_THRESHOLD_H:
            flagged.append({
                "ticker": tk,
                "event_type": ev,
                "latency_h": round(lat, 1),
                "first_channel": earliest["channel"],
                "first_source": earliest["source"],
                "first_seen": earliest["first_seen"],
                "published": earliest["published"],
                "title": earliest["title"],
                "n_articles": len(arts),
            })

    flagged.sort(key=lambda f: -f["latency_h"])
    return {
        "window_hours": hours,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "high_value_clusters": total_hv,
        "fast_covered": fast_covered,
        "flagged_count": len(flagged),
        "flagged": flagged,
    }


def write_reports(result: dict) -> None:
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Late / Missed High-Value Event Monitor",
        "",
        f"- Scanned: {result['scanned_at']}",
        f"- Window: last {result['window_hours']}h",
        f"- High-value event clusters: {result['high_value_clusters']}",
        f"- Fast-channel covered (tripwire/IR): {result['fast_covered']}",
        f"- **Flagged (slow-only AND >{LATE_THRESHOLD_H:.0f}h late): {result['flagged_count']}**",
        "",
    ]
    if result["flagged"]:
        lines += [
            "| Ticker | Event | Latency | First channel | Source | Title |",
            "|--------|-------|--------:|---------------|--------|-------|",
        ]
        for f in result["flagged"]:
            title = (f["title"] or "")[:70].replace("|", "\\|")
            lines.append(
                f"| {f['ticker']} | {f['event_type']} | {f['latency_h']}h | "
                f"{f['first_channel']} | {f['first_source']} | {title} |"
            )
        lines += [
            "",
            "**Interpretation**: each row is a high-value event we caught only via a",
            "slow secondary aggregator, hours after its claimed publish time. Investigate",
            "whether it first broke on a wire/source we don't poll (BusinessWire, Bloomberg",
            "exclusive, SEC 8-K) and consider extending coverage.",
        ]
    else:
        lines.append("No late slow-only high-value events in the window. ✅")
    lines.append("")
    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24,
                        help="Look-back window in hours (default 24)")
    args = parser.parse_args()

    result = scan(args.hours)
    write_reports(result)

    print(f"High-value clusters: {result['high_value_clusters']}  "
          f"fast-covered: {result['fast_covered']}  "
          f"flagged(slow+late): {result['flagged_count']}")
    for f in result["flagged"][:15]:
        print(f"  ⚠️ {f['ticker']:>6} {f['event_type']:16s} {f['latency_h']:5.1f}h "
              f"[{f['first_channel']}] {(f['title'] or '')[:55]}")
    print(f"Report: {REPORT_MD}")
    # Informational monitor — never fails the pipeline.
    return 0


if __name__ == "__main__":
    sys.exit(main())
