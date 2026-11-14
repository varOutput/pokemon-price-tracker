#!/usr/bin/env python3
"""
Backfill data/history/ from TCGCSV's daily price archive, which goes back to
2024-02-08 (https://tcgcsv.com/faq). Each archive day becomes one
data/history/<YYYY-MM-DD>.csv.gz in exactly the format fetch_prices.py
writes, holding Pokémon + Pokémon Japan prices.

Rules this follows (TCGCSV usage guidelines, https://tcgcsv.com/docs):
  - Archives are immutable; each day is downloaded at most once. Days that
    already have a history file are skipped, and days whose archive is
    missing or unusable are recorded in data/archive_missing_dates.txt so
    re-runs never request them again.
  - A pause between downloads and an identifiable User-Agent.

The archive holds prices only (not names/sets); names come from
data/latest.db's products table, which keeps delisted products too.

Requires the `7z` command (p7zip). GitHub's ubuntu runners have it;
locally: apt install p7zip-full / brew install p7zip.

Usage:
    python scripts/backfill_history.py                      # everything since 2024-02-08
    python scripts/backfill_history.py --since 2025-01-01
    python scripts/backfill_history.py --dry-run            # list days it would fetch
"""

import argparse
import csv
import gzip
import json
import logging
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_prices import (  # noqa: E402
    API_PRICE_FIELDS, BASE_URL, CATEGORIES, DATA_DIR, HISTORY_COLUMNS, HISTORY_DIR,
    MAX_THROTTLES, USER_AGENT, throttle_wait,
)

ARCHIVE_START = date(2024, 2, 8)
ARCHIVE_URL = BASE_URL + "/archive/tcgplayer/prices-{day}.ppmd.7z"
MISSING_FILE = DATA_DIR / "archive_missing_dates.txt"
# Archives are TCGCSV's main bandwidth cost; ~2 s apart got throttled (429)
# after ~110 days, so stay well below that.
PAUSE_SECONDS = 5.0
# Early-2024 snapshots are smaller than today's ~75k rows; below this a day
# is treated as broken and recorded as unusable rather than saved.
MIN_ARCHIVE_ROWS = 20_000

log = logging.getLogger("backfill_history")


def load_missing() -> dict:
    if not MISSING_FILE.exists():
        return {}
    out = {}
    for line in MISSING_FILE.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            day, _, reason = line.partition("\t")
            out[day.strip()] = reason.strip()
    return out


def record_missing(day: str, reason: str) -> None:
    new = not MISSING_FILE.exists()
    with open(MISSING_FILE, "a", encoding="utf-8") as f:
        if new:
            f.write("# Days whose TCGCSV price archive was missing or unusable.\n"
                    "# backfill_history.py never re-downloads these. Delete a line to retry it.\n")
        f.write(f"{day}\t{reason}\n")


def archive_rows(extract_dir: Path, day: str) -> list:
    """Price rows for the tracked categories from an extracted archive day."""
    rows = []
    for cat_id in CATEGORIES:
        cat_dir = extract_dir / day / str(cat_id)
        if not cat_dir.is_dir():
            continue
        for prices_file in cat_dir.glob("*/prices"):
            group_id = int(prices_file.parent.name)
            with open(prices_file, encoding="utf-8") as f:
                body = json.load(f)
            for pr in body.get("results") or []:
                rows.append((pr["productId"], pr.get("subTypeName") or "", day, cat_id, group_id,
                             *(pr.get(k) for k in API_PRICE_FIELDS)))
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows


def fetch_day(session: requests.Session, day: str, tmp: Path) -> tuple[list | None, str]:
    """Download + extract one archive day. Returns (rows, reason_if_none)."""
    archive = tmp / f"prices-{day}.ppmd.7z"
    for throttles in range(MAX_THROTTLES + 1):
        r = session.get(ARCHIVE_URL.format(day=day), timeout=300, stream=True)
        if r.status_code != 429:
            break
        r.close()
        if throttles == MAX_THROTTLES:
            # Not the day's fault - stop the run (nothing recorded); re-run later.
            raise RuntimeError(f"Still throttled (429) after {MAX_THROTTLES} waits; "
                               f"stopping. Re-run later to continue from {day}.")
        wait = throttle_wait(r)
        log.warning(f"  Throttled by TCGCSV (429); waiting {wait}s before continuing")
        time.sleep(wait)
    if r.status_code in (403, 404):
        return None, f"archive not found (HTTP {r.status_code})"
    r.raise_for_status()
    with open(archive, "wb") as f:
        for chunk in r.iter_content(1024 * 1024):
            f.write(chunk)
    out_dir = tmp / "x"
    patterns = [f"{day}/{c}/*" for c in CATEGORIES]
    proc = subprocess.run(["7z", "x", "-y", "-bso0", "-bsp0", f"-o{out_dir}", str(archive), *patterns],
                          capture_output=True, text=True)
    try:
        if proc.returncode != 0:
            return None, f"7z failed: {proc.stderr.strip()[:200]}"
        return archive_rows(out_dir, day), ""
    finally:
        archive.unlink(missing_ok=True)
        shutil.rmtree(out_dir, ignore_errors=True)


def write_day(day: str, rows: list) -> Path:
    final = HISTORY_DIR / f"{day}.csv.gz"
    tmp = HISTORY_DIR / f".tmp-{day}.csv.gz"
    try:
        with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(HISTORY_COLUMNS)
            w.writerows([["" if v is None else v for v in r] for r in rows])
        tmp.replace(final)
    finally:
        tmp.unlink(missing_ok=True)
    return final


def backfill(*, since: date | None = None, days_back: int | None = None,
             until: str | None = None, dry_run: bool = False) -> dict:
    """Fill missing history days in [since, until]. Returns counts."""
    end = date.fromisoformat(until) if until else datetime.now(timezone.utc).date() - timedelta(days=1)
    start = since or ARCHIVE_START
    if days_back is not None:
        start = max(start, end - timedelta(days=days_back))
    start = max(start, ARCHIVE_START)

    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    missing = load_missing()
    todo = []
    d = start
    while d <= end:
        day = d.isoformat()
        if not (HISTORY_DIR / f"{day}.csv.gz").exists() and day not in missing:
            todo.append(day)
        d += timedelta(days=1)

    counts = {"written": 0, "unavailable": 0, "todo": len(todo)}
    if not todo:
        log.info(f"History complete from {start} to {end}; nothing to backfill.")
        return counts
    log.info(f"{len(todo)} day(s) to backfill between {todo[0]} and {todo[-1]}")
    if dry_run:
        log.info("Dry run: " + ", ".join(todo))
        return counts
    if not shutil.which("7z"):
        raise RuntimeError("The `7z` command is required (apt install p7zip-full / brew install p7zip)")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    with tempfile.TemporaryDirectory() as tmp:
        for i, day in enumerate(todo, 1):
            rows, reason = fetch_day(session, day, Path(tmp))
            if rows is not None and len(rows) < MIN_ARCHIVE_ROWS:
                rows, reason = None, f"only {len(rows):,} rows (expected >= {MIN_ARCHIVE_ROWS:,})"
            if rows is None:
                log.warning(f"  {day}: {reason} - recorded, won't retry")
                record_missing(day, reason)
                counts["unavailable"] += 1
            else:
                write_day(day, rows)
                counts["written"] += 1
                log.info(f"  [{i}/{len(todo)}] {day}: {len(rows):,} rows")
            time.sleep(PAUSE_SECONDS)
    log.info(f"Backfill done: {counts['written']} written, {counts['unavailable']} unavailable")
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", type=date.fromisoformat, help="First day (YYYY-MM-DD)")
    parser.add_argument("--until", help="Last day (default: yesterday, UTC)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    backfill(since=args.since, until=args.until, dry_run=args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger("backfill_history").exception("backfill failed")
        sys.exit(1)
