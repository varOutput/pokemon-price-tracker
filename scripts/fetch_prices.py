#!/usr/bin/env python3
"""
Daily Pokémon TCG price puller (singles, sets and sealed product).

Pulls TCGplayer's catalog and prices for Pokémon (English, category 3) and
Pokémon Japan (category 85) from TCGCSV - a free, daily mirror of
TCGplayer's API (https://tcgcsv.com) - and writes:

  1. data/latest.db                    SQLite DB: sets, products (cards AND
                                        sealed product) and today's prices
  2. data/history/<YYYY-MM-DD>.csv.gz  that day's full price snapshot

TCGCSV rebuilds once a day (~20:05 UTC). Following its usage guidelines
(https://tcgcsv.com/docs), this script checks last-updated.txt first and
does nothing if that build is already saved; otherwise it makes ~1,400
requests (2 per set), with a custom User-Agent and a pause between requests.
The history file is named after TCGCSV's build date, so it lines up with
TCGCSV's price archive (see backfill_history.py).

After saving, it also fills any of the last few days that are missing
(e.g. a skipped run) from TCGCSV's price archive, one download per day.
"""

import csv
import gzip
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# --- Config -----------------------------------------------------------

# TCGCSV asks for an identifiable User-Agent; generic ones may be blocked.
USER_AGENT = "PokemonPriceTracker/1.0"
BASE_URL = "https://tcgcsv.com"
CATEGORIES = {3: "Pokemon", 85: "Pokemon Japan"}

REQUEST_TIMEOUT = 60
REQUEST_ATTEMPTS = 4
PAUSE_SECONDS = 0.15          # TCGCSV asks for >= 100 ms between requests
GAP_FILL_DAYS = 7             # fill missing history days this far back
# TCGCSV throttles over-eager clients for 10 minutes (HTTP 429). Wait it
# out rather than retrying into it; give up after this many throttles.
THROTTLE_WAIT_SECONDS = 660
MAX_THROTTLES = 2

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"
DB_PATH = DATA_DIR / "latest.db"

PRICE_FIELDS = ["low", "mid", "high", "market", "direct_low"]
API_PRICE_FIELDS = ["lowPrice", "midPrice", "highPrice", "marketPrice", "directLowPrice"]
HISTORY_COLUMNS = ["product_id", "sub_type", "snapshot_date", "category_id", "group_id",
                   *PRICE_FIELDS]

# A full Pokémon + Pokémon Japan snapshot was ~75k price rows in Nov 2026.
# Fewer than this means a partial/broken pull - fail rather than save it.
MIN_PRICE_ROWS = 50_000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("fetch_prices")


# --- TCGCSV access --------------------------------------------------------

def throttle_wait(response: requests.Response) -> int:
    """Seconds to wait after a 429: Retry-After if given, else TCGCSV's 10 min + margin."""
    try:
        return max(int(response.headers.get("Retry-After", "")), 1)
    except ValueError:
        return THROTTLE_WAIT_SECONDS


class TcgCsv:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        self.requests_made = 0

    def get(self, path: str, *, raw: bool = False):
        """GET with retries. Returns parsed JSON results list (or text if raw).
        A 404 means "no content" for products/prices and returns []."""
        url = f"{BASE_URL}/{path}"
        throttles = 0
        attempt = 0
        while attempt < REQUEST_ATTEMPTS:
            attempt += 1
            self.requests_made += 1
            try:
                r = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if r.status_code == 429:
                    throttles += 1
                    if throttles > MAX_THROTTLES:
                        raise RuntimeError(f"{url}: still throttled (429) after "
                                           f"{MAX_THROTTLES} waits - giving up for today")
                    wait = throttle_wait(r)
                    log.warning(f"  Throttled by TCGCSV (429); waiting {wait}s before continuing")
                    time.sleep(wait)
                    attempt -= 1  # a throttle isn't a failed attempt
                    continue
                if r.status_code == 404 and not raw:
                    return []
                r.raise_for_status()
                time.sleep(PAUSE_SECONDS)
                if raw:
                    return r.text
                body = r.json()
                if body.get("success") is False:
                    raise RuntimeError(f"{url}: {body.get('errors')}")
                return body.get("results") or []
            except (requests.RequestException, ValueError) as e:
                if attempt == REQUEST_ATTEMPTS:
                    raise
                wait = 10 * attempt
                log.warning(f"  {url} failed ({e}); retrying in {wait}s")
                time.sleep(wait)
        raise AssertionError("unreachable")


# --- Product classification ---------------------------------------------

# Ordered: first match wins (so "Booster Box Case" isn't filed as a box).
SEALED_TYPES = [
    ("booster_box_case", r"booster box case|booster display case"),
    ("etb_case", r"elite trainer box case"),
    ("case", r"\bcase\b"),
    ("booster_box", r"booster box|booster display"),
    ("display", r"\bdisplay\b"),            # e.g. Build & Battle Box Display
    ("elite_trainer_box", r"elite trainer box|\betb\b"),
    ("build_and_battle", r"build (?:&|and) battle"),
    ("booster_bundle", r"booster bundle"),
    ("blister", r"blister"),                # before packs: "3 Pack Blister"
    ("booster_pack", r"booster pack|sleeved booster|\bpack\b"),
    ("tin", r"\btins?\b"),
    ("prerelease_kit", r"prerelease"),
    ("deck", r"\bdecks?\b"),               # theme / battle / league decks
    ("collection_box", r"collection|premium|\bbox\b|chest"),
]
SEALED_TYPE_RES = [(t, re.compile(p, re.I)) for t, p in SEALED_TYPES]


def extended(product: dict) -> dict:
    return {e.get("name"): e.get("value") for e in product.get("extendedData") or []}


def classify(product: dict, ext: dict) -> tuple[int, str | None]:
    """(is_sealed, sealed_type). Per TCGCSV's docs, cards carry a Rarity or
    Number in extendedData; anything without either is sealed/other product."""
    if ext.get("Rarity") or ext.get("Number"):
        return 0, None
    name = product.get("name") or ""
    for sealed_type, pattern in SEALED_TYPE_RES:
        if pattern.search(name):
            return 1, sealed_type
    return 1, "other"


# --- Database ----------------------------------------------------------

SCHEMA = """
CREATE TABLE groups (
    group_id INTEGER PRIMARY KEY,
    category_id INTEGER,
    category_name TEXT,
    name TEXT,
    abbreviation TEXT,
    published_on TEXT,
    modified_on TEXT
);
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    category_id INTEGER,
    group_id INTEGER,
    name TEXT,
    clean_name TEXT,
    is_sealed INTEGER,          -- 1 = sealed/other product, 0 = single card
    sealed_type TEXT,           -- booster_box, elite_trainer_box, ... (sealed only)
    number TEXT,
    rarity TEXT,
    card_type TEXT,
    url TEXT,
    image_url TEXT,
    still_listed INTEGER,       -- 0 = gone from TCGplayer's catalog, kept for history
    last_seen TEXT
);
CREATE TABLE latest_prices (
    product_id INTEGER,
    sub_type TEXT,              -- Normal, Holofoil, Reverse Holofoil, 1st Edition, ...
    snapshot_date TEXT,
    low REAL, mid REAL, high REAL, market REAL, direct_low REAL,
    PRIMARY KEY (product_id, sub_type)
);
CREATE INDEX idx_products_group ON products(group_id);
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_sealed ON products(is_sealed, sealed_type);

-- Per-set value snapshot: what the singles add up to vs. what sealed costs.
CREATE VIEW set_values AS
SELECT g.group_id, g.category_name, g.name AS set_name, g.published_on,
       SUM(p.is_sealed = 0 AND lp.market IS NOT NULL)            AS priced_singles,
       ROUND(SUM(CASE WHEN p.is_sealed = 0 THEN lp.market END), 2) AS singles_market_total,
       MIN(CASE WHEN p.sealed_type = 'booster_box' THEN lp.market END)        AS booster_box_market,
       MIN(CASE WHEN p.sealed_type = 'elite_trainer_box' THEN lp.market END)  AS etb_market,
       MIN(CASE WHEN p.sealed_type = 'booster_pack' THEN lp.market END)       AS booster_pack_market
FROM groups g
JOIN products p ON p.group_id = g.group_id
LEFT JOIN latest_prices lp ON lp.product_id = p.product_id
GROUP BY g.group_id;
"""


def previous_products(db_path: Path) -> dict:
    """Products from the last latest.db, so delisted ones aren't forgotten."""
    if not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        rows = {r["product_id"]: dict(r) for r in conn.execute("SELECT * FROM products")}
        conn.close()
        return rows
    except sqlite3.DatabaseError as e:
        log.warning(f"Couldn't read previous products from {db_path} ({e}); starting fresh")
        return {}


# --- Main -----------------------------------------------------------------

def build_date(api: TcgCsv) -> str:
    stamp = api.get("last-updated.txt", raw=True).strip()
    return datetime.fromisoformat(stamp.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%d")


def sync(api: TcgCsv, snapshot_date: str) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    groups, products, prices = [], {}, []
    for cat_id, cat_name in CATEGORIES.items():
        cat_groups = api.get(f"tcgplayer/{cat_id}/groups")
        log.info(f"{cat_name}: {len(cat_groups)} sets")
        for i, g in enumerate(cat_groups, 1):
            gid = g["groupId"]
            groups.append((gid, cat_id, cat_name, g.get("name"), g.get("abbreviation"),
                           g.get("publishedOn"), g.get("modifiedOn")))
            for p in api.get(f"tcgplayer/{cat_id}/{gid}/products"):
                ext = extended(p)
                is_sealed, sealed_type = classify(p, ext)
                products[p["productId"]] = dict(
                    product_id=p["productId"], category_id=cat_id, group_id=gid,
                    name=p.get("name"), clean_name=p.get("cleanName"),
                    is_sealed=is_sealed, sealed_type=sealed_type,
                    number=ext.get("Number"), rarity=ext.get("Rarity"),
                    card_type=ext.get("Card Type"), url=p.get("url"),
                    image_url=p.get("imageUrl"), still_listed=1, last_seen=snapshot_date)
            for pr in api.get(f"tcgplayer/{cat_id}/{gid}/prices"):
                prices.append((pr["productId"], pr.get("subTypeName") or "", snapshot_date,
                               cat_id, gid, *(pr.get(f) for f in API_PRICE_FIELDS)))
            if i % 100 == 0:
                log.info(f"  {i}/{len(cat_groups)} sets, {api.requests_made} requests so far")

    log.info(f"Fetched {len(groups)} sets, {len(products):,} products, {len(prices):,} price rows "
             f"in {api.requests_made} requests")
    if len(prices) < MIN_PRICE_ROWS:
        raise RuntimeError(f"Only {len(prices):,} price rows (expected >= {MIN_PRICE_ROWS:,}) - "
                           f"looks like a partial pull; not saving")

    # Keep products that have dropped out of the catalog, flagged as delisted.
    kept = 0
    for pid, row in previous_products(DB_PATH).items():
        if pid not in products:
            row["still_listed"] = 0
            products[pid] = row
            kept += 1
    if kept:
        log.info(f"Kept {kept:,} delisted products from the previous latest.db")

    prices.sort(key=lambda r: (r[0], r[1]))  # stable order -> smaller, diffable files
    db_tmp = DATA_DIR / f".tmp-{snapshot_date}-latest.db"
    csv_tmp = HISTORY_DIR / f".tmp-{snapshot_date}.csv.gz"
    try:
        if db_tmp.exists():
            db_tmp.unlink()
        conn = sqlite3.connect(db_tmp)
        conn.executescript(SCHEMA)
        conn.executemany("INSERT OR REPLACE INTO groups VALUES (?,?,?,?,?,?,?)", groups)
        cols = ["product_id", "category_id", "group_id", "name", "clean_name", "is_sealed",
                "sealed_type", "number", "rarity", "card_type", "url", "image_url",
                "still_listed", "last_seen"]
        conn.executemany(f"INSERT INTO products VALUES ({','.join('?' * len(cols))})",
                         [tuple(p[c] for c in cols) for p in products.values()])
        conn.executemany("INSERT OR REPLACE INTO latest_prices VALUES (?,?,?,?,?,?,?,?)",
                         [(r[0], r[1], r[2], *r[5:]) for r in prices])
        conn.commit()
        conn.close()

        with gzip.open(csv_tmp, "wt", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(HISTORY_COLUMNS)
            w.writerows([["" if v is None else v for v in r] for r in prices])

        db_tmp.replace(DB_PATH)
        csv_tmp.replace(HISTORY_DIR / f"{snapshot_date}.csv.gz")
        log.info(f"Wrote {DB_PATH} and {HISTORY_DIR / f'{snapshot_date}.csv.gz'}")
    finally:
        for p in (db_tmp, csv_tmp):
            if p.exists():
                p.unlink()


def main() -> None:
    api = TcgCsv()
    snapshot_date = build_date(api)
    log.info(f"TCGCSV build date: {snapshot_date}")

    if (HISTORY_DIR / f"{snapshot_date}.csv.gz").exists():
        log.info("That build is already saved - nothing to sync.")
    else:
        sync(api, snapshot_date)

    # Fill recent gaps (missed runs) from the immutable price archive.
    from backfill_history import backfill  # local import: same scripts/ folder
    backfill(days_back=GAP_FILL_DAYS, until=snapshot_date)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    try:
        main()
    except Exception:
        log.exception("fetch_prices.py failed")
        sys.exit(1)
