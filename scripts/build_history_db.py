#!/usr/bin/env python3
"""
Combine the daily data/history/*.csv.gz snapshots into one local SQLite
database for querying price history, with the catalog (sets, products,
sealed flags) from data/latest.db attached alongside.

Run locally, on demand; the output isn't committed (it only duplicates
the history files).

Usage:
    python scripts/build_history_db.py
    python scripts/build_history_db.py --out my_history.db --since 2025-01-01
"""

import argparse
import csv
import gzip
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORY_DIR = REPO_ROOT / "data" / "history"
LATEST_DB = REPO_ROOT / "data" / "latest.db"
PRICE_FIELDS = ["low", "mid", "high", "market", "direct_low"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="history.db", help="Output SQLite file (default: history.db)")
    parser.add_argument("--since", default=None, help="Only include snapshots on/after YYYY-MM-DD")
    args = parser.parse_args()

    files = [f for f in sorted(HISTORY_DIR.glob("*.csv.gz")) if not f.name.startswith(".tmp-")]
    if args.since:
        files = [f for f in files if f.name.removesuffix(".csv.gz") >= args.since]
    if not files:
        print(f"No history files found in {HISTORY_DIR} (matching --since, if given).")
        return

    out_path = Path(args.out)
    if out_path.exists():
        out_path.unlink()
    conn = sqlite3.connect(out_path)
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE price_history (
            product_id INTEGER,
            sub_type TEXT,
            snapshot_date TEXT,
            category_id INTEGER,
            group_id INTEGER,
            {", ".join(f"{f} REAL" for f in PRICE_FIELDS)}
        )
    """)
    insert_sql = f"INSERT INTO price_history VALUES ({','.join('?' * (5 + len(PRICE_FIELDS)))})"

    total = 0
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8", newline="") as fh:
            rows = [
                (int(r["product_id"]), r["sub_type"], r["snapshot_date"], int(r["category_id"]),
                 int(r["group_id"]), *(float(r[c]) if r.get(c) else None for c in PRICE_FIELDS))
                for r in csv.DictReader(fh)
            ]
        cur.executemany(insert_sql, rows)
        total += len(rows)
        print(f"  loaded {f.name}: {len(rows):,} rows")

    # Copy the catalog in so history can be joined to names, sets and
    # sealed types without attaching a second database.
    if LATEST_DB.exists():
        cur.execute("ATTACH DATABASE ? AS latest", (str(LATEST_DB),))
        cur.execute("CREATE TABLE groups AS SELECT * FROM latest.groups")
        cur.execute("CREATE TABLE products AS SELECT * FROM latest.products")
        conn.commit()
        cur.execute("DETACH DATABASE latest")
        cur.execute("CREATE INDEX idx_products_group ON products(group_id)")
        cur.execute("CREATE INDEX idx_products_name ON products(name)")
    else:
        print(f"Note: {LATEST_DB} not found - history has IDs only, no names.")

    print("Indexing...")
    cur.execute("CREATE INDEX idx_hist_product_date ON price_history(product_id, sub_type, snapshot_date)")
    cur.execute("CREATE INDEX idx_hist_group_date ON price_history(group_id, snapshot_date)")
    conn.commit()
    conn.close()

    print(f"\nDone: {total:,} rows from {len(files)} day(s) written to {out_path}")
    print(f'Example query: sqlite3 {out_path} "SELECT h.snapshot_date, h.market FROM price_history h '
          f"JOIN products p USING (product_id) WHERE p.name = 'Silver Tempest Booster Box' "
          f'ORDER BY h.snapshot_date;"')


if __name__ == "__main__":
    main()
