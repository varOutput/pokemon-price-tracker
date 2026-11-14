# Pokémon Price Tracker

Keeps a daily price history for every Pokémon TCG product on TCGplayer:
single cards, sets, and sealed product (booster boxes, Elite Trainer Boxes,
packs, tins, collections, cases...), in English and Japanese. Runs on GitHub
Actions; no API keys needed.

## What it does

- Every day, a GitHub Actions workflow pulls TCGplayer's catalog and prices
  for **Pokémon** (TCGplayer category 3) and **Pokémon Japan** (category 85)
  from [TCGCSV](https://tcgcsv.com), a free daily mirror of TCGplayer's API.
- It writes:
  - **`data/latest.db`** - SQLite with every set, every product (cards and
    sealed) and today's prices. Overwritten each run.
  - **`data/history/YYYY-MM-DD.csv.gz`** - one compressed file per day,
    never overwritten, with that day's full price snapshot.
- History goes back to **2024-02-08** for English cards, backfilled from
  TCGCSV's price archive (`scripts/backfill_history.py`), with no missing
  days. **Pokémon Japan starts 2025-01-07**, when it first appears in
  TCGplayer's data. Any archive days found missing in future are listed in
  `data/archive_missing_dates.txt`.
- The daily job also fills any missed day from the last week out of the
  same archive, so a skipped run doesn't leave a gap.

## Setup

1. Go to the **Actions** tab and enable workflows if prompted.
2. Optional: run it once by hand - Actions → "Fetch Pokémon prices" →
   **Run workflow**.
3. After that it runs daily at 21:23 UTC (TCGCSV rebuilds around 20:05 UTC).
   Edit the `cron` line in `.github/workflows/fetch-prices.yml` to change it.

## The data

### Prices

TCGplayer publishes one price row per product **and printing variant**
(`sub_type`: `Normal`, `Holofoil`, `Reverse Holofoil`, `1st Edition`,
`Unlimited`, ...). Sealed product is `Normal`. All prices are USD:

| column | meaning |
|---|---|
| `market` | TCGplayer Market Price - based on recent sales. The one to use. |
| `low` | lowest current listing (closest thing to "what it costs right now") |
| `mid` | midpoint of current listings |
| `high` | highest listing - **mostly noise**: sellers "park" listings at absurd prices |
| `direct_low` | lowest TCGplayer Direct listing |

These are near-mint, ungraded prices. There's no condition breakdown, no
graded (PSA/BGS/CGC) prices, no buylist and no Cardmarket (EU) data.

### Products and sets (`latest.db`)

- **`groups`** - one row per set (TCGplayer calls them groups), with
  `category_name` (`Pokemon` / `Pokemon Japan`) and release date.
- **`products`** - every card and sealed item:
  - `is_sealed` - 0 for single cards (anything TCGplayer gives a card
    number or rarity), 1 for everything else.
  - `sealed_type` - from the product name: `booster_box`,
    `booster_box_case`, `elite_trainer_box`, `etb_case`, `booster_pack`,
    `booster_bundle`, `blister`, `tin`, `collection_box`, `deck`,
    `build_and_battle`, `display`, `case`, `prerelease_kit`, `other`.
  - `still_listed` - 0 for products that have since vanished from
    TCGplayer's catalog. They're kept so old history still has names.
- **`latest_prices`** - today's prices.
- **`set_values`** (view) - per set: number of priced singles, the sum of
  their market prices (every card in every variant - a "master set"), and
  booster box / Elite Trainer Box / pack market prices.

```sql
-- Today's sealed prices for a set
SELECT p.name, p.sealed_type, lp.market, lp.low
FROM products p JOIN latest_prices lp USING (product_id)
JOIN groups g USING (group_id)
WHERE g.name = 'SWSH12: Silver Tempest' AND p.is_sealed = 1
ORDER BY lp.market DESC;

-- Most valuable English cards
SELECT p.name, g.name AS set_name, lp.sub_type, lp.market
FROM products p JOIN latest_prices lp USING (product_id) JOIN groups g USING (group_id)
WHERE p.is_sealed = 0 AND p.category_id = 3
ORDER BY lp.market DESC LIMIT 20;

-- Singles value vs. booster box price, newest English sets
SELECT set_name, priced_singles, singles_market_total, booster_box_market
FROM set_values WHERE category_name = 'Pokemon' AND booster_box_market IS NOT NULL
ORDER BY published_on DESC LIMIT 10;
```

### Full price history

History files hold prices plus IDs (`product_id`, `sub_type`,
`snapshot_date`, `category_id`, `group_id`); names come from `latest.db`.
To query history, build a combined database locally:

```bash
pip install -r requirements.txt
python scripts/build_history_db.py              # -> history.db
python scripts/build_history_db.py --since 2025-01-01
```

```sql
-- A booster box's market price over time
SELECT h.snapshot_date, h.market
FROM price_history h JOIN products p USING (product_id)
WHERE p.name = 'Silver Tempest Booster Box'
ORDER BY h.snapshot_date;
```

`history.db` includes the `products` and `groups` tables, and isn't
committed - rebuild it whenever you want it.

### Backfilling

```bash
python scripts/backfill_history.py                  # all missing days since 2024-02-08
python scripts/backfill_history.py --since 2025-06-01
python scripts/backfill_history.py --dry-run
```

Needs the `7z` command (`apt install p7zip-full` / `brew install p7zip`).
Each archive day is downloaded at most once: days already saved are skipped,
and unavailable days are recorded in `data/archive_missing_dates.txt` (delete
a line to retry that day).

## Being a good TCGCSV citizen

TCGCSV is a free, one-person hobby project. This repo follows its
[usage guidelines](https://tcgcsv.com/docs): it checks `last-updated.txt`
and skips the sync if that build is already saved, sends an identifiable
User-Agent, pauses between requests, makes ~1,400 requests per daily sync
(limit: 10,000/day), and never re-downloads an archive day. Please keep it
that way if you change the scripts. A thank-you link to
[tcgcsv.com](https://tcgcsv.com) is appreciated by its maintainer.

## A note on scope and licensing

The prices are TCGplayer's, redistributed by TCGCSV. TCGCSV needs no
permission, but check TCGplayer's terms before using the data in a
commercial product, and see TCGCSV's FAQ on card images and copyright:
free community tools are generally fine; products that charge for the data
are on much shakier ground.

## Files

```
.github/workflows/fetch-prices.yml   the daily job
scripts/fetch_prices.py               daily sync: latest.db + today's history file, then fills recent gaps
scripts/backfill_history.py           fills history from TCGCSV's archive (back to 2024-02-08)
scripts/build_history_db.py           (run locally) combines history/*.csv.gz into one queryable DB
data/latest.db                        current catalog + prices
data/history/YYYY-MM-DD.csv.gz        one file per day
data/archive_missing_dates.txt        archive days that don't exist (created if any)
```
