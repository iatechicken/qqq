```md
# QQQ 1-Minute Data Ingestion (Schwab) → Parquet

This repo ingests **QQQ 1-minute candles** from the **Schwab Market Data API**, filters to **premarket + RTH (US/Eastern)**, and writes a canonical dataset as **Parquet partitions** by `date_et`.

It also includes validation utilities to sanity-check Parquet outputs (schema, OHLC integrity, duplicates, missing minutes, gaps, etc.).

---

## Repo Layout

```

data/
candles/
date_et=YYYY-MM-DD/
part.parquet

logs/
validate_summary.csv
backfill_state.json

scripts/
schwab_oauth_init.py
run_small_backfill.py
backfill_18m.py
update_daily.py
validate_range.py

src/
qqq_ingest/
...
tokens.json        # local only (DO NOT COMMIT)
.env               # local only (DO NOT COMMIT)

````

Parquet partitions are written to:

- `data/candles/date_et=YYYY-MM-DD/part.parquet`

---

## Requirements

- macOS / Linux terminal
- Python 3.10+
- A Schwab developer app with API credentials

---

## Setup

### 1) Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
````

### 2) Create `.env` with Schwab credentials (local only)

Create a `.env` file in the repo root (make sure `.env` is in `.gitignore`):

```bash
SCHWAB_CLIENT_ID="YOUR_CLIENT_ID"
SCHWAB_CLIENT_SECRET="YOUR_CLIENT_SECRET"
SCHWAB_REDIRECT_URI="https://127.0.0.1"
```

> `SCHWAB_REDIRECT_URI` must match the callback URL you registered in the Schwab Developer Portal (exact match).

---

## Schwab OAuth (One-Time Init)

Before calling the Market Data endpoints, generate `tokens.json`:

```bash
source .venv/bin/activate
python scripts/schwab_oauth_init.py
```

This script will:

1. print an authorization URL
2. you open it in a browser and approve access
3. you paste the final redirect URL back into the script
4. it writes `tokens.json` in the repo root

### Token notes

* If you start seeing 401/authorization errors, rerun:

```bash
python scripts/schwab_oauth_init.py
```

---

## Session Window Policy

We ingest **premarket + RTH** and **drop after-hours** by default.

Recommended window for QQQ options workflows:

* **Premarket:** 07:00–09:29 ET
* **RTH:** 09:30–15:59 ET (390 minutes)

This avoids accidentally treating overnight (e.g., 00:00–07:00) prints as “premarket”.

---

## Run Ingestion

### 1) Small backfill test (recommended first)

```bash
source .venv/bin/activate
python scripts/run_small_backfill.py
```

Confirm Parquet appears under:

* `data/candles/date_et=YYYY-MM-DD/part.parquet`

### 2) Full backfill (18 months)

This script runs a ~18 month backfill with:

* chunking
* adaptive chunk shrink on failures
* checkpointing in `logs/backfill_state.json`

```bash
source .venv/bin/activate
python scripts/backfill_18m.py
```

### 3) Daily incremental update

Pull yesterday + today (recommended once you’re live):

```bash
source .venv/bin/activate
python scripts/update_daily.py
```

---

## Validate Parquet Outputs

### Validate the full range

Scans all partitions:

* `data/candles/date_et=*/part.parquet`

Writes:

* `logs/validate_summary.csv`

```bash
source .venv/bin/activate
python scripts/validate_range.py
```

The script prints a list of “Flagged days” based on thresholds.

### Validate a single day (quick ad-hoc)

Example: inspect Jan 8, 2026

```bash
python - <<'PY'
import pandas as pd
DATE="2026-01-08"
p=f"data/candles/date_et={DATE}/part.parquet"
df=pd.read_parquet(p).sort_values("ts_et")
print("rows:", len(df))
print("sessions:", df["session"].value_counts().to_dict())
print("range:", df["ts_et"].min(), "->", df["ts_et"].max())
print(df.head(3))
PY
```

---

## Interpreting “Flagged Days”

Some flagged days are **expected** because of the US market calendar:

* **Early close** days (e.g., day after Thanksgiving, Christmas Eve) can look like missing RTH minutes if the validator assumes a 16:00 ET close.
* The **latest date partition** may be incomplete if ingestion runs before the session ends.

If you see large missing blocks on normal trading days, reduce backfill chunk size (or rerun ingestion for that day) and revalidate.

---

## Re-running Scripts

From repo root:

```bash
source .venv/bin/activate
python scripts/validate_range.py
```

If you changed ingestion logic:

* rerun `scripts/run_small_backfill.py` (small test)
* rerun `scripts/backfill_18m.py` (full backfill)
* rerun `scripts/update_daily.py` (incremental refresh)

---

## Git Hygiene / Safety

Do **not** commit:

* `.env`
* `tokens.json`
* `data/` outputs
* `logs/` outputs

Confirm `.gitignore` includes:

```
.venv/
__pycache__/
*.pyc
.DS_Store
data/
logs/
tokens.json
.env
```

```
::contentReference[oaicite:0]{index=0}
```
