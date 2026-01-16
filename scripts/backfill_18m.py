# scripts/backfill_18m.py
from __future__ import annotations
import json, time
from pathlib import Path
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from qqq_ingest.ingest import IngestConfig, run_backfill
from qqq_ingest.timeutil import TZ_ET
from qqq_ingest.schwab import SchwabPriceHistoryFetcher

STATE_PATH = Path("logs/backfill_state.json")

def save_state(next_start_iso: str, chunk_days: int):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps({
        "next_start_et": next_start_iso,
        "chunk_days": chunk_days,
        "updated_at": datetime.now().isoformat(),
    }, indent=2))

def load_state():
    if not STATE_PATH.exists():
        return None
    return json.loads(STATE_PATH.read_text())

def main():
    cfg = IngestConfig(
        chunk_days=7,               # initial chunk
        premarket_start=None,       # keep all pre-09:30 that Schwab returns (you can set time(7,0) if you want)
        candles_root="data/candles",
        symbol="QQQ",
    )

    fetcher = SchwabPriceHistoryFetcher(need_extended_hours=True)

    now_et = datetime.now(TZ_ET).replace(second=0, microsecond=0)
    default_start_et = (now_et - relativedelta(months=18)).replace(second=0, microsecond=0)

    st = load_state()
    if st:
        start_et = datetime.fromisoformat(st["next_start_et"])
        chunk_days = int(st.get("chunk_days", cfg.chunk_days))
        print(f"Resuming from state: start_et={start_et} chunk_days={chunk_days}")
    else:
        start_et = default_start_et
        chunk_days = cfg.chunk_days

    # We’ll backfill in windows and checkpoint after each successful window.
    # Use run_backfill for each window so all your QC/session logic stays centralized.
    cur = start_et
    end_et = now_et

    while cur < end_et:
        nxt = min(cur + timedelta(days=chunk_days), end_et)
        print(f"\nBackfill window: {cur} -> {nxt} (chunk_days={chunk_days})")

        try:
            # run_backfill expects an object with .fetch(symbol, start_utc, end_utc)
            # SchwabPriceHistoryFetcher matches that.
            run_backfill(fetcher, cur, nxt, cfg)

            # success: advance + reset to default chunk size
            cur = nxt
            chunk_days = cfg.chunk_days
            save_state(cur.isoformat(), chunk_days)

            # small courtesy sleep
            time.sleep(0.2)

        except Exception as e:
            print(f"⚠️ Window failed: {e!r}")
            # shrink chunk and retry
            if chunk_days > 1:
                chunk_days = max(1, chunk_days // 2)
                print(f"Retrying with smaller chunk_days={chunk_days}")
                save_state(cur.isoformat(), chunk_days)
                time.sleep(1.0)
            else:
                # skip 1 day if even a 1-day chunk is failing, but keep moving
                print("Skipping 1-day window after repeated failure.")
                cur = nxt
                chunk_days = cfg.chunk_days
                save_state(cur.isoformat(), chunk_days)
                time.sleep(1.0)

    print("\n✅ Backfill complete.")
    print(f"State saved at {STATE_PATH}")

if __name__ == "__main__":
    main()
