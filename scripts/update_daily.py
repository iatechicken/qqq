# scripts/update_daily.py
from __future__ import annotations
from datetime import datetime, timedelta, time

from qqq_ingest.ingest import IngestConfig, run_backfill
from qqq_ingest.timeutil import TZ_ET
from qqq_ingest.schwab import SchwabPriceHistoryFetcher

def main():
    cfg = IngestConfig(
        chunk_days=2,                 # just for the update window
        premarket_start=None,         # keep all pre-09:30 returned
        candles_root="data/candles",
        symbol="QQQ",
    )
    fetcher = SchwabPriceHistoryFetcher(need_extended_hours=True)

    now_et = datetime.now(TZ_ET).replace(second=0, microsecond=0)
    start_et = (now_et - timedelta(days=2)).replace(second=0, microsecond=0)

    run_backfill(fetcher, start_et, now_et, cfg)

if __name__ == "__main__":
    main()
