from __future__ import annotations
from datetime import datetime, timedelta

from qqq_ingest.ingest import run_backfill, IngestConfig
from qqq_ingest.fetchers import SyntheticFetcher
from qqq_ingest.timeutil import TZ_ET

if __name__ == "__main__":
    cfg = IngestConfig(chunk_days=2)
    fetcher = SyntheticFetcher()

    now_et = datetime.now(TZ_ET).replace(second=0, microsecond=0)
    start_et = (now_et - timedelta(days=5)).replace(second=0, microsecond=0)

    run_backfill(fetcher, start_et, now_et, cfg)
