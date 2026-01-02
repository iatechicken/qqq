from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, time
import pytz
import pandas as pd
from tqdm import tqdm

from .timeutil import TZ_ET, to_utc, add_et_cols, session_label
from .qc import qc_clean, dedupe_sort
from .storage import write_by_date
from .fetchers import CandleFetcher

@dataclass
class IngestConfig:
    symbol: str = "QQQ"
    candles_root: str = "data/candles"
    chunk_days: int = 2
    premarket_start: time | None = time(7,0)

def run_backfill(fetcher: CandleFetcher, start_et: datetime, end_et: datetime, cfg: IngestConfig) -> None:
    if start_et.tzinfo is None or end_et.tzinfo is None:
        raise ValueError("start_et/end_et must be tz-aware (US/Eastern)")

    cur = start_et
    total_written = 0
    pbar = tqdm(total=int((end_et - start_et).total_seconds()), unit="sec", desc="Ingest")

    while cur < end_et:
        nxt = min(cur + timedelta(days=cfg.chunk_days), end_et)

        df = fetcher.fetch(cfg.symbol, to_utc(cur), to_utc(nxt))
        if df is None or df.empty:
            pbar.update(int((nxt - cur).total_seconds()))
            cur = nxt
            continue

        df = df.copy()
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

        df = qc_clean(df)
        df = dedupe_sort(df)
        df = add_et_cols(df, ts_col="ts_utc")

        df["session"] = df["time_et"].apply(lambda t: session_label(t, premarket_start=cfg.premarket_start))
        df = df[df["session"].notna()].copy()

        if not df.empty:
            write_by_date(df, cfg.candles_root)
            total_written += len(df)

        pbar.update(int((nxt - cur).total_seconds()))
        cur = nxt

    pbar.close()
    print(f"✅ Done. Wrote/updated ~{total_written:,} rows to {cfg.candles_root}")
