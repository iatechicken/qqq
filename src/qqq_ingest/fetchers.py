from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

@dataclass
class CandleFetcher:
    def fetch(self, symbol: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
        raise NotImplementedError

@dataclass
class SyntheticFetcher(CandleFetcher):
    seed: int = 7
    start_price: float = 400.0

    def fetch(self, symbol: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
        # 1-min grid (inclusive start, exclusive end)
        idx = pd.date_range(start=start_utc, end=end_utc, freq="1min", inclusive="left", tz="UTC")
        if len(idx) == 0:
            return pd.DataFrame(columns=["ts_utc","open","high","low","close","volume"])

        rng = np.random.default_rng(self.seed)
        # random walk for close
        rets = rng.normal(0, 0.0005, size=len(idx))
        close = self.start_price * np.exp(np.cumsum(rets))
        open_ = np.r_[close[0], close[:-1]]
        # small ranges
        spread = np.abs(rng.normal(0.0, 0.0015, size=len(idx))) * close
        high = np.maximum(open_, close) + spread
        low = np.minimum(open_, close) - spread
        vol = rng.integers(1000, 50000, size=len(idx))

        return pd.DataFrame({
            "ts_utc": idx,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": vol,
        })
