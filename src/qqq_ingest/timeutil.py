from __future__ import annotations
from datetime import datetime, time
import pytz
import pandas as pd

TZ_ET = pytz.timezone("US/Eastern")

def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt.astimezone(pytz.UTC)

def add_et_cols(df: pd.DataFrame, ts_col: str = "ts_utc") -> pd.DataFrame:
    out = df.copy()
    out[ts_col] = pd.to_datetime(out[ts_col], utc=True)
    ts_et = out[ts_col].dt.tz_convert(TZ_ET)
    out["ts_et"] = ts_et
    out["date_et"] = ts_et.dt.strftime("%Y-%m-%d")
    out["time_et"] = ts_et.dt.time
    return out

def session_label(t: time, premarket_start: time | None = time(7,0)) -> str | None:
    # keep premarket + RTH; drop after-hours
    if t < time(9,30):
        if premarket_start is None or t >= premarket_start:
            return "premarket"
        return None
    if time(9,30) <= t < time(16,0):
        return "rth"
    return None
