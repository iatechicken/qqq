from __future__ import annotations
import pandas as pd

REQUIRED = ["ts_utc", "open", "high", "low", "close", "volume"]

def validate_schema(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

def qc_clean(df: pd.DataFrame) -> pd.DataFrame:
    validate_schema(df)
    df = df.copy()
    bad = (
        (df["open"] <= 0) | (df["high"] <= 0) | (df["low"] <= 0) | (df["close"] <= 0) |
        (df["volume"] < 0) |
        (df["high"] < df[["open","close"]].max(axis=1)) |
        (df["low"] > df[["open","close"]].min(axis=1)) |
        (df["low"] > df["high"])
    )
    return df.loc[~bad].copy()

def dedupe_sort(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.sort_values("ts_utc")
    df = df.drop_duplicates(subset=["ts_utc"], keep="last")
    return df
