# scripts/validate_range.py
from __future__ import annotations
import glob
from pathlib import Path
import pandas as pd
import pytz

ROOT = "data/candles"
OUT_CSV = "logs/validate_summary.csv"

ET = pytz.timezone("US/Eastern")

# expected grid (adjust if you change premarket window)
PM_START = "07:00:00"
RTH_START = "09:30:00"
RTH_END = "16:00:00"

def expected_counts():
    # premarket: 07:00-09:30 => 150 mins, rth: 09:30-16:00 => 390 mins
    return 150, 390

def load_partition(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    # ts_et may already be tz-aware; normalize
    df["ts_et"] = pd.to_datetime(df["ts_et"])
    return df.sort_values("ts_utc")

def validate_day(df: pd.DataFrame, date_et: str) -> dict:
    req = {"ts_utc","open","high","low","close","volume","ts_et","date_et","session"}
    missing_cols = sorted(list(req - set(df.columns)))

    # schema/value checks
    prices = df[["open","high","low","close"]] if all(c in df.columns for c in ["open","high","low","close"]) else None
    non_pos_price = int((prices <= 0).any(axis=1).sum()) if prices is not None else None
    neg_vol = int((df["volume"] < 0).sum()) if "volume" in df.columns else None

    ohlc_bad = 0
    if prices is not None:
        hi_ok = df["high"] >= df[["open","close"]].max(axis=1)
        lo_ok = df["low"]  <= df[["open","close"]].min(axis=1)
        hl_ok = df["low"] <= df["high"]
        ohlc_bad = int((~(hi_ok & lo_ok & hl_ok)).sum())

    dup_ts = int(df["ts_utc"].duplicated().sum())
    mono = bool(df["ts_utc"].is_monotonic_increasing)

    # session checks
    bad_session = int((~df["session"].isin(["premarket","rth"])).sum())

    # date checks
    date_mismatch = int((df["date_et"].astype(str) != date_et).sum())
    ts_et_date = df["ts_et"].dt.strftime("%Y-%m-%d")
    ts_et_mismatch = int((ts_et_date != date_et).sum())

    # gaps
    gap_min = df["ts_et"].diff().dt.total_seconds().div(60)
    max_gap = float(gap_min.max()) if gap_min.notna().any() else 0.0

    # expected missing minutes
    pm_expected_n, rth_expected_n = expected_counts()

    pm_start = pd.Timestamp(f"{date_et} {PM_START}", tz=ET)
    rth_start = pd.Timestamp(f"{date_et} {RTH_START}", tz=ET)
    rth_end = pd.Timestamp(f"{date_et} {RTH_END}", tz=ET)

    expected_pm = pd.date_range(pm_start, rth_start, freq="1min", inclusive="left")
    expected_rth = pd.date_range(rth_start, rth_end, freq="1min", inclusive="left")

    have_pm = set(df.loc[df["session"]=="premarket", "ts_et"].tolist())
    have_r  = set(df.loc[df["session"]=="rth", "ts_et"].tolist())

    missing_pm = sum(1 for t in expected_pm if t.to_pydatetime() not in have_pm)
    missing_r  = sum(1 for t in expected_rth if t.to_pydatetime() not in have_r)

    pm_rows = int((df["session"]=="premarket").sum())
    rth_rows = int((df["session"]=="rth").sum())

    return {
        "date_et": date_et,
        "rows": len(df),
        "pm_rows": pm_rows,
        "rth_rows": rth_rows,
        "pm_missing": missing_pm,
        "rth_missing": missing_r,
        "pm_expected": pm_expected_n,
        "rth_expected": rth_expected_n,
        "max_gap_min": max_gap,
        "dup_ts": dup_ts,
        "monotonic": mono,
        "ohlc_bad": ohlc_bad,
        "non_pos_price": non_pos_price,
        "neg_vol": neg_vol,
        "bad_session": bad_session,
        "date_mismatch": date_mismatch,
        "ts_et_mismatch": ts_et_mismatch,
        "missing_cols": ",".join(missing_cols),
    }

def main():
    Path("logs").mkdir(parents=True, exist_ok=True)
    paths = sorted(glob.glob(f"{ROOT}/date_et=*/part.parquet"))
    if not paths:
        raise SystemExit(f"No partitions found under {ROOT}")

    rows = []
    for p in paths:
        date_et = Path(p).parent.name.split("date_et=")[1]
        df = load_partition(p)
        rows.append(validate_day(df, date_et))

    out = pd.DataFrame(rows).sort_values("date_et")
    out.to_csv(OUT_CSV, index=False)
    print(f"✅ Wrote {OUT_CSV} with {len(out)} days")

    # Flag “bad” days (tune thresholds as you like)
    flagged = out[
        (out["missing_cols"] != "") |
        (out["ohlc_bad"] > 0) |
        (out["dup_ts"] > 0) |
        (out["bad_session"] > 0) |
        (out["date_mismatch"] > 0) |
        (out["ts_et_mismatch"] > 0) |
        (out["rth_missing"] > 0) |
        (out["max_gap_min"] >= 10) |
        (out["pm_missing"] >= 10)
    ]

    if len(flagged):
        print("\n⚠️ Flagged days:")
        print(flagged[["date_et","rows","pm_missing","rth_missing","max_gap_min","ohlc_bad","dup_ts","missing_cols"]]
              .to_string(index=False))
    else:
        print("✅ No flagged days by current rules")

if __name__ == "__main__":
    main()
