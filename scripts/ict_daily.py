#!/usr/bin/env python3
"""
QQQ Daily ICT Analysis — CLI Report

Fetches daily candles from Schwab, runs the full ICT analysis pipeline,
and prints a formatted report with directional bias, active zones, and signals.

Usage:
    python scripts/ict_daily.py              # default 18-month lookback
    python scripts/ict_daily.py --months 6   # 6-month lookback
    python scripts/ict_daily.py --save       # also save data/daily_candles.parquet
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
import pandas as pd
import numpy as np

from qqq_ingest.schwab import SchwabDailyFetcher
from qqq_ingest.ict import (
    run_ict_analysis, signals, active_order_blocks, active_fvgs,
    evaluate_setups,
)

TZ_ET = pytz.timezone("US/Eastern")
SEPARATOR = "-" * 72


def fetch_data(months: int) -> pd.DataFrame:
    end = datetime.now(TZ_ET)
    start = end - relativedelta(months=months)
    print(f"Fetching daily QQQ candles: {start.date()} -> {end.date()}")
    fetcher = SchwabDailyFetcher()
    df = fetcher.fetch("QQQ", start, end)
    if df.empty:
        print("No data returned. Check tokens.json / Schwab credentials.")
        sys.exit(1)
    print(f"  {len(df)} candles loaded\n")
    return df


def print_header(df: pd.DataFrame):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    chg = last["close"] - prev["close"]
    chg_pct = chg / prev["close"] * 100
    direction = "+" if chg >= 0 else ""
    print(SEPARATOR)
    print(f"  QQQ DAILY ICT ANALYSIS  |  {last['date']}  |  Close: {last['close']:.2f}  "
          f"({direction}{chg:.2f} / {direction}{chg_pct:.2f}%)")
    print(SEPARATOR)


def print_bias(df: pd.DataFrame):
    print("\n== DIRECTIONAL BIAS ==\n")
    struct = df[df["structure_event"].notna()].tail(5)
    if struct.empty:
        print("  No recent structure events.")
        return

    latest = struct.iloc[-1]
    event = latest["structure_event"]
    bias = "BULLISH" if "bullish" in event else "BEARISH"
    label = event.replace("_", " ").upper()

    print(f"  Bias:  {bias}")
    print(f"  Last event:  {label}  on {latest['date']}  @ {latest['structure_level']:.2f}")
    print()

    # Show recent structure
    print("  Recent structure:")
    for _, r in struct.iterrows():
        evt = r["structure_event"].replace("_", " ").upper()
        print(f"    {r['date']}  {evt:20s}  @ {r['structure_level']:.2f}")


def print_dwm_levels(df: pd.DataFrame):
    last = df.iloc[-1]
    print(f"\n== KEY LEVELS ==\n")
    print(f"  {'Level':<25s} {'Value':>10s}  {'Distance':>8s}")
    print(f"  {'-'*25} {'-'*10}  {'-'*8}")

    price = last["close"]
    levels = []

    def add(name, val):
        if not np.isnan(val):
            dist = (val - price) / price * 100
            levels.append((name, val, dist))

    add("Prev Day High (PDH)", last.get("pdh", np.nan))
    add("Prev Day Low (PDL)", last.get("pdl", np.nan))
    add("Prev Day Open (PDO)", last.get("pdo", np.nan))
    add("Prev Week High (PWH)", last.get("pwh", np.nan))
    add("Prev Week Low (PWL)", last.get("pwl", np.nan))
    add("Prev Week Open (PWO)", last.get("pwo", np.nan))
    add("Prev Month High (PMH)", last.get("pmh", np.nan))
    add("Prev Month Low (PML)", last.get("pml", np.nan))
    add("Prev Month Open (PMO)", last.get("pmo", np.nan))

    # Add NWOG if present on any recent bar
    nwog = df[df["nwog_top"].notna()].tail(1)
    if not nwog.empty:
        r = nwog.iloc[-1]
        add(f"NWOG Top ({r['date']})", r["nwog_top"])
        add(f"NWOG CE", r["nwog_ce"])
        add(f"NWOG Bottom", r["nwog_bottom"])

    # Sort by distance
    levels.sort(key=lambda x: -x[1])
    for name, val, dist in levels:
        arrow = "+" if dist > 0 else ""
        print(f"  {name:<25s} {val:>10.2f}  {arrow}{dist:>7.2f}%")
        if abs(dist) < 0.1:
            print(f"  {'':25s} {'** AT LEVEL **':>20s}")


def print_active_zones(df: pd.DataFrame):
    print(f"\n== ACTIVE ORDER BLOCKS ==\n")
    obs = active_order_blocks(df)
    active_obs = obs[~obs["type"].str.contains("breaker")]
    if active_obs.empty:
        print("  None")
    else:
        # Show closest 5
        active_obs = active_obs.reindex(
            active_obs["distance_%"].abs().sort_values().index).head(5)
        for _, r in active_obs.iterrows():
            arrow = "above" if r["distance_%"] > 0 else "below"
            print(f"  {r['type']:<25s}  {r['bottom']:.2f} - {r['top']:.2f}  "
                  f"({abs(r['distance_%']):.2f}% {arrow})  [{r['date']}]")

    # Breakers
    breakers = obs[obs["type"].str.contains("breaker")]
    if not breakers.empty:
        print(f"\n== BREAKER BLOCKS ==\n")
        breakers = breakers.reindex(
            breakers["distance_%"].abs().sort_values().index).head(3)
        for _, r in breakers.iterrows():
            arrow = "above" if r["distance_%"] > 0 else "below"
            print(f"  {r['type']:<25s}  {r['bottom']:.2f} - {r['top']:.2f}  "
                  f"({abs(r['distance_%']):.2f}% {arrow})  [{r['date']}]")

    print(f"\n== ACTIVE FAIR VALUE GAPS ==\n")
    fvgs = active_fvgs(df)
    if fvgs.empty:
        print("  None")
    else:
        fvgs = fvgs.reindex(fvgs["distance_%"].abs().sort_values().index).head(5)
        for _, r in fvgs.iterrows():
            arrow = "above" if r["distance_%"] > 0 else "below"
            print(f"  {r['type']:<18s}  {r['bottom']:.2f} - {r['top']:.2f}  "
                  f"({abs(r['distance_%']):.2f}% {arrow})  [{r['date']}]")


def print_volume(df: pd.DataFrame):
    print(f"\n== VOLUME ANALYSIS ==\n")
    last = df.iloc[-1]
    rvol = last.get("rvol", np.nan)
    vol = last.get("volume", 0)
    vol_sma = last.get("vol_sma", np.nan)

    rvol_label = "LOW" if rvol < 0.8 else "NORMAL" if rvol < 1.3 else "ELEVATED" if rvol < 2.0 else "CLIMACTIC"
    print(f"  Today's volume:   {vol:>14,.0f}")
    print(f"  20-day avg:       {vol_sma:>14,.0f}")
    print(f"  Relative volume:  {rvol:>14.2f}x  ({rvol_label})")

    # Recent volume-confirmed events
    confirmed = df[df["vol_confirmed"]].tail(5)
    if not confirmed.empty:
        print(f"\n  Volume-confirmed events (last 5):")
        for _, r in confirmed.iterrows():
            evt = r.get("structure_event") or r.get("displacement") or ""
            print(f"    {r['date']}  {evt:20s}  rvol={r['rvol']:.1f}x  vol={r['volume']:,.0f}")

    # Volume divergences
    divs = df[df["vol_divergence"].notna()].tail(3)
    if not divs.empty:
        print(f"\n  Volume divergences:")
        for _, r in divs.iterrows():
            print(f"    {r['date']}  {r['vol_divergence']:15s}  "
                  f"{'high' if r['swing_high'] else 'low'}={r['high'] if r['swing_high'] else r['low']:.2f}  "
                  f"vol={r['volume']:,.0f}")

    # 5-day volume trend
    recent = df.tail(5)
    avg_recent = recent["volume"].mean()
    avg_prior = df.tail(25).head(20)["volume"].mean()
    if avg_prior > 0:
        vol_trend = (avg_recent - avg_prior) / avg_prior * 100
        direction = "above" if vol_trend > 0 else "below"
        print(f"\n  5-day avg volume is {abs(vol_trend):.0f}% {direction} the 20-day avg "
              f"({'sellers active' if vol_trend > 0 and df.iloc[-1]['close'] < df.iloc[-5]['close'] else 'buyers active' if vol_trend > 0 else 'declining participation'})")


def print_signals(df: pd.DataFrame):
    print(f"\n== RECENT SIGNALS (Last 15 bars) ==\n")
    sig = signals(df, last_n=15)
    if sig.empty:
        print("  No signals in recent bars.")
        return
    for _, r in sig.iterrows():
        print(f"  {r['date']}  close={r['close']:.2f}")
        for s in r["signals"].split(" | "):
            print(f"    -> {s}")
        print()


def _nearest_zones(zones_df: pd.DataFrame, max_dist_pct: float = 5.0, n: int = 3):
    """Filter to zones within max_dist_pct and return closest n."""
    nearby = zones_df[zones_df["distance_%"].abs() <= max_dist_pct]
    if nearby.empty:
        return zones_df.head(0)
    return nearby.reindex(nearby["distance_%"].abs().sort_values().index).head(n)


def print_trade_ideas(df: pd.DataFrame):
    print(f"\n== TRADE SETUPS (Backtest-Validated) ==\n")
    price = df["close"].iloc[-1]

    setups = evaluate_setups(df)

    if not setups:
        print("  No actionable setups at current price.")
        return

    # Backtest context header
    print("  Strategy stats (500-day backtest, selective mode):")
    print("  22 trades | 73% win rate | 2.87 profit factor | avg hold 1.6 days")
    print()

    # Show conviction labels
    conv_labels = {0: "SKIP", 1: "WEAK", 2: "LOW", 3: "MODERATE", 4: "HIGH", 5: "STRONG"}

    shown = 0
    for setup in setups:
        if shown >= 3:
            break

        label = conv_labels.get(setup.conviction, "?")
        stars = "*" * setup.conviction
        direction = "SHORT" if setup.direction == "short" else "LONG"

        print(f"  [{label}] {direction}  conviction={setup.conviction}/5 {stars}")
        print(f"    Zone:    {setup.entry_zone}")
        print(f"    Stop:    {setup.stop:.2f}  (risk: {setup.risk_pct:.2f}%)")
        print(f"    Target:  {setup.target:.2f}  (reward: {setup.reward_pct:.2f}%)")
        print(f"    Max hold: {setup.max_hold_days} trading days")

        if setup.reasons:
            print(f"    Why:")
            for r in setup.reasons:
                print(f"      + {r}")
        if setup.warnings:
            print(f"    Caution:")
            for w in setup.warnings:
                print(f"      ! {w}")
        print()
        shown += 1

    # Summary guidance
    high_conv = [s for s in setups if s.conviction >= 3]
    if high_conv:
        best = high_conv[0]
        print(f"  >> Best setup: {best.direction.upper()} at {best.entry_zone} "
              f"(conviction {best.conviction}/5)")
        print(f"     If using options: 1-2 week expiry sufficient (avg winner resolves in 1.2 days)")
    else:
        print("  >> No high-conviction setups. Consider waiting for BOS confirmation or confluence.")


def main():
    parser = argparse.ArgumentParser(description="QQQ Daily ICT Analysis")
    parser.add_argument("--months", type=int, default=18, help="Lookback months (default: 18)")
    parser.add_argument("--save", action="store_true", help="Save to data/daily_candles.parquet")
    parser.add_argument("--load", action="store_true",
                        help="Load from data/daily_candles.parquet instead of fetching")
    args = parser.parse_args()

    parquet_path = os.path.join(os.path.dirname(__file__), "..", "data", "daily_candles.parquet")

    if args.load:
        if not os.path.exists(parquet_path):
            print(f"No saved data at {parquet_path}. Run with --save first.")
            sys.exit(1)
        df_raw = pd.read_parquet(parquet_path)
        print(f"Loaded {len(df_raw)} candles from {parquet_path}\n")
    else:
        df_raw = fetch_data(args.months)
        if args.save:
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            df_raw.to_parquet(parquet_path, index=False, engine="pyarrow")
            print(f"Saved -> {parquet_path}\n")

    print("Running ICT analysis...")
    df = run_ict_analysis(df_raw)
    print()

    print_header(df)
    print_bias(df)
    print_dwm_levels(df)
    print_volume(df)
    print_active_zones(df)
    print_signals(df)
    print_trade_ideas(df)

    print(f"\n{SEPARATOR}")
    print("  Disclaimer: This is analysis, not financial advice.")
    print(f"{SEPARATOR}\n")


if __name__ == "__main__":
    main()
