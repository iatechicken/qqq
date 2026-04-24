#!/usr/bin/env python3
"""
Full Daily ICT Report — all 9 ETFs in one shot.

Fetches candles, runs ICT analysis, scores setups, prints trading plan.

Usage:
    python3 scripts/daily_report.py          # auto: fetch only if data is stale
    python3 scripts/daily_report.py --load   # force use saved parquets
    python3 scripts/daily_report.py --fetch  # force fresh fetch
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pytz
import pandas as pd
import numpy as np

from qqq_ingest.schwab import SchwabDailyFetcher
from qqq_ingest.ict import (
    run_ict_analysis, signals, active_order_blocks, active_fvgs,
    evaluate_setups, evaluate_setups_iwm,
)

TZ_ET = pytz.timezone("US/Eastern")
SEP = "=" * 72
THIN = "-" * 72

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

SYMBOLS = {
    "QQQ":  {"path": "daily_candles.parquet",       "group": "nasdaq",      "eval": "qqq"},
    "TQQQ": {"path": "tqqq_daily_candles.parquet",  "group": "nasdaq",      "eval": "generic"},
    "SQQQ": {"path": "sqqq_daily_candles.parquet",  "group": "nasdaq",      "eval": "generic"},
    "IWM":  {"path": "iwm_daily_candles.parquet",   "group": "russell",     "eval": "iwm"},
    "TNA":  {"path": "tna_daily_candles.parquet",   "group": "russell_lev", "eval": "generic"},
    "TZA":  {"path": "tza_daily_candles.parquet",   "group": "russell_lev", "eval": "generic"},
    "TLT":  {"path": "tlt_daily_candles.parquet",   "group": "bonds",       "eval": "generic"},
    "TMF":  {"path": "tmf_daily_candles.parquet",   "group": "bonds",       "eval": "generic"},
    "TMV":  {"path": "tmv_daily_candles.parquet",   "group": "bonds",       "eval": "generic"},
}

BACKTEST_REF = """
  Backtest reference (3-year, selective mode):
  | Strategy                     | Trades | WR    | Total   | PF   |
  |------------------------------|--------|-------|---------|------|
  | QQQ Aggressive (conv>=3)     |     23 | 69.6% | +14.12% | 2.55 |
  | TQQQ Aggressive (conv>=3)    |     16 | 75.0% | +46.20% | 3.05 |
  | SQQQ Conservative (conv>=4)  |      7 | 85.7% | +28.04% | 7.18 |
  | IWM Recalibrated (conv>=4)   |     14 | 64.0% | +6.82%  | 1.80 |
  | Bonds (TLT/TMF/TMV)          |      - |     - |       - | none |
"""

CONV_LABELS = {0: "SKIP", 1: "WEAK", 2: "LOW", 3: "MODERATE", 4: "HIGH", 5: "STRONG"}


# ── Freshness check ──────────────────────────────────────────────────

def last_completed_trading_day() -> date:
    """Return the most recent completed US trading day."""
    now_et = datetime.now(TZ_ET)
    today = now_et.date()

    # If market hasn't closed yet (before 4 PM ET), last completed = yesterday
    if now_et.hour < 16:
        today = today - timedelta(days=1)

    # Walk back over weekends
    while today.weekday() >= 5:  # Sat=5, Sun=6
        today = today - timedelta(days=1)

    return today


def data_is_fresh() -> bool:
    """Check if QQQ parquet already has the last completed trading day."""
    pq = os.path.join(DATA_DIR, SYMBOLS["QQQ"]["path"])
    if not os.path.exists(pq):
        return False

    df = pd.read_parquet(pq)
    if df.empty:
        return False

    # Get the last candle date from the parquet
    last_row = df.iloc[-1]
    if "date" in df.columns:
        last_date = pd.Timestamp(last_row["date"]).date()
    else:
        last_date = pd.Timestamp(df.index[-1]).date()

    target = last_completed_trading_day()
    fresh = last_date >= target

    if fresh:
        print(f"Data is fresh (last candle: {last_date}, target: {target}) — using cached parquets\n")
    else:
        print(f"Data is stale (last candle: {last_date}, target: {target}) — fetching fresh data\n")

    return fresh


# ── Fetch ────────────────────────────────────────────────────────────

def fetch_all(months: int = 24):
    """Fetch daily candles for all 9 symbols and save to parquet."""
    fetcher = SchwabDailyFetcher()
    end = datetime.now(TZ_ET)
    start = end - relativedelta(months=months)
    print(f"Fetching {months}-month daily candles: {start.date()} -> {end.date()}\n")

    results = {}
    for sym, meta in SYMBOLS.items():
        pq = os.path.join(DATA_DIR, meta["path"])
        df = fetcher.fetch(sym, start, end)
        df.to_parquet(pq)
        results[sym] = df
        print(f"  {sym:5s}  {len(df)} candles  -> {meta['path']}")
    print()
    return results


def load_all():
    """Load all 9 symbols from saved parquets."""
    results = {}
    for sym, meta in SYMBOLS.items():
        pq = os.path.join(DATA_DIR, meta["path"])
        if not os.path.exists(pq):
            print(f"  WARNING: {pq} not found, skipping {sym}")
            continue
        results[sym] = pd.read_parquet(pq)
    print(f"Loaded {len(results)} symbols from parquet\n")
    return results


# ── Analysis helpers ─────────────────────────────────────────────────

def get_bias(enriched):
    """Extract bias info from enriched DataFrame."""
    struct = enriched[enriched["structure_event"].notna()].tail(5)
    if struct.empty:
        return "NEUTRAL", "none", "", 0.0, pd.DataFrame()
    latest = struct.iloc[-1]
    evt = latest["structure_event"]
    bias = "BULLISH" if "bullish" in evt else "BEARISH"
    label = evt.replace("_", " ").upper()
    return bias, label, latest["date"], latest["structure_level"], struct


def get_rvol(enriched):
    last = enriched.iloc[-1]
    vol_20 = enriched["volume"].rolling(20).mean().iloc[-1]
    return last["volume"] / vol_20 if vol_20 > 0 else 0.0


def get_change(enriched):
    last = enriched.iloc[-1]
    prev = enriched.iloc[-2]
    return (last["close"] - prev["close"]) / prev["close"] * 100


# ── QQQ full report (mirrors ict_daily.py) ───────────────────────────

def print_qqq_full(enriched):
    last = enriched.iloc[-1]
    prev = enriched.iloc[-2]
    chg = last["close"] - prev["close"]
    chg_pct = chg / prev["close"] * 100
    sign = "+" if chg >= 0 else ""

    print(SEP)
    print(f"  QQQ DAILY ICT ANALYSIS  |  {last['date']}  |  Close: {last['close']:.2f}  "
          f"({sign}{chg:.2f} / {sign}{chg_pct:.2f}%)")
    print(SEP)

    # Bias
    bias, label, evt_date, evt_level, struct = get_bias(enriched)
    print(f"\n== DIRECTIONAL BIAS ==\n")
    print(f"  Bias:  {bias}")
    print(f"  Last event:  {label}  on {evt_date}  @ {evt_level:.2f}\n")
    if not struct.empty:
        print("  Recent structure:")
        for _, r in struct.iterrows():
            e = r["structure_event"].replace("_", " ").upper()
            print(f"    {r['date']}  {e:20s}  @ {r['structure_level']:.2f}")

    # Key levels
    print(f"\n== KEY LEVELS ==\n")
    print(f"  {'Level':<25s} {'Value':>10s}  {'Distance':>8s}")
    print(f"  {'-'*25} {'-'*10}  {'-'*8}")
    price = last["close"]
    levels = []

    def add(name, val):
        if pd.notna(val):
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

    nwog = enriched[enriched["nwog_top"].notna()].tail(1)
    if not nwog.empty:
        r = nwog.iloc[-1]
        add(f"NWOG Top ({r['date']})", r["nwog_top"])
        add("NWOG CE", r["nwog_ce"])
        add("NWOG Bottom", r["nwog_bottom"])

    levels.sort(key=lambda x: -x[1])
    for name, val, dist in levels:
        arrow = "+" if dist > 0 else ""
        print(f"  {name:<25s} {val:>10.2f}  {arrow}{dist:>7.2f}%")

    # Volume
    print(f"\n== VOLUME ANALYSIS ==\n")
    rvol = last.get("rvol", np.nan)
    vol_sma = last.get("vol_sma", np.nan)
    rvol_label = "LOW" if rvol < 0.8 else "NORMAL" if rvol < 1.3 else "ELEVATED" if rvol < 2.0 else "CLIMACTIC"
    print(f"  Today's volume:   {last['volume']:>14,.0f}")
    print(f"  20-day avg:       {vol_sma:>14,.0f}")
    print(f"  Relative volume:  {rvol:>14.2f}x  ({rvol_label})")

    confirmed = enriched[enriched["vol_confirmed"]].tail(5)
    if not confirmed.empty:
        print(f"\n  Volume-confirmed events (last 5):")
        for _, r in confirmed.iterrows():
            evt = r.get("structure_event") or r.get("displacement") or ""
            print(f"    {r['date']}  {evt:20s}  rvol={r['rvol']:.1f}x  vol={r['volume']:,.0f}")

    divs = enriched[enriched["vol_divergence"].notna()].tail(3)
    if not divs.empty:
        print(f"\n  Volume divergences:")
        for _, r in divs.iterrows():
            print(f"    {r['date']}  {r['vol_divergence']:15s}  "
                  f"{'high' if r['swing_high'] else 'low'}={r['high'] if r['swing_high'] else r['low']:.2f}  "
                  f"vol={r['volume']:,.0f}")

    recent = enriched.tail(5)
    avg_recent = recent["volume"].mean()
    avg_prior = enriched.tail(25).head(20)["volume"].mean()
    if avg_prior > 0:
        vol_trend = (avg_recent - avg_prior) / avg_prior * 100
        direction = "above" if vol_trend > 0 else "below"
        print(f"\n  5-day avg volume is {abs(vol_trend):.0f}% {direction} the 20-day avg "
              f"({'declining participation' if vol_trend < 0 else 'increasing participation'})")

    # Order blocks
    print(f"\n== ACTIVE ORDER BLOCKS ==\n")
    obs = active_order_blocks(enriched)
    active_obs = obs[~obs["type"].str.contains("breaker")]
    if active_obs.empty:
        print("  None")
    else:
        active_obs = active_obs.reindex(
            active_obs["distance_%"].abs().sort_values().index).head(5)
        for _, r in active_obs.iterrows():
            arrow = "above" if r["distance_%"] > 0 else "below"
            print(f"  {r['type']:<25s}  {r['bottom']:.2f} - {r['top']:.2f}  "
                  f"({abs(r['distance_%']):.2f}% {arrow})  [{r['date']}]")

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
    fvgs = active_fvgs(enriched)
    if fvgs.empty:
        print("  None")
    else:
        fvgs = fvgs.reindex(fvgs["distance_%"].abs().sort_values().index).head(5)
        for _, r in fvgs.iterrows():
            arrow = "above" if r["distance_%"] > 0 else "below"
            print(f"  {r['type']:<18s}  {r['bottom']:.2f} - {r['top']:.2f}  "
                  f"({abs(r['distance_%']):.2f}% {arrow})  [{r['date']}]")

    # Recent signals
    print(f"\n== RECENT SIGNALS (Last 15 bars) ==\n")
    sig = signals(enriched, last_n=15)
    if sig.empty:
        print("  No signals in recent bars.")
    else:
        for _, r in sig.iterrows():
            print(f"  {r['date']}  close={r['close']:.2f}")
            for s in r["signals"].split(" | "):
                print(f"    -> {s}")
            print()

    # Trade setups
    print_setups("QQQ", enriched, evaluate_setups, min_conv=3)


# ── Generic symbol report ────────────────────────────────────────────

def print_symbol(sym, enriched, eval_fn, min_conv):
    last = enriched.iloc[-1]
    chg = get_change(enriched)
    bias, label, evt_date, evt_level, struct = get_bias(enriched)
    rvol = get_rvol(enriched)
    sign = "+" if chg >= 0 else ""

    print(f"\n{THIN}")
    print(f"  {sym}  |  Close: {last['close']:.2f}  ({sign}{chg:.2f}%)"
          f"  |  Bias: {bias}  |  rVol: {rvol:.2f}x")
    print(f"  Last: {label} @ {evt_level:.2f} ({evt_date})")
    print(THIN)

    if not struct.empty:
        print("  Recent structure:")
        for _, r in struct.iterrows():
            e = r["structure_event"].replace("_", " ").upper()
            print(f"    {r['date']}  {e:20s}  @ {r['structure_level']:.2f}")

    print_setups(sym, enriched, eval_fn, min_conv)


def print_setups(sym, enriched, eval_fn, min_conv):
    setups = eval_fn(enriched)
    if not setups:
        print(f"\n  No scored setups for {sym}.\n")
        return

    print(f"\n  Top setups:")
    shown = 0
    for s in setups:
        if shown >= 3:
            break
        label = CONV_LABELS.get(s.conviction, "?")
        direction = s.direction.upper()
        actionable = s.conviction >= min_conv
        tag = "" if actionable else "  (below threshold)"

        print(f"    [{label} {s.conviction}/5] {direction} {s.entry_zone}{tag}")
        print(f"      Stop: {s.stop:.2f} ({s.risk_pct:.2f}%)  |  Target: {s.target:.2f} ({s.reward_pct:.2f}%)  |  Hold: {s.max_hold_days}d")
        for r in s.reasons:
            print(f"        + {r}")
        for w in s.warnings:
            print(f"        ! {w}")
        shown += 1
    print()


# ── Trading plan ─────────────────────────────────────────────────────

def print_trading_plan(all_enriched):
    print(f"\n{'#' * 72}")
    print(f"  TRADING PLAN")
    print(f"{'#' * 72}")

    # Cross-group bias
    group_bias = {}
    for sym in ("QQQ", "IWM", "TLT"):
        if sym in all_enriched:
            bias, label, _, _, _ = get_bias(all_enriched[sym])
            bos = "BOS" in label
            group_bias[sym] = (bias, bos)

    print(f"\n  Cross-Group Bias:")
    print(f"  {'Group':<12s} {'Bias':<10s} {'Confirmed':>10s} {'Volume':>8s}")
    print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*8}")
    for sym, group_name in [("QQQ", "Nasdaq"), ("IWM", "Russell"), ("TLT", "Bonds")]:
        if sym in all_enriched:
            bias, bos = group_bias.get(sym, ("?", False))
            rvol = get_rvol(all_enriched[sym])
            conf = "BOS" if bos else "CHoCH only"
            print(f"  {group_name:<12s} {bias:<10s} {conf:>10s} {rvol:>7.2f}x")

    # Divergence check
    if "QQQ" in group_bias and "IWM" in group_bias:
        if group_bias["QQQ"][0] != group_bias["IWM"][0]:
            print(f"\n  ** DIVERGENCE: Nasdaq {group_bias['QQQ'][0]} vs Russell {group_bias['IWM'][0]} — watch for rotation **")
        else:
            print(f"\n  Nasdaq and Russell aligned {group_bias['QQQ'][0]}. No divergence.")

    if "TLT" in group_bias:
        print(f"  Bonds: {group_bias['TLT'][0]} {('BOS' if group_bias['TLT'][1] else 'CHoCH only')} — context only, no trade.")

    # Actionable setups
    print(f"\n  {'='*60}")
    print(f"  ACTIONABLE SETUPS")
    print(f"  {'='*60}")

    any_actionable = False

    # Nasdaq group
    for sym in ("QQQ", "TQQQ", "SQQQ"):
        if sym not in all_enriched:
            continue
        enriched = all_enriched[sym]
        setups = evaluate_setups(enriched)
        min_conv = 3
        actionable = [s for s in setups if s.conviction >= min_conv]
        if actionable:
            any_actionable = True
            for s in actionable[:2]:
                label = CONV_LABELS.get(s.conviction, "?")
                print(f"\n  ** [{label} {s.conviction}/5] {sym} {s.direction.upper()} **")
                print(f"     Zone: {s.entry_zone}")
                print(f"     Stop: {s.stop:.2f} ({s.risk_pct:.2f}%)  |  Target: {s.target:.2f} ({s.reward_pct:.2f}%)")
                print(f"     Max hold: {s.max_hold_days} trading days")

    # IWM — conv>=4 only
    if "IWM" in all_enriched:
        setups = evaluate_setups_iwm(all_enriched["IWM"])
        actionable = [s for s in setups if s.conviction >= 4]
        if actionable:
            any_actionable = True
            for s in actionable[:2]:
                label = CONV_LABELS.get(s.conviction, "?")
                print(f"\n  ** [{label} {s.conviction}/5] IWM {s.direction.upper()} **")
                print(f"     Zone: {s.entry_zone}")
                print(f"     Stop: {s.stop:.2f} ({s.risk_pct:.2f}%)  |  Target: {s.target:.2f} ({s.reward_pct:.2f}%)")
                print(f"     Max hold: {s.max_hold_days} trading days  (IWM: 1.5R target, 3d hold)")
        else:
            print(f"\n  IWM: conv=3 setups exist but not actionable (IWM needs conv>=4, 38% WR at conv=3)")

    # Bonds — context only
    print(f"\n  Bonds (TLT/TMF/TMV): context only — no demonstrated ICT edge. No trade.")

    if not any_actionable:
        print(f"\n  >> NO ACTIONABLE SETUPS. No trade today.")

    # Volume context
    if "QQQ" in all_enriched:
        qqq = all_enriched["QQQ"]
        avg5 = qqq.tail(5)["volume"].mean()
        avg20 = qqq.tail(25).head(20)["volume"].mean()
        if avg20 > 0:
            trend = (avg5 - avg20) / avg20 * 100
            dir_str = "above" if trend > 0 else "below"
            print(f"\n  Volume context: QQQ 5-day avg {abs(trend):.0f}% {dir_str} 20-day avg"
                  f" ({'declining participation' if trend < 0 else 'increasing participation'})")

    # Backtest ref
    print(BACKTEST_REF)


# ── Last 3 candles table ─────────────────────────────────────────────

def print_candle_table(raw_data):
    print(f"\n== LAST 3 CANDLES ==\n")
    print(f"  {'Symbol':<6s} {'Date':>12s} {'Open':>10s} {'High':>10s} {'Low':>10s} {'Close':>10s} {'Volume':>12s}")
    print(f"  {'-'*6} {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

    for sym in SYMBOLS:
        if sym not in raw_data:
            continue
        df = raw_data[sym]
        for i in range(-3, 0):
            row = df.iloc[i]
            date_str = str(row.get("date", df.index[len(df) + i]))[:10]
            print(f"  {sym:<6s} {date_str:>12s} {row['open']:>10.2f} {row['high']:>10.2f}"
                  f" {row['low']:>10.2f} {row['close']:>10.2f} {row['volume']:>12,.0f}")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily ICT Report — all 9 ETFs")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--load", action="store_true", help="Force use saved parquets (skip fetch)")
    group.add_argument("--fetch", action="store_true", help="Force fresh fetch from Schwab API")
    parser.add_argument("--months", type=int, default=24, help="Lookback months (default: 24)")
    args = parser.parse_args()

    # 1. Decide: fetch or load
    if args.load:
        raw_data = load_all()
    elif args.fetch:
        raw_data = fetch_all(args.months)
    elif data_is_fresh():
        raw_data = load_all()
    else:
        raw_data = fetch_all(args.months)

    # 2. Show last 3 candles
    print_candle_table(raw_data)

    # 3. Run ICT analysis on all symbols
    all_enriched = {}
    for sym, df in raw_data.items():
        all_enriched[sym] = run_ict_analysis(df)

    # 4. QQQ full report
    print()
    print_qqq_full(all_enriched["QQQ"])

    # 5. Other symbols
    for sym in ("TQQQ", "SQQQ"):
        if sym in all_enriched:
            print_symbol(sym, all_enriched[sym], evaluate_setups, min_conv=3)

    for sym in ("IWM",):
        if sym in all_enriched:
            print_symbol(sym, all_enriched[sym], evaluate_setups_iwm, min_conv=4)

    for sym in ("TNA", "TZA"):
        if sym in all_enriched:
            print_symbol(sym, all_enriched[sym], evaluate_setups, min_conv=3)
            print("  (no IWM-specific calibration for leveraged)")

    for sym in ("TLT", "TMF", "TMV"):
        if sym in all_enriched:
            print_symbol(sym, all_enriched[sym], evaluate_setups, min_conv=3)
            print("  (bonds: context only, no demonstrated ICT edge)")

    # 6. Trading plan
    print_trading_plan(all_enriched)

    print(f"\n{SEP}")
    print("  Disclaimer: This is analysis, not financial advice.")
    print(f"{SEP}\n")


if __name__ == "__main__":
    main()
