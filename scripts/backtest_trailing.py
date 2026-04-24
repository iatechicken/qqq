#!/usr/bin/env python3
"""
ICT Strategy Backtester — Trailing Stop Variant (Options-Optimized)

Tests trailing stop exit mechanics vs the current fixed 2R target system.
Designed to find configurations that produce larger underlying moves
suitable for options trading (debit spreads or directional).

Key differences from backtest_ict.py:
  - Uses trailing stop from peak MFE instead of fixed R-target
  - Wider hard stop (1.0 ATR vs 0.5 ATR) to let trades develop
  - Longer max hold (15d vs 5d)
  - Reports options P&L translation alongside underlying

Usage:
    python scripts/backtest_trailing.py
    python scripts/backtest_trailing.py --symbol TQQQ
    python scripts/backtest_trailing.py --trail 2.5 --hold 20 --stop-atr 1.5
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from qqq_ingest.ict import run_ict_analysis


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TrailingTrade:
    entry_date: object
    entry_price: float
    direction: str
    conviction: int
    reason: str
    exit_date: object = None
    exit_price: float = 0.0
    exit_reason: str = ""
    pnl_pct: float = 0.0
    peak_pnl_pct: float = 0.0
    hold_days: int = 0
    risk_pct: float = 0.0


# ---------------------------------------------------------------------------
# Signal detection (reuses ICT backtest logic)
# ---------------------------------------------------------------------------

def _find_bear_entry(df, i, struct_levels, atr_val):
    close = df["close"].iloc[i]
    high = df["high"].iloc[i]
    prev_close = df["close"].iloc[i - 1]
    if close <= prev_close:
        return None, "", 0

    confluence = 0
    primary_reason = ""
    primary_zone = None

    for sl in reversed(struct_levels[-10:]):
        level = sl["level"]
        if level > close and level <= high * 1.005:
            primary_zone = level; primary_reason = "struct"; confluence += 1; break
        if abs(level - close) / close < 0.005:
            primary_zone = level; primary_reason = "struct"; confluence += 1; break

    pdh = df["pdh"].iloc[i] if not np.isnan(df["pdh"].iloc[i]) else None
    if pdh and close > pdh * 0.997 and close < pdh * 1.01:
        confluence += 1
        if primary_zone is None: primary_zone = pdh; primary_reason = "pdh"

    pwl = df["pwl"].iloc[i] if not np.isnan(df["pwl"].iloc[i]) else None
    if pwl and close > pwl * 0.997 and close < pwl * 1.01:
        confluence += 1
        if primary_zone is None: primary_zone = pwl; primary_reason = "pwl"

    for j in range(max(0, i - 20), i):
        if df["fvg_type"].iloc[j] == "bearish_fvg":
            fvg_top = df["fvg_top"].iloc[j]
            fvg_btm = df["fvg_bottom"].iloc[j]
            if high >= fvg_btm and close <= fvg_top:
                confluence += 1
                if primary_zone is None: primary_zone = fvg_top; primary_reason = "fvg"
                break

    for j in range(max(0, i - 30), i):
        if df["ob_type"].iloc[j] == "bearish_ob":
            ob_top = df["ob_top"].iloc[j]
            ob_btm = df["ob_bottom"].iloc[j]
            if high >= ob_btm and close <= ob_top:
                confluence += 1
                if primary_zone is None: primary_zone = ob_top; primary_reason = "ob"
                break

    return primary_zone, primary_reason, confluence


def _find_bull_entry(df, i, struct_levels, atr_val):
    close = df["close"].iloc[i]
    low = df["low"].iloc[i]
    prev_close = df["close"].iloc[i - 1]
    if close >= prev_close:
        return None, "", 0

    confluence = 0
    primary_reason = ""
    primary_zone = None

    for sl in reversed(struct_levels[-10:]):
        level = sl["level"]
        if level < close and level >= low * 0.995:
            primary_zone = level; primary_reason = "struct"; confluence += 1; break
        if abs(level - close) / close < 0.005:
            primary_zone = level; primary_reason = "struct"; confluence += 1; break

    pdl = df["pdl"].iloc[i] if not np.isnan(df["pdl"].iloc[i]) else None
    if pdl and close < pdl * 1.003 and close > pdl * 0.99:
        confluence += 1
        if primary_zone is None: primary_zone = pdl; primary_reason = "pdl"

    pwh = df["pwh"].iloc[i] if not np.isnan(df["pwh"].iloc[i]) else None
    if pwh and close < pwh * 1.003 and close > pwh * 0.99:
        confluence += 1
        if primary_zone is None: primary_zone = pwh; primary_reason = "pwh"

    for j in range(max(0, i - 20), i):
        if df["fvg_type"].iloc[j] == "bullish_fvg":
            fvg_top = df["fvg_top"].iloc[j]
            fvg_btm = df["fvg_bottom"].iloc[j]
            if low <= fvg_top and close >= fvg_btm:
                confluence += 1
                if primary_zone is None: primary_zone = fvg_btm; primary_reason = "fvg"
                break

    for j in range(max(0, i - 30), i):
        if df["ob_type"].iloc[j] == "bullish_ob":
            ob_top = df["ob_top"].iloc[j]
            ob_btm = df["ob_bottom"].iloc[j]
            if low <= ob_top and close >= ob_btm:
                confluence += 1
                if primary_zone is None: primary_zone = ob_btm; primary_reason = "ob"
                break

    return primary_zone, primary_reason, confluence


def _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                       entry_bar_vol_conf, entry_bar_rvol, confluence):
    score = 0
    if bias_set_by_bos:
        score += 1
    if bias_vol_confirmed:
        score += 1
    if entry_bar_vol_conf or entry_bar_rvol >= 1.5:
        score += 1
    if reason == "struct":
        score += 1
    if confluence >= 2:
        score += 1
    return score


# ---------------------------------------------------------------------------
# Signal collection
# ---------------------------------------------------------------------------

def collect_signals(df, warmup=60, cooldown=5, min_conv=3):
    """Find all entry signals with conviction >= min_conv."""
    n = len(df)
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    atr = pd.Series(highs - lows).rolling(14, min_periods=5).mean().values

    current_bias = None
    bias_set_bar = 0
    bias_set_by_bos = False
    bias_vol_confirmed = False
    structure_levels = []
    last_signal_bar = -999
    signals = []

    for i in range(warmup, n):
        evt = df["structure_event"].iloc[i]
        if evt:
            new_bias = "bullish" if "bullish" in evt else "bearish"
            level = df["structure_level"].iloc[i]
            is_bos = "bos" in evt
            bar_vol_conf = bool(df.get("vol_confirmed", pd.Series([False] * n)).iloc[i])

            if new_bias != current_bias:
                current_bias = new_bias
                bias_set_bar = i
                bias_set_by_bos = is_bos
                bias_vol_confirmed = bar_vol_conf
                structure_levels.append({"level": level, "type": evt, "bar": i})

            if is_bos:
                structure_levels.append({"level": level, "type": evt, "bar": i})
                if not bias_set_by_bos:
                    bias_set_by_bos = True
                    if bar_vol_conf:
                        bias_vol_confirmed = True

        if current_bias is None or i - bias_set_bar < 1:
            continue
        if i - last_signal_bar < cooldown:
            continue

        cur_atr = atr[i] if not np.isnan(atr[i]) else (highs[i] - lows[i])
        entry_bar_vol_conf = bool(df.get("vol_confirmed", pd.Series([False] * n)).iloc[i])
        entry_bar_rvol = df["rvol"].iloc[i] if "rvol" in df.columns else 0.0
        vol_div = df["vol_divergence"].iloc[i] if "vol_divergence" in df.columns else None

        direction = None
        reason = ""
        confluence = 0

        if current_bias == "bearish" and bias_set_by_bos:
            ez, reason, confluence = _find_bear_entry(df, i, structure_levels, cur_atr)
            if ez is not None:
                # Check selective filters (no FVG-only, no adverse vol div)
                if reason == "fvg":
                    continue
                if vol_div == "bullish_div":
                    continue
                conv = _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                                          entry_bar_vol_conf, entry_bar_rvol, confluence)
                if conv >= min_conv:
                    direction = "short"

        elif current_bias == "bullish" and bias_set_by_bos:
            ez, reason, confluence = _find_bull_entry(df, i, structure_levels, cur_atr)
            if ez is not None:
                if reason == "fvg":
                    continue
                if vol_div == "bearish_div":
                    continue
                conv = _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                                          entry_bar_vol_conf, entry_bar_rvol, confluence)
                if conv >= min_conv:
                    direction = "long"

        if direction is None:
            continue

        last_signal_bar = i
        signals.append({
            "bar": i,
            "date": df["date"].iloc[i],
            "direction": direction,
            "entry": closes[i],
            "conv": conv,
            "reason": reason,
            "atr": cur_atr,
            "stop_dist_pct": cur_atr / closes[i] * 100,  # 1.0 ATR as base
        })

    return signals


# ---------------------------------------------------------------------------
# Trailing stop simulation
# ---------------------------------------------------------------------------

def simulate_trailing(df, signals, trail_pct=2.0, max_hold=15,
                       hard_stop_atr=1.0):
    """Simulate trailing stop exits for all signals."""
    n = len(df)
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    atr = pd.Series(highs - lows).rolling(14, min_periods=5).mean().values

    trades = []

    for sig in signals:
        i = sig["bar"]
        entry = sig["entry"]
        d = sig["direction"]
        hard_stop_dist = sig["atr"] * hard_stop_atr / entry * 100

        peak_pnl = 0.0
        exit_pnl = None
        exit_day = 0
        exit_reason = ""

        for j in range(1, min(max_hold + 1, n - i)):
            if d == "long":
                day_pnl = (closes[i + j] - entry) / entry * 100
                day_high_pnl = (highs[i + j] - entry) / entry * 100
                if closes[i + j] < entry * (1 - hard_stop_dist / 100):
                    exit_pnl = day_pnl
                    exit_day = j
                    exit_reason = "stop"
                    break
            else:
                day_pnl = (entry - closes[i + j]) / entry * 100
                day_high_pnl = (entry - lows[i + j]) / entry * 100
                if closes[i + j] > entry * (1 + hard_stop_dist / 100):
                    exit_pnl = day_pnl
                    exit_day = j
                    exit_reason = "stop"
                    break

            peak_pnl = max(peak_pnl, day_high_pnl)

            # Trail: if pulled back more than trail_pct from peak
            if peak_pnl > trail_pct and day_pnl < peak_pnl - trail_pct:
                exit_pnl = day_pnl
                exit_day = j
                exit_reason = "trail"
                break

        if exit_pnl is None:
            end_j = min(max_hold, n - i - 1)
            if d == "long":
                exit_pnl = (closes[i + end_j] - entry) / entry * 100
            else:
                exit_pnl = (entry - closes[i + end_j]) / entry * 100
            exit_day = end_j
            exit_reason = "timeout"

        trades.append(TrailingTrade(
            entry_date=sig["date"],
            entry_price=entry,
            direction=d,
            conviction=sig["conv"],
            reason=sig["reason"],
            exit_date=df["date"].iloc[min(i + exit_day, n - 1)],
            exit_price=closes[min(i + exit_day, n - 1)],
            exit_reason=exit_reason,
            pnl_pct=exit_pnl,
            peak_pnl_pct=peak_pnl,
            hold_days=exit_day,
            risk_pct=hard_stop_dist,
        ))

    return trades


# ---------------------------------------------------------------------------
# MFE analysis
# ---------------------------------------------------------------------------

def mfe_analysis(df, signals):
    """Measure max favorable excursion at various horizons."""
    n = len(df)
    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    horizons = [1, 2, 3, 5, 7, 10, 15, 20]

    rows = []
    for sig in signals:
        i = sig["bar"]
        d = sig["direction"]
        entry = sig["entry"]
        row = {"date": sig["date"], "direction": d, "conv": sig["conv"]}

        for h in horizons:
            end_idx = min(i + h, n - 1)
            if end_idx <= i:
                row[f"mfe_{h}d"] = 0.0
                row[f"mae_{h}d"] = 0.0
                continue
            if d == "long":
                mfe = (max(highs[i + 1:end_idx + 1]) - entry) / entry * 100
                mae = (entry - min(lows[i + 1:end_idx + 1])) / entry * 100
            else:
                mfe = (entry - min(lows[i + 1:end_idx + 1])) / entry * 100
                mae = (max(highs[i + 1:end_idx + 1]) - entry) / entry * 100
            row[f"mfe_{h}d"] = mfe
            row[f"mae_{h}d"] = mae

        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Options P&L estimation
# ---------------------------------------------------------------------------

def estimate_options_pnl(trades, dte=21, premium_pct=1.6, delta=0.50,
                          theta_daily_pct=4.5):
    """
    Rough options P&L translation for ATM calls/puts.

    Args:
        dte: days to expiration at entry
        premium_pct: option premium as % of underlying (e.g., 1.6% = ~$10 on $625)
        delta: initial delta
        theta_daily_pct: daily theta as % of premium
    """
    results = []
    for t in trades:
        # Delta P&L (simplified — ignores gamma)
        delta_pnl_pct = t.pnl_pct * delta / premium_pct * 100  # as % of premium

        # Theta drag
        theta_drag_pct = t.hold_days * theta_daily_pct

        # Net option P&L as % of premium
        net_pct = delta_pnl_pct - theta_drag_pct

        results.append({
            "date": t.entry_date,
            "direction": t.direction,
            "underlying_pnl": t.pnl_pct,
            "hold_days": t.hold_days,
            "delta_pnl_pct": delta_pnl_pct,
            "theta_drag_pct": theta_drag_pct,
            "net_option_pct": net_pct,
            "exit_reason": t.exit_reason,
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

SEP = "-" * 90


def print_mfe_report(mfe_df):
    print(f"\n{SEP}")
    print("  MAX FAVORABLE EXCURSION (MFE) ANALYSIS")
    print(SEP)
    print("\n  How far do trades go in your favor before reversing?\n")

    horizons = [1, 2, 3, 5, 7, 10, 15, 20]
    print(f"  {'Days':>5}  {'Mean':>7}  {'Median':>7}  {'P25':>7}  {'P75':>7}  "
          f"{'Max':>7}  {'>2%':>5}  {'>3%':>5}  {'>5%':>5}")
    print(f"  {'-' * 5}  {'-' * 7}  {'-' * 7}  {'-' * 7}  {'-' * 7}  "
          f"{'-' * 7}  {'-' * 5}  {'-' * 5}  {'-' * 5}")

    for h in horizons:
        col = f"mfe_{h}d"
        v = mfe_df[col]
        gt2 = (v >= 2.0).mean() * 100
        gt3 = (v >= 3.0).mean() * 100
        gt5 = (v >= 5.0).mean() * 100
        print(f"  {h:>5}  {v.mean():>6.2f}%  {v.median():>6.2f}%  {v.quantile(.25):>6.2f}%  "
              f"{v.quantile(.75):>6.2f}%  {v.max():>6.2f}%  {gt2:>4.0f}%  {gt3:>4.0f}%  {gt5:>4.0f}%")

    print(f"\n  Key insight: {(mfe_df['mfe_15d'] >= 2.0).mean() * 100:.0f}% of trades reach "
          f"MFE > 2% by day 15. Median MFE at 15d: {mfe_df['mfe_15d'].median():.2f}%")


def print_trailing_report(trades, label=""):
    print(f"\n{SEP}")
    print(f"  TRAILING STOP BACKTEST — {label}")
    print(SEP)

    if not trades:
        print("\n  No trades.")
        return

    total = len(trades)
    winners = [t for t in trades if t.pnl_pct > 0]
    losers = [t for t in trades if t.pnl_pct <= 0]
    wr = len(winners) / total * 100

    avg_win = np.mean([t.pnl_pct for t in winners]) if winners else 0
    avg_loss = np.mean([t.pnl_pct for t in losers]) if losers else 0
    total_pnl = sum(t.pnl_pct for t in trades)
    total_loss = sum(t.pnl_pct for t in losers)
    pf = abs(sum(t.pnl_pct for t in winners) / total_loss) if total_loss else 0

    avg_hold_w = np.mean([t.hold_days for t in winners]) if winners else 0
    avg_hold_l = np.mean([t.hold_days for t in losers]) if losers else 0
    avg_hold = np.mean([t.hold_days for t in trades])
    avg_peak = np.mean([t.peak_pnl_pct for t in trades])

    targets = [t for t in trades if t.exit_reason == "trail"]
    stops = [t for t in trades if t.exit_reason == "stop"]
    timeouts = [t for t in trades if t.exit_reason == "timeout"]

    print(f"\n  Total trades:     {total}")
    print(f"  Winners:          {len(winners)}  ({wr:.1f}%)")
    print(f"  Losers:           {len(losers)}")
    print(f"  Avg winner:       +{avg_win:.2f}%  (held {avg_hold_w:.1f}d)")
    print(f"  Avg loser:        {avg_loss:.2f}%  (held {avg_hold_l:.1f}d)")
    print(f"  Avg hold:         {avg_hold:.1f} days")
    print(f"  Avg peak MFE:     +{avg_peak:.2f}%")
    print(f"  Total return:     {total_pnl:+.2f}%")
    if total_loss:
        print(f"  Profit factor:    {pf:.2f}")

    print(f"\n  Exit reasons:")
    print(f"    Trail stop:     {len(targets):>3d}  ({len(targets) / total * 100:.0f}%)")
    print(f"    Hard stop:      {len(stops):>3d}  ({len(stops) / total * 100:.0f}%)")
    print(f"    Timeout:        {len(timeouts):>3d}  ({len(timeouts) / total * 100:.0f}%)")

    # By conviction
    print(f"\n  By conviction:")
    for c in sorted(set(t.conviction for t in trades)):
        subset = [t for t in trades if t.conviction == c]
        w = [t for t in subset if t.pnl_pct > 0]
        sw = len(w) / len(subset) * 100 if subset else 0
        print(f"    conv={c}  n={len(subset):>3d}  WR={sw:.0f}%  "
              f"avg={np.mean([t.pnl_pct for t in subset]):+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # Fat tails
    fat = [t for t in trades if t.pnl_pct >= 3.0]
    print(f"\n  Fat tail trades (>= +3%):  {len(fat)}  "
          f"({len(fat) / total * 100:.0f}% of trades, "
          f"{sum(t.pnl_pct for t in fat):+.2f}% total contribution)")

    # Trade log
    print(f"\n  {'Entry':<12} {'Exit':<12} {'Dir':5} {'Conv':>4} {'Days':>4} "
          f"{'Peak':>7} {'PnL':>7} {'How':>7}")
    print(f"  {'-' * 12} {'-' * 12} {'-' * 5} {'-' * 4} {'-' * 4} "
          f"{'-' * 7} {'-' * 7} {'-' * 7}")
    for t in trades:
        print(f"  {str(t.entry_date):<12} {str(t.exit_date):<12} {t.direction:5} "
              f"{t.conviction:>4} {t.hold_days:>4} "
              f"{t.peak_pnl_pct:>+6.2f}% {t.pnl_pct:>+6.2f}% {t.exit_reason:>7}")


def print_options_report(opt_df, label=""):
    print(f"\n{SEP}")
    print(f"  OPTIONS P&L TRANSLATION — {label}")
    print(SEP)

    print(f"\n  Assumptions: ATM option, 21 DTE, delta=0.50, theta=4.5%/day of premium")
    print()

    wins = opt_df[opt_df["net_option_pct"] > 0]
    losses = opt_df[opt_df["net_option_pct"] <= 0]
    total = len(opt_df)
    wr = len(wins) / total * 100

    print(f"  Naked options:")
    print(f"    Win rate:       {wr:.1f}% ({len(wins)}/{total})")
    print(f"    Avg winner:     +{wins['net_option_pct'].mean():.1f}% of premium" if len(wins) else "")
    print(f"    Avg loser:      {losses['net_option_pct'].mean():.1f}% of premium" if len(losses) else "")
    print(f"    Total:          {opt_df['net_option_pct'].sum():+.1f}% of premium")

    # EV per trade
    avg_w = wins["net_option_pct"].mean() if len(wins) else 0
    avg_l = losses["net_option_pct"].mean() if len(losses) else 0
    ev = (wr / 100) * avg_w + (1 - wr / 100) * avg_l
    print(f"    EV per trade:   {ev:+.1f}% of premium")

    # Debit spread estimate (caps theta, caps gain)
    print(f"\n  Debit spread estimate (3% wide, 21 DTE):")
    spread_width = 3.0  # percent
    spread_cost_pct = 40  # rough: debit is ~40% of spread width for ATM
    for _, r in opt_df.iterrows():
        # Spread P&L: min(underlying_move * delta, spread_width) - cost
        # Simplified: if underlying moves > spread width, max profit
        pass

    spread_wins = 0
    spread_pnls = []
    for _, r in opt_df.iterrows():
        und_move = r["underlying_pnl"]
        if und_move > 0:
            # Winner: gain is proportional to move, capped at spread width
            gain_pct = min(abs(und_move), spread_width) / spread_width * 100
            net = gain_pct - spread_cost_pct  # as % of max spread value
        else:
            # Loser: lose some or all of debit paid
            loss_pct = min(abs(und_move) * 1.2, spread_cost_pct)  # delta + theta
            net = -loss_pct
        spread_pnls.append(net)
        if net > 0:
            spread_wins += 1

    spread_pnls = np.array(spread_pnls)
    swr = spread_wins / total * 100
    s_avg_w = spread_pnls[spread_pnls > 0].mean() if spread_wins else 0
    s_avg_l = spread_pnls[spread_pnls <= 0].mean() if spread_wins < total else 0
    s_ev = swr / 100 * s_avg_w + (1 - swr / 100) * s_avg_l
    print(f"    Win rate:       {swr:.1f}%")
    print(f"    Avg winner:     +{s_avg_w:.1f}% of max spread")
    print(f"    Avg loser:      {s_avg_l:.1f}% of max spread")
    print(f"    EV per trade:   {s_ev:+.1f}% of max spread")
    print(f"    Total:          {spread_pnls.sum():+.1f}% of max spread across {total} trades")


def print_grid_comparison(df, signals):
    """Test a grid of trailing stop parameters."""
    print(f"\n{SEP}")
    print("  PARAMETER GRID SEARCH")
    print(SEP)
    print(f"\n  {'Config':<38} {'N':>4} {'WR':>6} {'AvgW':>7} {'AvgL':>7} "
          f"{'Total':>8} {'PF':>6} {'Hold':>5}")
    print(f"  {'-' * 38} {'-' * 4} {'-' * 6} {'-' * 7} {'-' * 7} "
          f"{'-' * 8} {'-' * 6} {'-' * 5}")

    configs = [
        (1.5, 10, 0.5, "Trail 1.5% / 10d / 0.5ATR (tighter)"),
        (1.5, 10, 1.0, "Trail 1.5% / 10d / 1.0ATR"),
        (2.0, 10, 1.0, "Trail 2.0% / 10d / 1.0ATR"),
        (1.5, 15, 1.0, "Trail 1.5% / 15d / 1.0ATR"),
        (2.0, 15, 1.0, "Trail 2.0% / 15d / 1.0ATR  *** BEST"),
        (2.5, 15, 1.0, "Trail 2.5% / 15d / 1.0ATR"),
        (2.0, 20, 1.0, "Trail 2.0% / 20d / 1.0ATR"),
        (2.5, 20, 1.0, "Trail 2.5% / 20d / 1.0ATR"),
        (3.0, 20, 1.5, "Trail 3.0% / 20d / 1.5ATR"),
    ]

    for trail, hold, stop_atr, label in configs:
        trades = simulate_trailing(df, signals, trail_pct=trail,
                                    max_hold=hold, hard_stop_atr=stop_atr)
        if not trades:
            continue
        total = len(trades)
        winners = [t for t in trades if t.pnl_pct > 0]
        losers = [t for t in trades if t.pnl_pct <= 0]
        wr = len(winners) / total * 100
        avg_w = np.mean([t.pnl_pct for t in winners]) if winners else 0
        avg_l = np.mean([t.pnl_pct for t in losers]) if losers else 0
        total_pnl = sum(t.pnl_pct for t in trades)
        total_loss = sum(t.pnl_pct for t in losers)
        pf = abs(sum(t.pnl_pct for t in winners) / total_loss) if total_loss else 0
        avg_hold = np.mean([t.hold_days for t in trades])
        print(f"  {label:<38} {total:>4} {wr:>5.1f}% {avg_w:>+6.2f}% {avg_l:>+6.2f}% "
              f"{total_pnl:>+7.2f}% {pf:>5.2f} {avg_hold:>4.1f}d")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ICT Trailing Stop Backtester")
    parser.add_argument("--symbol", default="QQQ",
                        help="Symbol to test (default: QQQ)")
    parser.add_argument("--trail", type=float, default=2.0,
                        help="Trailing stop distance in %% (default: 2.0)")
    parser.add_argument("--hold", type=int, default=15,
                        help="Max hold days (default: 15)")
    parser.add_argument("--stop-atr", type=float, default=1.0,
                        help="Hard stop in ATR multiples (default: 1.0)")
    parser.add_argument("--min-conv", type=int, default=3,
                        help="Minimum conviction score (default: 3)")
    parser.add_argument("--grid", action="store_true",
                        help="Run parameter grid search")
    args = parser.parse_args()

    # Load data
    sym = args.symbol.upper()
    if sym == "QQQ":
        path = os.path.join(os.path.dirname(__file__), "..", "data", "daily_candles.parquet")
    else:
        path = os.path.join(os.path.dirname(__file__), "..",
                            "data", f"{sym.lower()}_daily_candles.parquet")

    if not os.path.exists(path):
        print(f"No data at {path}. Run /daily first to fetch data.")
        sys.exit(1)

    df_raw = pd.read_parquet(path)
    print(f"Loaded {len(df_raw)} candles for {sym}: "
          f"{df_raw['date'].iloc[0]} -> {df_raw['date'].iloc[-1]}")

    df = run_ict_analysis(df_raw)
    signals = collect_signals(df, min_conv=args.min_conv)
    print(f"Found {len(signals)} entry signals (conv >= {args.min_conv})")

    # MFE analysis
    mfe_df = mfe_analysis(df, signals)
    print_mfe_report(mfe_df)

    # Grid search
    if args.grid:
        print_grid_comparison(df, signals)

    # Main trailing stop test
    label = (f"Trail {args.trail}% / {args.hold}d / "
             f"{args.stop_atr}ATR stop / {sym}")
    trades = simulate_trailing(df, signals, trail_pct=args.trail,
                                max_hold=args.hold,
                                hard_stop_atr=args.stop_atr)
    print_trailing_report(trades, label=label)

    # Options translation
    opt_df = estimate_options_pnl(trades)
    print_options_report(opt_df, label=label)

    # Compare to original 2R/5d
    from scripts.backtest_ict import run_backtest
    orig_trades, _, _ = run_backtest(df_raw, max_hold_days=5, reward_ratio=2.0,
                                      warmup=60, selective=True, cooldown_bars=5,
                                      next_day_entry=False, min_conviction=0)
    print(f"\n{SEP}")
    print("  HEAD-TO-HEAD: TRAILING vs ORIGINAL")
    print(SEP)

    for lbl, tlist in [("Original 2R/5d", orig_trades), ("Trailing stop", trades)]:
        if not tlist:
            continue
        t = len(tlist)
        w = [x for x in tlist if (x.pnl_pct if hasattr(x, 'pnl_pct') else 0) > 0]
        pnls = [x.pnl_pct for x in tlist]
        wr = len(w) / t * 100
        total = sum(pnls)
        avg = np.mean(pnls)
        loss_sum = sum(p for p in pnls if p <= 0)
        pf = abs(sum(p for p in pnls if p > 0) / loss_sum) if loss_sum else 0
        hold = np.mean([x.hold_days if hasattr(x, 'hold_days') else 0 for x in tlist])
        print(f"\n  {lbl:<20} trades={t:>3}  WR={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={total:+.2f}%  PF={pf:.2f}  hold={hold:.1f}d")

    fat = [t for t in trades if t.pnl_pct >= 3.0]
    print(f"\n  Trailing fat tails (>= +3%): {len(fat)} trades, "
          f"{sum(t.pnl_pct for t in fat):+.2f}% contribution")
    print(f"  These are your options-viable trades.\n")

    print(SEP)
    print("  Disclaimer: This is analysis, not financial advice.")
    print(SEP)


if __name__ == "__main__":
    main()
