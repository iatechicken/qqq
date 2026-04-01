#!/usr/bin/env python3
"""
ICT Strategy Backtester — Selective Mode

Two backtest modes:
  ALL:       takes every signal (original behavior)
  SELECTIVE: only takes high-conviction setups based on learned filters

Selective filters (derived from initial 500-day backtest findings):
  1. Bias must be set by BOS (not CHoCH — CHoCH is too early, 22% 5d accuracy)
  2. Volume confirmation required: rvol >= 1.5 on bias-setting BOS OR entry bar
  3. No FVG-only entries (16.7% WR historically)
  4. Confluence bonus: structure level + PDH/PDL/OB stacking within 1%
  5. Volume divergence against trade direction = skip
  6. Cooldown: min 5 bars between trades

Also tests bias-only accuracy (does the bias predict next N-bar direction?)
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from qqq_ingest.ict import run_ict_analysis


@dataclass
class Trade:
    entry_date: object
    entry_price: float
    direction: str
    entry_reason: str
    stop: float
    target: float
    conviction: int = 0          # conviction score (higher = better)
    vol_confirmed: bool = False
    bias_vol_confirmed: bool = False  # was the bias-setting BOS vol confirmed?
    confluence_count: int = 0    # how many levels stack near entry
    exit_date: object = None
    exit_price: float = 0.0
    exit_reason: str = ""
    pnl_pct: float = 0.0
    risk_pct: float = 0.0
    r_multiple: float = 0.0


def run_backtest(df_raw: pd.DataFrame, max_hold_days: int = 15,
                 reward_ratio: float = 2.0, warmup: int = 60,
                 selective: bool = False, cooldown_bars: int = 5,
                 next_day_entry: bool = False, min_conviction: int = 0):
    """
    Run ICT backtest.

    If next_day_entry=True, signal is detected on bar i (after close) but
    the position is opened at bar i+1's open price. This models waiting for
    the full daily candle to confirm before entering.

    min_conviction: only take trades with conviction >= this value (0-5).
    """
    df = run_ict_analysis(df_raw)
    n = len(df)

    closes = df["close"].values
    highs = df["high"].values
    lows = df["low"].values
    opens = df["open"].values

    atr = pd.Series(highs - lows).rolling(14, min_periods=5).mean().values

    trades: list[Trade] = []
    open_trade: Trade | None = None
    current_bias = None
    bias_set_bar = 0
    bias_set_by_bos = False
    bias_vol_confirmed = False
    last_trade_bar = -999

    # Pending signal: when next_day_entry=True, store the setup and execute next bar
    pending_entry: dict | None = None

    structure_levels: list[dict] = []

    # --- Bias accuracy tracking ---
    bias_events = []

    for i in range(warmup, n):
        evt = df["structure_event"].iloc[i]
        if evt:
            new_bias = "bullish" if "bullish" in evt else "bearish"
            level = df["structure_level"].iloc[i]
            is_bos = "bos" in evt

            fwd_5 = closes[min(i + 5, n - 1)] - closes[i]
            fwd_10 = closes[min(i + 10, n - 1)] - closes[i]
            if new_bias == "bearish":
                fwd_5 = -fwd_5
                fwd_10 = -fwd_10

            bar_rvol = df["rvol"].iloc[i] if "rvol" in df.columns else np.nan
            bar_vol_conf = bool(df.get("vol_confirmed", pd.Series([False] * n)).iloc[i])

            bias_events.append({
                "date": df["date"].iloc[i],
                "event": evt,
                "level": level,
                "bias": new_bias,
                "close": closes[i],
                "fwd_5d": fwd_5 / closes[i] * 100,
                "fwd_10d": fwd_10 / closes[i] * 100,
                "rvol": bar_rvol,
            })

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

        # --- Execute pending next-day entry ---
        if pending_entry is not None and open_trade is None:
            p = pending_entry
            pending_entry = None
            entry_price = opens[i]  # enter at next day's open

            if p["direction"] == "short":
                stop = p["zone_level"] + p["atr"] * 0.5
                risk = stop - entry_price
                if risk > 0:
                    target = entry_price - risk * reward_ratio
                    open_trade = Trade(
                        entry_date=df["date"].iloc[i],
                        entry_price=entry_price,
                        direction="short",
                        entry_reason=p["reason"],
                        stop=stop, target=target,
                        conviction=p["conviction"],
                        vol_confirmed=p["vol_confirmed"],
                        bias_vol_confirmed=p["bias_vol_confirmed"],
                        confluence_count=p["confluence"],
                        risk_pct=risk / entry_price * 100,
                    )
            else:
                stop = p["zone_level"] - p["atr"] * 0.5
                risk = entry_price - stop
                if risk > 0:
                    target = entry_price + risk * reward_ratio
                    open_trade = Trade(
                        entry_date=df["date"].iloc[i],
                        entry_price=entry_price,
                        direction="long",
                        entry_reason=p["reason"],
                        stop=stop, target=target,
                        conviction=p["conviction"],
                        vol_confirmed=p["vol_confirmed"],
                        bias_vol_confirmed=p["bias_vol_confirmed"],
                        confluence_count=p["confluence"],
                        risk_pct=risk / entry_price * 100,
                    )

        # --- Manage open trade ---
        if open_trade is not None:
            entry_bar = _bar_of_date(df, open_trade.entry_date)
            days_held = i - entry_bar

            if open_trade.direction == "short":
                if lows[i] <= open_trade.target:
                    _close_trade(open_trade, df, i, open_trade.target, "target")
                elif closes[i] > open_trade.stop:
                    _close_trade(open_trade, df, i, closes[i], "stop")
                elif days_held >= max_hold_days:
                    _close_trade(open_trade, df, i, closes[i], "timeout")
                else:
                    continue
            else:
                if highs[i] >= open_trade.target:
                    _close_trade(open_trade, df, i, open_trade.target, "target")
                elif closes[i] < open_trade.stop:
                    _close_trade(open_trade, df, i, closes[i], "stop")
                elif days_held >= max_hold_days:
                    _close_trade(open_trade, df, i, closes[i], "timeout")
                else:
                    continue

            trades.append(open_trade)
            open_trade = None
            last_trade_bar = i

        # --- Look for new entries ---
        if open_trade is not None or current_bias is None:
            continue
        if i - bias_set_bar < 1:
            continue
        if i - last_trade_bar < cooldown_bars:
            continue
        # Don't queue a new signal if one is already pending
        if pending_entry is not None:
            continue

        cur_atr = atr[i] if not np.isnan(atr[i]) else (highs[i] - lows[i])
        entry_bar_vol_conf = bool(df.get("vol_confirmed", pd.Series([False] * n)).iloc[i])
        entry_bar_rvol = df["rvol"].iloc[i] if "rvol" in df.columns else 0.0
        vol_div = df["vol_divergence"].iloc[i] if "vol_divergence" in df.columns else None

        if current_bias == "bearish":
            entry_zone, reason, confluence = _find_bear_entry(df, i, structure_levels, cur_atr)
            if entry_zone is None:
                continue

            if selective:
                if not _passes_selective_filter(
                    reason=reason,
                    bias_set_by_bos=bias_set_by_bos,
                    bias_vol_confirmed=bias_vol_confirmed,
                    entry_bar_vol_conf=entry_bar_vol_conf,
                    entry_bar_rvol=entry_bar_rvol,
                    vol_div=vol_div,
                    direction="short",
                    confluence=confluence,
                ):
                    continue

            conv = _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                                     entry_bar_vol_conf, entry_bar_rvol, confluence)
            if conv < min_conviction:
                continue

            if next_day_entry and i + 1 < n:
                pending_entry = {
                    "direction": "short", "zone_level": entry_zone, "reason": reason,
                    "atr": cur_atr, "conviction": conv, "vol_confirmed": entry_bar_vol_conf,
                    "bias_vol_confirmed": bias_vol_confirmed, "confluence": confluence,
                }
            else:
                entry_price = closes[i]
                stop = entry_zone + cur_atr * 0.5
                risk = stop - entry_price
                if risk <= 0:
                    continue
                target = entry_price - risk * reward_ratio
                open_trade = Trade(
                    entry_date=df["date"].iloc[i], entry_price=entry_price,
                    direction="short", entry_reason=reason, stop=stop, target=target,
                    conviction=conv, vol_confirmed=entry_bar_vol_conf,
                    bias_vol_confirmed=bias_vol_confirmed, confluence_count=confluence,
                    risk_pct=risk / entry_price * 100,
                )

        elif current_bias == "bullish":
            entry_zone, reason, confluence = _find_bull_entry(df, i, structure_levels, cur_atr)
            if entry_zone is None:
                continue

            if selective:
                if not _passes_selective_filter(
                    reason=reason,
                    bias_set_by_bos=bias_set_by_bos,
                    bias_vol_confirmed=bias_vol_confirmed,
                    entry_bar_vol_conf=entry_bar_vol_conf,
                    entry_bar_rvol=entry_bar_rvol,
                    vol_div=vol_div,
                    direction="long",
                    confluence=confluence,
                ):
                    continue

            conv = _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                                     entry_bar_vol_conf, entry_bar_rvol, confluence)
            if conv < min_conviction:
                continue

            if next_day_entry and i + 1 < n:
                pending_entry = {
                    "direction": "long", "zone_level": entry_zone, "reason": reason,
                    "atr": cur_atr, "conviction": conv, "vol_confirmed": entry_bar_vol_conf,
                    "bias_vol_confirmed": bias_vol_confirmed, "confluence": confluence,
                }
            else:
                entry_price = closes[i]
                stop = entry_zone - cur_atr * 0.5
                risk = entry_price - stop
                if risk <= 0:
                    continue
                target = entry_price + risk * reward_ratio
                open_trade = Trade(
                    entry_date=df["date"].iloc[i], entry_price=entry_price,
                    direction="long", entry_reason=reason, stop=stop, target=target,
                    conviction=conv, vol_confirmed=entry_bar_vol_conf,
                    bias_vol_confirmed=bias_vol_confirmed, confluence_count=confluence,
                    risk_pct=risk / entry_price * 100,
                )

    # Close remaining
    if open_trade is not None:
        _close_trade(open_trade, df, n - 1, closes[-1], "timeout")
        trades.append(open_trade)

    return trades, bias_events, df


def _passes_selective_filter(reason, bias_set_by_bos, bias_vol_confirmed,
                              entry_bar_vol_conf, entry_bar_rvol, vol_div,
                              direction, confluence):
    """Return True if this trade passes the selective quality filter."""

    # 1. No FVG-only entries (proven losers)
    if reason == "fvg":
        return False

    # 2. Bias must be confirmed by BOS (CHoCH alone is too early)
    if not bias_set_by_bos:
        return False

    # 3. Volume confirmation: need it on either the bias event or the entry bar
    has_volume = bias_vol_confirmed or entry_bar_vol_conf or entry_bar_rvol >= 1.3
    if not has_volume:
        return False

    # 4. Volume divergence against trade = skip
    if direction == "short" and vol_div == "bullish_div":
        return False  # seller exhaustion while trying to short
    if direction == "long" and vol_div == "bearish_div":
        return False  # buyer exhaustion while trying to go long

    # 5. Confluence bonus: if no volume confirmation on entry, need confluence >= 2
    if not entry_bar_vol_conf and entry_bar_rvol < 1.3 and confluence < 2:
        return False

    return True


def _conviction_score(reason, bias_set_by_bos, bias_vol_confirmed,
                       entry_bar_vol_conf, entry_bar_rvol, confluence):
    """Score 0-5 how strong this setup is."""
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


def _find_bear_entry(df, i, struct_levels, atr_val):
    """Check if current bar is a short entry. Returns (zone_level, reason, confluence_count)."""
    close = df["close"].iloc[i]
    high = df["high"].iloc[i]
    prev_close = df["close"].iloc[i - 1]

    if close <= prev_close:
        return None, "", 0

    # Count how many levels stack near the current price (within 1%)
    confluence = 0
    primary_reason = ""
    primary_zone = None

    # Structure level retest
    for sl in reversed(struct_levels[-10:]):
        level = sl["level"]
        if level > close and level <= high * 1.005:
            primary_zone = level
            primary_reason = "struct"
            confluence += 1
            break
        if abs(level - close) / close < 0.005:
            primary_zone = level
            primary_reason = "struct"
            confluence += 1
            break

    # PDH proximity
    pdh = df["pdh"].iloc[i] if not np.isnan(df["pdh"].iloc[i]) else None
    if pdh and close > pdh * 0.997 and close < pdh * 1.01:
        confluence += 1
        if primary_zone is None:
            primary_zone = pdh
            primary_reason = "pdh"

    # PWL (broken support = resistance)
    pwl = df["pwl"].iloc[i] if not np.isnan(df["pwl"].iloc[i]) else None
    if pwl and close > pwl * 0.997 and close < pwl * 1.01:
        confluence += 1
        if primary_zone is None:
            primary_zone = pwl
            primary_reason = "pwl"

    # Bearish FVG — count as confluence, not primary
    for j in range(max(0, i - 20), i):
        if df["fvg_type"].iloc[j] == "bearish_fvg":
            fvg_top = df["fvg_top"].iloc[j]
            fvg_btm = df["fvg_bottom"].iloc[j]
            if high >= fvg_btm and close <= fvg_top:
                confluence += 1
                if primary_zone is None:
                    primary_zone = fvg_top
                    primary_reason = "fvg"
                break

    # Bearish OB
    for j in range(max(0, i - 30), i):
        if df["ob_type"].iloc[j] == "bearish_ob":
            ob_top = df["ob_top"].iloc[j]
            ob_btm = df["ob_bottom"].iloc[j]
            if high >= ob_btm and close <= ob_top:
                confluence += 1
                if primary_zone is None:
                    primary_zone = ob_top
                    primary_reason = "ob"
                break

    return primary_zone, primary_reason, confluence


def _find_bull_entry(df, i, struct_levels, atr_val):
    """Check if current bar is a long entry. Returns (zone_level, reason, confluence_count)."""
    close = df["close"].iloc[i]
    low = df["low"].iloc[i]
    prev_close = df["close"].iloc[i - 1]

    if close >= prev_close:
        return None, "", 0

    confluence = 0
    primary_reason = ""
    primary_zone = None

    # Structure level retest
    for sl in reversed(struct_levels[-10:]):
        level = sl["level"]
        if level < close and level >= low * 0.995:
            primary_zone = level
            primary_reason = "struct"
            confluence += 1
            break
        if abs(level - close) / close < 0.005:
            primary_zone = level
            primary_reason = "struct"
            confluence += 1
            break

    # PDL proximity
    pdl = df["pdl"].iloc[i] if not np.isnan(df["pdl"].iloc[i]) else None
    if pdl and close < pdl * 1.003 and close > pdl * 0.99:
        confluence += 1
        if primary_zone is None:
            primary_zone = pdl
            primary_reason = "pdl"

    # PWH (broken resistance = support)
    pwh = df["pwh"].iloc[i] if not np.isnan(df["pwh"].iloc[i]) else None
    if pwh and close < pwh * 1.003 and close > pwh * 0.99:
        confluence += 1
        if primary_zone is None:
            primary_zone = pwh
            primary_reason = "pwh"

    # Bullish FVG
    for j in range(max(0, i - 20), i):
        if df["fvg_type"].iloc[j] == "bullish_fvg":
            fvg_top = df["fvg_top"].iloc[j]
            fvg_btm = df["fvg_bottom"].iloc[j]
            if low <= fvg_top and close >= fvg_btm:
                confluence += 1
                if primary_zone is None:
                    primary_zone = fvg_btm
                    primary_reason = "fvg"
                break

    # Bullish OB
    for j in range(max(0, i - 30), i):
        if df["ob_type"].iloc[j] == "bullish_ob":
            ob_top = df["ob_top"].iloc[j]
            ob_btm = df["ob_bottom"].iloc[j]
            if low <= ob_top and close >= ob_btm:
                confluence += 1
                if primary_zone is None:
                    primary_zone = ob_btm
                    primary_reason = "ob"
                break

    return primary_zone, primary_reason, confluence


def _close_trade(trade, df, bar_idx, price, reason):
    trade.exit_date = df["date"].iloc[bar_idx]
    trade.exit_price = price
    trade.exit_reason = reason
    if trade.direction == "short":
        trade.pnl_pct = (trade.entry_price - trade.exit_price) / trade.entry_price * 100
    else:
        trade.pnl_pct = (trade.exit_price - trade.entry_price) / trade.entry_price * 100
    if trade.risk_pct > 0:
        trade.r_multiple = trade.pnl_pct / trade.risk_pct


def _bar_of_date(df, date_val):
    matches = df.index[df["date"] == date_val].tolist()
    return matches[0] if matches else 0


def print_results(trades, bias_events, label=""):
    SEP = "-" * 80

    print(f"\n{SEP}")
    print(f"  ICT STRATEGY BACKTEST — {label}")
    print(f"{SEP}")

    # ===================== BIAS ACCURACY =====================
    print(f"\n== BIAS ACCURACY ==\n")
    print(f"  Does the bias (CHoCH/BOS) correctly predict direction?\n")

    if bias_events:
        be_df = pd.DataFrame(bias_events)

        for lbl, subset in [("All events", be_df),
                             ("CHoCH only", be_df[be_df["event"].str.contains("choch")]),
                             ("BOS only", be_df[be_df["event"].str.contains("bos")])]:
            if subset.empty:
                continue
            correct_5d = (subset["fwd_5d"] > 0).sum()
            correct_10d = (subset["fwd_10d"] > 0).sum()
            total = len(subset)
            avg_5d = subset["fwd_5d"].mean()
            avg_10d = subset["fwd_10d"].mean()
            print(f"  {lbl:15s}  n={total:>3d}  "
                  f"5d_accuracy={correct_5d/total*100:.0f}%  avg_5d={avg_5d:+.2f}%  "
                  f"10d_accuracy={correct_10d/total*100:.0f}%  avg_10d={avg_10d:+.2f}%")

        vol_conf = be_df[be_df["rvol"] >= 1.5]
        vol_no = be_df[be_df["rvol"] < 1.5]
        print()
        for lbl, subset in [("Vol confirmed", vol_conf), ("No vol confirm", vol_no)]:
            if subset.empty:
                continue
            correct_5d = (subset["fwd_5d"] > 0).sum()
            total = len(subset)
            avg_5d = subset["fwd_5d"].mean()
            print(f"  {lbl:15s}  n={total:>3d}  "
                  f"5d_accuracy={correct_5d/total*100:.0f}%  avg_5d={avg_5d:+.2f}%")

        print()
        for bias_dir in ["bearish", "bullish"]:
            subset = be_df[be_df["bias"] == bias_dir]
            if subset.empty:
                continue
            correct_5d = (subset["fwd_5d"] > 0).sum()
            total = len(subset)
            avg_5d = subset["fwd_5d"].mean()
            avg_10d = subset["fwd_10d"].mean()
            print(f"  {bias_dir:15s}  n={total:>3d}  "
                  f"5d_accuracy={correct_5d/total*100:.0f}%  avg_5d={avg_5d:+.2f}%  "
                  f"avg_10d={avg_10d:+.2f}%")

    # ===================== TRADE RESULTS =====================
    if not trades:
        print("\n  No trades generated.")
        return

    total = len(trades)
    winners = [t for t in trades if t.pnl_pct > 0]
    losers = [t for t in trades if t.pnl_pct <= 0]
    win_rate = len(winners) / total * 100

    avg_win = np.mean([t.pnl_pct for t in winners]) if winners else 0
    avg_loss = np.mean([t.pnl_pct for t in losers]) if losers else 0
    total_pnl = sum(t.pnl_pct for t in trades)
    avg_pnl = total_pnl / total
    total_loss = sum(t.pnl_pct for t in losers)

    targets = [t for t in trades if t.exit_reason == "target"]
    stops = [t for t in trades if t.exit_reason == "stop"]
    timeouts = [t for t in trades if t.exit_reason == "timeout"]

    shorts = [t for t in trades if t.direction == "short"]
    longs = [t for t in trades if t.direction == "long"]

    print(f"\n== TRADE RESULTS ==\n")
    print(f"  Total trades:     {total}")
    print(f"  Winners:          {len(winners)}  ({win_rate:.1f}%)")
    print(f"  Losers:           {len(losers)}")
    print(f"  Avg winner:       +{avg_win:.2f}%")
    print(f"  Avg loser:        {avg_loss:.2f}%")
    print(f"  Avg trade:        {avg_pnl:+.2f}%")
    print(f"  Total return:     {total_pnl:+.2f}%")
    if total_loss != 0:
        print(f"  Profit factor:    {abs(sum(t.pnl_pct for t in winners) / total_loss):.2f}")
    if winners and losers:
        print(f"  Avg R-multiple:   {np.mean([t.r_multiple for t in trades]):+.2f}R")

    print(f"\n== EXIT REASONS ==\n")
    print(f"  Target hit:       {len(targets):>3d}  ({len(targets)/total*100:.0f}%)")
    print(f"  Stopped out:      {len(stops):>3d}  ({len(stops)/total*100:.0f}%)")
    print(f"  Timeout:          {len(timeouts):>3d}  ({len(timeouts)/total*100:.0f}%)")

    print(f"\n== BY DIRECTION ==\n")
    for lbl, subset in [("Short", shorts), ("Long", longs)]:
        if not subset:
            continue
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {lbl:6s}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    print(f"\n== BY ENTRY TYPE ==\n")
    entry_types = set(t.entry_reason for t in trades)
    for reason in sorted(entry_types):
        subset = [t for t in trades if t.entry_reason == reason]
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {reason:8s}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # ===================== CONVICTION ANALYSIS =====================
    print(f"\n== BY CONVICTION SCORE ==\n")
    for score in sorted(set(t.conviction for t in trades)):
        subset = [t for t in trades if t.conviction == score]
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  score={score}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # ===================== CONFLUENCE =====================
    print(f"\n== BY CONFLUENCE (levels stacking near entry) ==\n")
    for c in sorted(set(t.confluence_count for t in trades)):
        subset = [t for t in trades if t.confluence_count == c]
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {c} levels  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # ===================== VOLUME DEEP DIVE =====================
    print(f"\n== VOLUME ANALYSIS ==\n")

    vol_conf = [t for t in trades if t.vol_confirmed]
    no_vol = [t for t in trades if not t.vol_confirmed]
    for lbl, subset in [("Entry bar vol", vol_conf), ("No entry vol", no_vol)]:
        if not subset:
            print(f"  {lbl:18s}  no trades")
            continue
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {lbl:18s}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    bias_vc = [t for t in trades if t.bias_vol_confirmed]
    no_bias_vc = [t for t in trades if not t.bias_vol_confirmed]
    print()
    for lbl, subset in [("Bias BOS vol", bias_vc), ("No bias vol", no_bias_vc)]:
        if not subset:
            print(f"  {lbl:18s}  no trades")
            continue
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {lbl:18s}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # Combined: both entry + bias vol
    both_vol = [t for t in trades if t.vol_confirmed and t.bias_vol_confirmed]
    any_vol = [t for t in trades if t.vol_confirmed or t.bias_vol_confirmed]
    neither = [t for t in trades if not t.vol_confirmed and not t.bias_vol_confirmed]
    print()
    for lbl, subset in [("Both vol signals", both_vol),
                         ("Any vol signal", any_vol),
                         ("No vol at all", neither)]:
        if not subset:
            print(f"  {lbl:18s}  no trades")
            continue
        w = [t for t in subset if t.pnl_pct > 0]
        wr = len(w) / len(subset) * 100
        avg = np.mean([t.pnl_pct for t in subset])
        print(f"  {lbl:18s}  trades={len(subset):>3d}  "
              f"win_rate={wr:.1f}%  avg={avg:+.2f}%  "
              f"total={sum(t.pnl_pct for t in subset):+.2f}%")

    # Monthly
    print(f"\n== MONTHLY BREAKDOWN ==\n")
    monthly: dict[str, list[Trade]] = {}
    for t in trades:
        key = str(t.entry_date)[:7]
        monthly.setdefault(key, []).append(t)

    print(f"  {'Month':<10s} {'Trades':>6s} {'Win%':>6s} {'Avg':>8s} {'Total':>8s} {'Avg R':>6s}")
    print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*8} {'-'*8} {'-'*6}")
    for month in sorted(monthly.keys()):
        m_trades = monthly[month]
        m_wins = [t for t in m_trades if t.pnl_pct > 0]
        m_wr = len(m_wins) / len(m_trades) * 100
        m_avg = np.mean([t.pnl_pct for t in m_trades])
        m_total = sum(t.pnl_pct for t in m_trades)
        m_r = np.mean([t.r_multiple for t in m_trades])
        print(f"  {month:<10s} {len(m_trades):>6d} {m_wr:>5.0f}% {m_avg:>+7.2f}% {m_total:>+7.2f}% {m_r:>+5.1f}R")

    # Trade log
    print(f"\n== TRADE LOG ==\n")
    print(f"  {'Entry':<12s} {'Exit':<12s} {'Dir':5s} {'Why':6s} "
          f"{'Entry$':>8s} {'Exit$':>8s} {'PnL%':>7s} {'R':>5s} {'Conv':>4s} {'Confl':>5s} {'How':7s}")
    print(f"  {'-'*12} {'-'*12} {'-'*5} {'-'*6} {'-'*8} {'-'*8} {'-'*7} {'-'*5} {'-'*4} {'-'*5} {'-'*7}")
    for t in trades:
        print(f"  {str(t.entry_date):<12s} {str(t.exit_date):<12s} {t.direction:5s} {t.entry_reason:6s} "
              f"{t.entry_price:>8.2f} {t.exit_price:>8.2f} {t.pnl_pct:>+6.2f}% {t.r_multiple:>+4.1f}R "
              f"{t.conviction:>4d} {t.confluence_count:>5d} {t.exit_reason:7s}")

    print(f"\n{SEP}")


def main():
    parquet_path = os.path.join(os.path.dirname(__file__), "..", "data", "daily_candles.parquet")
    if not os.path.exists(parquet_path):
        print(f"No data at {parquet_path}. Run: python scripts/ict_daily.py --months 24 --save")
        sys.exit(1)

    df_raw = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df_raw)} daily candles: {df_raw['date'].iloc[0]} -> {df_raw['date'].iloc[-1]}")

    # --- Run all three strategy modes ---
    configs = [
        {
            "label": "AGGRESSIVE — same-day entry, score>=3",
            "short": "Aggressive",
            "max_hold": 5, "selective": True, "cooldown": 5,
            "next_day": False, "min_conv": 0,
        },
        {
            "label": "CONSERVATIVE — next-day entry, score>=4",
            "short": "Conservative",
            "max_hold": 5, "selective": True, "cooldown": 5,
            "next_day": True, "min_conv": 4,
        },
        {
            "label": "NEXT-DAY ALL — next-day entry, score>=3 (reference)",
            "short": "Next-day all",
            "max_hold": 5, "selective": True, "cooldown": 5,
            "next_day": True, "min_conv": 0,
        },
    ]

    results = []  # (label_short, trades_list)
    bias_events = None
    for cfg in configs:
        print("\n" + "=" * 80)
        print(f"  RUNNING {cfg['label']}")
        print("=" * 80)
        t, be, df = run_backtest(df_raw, max_hold_days=cfg["max_hold"],
                                  reward_ratio=2.0, warmup=60,
                                  selective=cfg["selective"],
                                  cooldown_bars=cfg["cooldown"],
                                  next_day_entry=cfg["next_day"],
                                  min_conviction=cfg["min_conv"])
        if bias_events is None:
            bias_events = be
        print_results(t, bias_events, label=cfg["label"])
        results.append((cfg["short"], t))

    # --- Head-to-head comparison ---
    print("\n" + "=" * 80)
    print("  HEAD-TO-HEAD COMPARISON")
    print("=" * 80)

    for lbl, trades in results:
        if not trades:
            print(f"\n  {lbl:15s}  no trades")
            continue
        total = len(trades)
        winners = [t for t in trades if t.pnl_pct > 0]
        wr = len(winners) / total * 100
        total_pnl = sum(t.pnl_pct for t in trades)
        avg_pnl = total_pnl / total
        losers = [t for t in trades if t.pnl_pct <= 0]
        total_loss = sum(t.pnl_pct for t in losers)
        pf = abs(sum(t.pnl_pct for t in winners) / total_loss) if total_loss else 0
        avg_r = np.mean([t.r_multiple for t in trades])
        print(f"\n  {lbl:15s}  trades={total:>3d}  win_rate={wr:.1f}%  "
              f"avg={avg_pnl:+.2f}%  total={total_pnl:+.2f}%  "
              f"PF={pf:.2f}  avg_R={avg_r:+.2f}")


if __name__ == "__main__":
    main()
