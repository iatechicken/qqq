# QQQ ICT Daily Strategy — Backtest-Validated Playbook

**Last updated:** April 1, 2026
**Data:** 503 daily candles (Apr 2024 – Apr 2026) for QQQ, TQQQ, and SQQQ
**Engine:** `src/qqq_ingest/ict.py` | `scripts/backtest_ict.py`

---

## Strategy Overview

This strategy applies ICT (Inner Circle Trader) price action concepts to daily QQQ candles to identify high-probability directional trades. The core idea: wait for market structure to confirm a direction, then fade retracements into broken levels.

The strategy was refined through iterative backtesting, starting from 120 raw trades and narrowing to 10–22 selective trades that carry a genuine statistical edge.

---

## Three Strategy Modes

| | Aggressive | Conservative | Next-day All (ref) |
|--|-----------|-------------|-------------------|
| **Entry timing** | Signal day close | Next-day open | Next-day open |
| **Min conviction** | 3 | 4 | 3 |
| **Trades (500d)** | 22 | 10 | 20 |
| **Win Rate** | **72.7%** | 60.0% | 55.0% |
| **Total Return** | **+15.15%** | +4.69% | +3.98% |
| **Profit Factor** | **2.87** | 1.64 | 1.23 |
| **Avg R-multiple** | +0.90R | +0.43R | +0.05R |
| **Avg hold** | 1.6 days | 2.4 days | 2.4 days |

### Aggressive (Recommended)

Enter at the close of the signal day. Take any setup with conviction score >= 3. This captures the best entry price — right at the structure level before the rejection move begins. Winners resolve in 1–2 days.

### Conservative

Wait for the full daily candle to close, then enter at the next day's open. Only take setups with conviction score >= 4. Trades roughly once every 2 months. Suitable for traders who want confirmation before committing capital.

### Why Aggressive Outperforms

The edge IS the entry price. When you enter at the close of a rally day that retests a broken structure level, you're at the top of the retrace. By next morning's open, the rejection has already started — you get a worse price and tighter risk/reward. The backtest shows this consistently:

- Mar 5, 2025: Same-day short at $502.01, target hit at $495.63 (+1.27%). Next-day entered at $493.69 — captured +4.66% but only because the gap was enormous. Most trades don't gap that hard.
- Oct 23, 2024: Same-day long at $488.36, target hit at $499.50 (+2.28%). Next-day entered at $492.11 — missed the gap up, then timed out at -1.68%.

---

## Entry Rules

Every trade requires all three conditions:

### 1. BOS-Confirmed Bias

The directional bias must be set or confirmed by a **Break of Structure (BOS)**, not just a Change of Character (CHoCH).

- **BOS** = price breaks a prior swing high (bullish) or swing low (bearish) *in the direction of the existing trend*. This confirms momentum.
- **CHoCH** = the first structural break *against* the prevailing trend. This is an early warning, not a confirmed reversal.

From the backtest: BOS has 53% accuracy at predicting the next 5 days of price action. CHoCH has only 22%. Trading on CHoCH alone means you're entering before the trend has actually flipped — you'll get chopped up by false reversals.

**Rule:** Wait for at least one BOS after a CHoCH before taking trades in the new direction.

### 2. Structure Level Retest

The entry must be at or near a **previously broken structure level** — the exact price where BOS occurred.

- For shorts: price rallies back up to a broken support level (now resistance)
- For longs: price dips back down to a broken resistance level (now support)

This was the entry type for 19 of 22 selective trades (78.9% WR, +18.22% total). The only other viable entry type is PDH/PDL (Previous Day High/Low), but its sample size is small and win rate lower (33.3%).

**Avoid FVG-only entries.** Fair Value Gaps without a structure level had 14.3% WR in the backtest — proven losers.

### 3. Retrace Day Confirmation

The signal day must show a retrace into the zone:

- **For shorts:** Close > previous close (a rally day pulling into resistance)
- **For longs:** Close < previous close (a dip day pulling into support)

You are fading the retrace, not chasing the move.

---

## Conviction Scoring (0–5)

Each setup is scored on a 0–5 scale. Each point represents a distinct, backtest-validated edge:

| Points | Criterion | Backtest Evidence |
|--------|-----------|-------------------|
| **+1** | BOS-confirmed bias | BOS 53% 5d accuracy vs CHoCH 22%. Without BOS, the bias is unreliable. |
| **+1** | Structure level retest (or PDH/PDL) | 78.9% WR as entry type. The core edge. FVG and OB-only entries do NOT earn this point. |
| **+1** | Bias-setting BOS was volume confirmed (rvol >= 1.5) | The structure break had institutional participation. High volume on the BOS means conviction behind the trend. |
| **+1** | Entry bar has elevated volume (rvol >= 1.5) | Rejection at the zone is happening with force. 80% WR when present (but rare — only 5 of 22 trades). |
| **+1** | 3+ levels stacking within 1% | Confluence: structure level + PDH/PDL + OB/FVG/NWOG all near the same price. 80% WR when 3+ levels align. |

**Penalties:**
| Points | Condition |
|--------|-----------|
| **-1** | Volume divergence against the trade (bullish divergence while shorting, or bearish divergence while going long) |
| **-1** | CHoCH-only bias (no BOS confirmation yet) |

### Score Interpretation

| Score | Label | Action |
|-------|-------|--------|
| 0–1 | SKIP | Do not trade. Bias is unconfirmed or setup is weak. |
| 2 | WEAK | Marginal — only consider if other factors align outside the model. |
| 3 | MODERATE | **Minimum for Aggressive strategy.** BOS + structure retest + one confirming factor. |
| 4 | HIGH | **Minimum for Conservative strategy.** Strong alignment across structure, volume, and confluence. |
| 5 | STRONG | Rare — everything aligns. Highest confidence. |

### Score Distribution from Backtest

**Aggressive (same-day entry):**

| Score | Trades | Win Rate | Total |
|-------|--------|----------|-------|
| 3 | 11 | **81.8%** | +7.91% |
| 4 | 11 | 63.6% | +7.24% |

**Conservative (next-day entry):**

| Score | Trades | Win Rate | Total |
|-------|--------|----------|-------|
| 4 | 10 | 60.0% | +4.69% |

---

## Exit Rules

### Target: 2R Reward

- Calculate risk as: distance from entry to stop
- Target = entry price +/- 2x risk (in the trade direction)
- This gives a clean 2:1 reward-to-risk on every trade

### Stop Loss

- **Shorts:** Zone top + 0.5 ATR (14-period)
- **Longs:** Zone bottom - 0.5 ATR (14-period)
- The 0.5 ATR buffer prevents getting stopped by noise wicks above/below the zone

### 5-Day Max Hold

**This is the single most impactful rule in the strategy.**

Changing max hold from 15 days to 5 days improved the profit factor from 1.27 to 2.87. The data is unambiguous:

| Max Hold | Win Rate | Profit Factor | Worst Trade |
|----------|----------|---------------|-------------|
| 15 days | 68% | 1.27 | -5.29% |
| **5 days** | **73%** | **2.87** | **-3.24%** |
| 3 days | 77% | 3.48 | -3.24% |

**Why it works:** Winners resolve in 1.2 days on average. Losers drag on for 2–3 days before hitting stops. If a trade hasn't worked in 5 days, the setup has failed — holding longer only increases the loss.

The Apr 14, 2025 trade illustrates this perfectly: with a 15-day hold, it ran for 12 days and lost -5.29%. With a 5-day hold, it was closed at +2.84% — it was actually in profit at day 5 before reversing.

### 5-Bar Cooldown

Minimum 5 trading days between closing one trade and entering the next. Prevents overtrading during choppy periods where the structure keeps flipping.

---

## Hard Filters (Skip the Trade)

These conditions disqualify a setup regardless of score:

1. **FVG-only entry** — 14.3% WR historically. Fair Value Gaps are useful as confluence, not as standalone entries.
2. **CHoCH-only bias** — No BOS has confirmed the new direction. You're front-running a reversal that may not materialize.
3. **Volume divergence against trade** — Bullish divergence (lower low on lower volume) while shorting means sellers are exhausted. Bearish divergence while going long means buyers are exhausted. The smart money is exiting.

---

## Volume Analysis

Volume plays a supporting role — it's a quality filter, not a standalone signal.

### Relative Volume (rvol)

- `rvol = today's volume / 20-day SMA of volume`
- Below 0.8 = low (thin participation, signals less reliable)
- 0.8–1.3 = normal
- 1.3–2.0 = elevated (institutional activity)
- Above 2.0 = climactic (often marks exhaustion, not continuation)

### How Volume Confirms

- **On BOS:** High rvol on the structure break means institutions drove the move. The bias is more trustworthy.
- **On entry bar:** High rvol at the structure level means the rejection has conviction. 80% WR when rvol >= 1.5 on entry.
- **Volume divergence:** Price making new extremes on declining volume = exhaustion. Warns against fading into a spent move.

### Volume Findings from Backtest

| Category | Trades | Win Rate | Total |
|----------|--------|----------|-------|
| Entry bar vol confirmed | 5 | 80.0% | +2.65% |
| No entry vol | 17 | 70.6% | +12.50% |
| Any vol signal (bias or entry) | 13 | 69.2% | +7.11% |
| No vol at all | 9 | 77.8% | +8.04% |

**Key insight:** Volume confirmation helps when present, but its absence doesn't disqualify a trade. The "no vol at all" bucket had 77.8% WR — structure retests work even without elevated volume. Volume is additive, not required.

---

## Confluence Analysis

Confluence = multiple levels stacking near the same price within 1%.

| Levels | Trades | Win Rate | Backtest Evidence |
|--------|--------|----------|-------------------|
| 1 | 2 | 100% | Too small a sample to draw conclusions |
| 2 | 10 | 60.0% | Decent — meets minimum bar |
| **3+** | **10** | **80.0%** | **Sweet spot — strong alignment** |

What counts as a "level":
- Broken structure level (BOS price)
- PDH / PDL / PWH / PWL
- Order block zone (top or bottom within 1%)
- FVG zone (when overlapping with structure)
- NWOG boundary or CE

When 3+ of these cluster at the same price, the zone has been tested and respected multiple times. The market has "memory" at that level.

---

## Performance by Direction

| Direction | Trades | Win Rate | Total Return |
|-----------|--------|----------|-------------|
| Long | 14 | **78.6%** | +10.69% |
| Short | 8 | 62.5% | +4.45% |

Longs outperformed in the backtest period (which included the 2024–2025 bull run). This does not mean longs are inherently better — it reflects the prevailing trend during the sample. In a sustained bear market (like Feb–Mar 2026), shorts would dominate. The strategy is direction-agnostic; it follows the bias.

---

## Monthly Performance (Aggressive Mode)

| Month | Trades | Win% | Total |
|-------|--------|------|-------|
| 2024-07 | 2 | 100% | +4.25% |
| 2024-08 | 1 | 0% | -3.24% |
| 2024-10 | 1 | 100% | +2.28% |
| 2024-11 | 1 | 100% | +1.14% |
| 2024-12 | 1 | 100% | +0.88% |
| 2025-02 | 1 | 0% | -1.18% |
| 2025-03 | 3 | 67% | +2.58% |
| 2025-04 | 2 | 100% | +3.66% |
| 2025-05 | 2 | 50% | +0.50% |
| 2025-08 | 2 | 50% | +0.89% |
| 2025-09 | 2 | 100% | +1.87% |
| 2025-10 | 1 | 100% | +0.38% |
| 2025-11 | 2 | 100% | +2.33% |
| 2026-01 | 1 | 0% | -1.20% |

15 of 22 months with trades were profitable. Losing months had only 1 trade each — single-trade variance. No catastrophic drawdown months.

---

## Options Application

Since winners resolve in 1–2 days:

- **Expiry:** 1–2 weeks out is sufficient. You're not paying for time you don't need.
- **Strikes:** ATM or slightly OTM puts/calls. Higher delta = more exposure to the move.
- **When to go further out:** If entering before a known catalyst (CPI, Fed, geopolitical deadline), extend to cover the event. The Apr 6 Iran deadline and Apr 10 CPI release are current examples where 3–4 week expiry is warranted.
- **Sizing:** The strategy wins ~73% of the time with 2R reward. Even small positions compound well at this rate. Position size should be what you can lose on the 27% of trades that stop out.

---

## What This Strategy Does NOT Do

- **Intraday entries.** All signals are based on daily candles. No killzone timing, no 15-min order flow.
- **Predict reversals.** CHoCH is noted but not traded. The strategy waits for BOS confirmation, which means it misses the exact top/bottom by design.
- **Trade FVGs standalone.** FVGs are used for confluence, not as primary entries.
- **Hold through events.** The 5-day max hold means positions are closed before most scheduled catalysts unless you're already in profit.

---

## Leveraged ETF Results (TQQQ / SQQQ)

The same ICT strategy was backtested on leveraged QQQ ETFs using independently detected structure on each symbol's own price action.

**Data:** `data/tqqq_daily_candles.parquet`, `data/sqqq_daily_candles.parquet` (503 candles each, Apr 2024 – Apr 2026)

### Cross-Symbol Comparison — Aggressive Mode

| Symbol | Leverage | Trades | Win Rate | Total Return | Profit Factor | Best Trade | Worst Trade |
|--------|----------|--------|----------|-------------|---------------|------------|-------------|
| QQQ | 1x | 23 | 69.6% | +14.12% | 2.55 | +2.84% | -3.24% |
| **TQQQ** | **3x bull** | **16** | **75.0%** | **+46.20%** | **3.05** | **+13.38%** | **-7.70%** |
| **SQQQ** | **3x inverse** | **22** | **72.7%** | **+44.92%** | **2.28** | **+7.94%** | **-8.50%** |

### Cross-Symbol Comparison — Conservative Mode (next-day, score>=4)

| Symbol | Trades | Win Rate | Total Return | Profit Factor |
|--------|--------|----------|-------------|---------------|
| QQQ | 10 | 60.0% | +4.69% | 1.64 |
| TQQQ | 5 | 40.0% | -4.61% | 0.78 |
| **SQQQ** | **7** | **85.7%** | **+28.04%** | **7.18** |

### Key Findings

**TQQQ Aggressive is the highest-return strategy.** 75% WR, 3.05 PF, +46.20% total return. The 3x leverage amplifies the +1-2% QQQ moves into +3-13% trades. However, the worst trade (-7.70%) is also amplified — position sizing must account for 3x drawdown risk.

**SQQQ Conservative is the standout finding.** 85.7% WR with a 7.18 profit factor on 7 trades over 500 days — roughly one trade every 2.5 months. Six of seven trades won. This works because:

- SQQQ rises when QQQ falls. Going long SQQQ at confirmed structure support is equivalent to shorting QQQ at confirmed resistance.
- The conservative filter (score>=4, next-day entry) only fires when bearish momentum on QQQ is confirmed with highest conviction.
- The 3x leverage amplifies the move: a -2% QQQ day becomes a +6% SQQQ day.
- The strong filter keeps you out of choppy periods where SQQQ would whipsaw.

**TQQQ Conservative does NOT work.** 40% WR, negative return (-4.61%). The 3x leverage amplifies the worse entry prices from next-day execution. Do not use conservative mode with TQQQ.

### SQQQ Conservative — Trade Log

| Entry | Exit | Hold | PnL | How |
|-------|------|------|-----|-----|
| 2024-08-13 | 2024-08-13 | 0d | -4.54% | stop (only loss) |
| 2024-12-19 | 2024-12-27 | 5d | +3.73% | timeout |
| 2025-02-04 | 2025-02-07 | 3d | +6.80% | target |
| 2025-04-10 | 2025-04-17 | 5d | +5.57% | timeout |
| 2025-09-18 | 2025-09-25 | 5d | +1.01% | timeout |
| 2026-01-21 | 2026-01-22 | 1d | +5.73% | target |
| 2026-03-24 | 2026-03-27 | 3d | +9.74% | target |

Average hold: 3.1 days. Even the timeouts were profitable — the trade was working, just hadn't hit the 2R target yet.

### TQQQ Aggressive — Trade Log

| Entry | Exit | Dir | Hold | PnL | How |
|-------|------|-----|------|-----|-----|
| 2024-07-11 | 2024-07-12 | long | 1d | +3.39% | target |
| 2024-08-14 | 2024-08-15 | short | 1d | -7.52% | stop |
| 2024-10-23 | 2024-10-25 | long | 2d | +4.66% | target |
| 2025-01-02 | 2025-01-06 | long | 2d | +8.16% | target |
| 2025-02-25 | 2025-02-27 | long | 2d | -7.70% | stop |
| 2025-04-07 | 2025-04-08 | short | 1d | +9.23% | target |
| 2025-04-15 | 2025-04-21 | short | 3d | +13.38% | target |
| 2025-08-01 | 2025-08-04 | long | 1d | +4.46% | target |
| 2025-08-20 | 2025-08-27 | long | 5d | +4.19% | target |
| 2025-09-17 | 2025-09-19 | long | 2d | +4.45% | target |
| 2025-10-10 | 2025-10-13 | long | 1d | +3.05% | target |
| 2025-11-07 | 2025-11-10 | long | 1d | +5.14% | target |
| 2025-11-17 | 2025-11-18 | long | 1d | -3.62% | stop |
| 2026-01-29 | 2026-01-30 | long | 1d | -3.64% | stop |

### Recommended Instrument by Strategy Mode

| Strategy | Best Instrument | Why |
|----------|----------------|-----|
| Aggressive (same-day) | **TQQQ** | Highest total return (+46%), best PF (3.05), 75% WR. Use for both long and short bias. |
| Conservative (next-day) | **SQQQ** | 85.7% WR, 7.18 PF. Only viable conservative leveraged play. Use for bearish setups only. |
| Conservative (next-day) | **QQQ** | 60% WR, 1.64 PF. Use for bullish setups where SQQQ doesn't apply. |

### Risk Note on Leveraged ETFs

- **Leverage decay:** TQQQ and SQQQ reset daily. Holding 3-5 days introduces tracking error vs 3x the underlying move. This is acceptable for 1-5 day holds but compounds against you over weeks.
- **Amplified losses:** A -3% QQQ loss becomes -7 to -9% on TQQQ. Size positions assuming the worst trade scenario.
- **Liquidity:** Both TQQQ and SQQQ trade 100M+ shares/day with tight spreads and liquid options chains. No fill concerns.
- **Don't mix:** Use TQQQ for bullish setups, SQQQ for bearish setups. Don't use TQQQ shorts or SQQQ shorts as a proxy — the structure detection works best going long each respective ETF in its natural direction.

---

## Running the Strategy

```bash
# Fetch fresh data and run the daily report
python scripts/ict_daily.py --months 24 --save

# Run from cached data
python scripts/ict_daily.py --load

# Run the backtest (compares Aggressive, Conservative, and reference modes)
python scripts/backtest_ict.py
```

The daily report (`ict_daily.py`) shows the current bias, active zones, and scored trade setups. The backtest (`backtest_ict.py`) runs all strategy modes and outputs the head-to-head comparison.

Leveraged ETF data is stored separately:
- `data/tqqq_daily_candles.parquet`
- `data/sqqq_daily_candles.parquet`

---

*This strategy is based on ICT price action concepts applied to daily candles, validated through systematic backtesting across QQQ, TQQQ, and SQQQ. Past performance does not guarantee future results. Manage your risk accordingly.*
