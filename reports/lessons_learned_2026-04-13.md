# Lessons Learned — March 27 to April 10, 2026

## The Cycle

QQQ sold off from 636 (Jan 28 high) to 555.60 (Mar 30 low), then rallied +8.6% to 611 over 10 sessions. The strategy correctly identified the bearish trend and produced winning short setups during the decline, but **did not participate in the 558→611 reversal rally**. On Apr 10, price tagged the STRUCT 613.18 short zone and rejected — re-entering the strategy's wheelhouse.

---

## Lesson 1: The Bullish Divergence Signal Is Real But Untraded

**What happened:** On Mar 30, a bullish divergence fired at the 555.60 low. Price then rallied +8.6% over 10 sessions. The strategy's BOS-bearish bias rule blocked all long entries.

**What we found:** All 4 bullish_div signals in the 2-year dataset produced a bounce:

| Date | Close | Max rally in 10d |
|------|-------|------------------|
| 2025-01-13 | 505.56 | +5.59% |
| 2025-03-13 | 468.34 | +5.40% |
| 2026-02-17 | 601.30 | +2.58% |
| 2026-03-30 | 558.28 | +9.34% |

**The lesson:** Bullish divergence at support with volume-confirmed displacement (3/31 rvol 1.3x) is a reliable reversal signal that the current strategy cannot trade. The 4/4 sample is tiny but consistent. A dedicated counter-trend sub-strategy built around this signal is worth backtesting with more data.

**What NOT to conclude:** "We should have just gone long." For every clear-in-hindsight reversal, there are failed bounces that look identical at entry. The backtest showed counter-trend CHoCH trades are 22% WR at 5 days. The BOS filter exists because it works over hundreds of trades — this one missed trade is the cost of the edge.

---

## Lesson 2: Low-Volume Rallies Can Run Further Than Expected

**What happened:** The 558→613 rally occurred on progressively declining volume:

| Date | Close | rvol | Cumulative move |
|------|-------|------|-----------------|
| 3/31 | 577.18 | **1.3x** | +3.4% |
| 4/02 | 584.98 | 0.7x | +4.8% |
| 4/06 | 588.50 | 0.5x | +5.4% |
| 4/07 | 588.59 | 0.8x | +5.4% |
| 4/08 | 606.09 | 0.94x | +8.6% |
| 4/09 | 610.19 | 0.58x | +9.3% |
| 4/10 | 611.07 | 0.54x | +9.4% |

Zero volume-confirmed bullish events across the entire rally. 5-day avg was 38% below 20-day by the end.

**The lesson:** Volume declining during a rally is a sign of exhaustion, but it does NOT mean the rally stops immediately. Low-volume rallies can persist for days/weeks as shorts cover and passive flows enter. Volume analysis helps with *conviction scoring*, not *timing*.

**Implication for strategy:** Don't rush to short a low-volume rally just because volume is weak. Wait for the price to reach a structural level. The strategy already does this correctly — the STRUCT 613 zone was the right place, not an arbitrary "it's overextended" call.

---

## Lesson 3: Gap Risk Is Real — Stops Saved the Trade

**What happened:** On Apr 7, the strategy had an active SHORT @ 589.05 (conv 3/5). Overnight, QQQ gapped up to 608.71 open on Apr 8 (+3.4% gap). The stop at 593.92 would have limited the loss to **-0.83%**.

**The lesson:** Gap risk is unavoidable with daily candle strategies. The stop discipline works: even with a 20-point gap-through, the loss was contained to a single R. Options with defined risk (puts, put spreads) are even better for gap management — max loss is premium paid.

**Implication:** When entering short at a structural level, consider using put options instead of short equity/ETFs, especially before major catalysts. The Apr 7 short going into an overnight session was inherently risky. Options would have capped the loss.

---

## Lesson 4: Structure Levels Work — 613 Held on CPI Day

**What happened:** The system identified STRUCT 613.18 + PMH 613.29 as the next short zone. On Apr 10 (CPI day), QQQ tagged 613.67 intraday and closed at 611.07 — a textbook rejection at the confluence level.

**The lesson:** ICT structure levels ARE predictive of reaction points. The 613 cluster (STRUCT + PMH + breaker block top) produced a rejection even on the most important macro catalyst of the month. This validates the scoring framework: levels that stack (3+ within 1%) have 80% WR in the backtest, and this real-world observation is consistent.

**Implication:** Trust the structure levels. The system works at predicting WHERE price will react. The challenge is determining IF the reaction is a reversal or just a pause before continuation.

---

## Lesson 5: "Transition Zones" Are Uncharted Territory for the Strategy

**What happened:** From Apr 8-10, QQQ was above the bearish CHoCH level (607.05) but below the swing high needed for a bullish CHoCH (613.29). The strategy was technically bearish but price was acting bullish. This created an awkward gap where:
- Short setups existed (per the still-bearish bias) but felt counter-intuitive against a rallying tape
- Long setups couldn't be generated (no bullish structure confirmation)
- The strategy effectively said "wait" for 3 days while price rallied +1%

**The lesson:** The strategy has a blind spot in structural transition zones — the gap between CHoCH and BOS confirmation. This is by design (waiting for confirmation avoids false signals), but it means there will be periods of 3-5 days where the strategy has no actionable guidance.

**Implication:** During transition zones, reduce position sizing on any setup. The uncertainty is structural, not just volatility-based. Don't force trades in either direction until the bias resolves.

---

## Lesson 6: Cross-Group Confirmation Has Signal Value

**What happened:** IWM flipped to bullish CHoCH on Apr 8, while QQQ remained bearish. SQQQ also confirmed bearish (inverse of QQQ's rally). Russell was leading Nasdaq — small caps bottomed first and flipped first.

**The lesson:** Cross-group divergence is a leading indicator. When Russell flips before Nasdaq, it signals risk-on rotation is starting. When both groups eventually align, the move tends to accelerate. This information was present but not formally scored — it could add predictive value.

**Implication (future development):** Consider adding a cross-group alignment bonus to the scoring system. Something like: +0.5 conviction when IWM bias aligns with QQQ trade direction. Needs backtesting but the signal is there.

---

## Lesson 7: The Schwab API Fetcher Takes datetime, Not Lookback

`SchwabDailyFetcher.fetch(symbol, start_utc, end_utc)` takes two `datetime` objects, not `lookback_months`. This caused wasted iterations in multiple sessions. Already corrected, but worth documenting as a recurring pitfall.

---

## Strategy Validation Summary

| Aspect | Verdict |
|--------|---------|
| BOS bias filter | **Working as designed** — prevented counter-trend trades with 22% WR |
| Structure level retest | **Validated** — 613 cluster rejected on CPI day |
| Volume scoring | **Validated** — low-volume rally (0.54x) confirms weak conviction |
| Stop discipline | **Validated** — gap-through on Apr 8 limited loss to -0.83% |
| Counter-trend detection | **Gap in strategy** — bullish_div (4/4 hit rate) is untraded |
| Cross-group analysis | **Informational** — Russell led the turn, but not yet scored |
| Transition zone handling | **Acknowledged gap** — 3-5 day blind spots are structural |

---

## Action Items

1. **Backtest bullish_div counter-trend strategy** — pull 5+ years of data, test bullish_div at BOS-bearish lows with volume confirmation next day. If viable (WR > 55%, PF > 1.5), implement as a separate sub-strategy with smaller position sizing.

2. **Consider cross-group alignment scoring** — add IWM bias alignment as a confluence factor for QQQ setups. Needs backtest.

3. **Options preference for overnight risk** — when entering at a structural level before a catalyst (CPI, earnings, FOMC), prefer defined-risk options over linear positions.

4. **Accept the cost of the edge** — the missed 8.6% rally is the price of the 69.6% WR and 2.55 PF. Do not loosen the BOS filter. If counter-trend trades are desired, build a separate strategy with separate rules and separate position sizing.

---

*Document created April 13, 2026*
