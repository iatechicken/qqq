# ICT Strategy Backtest — Full Results

**Date:** 2026-04-19
**Data:** 754 daily candles per symbol (Apr 2023 – Apr 2026)
**Symbols:** QQQ, TQQQ, SQQQ, IWM, TNA, TZA
**Engine:** `backtest_ict.py` — selective mode, 2R target, 5-day max hold, 5-bar cooldown

---

## 1. Strategy Rules

**Entry requirements (all three):**
1. BOS-confirmed directional bias (not CHoCH alone — CHoCH is 22% 5d accuracy)
2. Structure level retest (73.7% WR historically)
3. Retrace day (fading the retrace into the level, not chasing)

**Conviction scoring (0–5):**
- +1 BOS-confirmed bias
- +1 Bias-setting BOS was volume-confirmed
- +1 Entry bar rvol ≥ 1.5x
- +1 Structure level retest
- +1 Confluence ≥ 2 levels within 1%

**Exit rules:**
- Stop: zone boundary + 0.5 ATR
- Target: 2× risk (2R)
- Max hold: 5 trading days
- Cooldown: 5 bars between trades

---

## 2. Complete Results by Symbol, Conviction, and Entry Timing

### QQQ

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| 2024 | ≥3 | same-d | 12 | **83.3%** | +1.66% | −2.15% | **+12.31%** | **3.87** | 2.5d |
| 2025-26 | ≥3 | same-d | 17 | 64.7% | +1.34% | −1.02% | +8.60% | 2.41 | 2.6d |
| **Full** | **≥3** | **same-d** | **31** | **67.7%** | **+1.49%** | **−1.30%** | **+18.35%** | **2.42** | **2.7d** |
| 2024 | ≥3 | next-d | 10 | 50.0% | +2.10% | −1.74% | +1.81% | 1.21 | 3.8d |
| 2025-26 | ≥3 | next-d | 15 | 53.3% | +1.93% | −1.61% | +4.12% | 1.36 | 3.9d |
| Full | ≥3 | next-d | 27 | 51.9% | +1.91% | −1.59% | +6.00% | 1.29 | 3.7d |
| 2024 | ≥4 | same-d | 7 | **85.7%** | +1.64% | −2.48% | +7.33% | 3.95 | 2.4d |
| 2025-26 | ≥4 | same-d | 8 | 25.0% | +1.39% | −1.93% | −8.77% | 0.24 | 3.4d |
| Full | ≥4 | same-d | 15 | 53.3% | +1.57% | −2.00% | −1.44% | 0.90 | 2.9d |
| 2024 | ≥4 | next-d | 7 | 71.4% | +1.72% | −1.41% | +5.79% | 3.05 | 2.4d |
| 2025-26 | ≥4 | next-d | 5 | 60.0% | +2.00% | −2.22% | +1.57% | 1.35 | 5.4d |
| **Full** | **≥4** | **next-d** | **12** | **66.7%** | **+1.83%** | **−1.82%** | **+7.36%** | **2.01** | **3.7d** |

**QQQ verdict:**
- **Primary:** conv≥3 same-day (PF 2.42, 31 trades). Most consistent edge.
- **Singapore-viable:** conv≥4 next-day (PF 2.01, 12 trades). Survives next-day entry.
- **Trap:** conv≥4 same-day — regime-dependent (85.7% WR in 2024, 25% in 2025-26).

### TQQQ

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| 2024 | ≥3 | same-d | 9 | 66.7% | +5.39% | −5.67% | +15.30% | 1.90 | 2.0d |
| 2025-26 | ≥3 | same-d | 11 | 63.6% | +6.27% | −5.13% | +23.40% | 2.14 | 3.1d |
| **Full** | **≥3** | **same-d** | **22** | **63.6%** | **+5.88%** | **−5.02%** | **+42.20%** | **2.05** | **2.6d** |
| 2024 | ≥3 | next-d | 8 | 37.5% | +5.83% | −4.20% | −3.48% | 0.83 | 3.8d |
| 2025-26 | ≥3 | next-d | 11 | 54.5% | +5.92% | −5.52% | +7.96% | 1.29 | 4.8d |
| Full | ≥3 | next-d | 21 | 42.9% | +5.89% | −4.83% | −4.90% | 0.92 | 4.3d |
| 2024 | ≥4 | same-d | 5 | 60.0% | +6.36% | −5.06% | +8.94% | 1.88 | 2.8d |
| 2025-26 | ≥4 | same-d | 7 | 71.4% | +8.62% | −6.62% | +29.85% | 3.25 | 3.3d |
| **Full** | **≥4** | **same-d** | **13** | **61.5%** | **+7.77%** | **−5.20%** | **+36.17%** | **2.39** | **3.2d** |
| 2024 | ≥4 | next-d | 5 | 60.0% | +5.83% | −4.32% | +8.86% | 2.03 | 2.4d |
| 2025-26 | ≥4 | next-d | 5 | 20.0% | +12.97% | −6.72% | −13.91% | 0.48 | 4.6d |
| Full | ≥4 | next-d | 11 | 36.4% | +7.62% | −5.39% | −7.29% | 0.81 | 3.3d |

**TQQQ verdict:**
- **Best total return:** conv≥3 same-day (+42.20% over 3 years). Consistent across both periods.
- **Best quality:** conv≥4 same-day (PF 2.39, 13 trades). Fewer trades, higher conviction.
- **Next-day entry fails** — 3x leverage amplifies overnight gap damage.

### SQQQ

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| 2024 | ≥3 | same-d | 10 | 60.0% | +3.68% | −5.48% | +0.18% | 1.01 | 3.0d |
| **2025-26** | **≥3** | **same-d** | **16** | **75.0%** | **+5.51%** | **−4.90%** | **+46.50%** | **3.37** | **2.3d** |
| Full | ≥3 | same-d | 29 | 62.1% | +4.90% | −4.96% | +33.66% | 1.62 | 2.6d |
| 2024 | ≥3 | next-d | 10 | 50.0% | +3.43% | −4.07% | −3.17% | 0.84 | 3.9d |
| 2025-26 | ≥3 | next-d | 11 | 81.8% | +5.41% | −4.09% | +40.47% | 5.95 | 4.9d |
| Full | ≥3 | next-d | 23 | 65.2% | +4.43% | −3.85% | +35.67% | 2.16 | 4.1d |
| 2024 | ≥4 | same-d | 5 | 40.0% | +5.00% | −5.93% | −7.80% | 0.56 | 1.2d |
| 2025-26 | ≥4 | same-d | 8 | 62.5% | +5.53% | −6.07% | +9.46% | 1.52 | 1.8d |
| Full | ≥4 | same-d | 14 | 50.0% | +5.38% | −5.45% | −0.48% | 0.99 | 1.6d |
| 2024 | ≥4 | next-d | 5 | 40.0% | +4.55% | −3.36% | −0.99% | 0.90 | 2.8d |
| **2025-26** | **≥4** | **next-d** | **5** | **100%** | **+5.77%** | **—** | **+28.84%** | **∞** | **4.2d** |
| **Full** | **≥4** | **next-d** | **10** | **70.0%** | **+5.42%** | **−3.36%** | **+27.85%** | **3.76** | **3.5d** |

**SQQQ verdict:**
- **Regime-dependent:** flat in 2024 bull, monster in 2025-26 bear. Only trade when bias is bearish.
- **Best Singapore play:** conv≥4 next-day (PF 3.76, 70% WR). Bearish momentum gaps help the inverse ETF.
- **Same-day conv≥4 is a trap** (PF 0.99) — the next-day version is paradoxically better.

### IWM

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| 2024 | ≥3 | same-d | 22 | 59.1% | +1.26% | −1.81% | +0.06% | 1.00 | 3.5d |
| 2025-26 | ≥3 | same-d | 17 | 52.9% | +1.85% | −1.79% | +2.34% | 1.16 | 3.2d |
| Full | ≥3 | same-d | 43 | 55.8% | +1.50% | −1.74% | +2.87% | 1.09 | 3.3d |
| Full | ≥3 | next-d | 39 | 53.8% | +1.70% | −1.60% | +6.96% | 1.24 | 4.0d |
| 2024 | ≥4 | same-d | 19 | 63.2% | +1.47% | −2.00% | +3.61% | 1.26 | 3.6d |
| 2025-26 | ≥4 | same-d | 10 | 50.0% | +2.22% | −1.90% | +1.58% | 1.17 | 3.4d |
| Full | ≥4 | same-d | 31 | 61.3% | +1.66% | −1.96% | +8.05% | 1.34 | 3.5d |
| 2024 | ≥4 | next-d | 16 | 56.2% | +1.71% | −1.59% | +4.32% | 1.39 | 4.9d |
| 2025-26 | ≥4 | next-d | 10 | 60.0% | +2.13% | −1.76% | +5.72% | 1.81 | 3.1d |
| **Full** | **≥4** | **next-d** | **27** | **59.3%** | **+1.88%** | **−1.65%** | **+12.03%** | **1.66** | **4.3d** |

**IWM verdict:**
- No strong edge at any configuration. Conv≥3 is a coin flip (PF 1.00–1.09).
- **Best config:** conv≥4 next-day (PF 1.66, 27 trades) — IWM's smaller moves mean overnight gaps do less damage.
- Edge is thin. Trade only if Nasdaq/SQQQ offer nothing.

### TNA (3× Russell Bull)

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| 2024 | ≥3 | same-d | 17 | 58.8% | +4.24% | −4.41% | +11.48% | 1.37 | 3.9d |
| 2025-26 | ≥3 | same-d | 10 | 50.0% | +8.65% | −6.56% | +10.45% | 1.32 | 4.1d |
| Full | ≥3 | same-d | 30 | 53.3% | +5.60% | −5.11% | +18.08% | 1.25 | 3.8d |
| Full | ≥3 | next-d | 27 | 44.4% | +5.50% | −5.64% | −18.65% | 0.78 | 3.6d |
| Full | ≥4 | same-d | 17 | 52.9% | +5.20% | −5.50% | +2.86% | 1.07 | 3.6d |
| Full | ≥4 | next-d | 15 | 46.7% | +5.65% | −4.91% | +0.26% | 1.01 | 4.3d |

**TNA verdict:** Marginal edge same-day (PF 1.25), negative next-day. Not recommended.

### TZA (3× Russell Bear)

| Period | Conv | Entry | Trades | WR | Avg Win | Avg Loss | Total | PF | Hold |
|--------|------|-------|--------|-----|---------|----------|-------|-----|------|
| **2024** | **≥4** | **same-d** | **13** | **61.5%** | **+6.06%** | **−4.23%** | **+27.33%** | **2.29** | **4.3d** |
| 2025-26 | ≥4 | same-d | 1 | 0.0% | — | −7.24% | −7.24% | 0.00 | 6.0d |
| Full | ≥3 | same-d | 26 | 53.8% | +5.86% | −5.25% | +19.01% | 1.30 | 3.3d |
| Full | ≥4 | same-d | 15 | 53.3% | +6.06% | −4.41% | +17.62% | 1.57 | 4.2d |
| Full | ≥4 | next-d | 14 | 50.0% | +6.67% | −5.41% | +8.79% | 1.23 | 3.7d |

**TZA verdict:** Strong in 2024 at conv≥4 (PF 2.29) but only 1 trade in 2025-26 — insufficient data. May have real edge but sample too small to trust.

---

## 3. Exit Timing Analysis

### How trades exit (QQQ conv≥3 same-day, full period)

| Exit Type | Count | % | Auto? |
|-----------|-------|---|-------|
| Target hit | ~62% | Majority | Yes — bracket order |
| Stop hit | ~29% | | Yes — bracket order |
| Timeout (5d) | ~10% | Rare | Manual close needed |

**91% of trades exit automatically.** Bracket orders (OCO with stop + target) handle nearly everything. Timeout exits are rare and can be closed at next available session.

### Same-day vs Next-day Entry Impact

| Symbol | Conv | Same-day PF | Next-day PF | Delta |
|--------|------|-------------|-------------|-------|
| QQQ | ≥3 | 2.42 | 1.29 | −1.13 |
| QQQ | ≥4 | 0.90 | 2.01 | +1.11 |
| TQQQ | ≥3 | 2.05 | 0.92 | −1.13 |
| TQQQ | ≥4 | 2.39 | 0.81 | −1.58 |
| SQQQ | ≥3 | 1.62 | 2.16 | +0.54 |
| SQQQ | ≥4 | 0.99 | 3.76 | +2.77 |
| IWM | ≥4 | 1.34 | 1.66 | +0.32 |

**Pattern:** Next-day entry generally hurts EXCEPT for SQQQ (both conv levels) and QQQ/IWM at conv≥4. These are the Singapore-timezone plays.

---

## 4. Period Comparison: 2024 Bull vs 2025-26 Whipsaw

### 2024 Standouts (bull market)
- QQQ ≥3 same-day: 83.3% WR, PF 3.87 — the strategy's sweet spot
- QQQ ≥4 same-day: 85.7% WR, PF 3.95 — works in steady trends
- TZA ≥4 same-day: 61.5% WR, PF 2.29 — counter-trend bearish plays worked

### 2025-26 Standouts (tariff volatility / bear regime)
- SQQQ ≥3 same-day: 75.0% WR, PF 3.37 — inverse ETF thrives in bears
- SQQQ ≥4 next-day: 100% WR (5/5), PF ∞ — small sample but perfect
- TQQQ ≥4 same-day: 71.4% WR, PF 3.25 — leveraged bull plays still work selectively

### Regime-Dependent Strategies (use with caution)
- QQQ conv≥4 same-day: PF 3.95 in 2024, PF 0.24 in 2025-26
- SQQQ any config: PF ~1.0 in 2024, PF 3.37+ in 2025-26
- TZA conv≥4: PF 2.29 in 2024, insufficient data in 2025-26

### Regime-Robust Strategies (consistent across periods)
- **QQQ ≥3 same-day:** PF 3.87 → 2.41 (both profitable)
- **TQQQ ≥3 same-day:** PF 1.90 → 2.14 (both profitable, improving)
- **TQQQ ≥4 same-day:** PF 1.88 → 3.25 (both profitable, improving)

---

## 5. Trailing Stop Variant (Options-Optimized)

Tested via `scripts/backtest_trailing.py` — trail 2.0%, 15d hold, 1.0 ATR hard stop.

| Metric | Original 2R/5d | Trailing Stop |
|--------|---------------|---------------|
| Trades | 21 | 48 |
| WR | 66.7% | 45.8% |
| Avg winner | +1.48% | +3.39% (held 11.2d) |
| Avg loser | −1.33% | −2.17% (held 4.3d) |
| Total | +11.35% | +18.20% |
| PF | 2.22 | 1.32 |

**MFE analysis:** 79% of trades reach +2% by day 15. Median MFE at 15d: 3.45%.

**Fat tail trades:** 10 of 48 (21%) produced ≥+3%, contributing 53% of total return.

### Options P&L Translation
- **Naked ATM 21 DTE:** −21.7% EV per trade (LOSE MONEY)
- **3%-wide debit spread:** +16.1% EV per trade (VIABLE)

**Conclusion:** The ICT strategy is a small-edge/high-frequency system. Options buying needs big-edge/low-frequency. These are structurally incompatible unless using debit spreads or isolating fat-tail trades.

---

## 6. Recommended Configurations

### For Same-Day Entry (limit orders at structure levels)
1. **TQQQ ≥3** — best total return (+42.20%), consistent across regimes
2. **QQQ ≥3** — highest quality (PF 2.42), most trades (31)
3. **TQQQ ≥4** — best risk-adjusted (PF 2.39), fewer trades (13)

### For Singapore Timezone (next-day / bracket orders)
1. **SQQQ ≥4 next-day** — PF 3.76, 70% WR (bear regime only)
2. **QQQ ≥4 next-day** — PF 2.01, 66.7% WR (all regimes)
3. **IWM ≥4 next-day** — PF 1.66, 59.3% WR (thin edge, most trades)

### Do NOT Trade
- QQQ conv≥4 same-day (regime-dependent trap)
- TQQQ next-day any conv (leverage amplifies gap damage)
- IWM/TNA/TZA at conv≥3 (no edge)
- TLT/TMF/TMV (no demonstrated ICT edge)
- Naked options on any configuration

---

*Generated 2026-04-19. Data: 754 candles/symbol (Apr 2023 – Apr 2026). Engine: backtest_ict.py selective mode.*
