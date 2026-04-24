# ICT Trading System — US Stock ETFs

Daily ICT (Inner Circle Trader) analysis and backtested trade signals for US stock ETFs, powered by Schwab API data.

## What It Does

- Fetches daily OHLCV candles from the Schwab Market Data API
- Runs ICT structure analysis: BOS/CHoCH detection, order blocks, FVGs, breakers, displacements, volume divergences
- Scores trade setups with a conviction system (0-5) validated against a 3-year backtest
- Produces a daily trading plan across 3 groups with actionable entries, stops, and targets

## Symbol Groups

| Group | Symbols | Edge |
|-------|---------|------|
| **Nasdaq** | QQQ, TQQQ, SQQQ | Primary — backtested, calibrated scoring |
| **Russell** | IWM, TNA, TZA | IWM recalibrated (struct-only, conv>=4, 1.5R, 3d hold) |
| **Bonds** | TLT, TMF, TMV | Context only — no demonstrated ICT edge |

## Quick Start

### 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2) Schwab OAuth (one-time)

Create `.env` with your Schwab credentials:

```
SCHWAB_CLIENT_ID="YOUR_CLIENT_ID"
SCHWAB_CLIENT_SECRET="YOUR_CLIENT_SECRET"
SCHWAB_REDIRECT_URI="https://127.0.0.1"
```

Generate `tokens.json`:

```bash
python scripts/schwab_oauth_init.py
```

### 3) Run Daily Analysis

```bash
python3 scripts/daily_report.py
```

Auto-detects whether data needs fetching:
- If parquets already have the last completed trading day → uses cached data
- If US market is still open → uses cached data (avoids incomplete candles)
- Otherwise → fetches fresh 24-month candles from Schwab API

Flags: `--load` (force cached), `--fetch` (force fresh), `--months N` (lookback)

## Repo Layout

```
scripts/
  daily_report.py        # Full daily report — all 9 ETFs in one shot
  ict_daily.py           # QQQ-only ICT analysis (legacy, still works)
  backtest_ict.py        # ICT backtest engine
  backtest_trailing.py   # Trailing stop variant (options optimization)
  schwab_oauth_init.py   # Schwab OAuth token setup
  backfill_18m.py        # Historical 1-min candle backfill
  update_daily.py        # Incremental daily update
  validate_range.py      # Parquet validation

src/qqq_ingest/
  schwab.py              # Schwab API fetcher (daily + intraday)
  ict.py                 # ICT analysis engine, setup scoring, IWM calibration

data/
  daily_candles.parquet            # QQQ daily candles
  {symbol}_daily_candles.parquet   # TQQQ, SQQQ, IWM, TNA, TZA, TLT, TMF, TMV

reports/                 # Backtest results and analysis reports

.claude/commands/
  daily.md               # /daily slash command for Claude Code
```

## Backtest Results (3-Year, Apr 2023 – Apr 2026)

| Strategy | Trades | Win Rate | Total Return | Profit Factor |
|----------|--------|----------|-------------|---------------|
| TQQQ Aggressive (conv>=3) | 16 | 75.0% | +46.20% | 3.05 |
| SQQQ Conservative (conv>=4) | 7 | 85.7% | +28.04% | 7.18 |
| QQQ Aggressive (conv>=3) | 23 | 69.6% | +14.12% | 2.55 |
| IWM Recalibrated (conv>=4) | 14 | 64.0% | +6.82% | 1.80 |

## ICT Conviction Scoring

Each setup is scored 0-5:

- **+1** BOS-confirmed bias (not CHoCH alone)
- **+1** Bias BOS was volume-confirmed (rvol >= 1.5)
- **+1** Entry bar rvol >= 1.5
- **+1** Structure level retest
- **+1** Confluence >= 2 levels within 1%

IWM uses a different scoring: struct-only entries, confluence >= 3, short direction bonus, no bias vol credit.

## Singapore Timezone Workflow

Designed for traders who can't watch US market close. The system uses bracket orders (limit entry + OCO stop/target) that auto-execute:

1. Run `python3 scripts/daily_report.py` after US close (~5 AM SGT)
2. Place limit orders at structure levels with bracket (stop + target)
3. Orders trigger and exit automatically — 91% of trades exit via bracket

## Do Not Commit

- `.env` — Schwab credentials
- `tokens.json` — OAuth tokens
