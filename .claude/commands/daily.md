Run the daily ICT analysis by executing:

```
python3 scripts/daily_report.py
```

The script auto-detects whether data needs fetching:
- If parquet already has the last completed trading day's candle → uses cached data
- If market is still open (before 4 PM ET) → uses cached data (avoids incomplete candles)
- Otherwise → fetches fresh 24-month data from Schwab API

Override flags: `--load` (force cached), `--fetch` (force fresh)

After the script output, add:
- Upcoming catalysts with dates (FOMC, CPI, earnings, geopolitical deadlines within the next 10 trading days)
- If within 5 trading days of FOMC: note "FOMC exclusion window — no new entries"
- Note if market is still open (incomplete candle warning)
