"""
Fetch daily QQQ candles from Schwab and save to data/daily_candles.parquet.

Usage:
    python scripts/fetch_daily.py              # default 24 months
    python scripts/fetch_daily.py --months 6   # custom lookback
"""
import sys, os, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from qqq_ingest.schwab import SchwabDailyFetcher

TZ_ET = pytz.timezone("US/Eastern")


def main():
    parser = argparse.ArgumentParser(description="Fetch daily QQQ candles")
    parser.add_argument("--months", type=int, default=24, help="Lookback months (default: 24)")
    args = parser.parse_args()

    end = datetime.now(TZ_ET)
    start = end - relativedelta(months=args.months)

    print(f"Fetching daily QQQ candles: {start.date()} -> {end.date()}")

    fetcher = SchwabDailyFetcher()
    df = fetcher.fetch("QQQ", start, end)

    if df.empty:
        print("No data returned. Check tokens.json / Schwab credentials.")
        return

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "daily_candles.parquet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False, engine="pyarrow")
    print(f"Wrote {len(df)} daily candles -> {out_path}")


if __name__ == "__main__":
    main()
