from __future__ import annotations
from pathlib import Path
import pandas as pd

def ensure_dir(p: str | Path) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def write_by_date(df: pd.DataFrame, root: str) -> None:
    """
    Writes one parquet per date_et:
      data/candles/date_et=YYYY-MM-DD/part.parquet
    """
    ensure_dir(root)
    rootp = Path(root)
    for d, g in df.groupby("date_et", sort=True):
        outdir = rootp / f"date_et={d}"
        ensure_dir(outdir)
        (g.sort_values("ts_utc")
          .drop_duplicates(subset=["ts_utc"], keep="last")
          .to_parquet(outdir / "part.parquet", index=False, engine="pyarrow"))
