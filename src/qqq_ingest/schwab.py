from __future__ import annotations
import os, time, json, base64
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
PRICEHISTORY_URL = "https://api.schwabapi.com/marketdata/v1/pricehistory"
TOKENS_PATH = Path("tokens.json")

def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("utf-8")

def _load_tokens() -> dict:
    if not TOKENS_PATH.exists():
        raise RuntimeError("tokens.json not found. Run scripts/schwab_oauth_init.py first.")
    return json.loads(TOKENS_PATH.read_text())

def _save_tokens(tok: dict) -> None:
    TOKENS_PATH.write_text(json.dumps(tok, indent=2, sort_keys=True))

def _access_token_valid(tok: dict, skew_sec: int = 60) -> bool:
    obtained_at = tok.get("obtained_at")
    expires_in = tok.get("expires_in")
    if not obtained_at or not expires_in:
        return False
    return time.time() < (int(obtained_at) + int(expires_in) - skew_sec)

def _refresh_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    tok = r.json()
    tok["obtained_at"] = int(time.time())
    return tok

def get_access_token() -> str:
    client_id = os.environ["SCHWAB_CLIENT_ID"].strip()
    client_secret = os.environ["SCHWAB_CLIENT_SECRET"].strip()

    tok = _load_tokens()
    if _access_token_valid(tok):
        return tok["access_token"]

    if not tok.get("refresh_token"):
        raise RuntimeError("No refresh_token in tokens.json; rerun oauth init.")
    new_tok = _refresh_token(client_id, client_secret, tok["refresh_token"])
    _save_tokens(new_tok)
    return new_tok["access_token"]

def _epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("start/end must be tz-aware UTC datetimes")
    return int(dt.timestamp() * 1000)

@dataclass
class SchwabPriceHistoryFetcher:
    need_extended_hours: bool = True

    def fetch(self, symbol: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
        token = get_access_token()
        params = {
            "symbol": symbol,
            "frequencyType": "minute",
            "frequency": 1,
            "needExtendedHoursData": "true" if self.need_extended_hours else "false",
            "startDate": str(_epoch_ms(start_utc)),
            "endDate": str(_epoch_ms(end_utc)),
        }
        r = requests.get(
            PRICEHISTORY_URL,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        j = r.json()
        candles = j.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["ts_utc","open","high","low","close","volume"])

        df = pd.DataFrame(candles)
        # common field name is "datetime" in ms
        df["ts_utc"] = pd.to_datetime(df["datetime"], unit="ms", utc=True)
        df = df.rename(columns={"volume": "volume"})  # keep explicit
        return df[["ts_utc","open","high","low","close","volume"]].copy()


@dataclass
class SchwabDailyFetcher:
    """Fetch daily candles from Schwab Price History API.

    Chunks requests into 1-year windows because Schwab rejects large ranges.
    """
    chunk_days: int = 365

    def _fetch_chunk(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        token = get_access_token()
        params = {
            "symbol": symbol,
            "periodType": "year",
            "frequencyType": "daily",
            "frequency": 1,
            "startDate": str(_epoch_ms(start)),
            "endDate": str(_epoch_ms(end)),
        }
        r = requests.get(
            PRICEHISTORY_URL,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  Schwab API error {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        j = r.json()
        candles = j.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])

        df = pd.DataFrame(candles)
        df["date"] = pd.to_datetime(df["datetime"], unit="ms", utc=True).dt.tz_convert("US/Eastern").dt.date
        return df[["date","open","high","low","close","volume"]].copy()

    def fetch(self, symbol: str, start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
        from datetime import timedelta
        frames = []
        cur = start_utc
        while cur < end_utc:
            nxt = min(cur + timedelta(days=self.chunk_days), end_utc)
            print(f"  Chunk: {cur.date()} -> {nxt.date()}")
            chunk = self._fetch_chunk(symbol, cur, nxt)
            if not chunk.empty:
                frames.append(chunk)
            cur = nxt
        if not frames:
            return pd.DataFrame(columns=["date","open","high","low","close","volume"])
        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df
