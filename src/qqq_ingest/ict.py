"""
ICT (Inner Circle Trader) analysis for daily QQQ candles.

Concepts ported/improved from LuxAlgo "ICT Concepts" and TFO "ICT Killzones & Pivots":

- Market structure: swing highs/lows, BOS, CHoCH (MSS)
- Order blocks: swing-based detection with breaker block tracking
- Fair value gaps (FVGs) with mitigation tracking
- Volume imbalance (body-to-body gaps)
- Balance Price Range (BPR) — overlap of bullish + bearish FVGs
- Liquidity pools: ATR-based clustering of swing points (3+ touches)
- Displacement candles (LuxAlgo body/wick ratio method)
- NWOG (New Week Opening Gap) / NDOG (New Day Opening Gap)
- Previous Day/Week/Month highs, lows, opens
- Volume analysis: relative volume, volume divergence, confirmation on key events
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Market structure – swing highs / lows
# ---------------------------------------------------------------------------

def swing_points(df: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    """
    Tag each bar as a swing high, swing low, both, or neither.

    Uses ta.pivothigh/pivotlow logic: a swing high has a higher high than
    the `lookback` bars on each side. Default 5 matches LuxAlgo's default.

    Adds columns: swing_high (bool), swing_low (bool),
                   swing_high_price (float|NaN), swing_low_price (float|NaN).
    """
    out = df.copy()
    highs = out["high"].values
    lows = out["low"].values
    n = len(out)

    sh = np.zeros(n, dtype=bool)
    sl = np.zeros(n, dtype=bool)

    for i in range(lookback, n - lookback):
        left_h = highs[i - lookback : i]
        right_h = highs[i + 1 : i + lookback + 1]
        if highs[i] > left_h.max() and highs[i] > right_h.max():
            sh[i] = True

        left_l = lows[i - lookback : i]
        right_l = lows[i + 1 : i + lookback + 1]
        if lows[i] < left_l.min() and lows[i] < right_l.min():
            sl[i] = True

    out["swing_high"] = sh
    out["swing_low"] = sl
    out["swing_high_price"] = np.where(sh, highs, np.nan)
    out["swing_low_price"] = np.where(sl, lows, np.nan)
    return out


# ---------------------------------------------------------------------------
# BOS / CHoCH (MSS)
# ---------------------------------------------------------------------------

def market_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Walk through swing points and label BOS (continuation) and CHoCH/MSS (reversal).

    Mirrors LuxAlgo logic: tracks the two most recent swing points (iH/iL indices)
    and checks close breaks. CHoCH = first break reversing bias (called MSS in LuxAlgo).
    BOS = continuation break in same direction.

    Adds columns: structure_event, structure_level (the price level broken).
    """
    out = df.copy()
    n = len(out)
    events = [None] * n
    levels = [np.nan] * n
    bias = None  # 'bullish' or 'bearish'

    last_sh_price = None
    last_sl_price = None

    closes = out["close"].values
    highs = out["high"].values
    lows = out["low"].values
    sw_hi = out["swing_high"].values
    sw_lo = out["swing_low"].values

    for i in range(n):
        if sw_hi[i]:
            last_sh_price = highs[i]
        if sw_lo[i]:
            last_sl_price = lows[i]

        # Break above last swing high
        if last_sh_price is not None and closes[i] > last_sh_price:
            if bias == "bullish":
                events[i] = "bullish_bos"
            else:
                events[i] = "bullish_choch"
            levels[i] = last_sh_price
            bias = "bullish"
            last_sh_price = highs[i]

        # Break below last swing low
        if last_sl_price is not None and closes[i] < last_sl_price:
            if bias == "bearish":
                events[i] = "bearish_bos"
            else:
                events[i] = "bearish_choch"
            levels[i] = last_sl_price
            bias = "bearish"
            last_sl_price = lows[i]

    out["structure_event"] = events
    out["structure_level"] = levels
    return out


# ---------------------------------------------------------------------------
# Order blocks – swing-based (LuxAlgo method)
# ---------------------------------------------------------------------------

@dataclass
class _OBZone:
    top: float
    btm: float
    idx: int
    side: str           # 'bullish' or 'bearish'
    breaker: bool = False
    break_idx: int = -1

def order_blocks(df: pd.DataFrame, swing_lookback: int = 10) -> pd.DataFrame:
    """
    Detect order blocks using the swing-based method from LuxAlgo.

    Bullish OB: when close breaks above a swing high, the lowest candle body
    between the swing low and the break bar becomes the OB zone.
    Bearish OB: when close breaks below a swing low, the highest candle body
    between the swing high and the break bar becomes the OB zone.

    Breaker block: when an OB gets broken through, it flips polarity and
    becomes a breaker (acts as S/R in opposite direction).

    Adds columns: ob_type, ob_top, ob_bottom, breaker_type, breaker_top, breaker_bottom.
    """
    out = df.copy()
    n = len(out)
    closes = out["close"].values
    opens = out["open"].values
    highs = out["high"].values
    lows = out["low"].values
    mx = np.maximum(closes, opens)  # candle body max
    mn = np.minimum(closes, opens)  # candle body min

    # Find swing points with the given lookback
    upper = pd.Series(highs).rolling(swing_lookback, center=False).max().values
    lower = pd.Series(lows).rolling(swing_lookback, center=False).min().values

    # Track top/bottom swing state (like LuxAlgo's swings function)
    top_y, top_x = np.nan, 0
    btm_y, btm_x = np.nan, 0
    top_crossed = False
    btm_crossed = False
    os_state = 0  # 0 = bearish swing, 1 = bullish swing

    bull_obs: list[_OBZone] = []
    bear_obs: list[_OBZone] = []

    ob_type_arr = [None] * n
    ob_top_arr = [np.nan] * n
    ob_bot_arr = [np.nan] * n
    brk_type_arr = [None] * n
    brk_top_arr = [np.nan] * n
    brk_bot_arr = [np.nan] * n

    for i in range(swing_lookback + 1, n):
        prev_os = os_state
        if highs[i - 1] > upper[i - 2] if i >= 2 else False:
            os_state = 0
        if lows[i - 1] < lower[i - 2] if i >= 2 else False:
            os_state = 1

        if os_state == 0 and prev_os != 0:
            top_y = highs[i - 1]
            top_x = i - 1
            top_crossed = False

        if os_state == 1 and prev_os != 1:
            btm_y = lows[i - 1]
            btm_x = i - 1
            btm_crossed = False

        # Bullish OB: close breaks above swing high
        if not np.isnan(top_y) and closes[i] > top_y and not top_crossed:
            top_crossed = True
            # Find the lowest body candle between btm_x and i
            search_start = max(btm_x, 0)
            search_end = i
            if search_end > search_start:
                ob_mn = mn[search_start]
                ob_mx = mx[search_start]
                ob_loc = search_start
                for j in range(search_start, search_end):
                    if mn[j] < ob_mn:
                        ob_mn = mn[j]
                        ob_mx = mx[j]
                        ob_loc = j
                bull_obs.append(_OBZone(top=ob_mx, btm=ob_mn, idx=ob_loc, side="bullish"))
                ob_type_arr[ob_loc] = "bullish_ob"
                ob_top_arr[ob_loc] = ob_mx
                ob_bot_arr[ob_loc] = ob_mn

        # Bearish OB: close breaks below swing low
        if not np.isnan(btm_y) and closes[i] < btm_y and not btm_crossed:
            btm_crossed = True
            search_start = max(top_x, 0)
            search_end = i
            if search_end > search_start:
                ob_mx_val = mx[search_start]
                ob_mn_val = mn[search_start]
                ob_loc = search_start
                for j in range(search_start, search_end):
                    if mx[j] > ob_mx_val:
                        ob_mx_val = mx[j]
                        ob_mn_val = mn[j]
                        ob_loc = j
                bear_obs.append(_OBZone(top=ob_mx_val, btm=ob_mn_val, idx=ob_loc, side="bearish"))
                ob_type_arr[ob_loc] = "bearish_ob"
                ob_top_arr[ob_loc] = ob_mx_val
                ob_bot_arr[ob_loc] = ob_mn_val

        # Check existing OBs for mitigation / breaker conversion
        for ob in bull_obs:
            if not ob.breaker and mn[i] < ob.btm:
                ob.breaker = True
                ob.break_idx = i
                brk_type_arr[i] = "bearish_breaker"
                brk_top_arr[i] = ob.top
                brk_bot_arr[i] = ob.btm

        for ob in bear_obs:
            if not ob.breaker and mx[i] > ob.top:
                ob.breaker = True
                ob.break_idx = i
                brk_type_arr[i] = "bullish_breaker"
                brk_top_arr[i] = ob.top
                brk_bot_arr[i] = ob.btm

    out["ob_type"] = ob_type_arr
    out["ob_top"] = ob_top_arr
    out["ob_bottom"] = ob_bot_arr
    out["breaker_type"] = brk_type_arr
    out["breaker_top"] = brk_top_arr
    out["breaker_bottom"] = brk_bot_arr

    # Deduplicate OBs that land on the same candle with same zone
    seen = set()
    deduped_bull = []
    for ob in bull_obs:
        key = (ob.idx, round(ob.top, 4), round(ob.btm, 4))
        if key not in seen:
            seen.add(key)
            deduped_bull.append(ob)
    deduped_bear = []
    for ob in bear_obs:
        key = (ob.idx, round(ob.top, 4), round(ob.btm, 4))
        if key not in seen:
            seen.add(key)
            deduped_bear.append(ob)

    # Attach live lists for active zone queries
    out.attrs["_bull_obs"] = deduped_bull
    out.attrs["_bear_obs"] = deduped_bear
    return out


# ---------------------------------------------------------------------------
# Fair Value Gaps (FVGs) with mitigation tracking
# ---------------------------------------------------------------------------

@dataclass
class _FVGZone:
    top: float
    btm: float
    idx: int
    side: str       # 'bullish' or 'bearish'
    active: bool = True

def fair_value_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect Fair Value Gaps (3-candle imbalances) with mitigation tracking.

    Bullish FVG: candle[i+1].low > candle[i-1].high  (gap up)
    Bearish FVG: candle[i+1].high < candle[i-1].low  (gap down)

    Uses displacement candle as the middle candle (LuxAlgo method):
    the middle candle must have a body > average body and small wicks
    relative to body (perc_Body = 0.36).

    Adds columns: fvg_type, fvg_top, fvg_bottom, fvg_active.
    """
    out = df.copy()
    n = len(out)
    highs = out["high"].values
    lows = out["low"].values
    closes = out["close"].values
    opens = out["open"].values

    body = np.abs(closes - opens)
    avg_body = pd.Series(body).rolling(10, min_periods=3).mean().values
    mx = np.maximum(closes, opens)
    mn = np.minimum(closes, opens)

    perc_body = 0.36  # LuxAlgo's wick-to-body ratio threshold

    fvg_type = [None] * n
    fvg_top = [np.nan] * n
    fvg_bot = [np.nan] * n
    fvg_active = [False] * n

    fvg_zones: list[_FVGZone] = []

    for i in range(1, n - 1):
        # Check middle candle is a displacement candle
        wick_top = highs[i] - mx[i]
        wick_bot = mn[i] - lows[i]
        is_disp = (body[i] > avg_body[i] if not np.isnan(avg_body[i]) else False) and \
                  wick_top < body[i] * perc_body and \
                  wick_bot < body[i] * perc_body

        # Bullish FVG
        if lows[i + 1] > highs[i - 1] and is_disp and closes[i] > opens[i]:
            fvg_type[i] = "bullish_fvg"
            fvg_top[i] = lows[i + 1]
            fvg_bot[i] = highs[i - 1]
            fvg_active[i] = True
            fvg_zones.append(_FVGZone(top=lows[i + 1], btm=highs[i - 1], idx=i, side="bullish"))

        # Bearish FVG
        elif highs[i + 1] < lows[i - 1] and is_disp and closes[i] < opens[i]:
            fvg_type[i] = "bearish_fvg"
            fvg_top[i] = lows[i - 1]
            fvg_bot[i] = highs[i + 1]
            fvg_active[i] = True
            fvg_zones.append(_FVGZone(top=lows[i - 1], btm=highs[i + 1], idx=i, side="bearish"))

        # Check mitigation of existing FVGs
        for fvg in fvg_zones:
            if not fvg.active:
                continue
            if fvg.side == "bullish" and lows[i] < fvg.btm:
                fvg.active = False
                fvg_active[fvg.idx] = False
            elif fvg.side == "bearish" and highs[i] > fvg.top:
                fvg.active = False
                fvg_active[fvg.idx] = False

    out["fvg_type"] = fvg_type
    out["fvg_top"] = fvg_top
    out["fvg_bottom"] = fvg_bot
    out["fvg_active"] = fvg_active
    out.attrs["_fvg_zones"] = fvg_zones
    return out


# ---------------------------------------------------------------------------
# Volume Imbalance (body-to-body gaps)
# ---------------------------------------------------------------------------

def volume_imbalance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect volume imbalances — gaps between consecutive candle bodies.

    Bullish VI: current open > previous close, bodies don't overlap, bullish context.
    Bearish VI: current open < previous close, bodies don't overlap, bearish context.

    From LuxAlgo: checks open > close[1] and high[1] > low and close > close[1].

    Adds columns: vi_type ('bullish_vi' | 'bearish_vi' | None), vi_top, vi_bottom.
    """
    out = df.copy()
    n = len(out)
    vi_type = [None] * n
    vi_top = [np.nan] * n
    vi_bot = [np.nan] * n

    closes = out["close"].values
    opens = out["open"].values
    highs = out["high"].values
    lows = out["low"].values
    mx = np.maximum(closes, opens)
    mn = np.minimum(closes, opens)

    for i in range(1, n):
        # Bullish VI: gap up between bodies
        if (opens[i] > closes[i - 1] and
                highs[i - 1] > lows[i] and
                closes[i] > closes[i - 1] and
                opens[i] > opens[i - 1] and
                highs[i - 1] < mn[i]):
            vi_type[i] = "bullish_vi"
            vi_top[i] = mn[i]
            vi_bot[i] = mx[i - 1]

        # Bearish VI: gap down between bodies
        elif (opens[i] < closes[i - 1] and
              lows[i - 1] < highs[i] and
              closes[i] < closes[i - 1] and
              opens[i] < opens[i - 1] and
              lows[i - 1] > mx[i]):
            vi_type[i] = "bearish_vi"
            vi_top[i] = mn[i - 1]
            vi_bot[i] = mx[i]

    out["vi_type"] = vi_type
    out["vi_top"] = vi_top
    out["vi_bottom"] = vi_bot
    return out


# ---------------------------------------------------------------------------
# Balance Price Range (BPR) — overlap of bullish + bearish FVGs
# ---------------------------------------------------------------------------

def balance_price_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find Balance Price Ranges: zones where a bullish FVG overlaps with a bearish FVG.

    From LuxAlgo: when the most recent bullish and bearish FVGs overlap, the
    intersection zone is a BPR — a strong equilibrium/S&R area.

    Adds columns: bpr_top, bpr_bottom (NaN if no active BPR).
    """
    out = df.copy()
    n = len(out)
    bpr_top = [np.nan] * n
    bpr_bot = [np.nan] * n

    fvg_zones = out.attrs.get("_fvg_zones", [])
    bull_fvgs = [f for f in fvg_zones if f.side == "bullish"]
    bear_fvgs = [f for f in fvg_zones if f.side == "bearish"]

    # Check each bull/bear pair for overlap
    for bf in bull_fvgs:
        for sf in bear_fvgs:
            # Overlap exists if bull top > bear bottom and bear top > bull bottom
            overlap_top = min(bf.top, sf.top)
            overlap_bot = max(bf.btm, sf.btm)
            if overlap_top > overlap_bot:
                # Place BPR at the index of the later FVG
                bpr_idx = max(bf.idx, sf.idx)
                bpr_top[bpr_idx] = overlap_top
                bpr_bot[bpr_idx] = overlap_bot

    out["bpr_top"] = bpr_top
    out["bpr_bottom"] = bpr_bot
    return out


# ---------------------------------------------------------------------------
# Liquidity pools – ATR-based clustering (LuxAlgo method)
# ---------------------------------------------------------------------------

def liquidity_levels(df: pd.DataFrame, atr_divisor: float = 4.0,
                     min_touches: int = 3) -> pd.DataFrame:
    """
    Find liquidity pools using ATR-based tolerance and requiring 3+ swing touches.

    From LuxAlgo: swing highs/lows within ATR/divisor of each other form
    buyside/sellside liquidity clusters. Broken when price closes through.

    Adds columns: liq_type ('buyside' | 'sellside' | None), liq_level, liq_touches.
    """
    out = df.copy()
    n = len(out)
    atr = pd.Series(out["high"].values - out["low"].values).rolling(10, min_periods=3).mean().values

    liq_type = [None] * n
    liq_level = [np.nan] * n
    liq_touches = [0] * n

    sw_hi = out["swing_high"].values
    sw_lo = out["swing_low"].values
    highs = out["high"].values
    lows = out["low"].values

    # Collect all swing high/low points with their index
    sh_points = [(i, highs[i]) for i in range(n) if sw_hi[i]]
    sl_points = [(i, lows[i]) for i in range(n) if sw_lo[i]]

    # Buyside liquidity: clusters of swing highs
    used_hi = set()
    for a_pos, (a_idx, a_price) in enumerate(sh_points):
        if a_idx in used_hi:
            continue
        margin = atr[a_idx] / atr_divisor if not np.isnan(atr[a_idx]) else 0
        cluster = [(a_idx, a_price)]
        for b_pos in range(a_pos + 1, len(sh_points)):
            b_idx, b_price = sh_points[b_pos]
            if abs(b_price - a_price) <= margin:
                cluster.append((b_idx, b_price))
                used_hi.add(b_idx)
        if len(cluster) >= min_touches:
            avg_price = np.mean([p for _, p in cluster])
            last_idx = max(idx for idx, _ in cluster)
            liq_type[last_idx] = "buyside"
            liq_level[last_idx] = avg_price
            liq_touches[last_idx] = len(cluster)

    # Sellside liquidity: clusters of swing lows
    used_lo = set()
    for a_pos, (a_idx, a_price) in enumerate(sl_points):
        if a_idx in used_lo:
            continue
        margin = atr[a_idx] / atr_divisor if not np.isnan(atr[a_idx]) else 0
        cluster = [(a_idx, a_price)]
        for b_pos in range(a_pos + 1, len(sl_points)):
            b_idx, b_price = sl_points[b_pos]
            if abs(b_price - a_price) <= margin:
                cluster.append((b_idx, b_price))
                used_lo.add(b_idx)
        if len(cluster) >= min_touches:
            avg_price = np.mean([p for _, p in cluster])
            last_idx = max(idx for idx, _ in cluster)
            liq_type[last_idx] = "sellside"
            liq_level[last_idx] = avg_price
            liq_touches[last_idx] = len(cluster)

    out["liq_type"] = liq_type
    out["liq_level"] = liq_level
    out["liq_touches"] = liq_touches
    return out


# ---------------------------------------------------------------------------
# Displacement candles (LuxAlgo method)
# ---------------------------------------------------------------------------

def displacement_candles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag displacement candles using LuxAlgo criteria:
    body > average body AND top/bottom wick < 36% of body size.

    Adds column: displacement ('bullish_disp' | 'bearish_disp' | None).
    """
    out = df.copy()
    closes = out["close"].values
    opens = out["open"].values
    highs = out["high"].values
    lows = out["low"].values

    body = np.abs(closes - opens)
    avg_body = pd.Series(body).rolling(10, min_periods=3).mean().values
    mx = np.maximum(closes, opens)
    mn = np.minimum(closes, opens)

    perc_body = 0.36
    disp = [None] * len(out)

    for i in range(len(out)):
        if np.isnan(avg_body[i]) or body[i] == 0:
            continue
        wick_top = highs[i] - mx[i]
        wick_bot = mn[i] - lows[i]
        if body[i] > avg_body[i] and wick_top < body[i] * perc_body and wick_bot < body[i] * perc_body:
            disp[i] = "bullish_disp" if closes[i] > opens[i] else "bearish_disp"

    out["displacement"] = disp
    return out


# ---------------------------------------------------------------------------
# NWOG / NDOG (New Week/Day Opening Gaps)
# ---------------------------------------------------------------------------

def opening_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect New Week Opening Gaps (NWOG) and New Day Opening Gaps (NDOG).

    NWOG: gap between Friday's close and Monday's open.
    NDOG: gap between previous day's close and current day's open (every day).

    From LuxAlgo: these gaps act as equilibrium zones — price tends to trade
    back to their midpoint (CE = Consequent Encroachment).

    Adds columns: nwog_top, nwog_bottom, nwog_ce, ndog_top, ndog_bottom, ndog_ce.
    """
    out = df.copy()
    n = len(out)
    dates = pd.to_datetime(out["date"])
    opens_ = out["open"].values
    closes = out["close"].values

    nwog_top = [np.nan] * n
    nwog_bot = [np.nan] * n
    nwog_ce = [np.nan] * n
    ndog_top = [np.nan] * n
    ndog_bot = [np.nan] * n
    ndog_ce = [np.nan] * n

    for i in range(1, n):
        prev_close = closes[i - 1]
        cur_open = opens_[i]

        # NDOG: every day
        if prev_close != cur_open:
            top = max(prev_close, cur_open)
            bot = min(prev_close, cur_open)
            ndog_top[i] = top
            ndog_bot[i] = bot
            ndog_ce[i] = (top + bot) / 2

        # NWOG: Monday open vs Friday close
        cur_dow = dates.iloc[i].weekday()   # 0=Mon
        prev_dow = dates.iloc[i - 1].weekday()
        if cur_dow == 0 and prev_dow == 4:
            top = max(prev_close, cur_open)
            bot = min(prev_close, cur_open)
            nwog_top[i] = top
            nwog_bot[i] = bot
            nwog_ce[i] = (top + bot) / 2

    out["nwog_top"] = nwog_top
    out["nwog_bottom"] = nwog_bot
    out["nwog_ce"] = nwog_ce
    out["ndog_top"] = ndog_top
    out["ndog_bottom"] = ndog_bot
    out["ndog_ce"] = ndog_ce
    return out


# ---------------------------------------------------------------------------
# Previous D / W / M levels
# ---------------------------------------------------------------------------

def prev_dwm_levels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute previous day/week/month open, high, low.

    From TFO script: these are key ICT reference levels for bias and targets.

    Adds columns: pdh, pdl, pdo (prev day), pwh, pwl, pwo (prev week),
                   pmh, pml, pmo (prev month).
    """
    out = df.copy()
    dates = pd.to_datetime(out["date"])
    n = len(out)

    # Group bars by day/week/month
    out["_dow"] = dates.dt.weekday
    out["_week"] = dates.dt.isocalendar().week.values
    out["_year"] = dates.dt.year
    out["_month"] = dates.dt.month

    pdh = [np.nan] * n
    pdl = [np.nan] * n
    pdo = [np.nan] * n
    pwh = [np.nan] * n
    pwl = [np.nan] * n
    pwo = [np.nan] * n
    pmh = [np.nan] * n
    pml = [np.nan] * n
    pmo = [np.nan] * n

    opens_ = out["open"].values
    highs = out["high"].values
    lows = out["low"].values

    # Previous day is simply the prior bar on daily data
    for i in range(1, n):
        pdh[i] = highs[i - 1]
        pdl[i] = lows[i - 1]
        pdo[i] = opens_[i - 1]

    # Previous week
    week_groups: dict[tuple, list[int]] = {}
    for i in range(n):
        key = (out["_year"].iloc[i], out["_week"].iloc[i])
        week_groups.setdefault(key, []).append(i)

    sorted_weeks = sorted(week_groups.keys())
    for wi in range(1, len(sorted_weeks)):
        prev_indices = week_groups[sorted_weeks[wi - 1]]
        cur_indices = week_groups[sorted_weeks[wi]]
        pw_h = max(highs[j] for j in prev_indices)
        pw_l = min(lows[j] for j in prev_indices)
        pw_o = opens_[prev_indices[0]]
        for j in cur_indices:
            pwh[j] = pw_h
            pwl[j] = pw_l
            pwo[j] = pw_o

    # Previous month
    month_groups: dict[tuple, list[int]] = {}
    for i in range(n):
        key = (out["_year"].iloc[i], out["_month"].iloc[i])
        month_groups.setdefault(key, []).append(i)

    sorted_months = sorted(month_groups.keys())
    for mi in range(1, len(sorted_months)):
        prev_indices = month_groups[sorted_months[mi - 1]]
        cur_indices = month_groups[sorted_months[mi]]
        pm_h = max(highs[j] for j in prev_indices)
        pm_l = min(lows[j] for j in prev_indices)
        pm_o = opens_[prev_indices[0]]
        for j in cur_indices:
            pmh[j] = pm_h
            pml[j] = pm_l
            pmo[j] = pm_o

    out["pdh"] = pdh
    out["pdl"] = pdl
    out["pdo"] = pdo
    out["pwh"] = pwh
    out["pwl"] = pwl
    out["pwo"] = pwo
    out["pmh"] = pmh
    out["pml"] = pml
    out["pmo"] = pmo

    out.drop(columns=["_dow", "_week", "_year", "_month"], inplace=True)
    return out


# ---------------------------------------------------------------------------
# Volume analysis
# ---------------------------------------------------------------------------

def volume_analysis(df: pd.DataFrame, sma_period: int = 20) -> pd.DataFrame:
    """
    Add volume context to every bar and flag key volume patterns.

    Adds columns:
    - vol_sma: 20-period volume SMA
    - rvol: relative volume (volume / vol_sma). >1.5 = elevated, >2.0 = climactic
    - vol_confirmed: True if a displacement or structure event has rvol >= 1.5
    - vol_divergence: 'bullish_div' | 'bearish_div' | None
        bullish_div = price making new lows but volume declining (exhaustion)
        bearish_div = price making new highs but volume declining (exhaustion)
    """
    out = df.copy()
    n = len(out)
    volumes = out["volume"].values.astype(float)
    closes = out["close"].values
    lows = out["low"].values
    highs = out["high"].values

    vol_sma = pd.Series(volumes).rolling(sma_period, min_periods=5).mean().values
    rvol = np.where(vol_sma > 0, volumes / vol_sma, np.nan)

    out["vol_sma"] = vol_sma
    out["rvol"] = np.round(rvol, 2)

    # Volume confirmation: flag displacement and structure events with high rvol
    vol_confirmed = [False] * n
    for i in range(n):
        has_event = (out["displacement"].iloc[i] is not None or
                     out["structure_event"].iloc[i] is not None)
        if has_event and not np.isnan(rvol[i]) and rvol[i] >= 1.5:
            vol_confirmed[i] = True
    out["vol_confirmed"] = vol_confirmed

    # Volume divergence: compare recent swing lows/highs with their volume
    vol_div = [None] * n
    sw_hi = out["swing_high"].values
    sw_lo = out["swing_low"].values

    # Track last two swing lows for bullish divergence
    recent_sl = []  # list of (idx, low_price, volume)
    recent_sh = []  # list of (idx, high_price, volume)

    for i in range(n):
        if sw_lo[i]:
            recent_sl.append((i, lows[i], volumes[i]))
            if len(recent_sl) >= 2:
                prev = recent_sl[-2]
                curr = recent_sl[-1]
                # Price lower low but volume declining = bullish divergence (seller exhaustion)
                if curr[1] < prev[1] and curr[2] < prev[2]:
                    vol_div[i] = "bullish_div"

        if sw_hi[i]:
            recent_sh.append((i, highs[i], volumes[i]))
            if len(recent_sh) >= 2:
                prev = recent_sh[-2]
                curr = recent_sh[-1]
                # Price higher high but volume declining = bearish divergence (buyer exhaustion)
                if curr[1] > prev[1] and curr[2] < prev[2]:
                    vol_div[i] = "bearish_div"

    out["vol_divergence"] = vol_div
    return out


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_ict_analysis(df: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    """
    Run the full ICT analysis pipeline on a daily OHLCV DataFrame.

    Expects columns: date, open, high, low, close, volume.
    Returns the DataFrame with all ICT columns appended.
    """
    out = df.copy().reset_index(drop=True)
    out = swing_points(out, lookback=lookback)
    out = market_structure(out)
    out = order_blocks(out, swing_lookback=lookback * 2)
    out = fair_value_gaps(out)
    out = balance_price_range(out)
    out = volume_imbalance(out)
    out = liquidity_levels(out)
    out = displacement_candles(out)
    out = opening_gaps(out)
    out = prev_dwm_levels(out)
    out = volume_analysis(out)
    return out


# ---------------------------------------------------------------------------
# Active zones query helpers
# ---------------------------------------------------------------------------

def active_order_blocks(df: pd.DataFrame) -> pd.DataFrame:
    """Return unmitigated OBs and active breaker blocks with distance from current price."""
    current = df["close"].iloc[-1]
    bull_obs = df.attrs.get("_bull_obs", [])
    bear_obs = df.attrs.get("_bear_obs", [])
    rows = []

    for ob in bull_obs:
        mid = (ob.top + ob.btm) / 2
        dist = (mid - current) / current * 100
        status = "breaker" if ob.breaker else "active"
        rows.append({
            "date": df["date"].iloc[ob.idx] if ob.idx < len(df) else None,
            "type": f"bullish_ob ({status})",
            "top": ob.top, "bottom": ob.btm,
            "distance_%": round(dist, 2),
        })

    for ob in bear_obs:
        mid = (ob.top + ob.btm) / 2
        dist = (mid - current) / current * 100
        status = "breaker" if ob.breaker else "active"
        rows.append({
            "date": df["date"].iloc[ob.idx] if ob.idx < len(df) else None,
            "type": f"bearish_ob ({status})",
            "top": ob.top, "bottom": ob.btm,
            "distance_%": round(dist, 2),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "type", "top", "bottom", "distance_%"])


def active_fvgs(df: pd.DataFrame) -> pd.DataFrame:
    """Return unfilled FVGs with distance from current price."""
    current = df["close"].iloc[-1]
    fvg_zones = df.attrs.get("_fvg_zones", [])
    rows = []

    for fvg in fvg_zones:
        if not fvg.active:
            continue
        mid = (fvg.top + fvg.btm) / 2
        dist = (mid - current) / current * 100
        rows.append({
            "date": df["date"].iloc[fvg.idx] if fvg.idx < len(df) else None,
            "type": f"{fvg.side}_fvg",
            "top": fvg.top, "bottom": fvg.btm,
            "distance_%": round(dist, 2),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "type", "top", "bottom", "distance_%"])


# ---------------------------------------------------------------------------
# Signal summary
# ---------------------------------------------------------------------------

def signals(df: pd.DataFrame, last_n: int = 10) -> pd.DataFrame:
    """Summarise actionable ICT setups from the most recent `last_n` bars."""
    recent = df.tail(last_n).copy()
    rows = []

    for _, r in recent.iterrows():
        sigs = []
        rvol = r.get("rvol", np.nan)
        rvol_tag = f" rvol={rvol:.1f}x" if not np.isnan(rvol) else ""

        if r.get("structure_event"):
            confirmed = " [VOL CONFIRMED]" if r.get("vol_confirmed") else ""
            sigs.append(f"{r['structure_event']}{rvol_tag}{confirmed}")
        if r.get("ob_type"):
            sigs.append(f"{r['ob_type']} [{r['ob_bottom']:.2f}-{r['ob_top']:.2f}]")
        if r.get("breaker_type"):
            sigs.append(f"{r['breaker_type']} [{r['breaker_bottom']:.2f}-{r['breaker_top']:.2f}]")
        if r.get("fvg_type"):
            active = " (active)" if r.get("fvg_active") else " (filled)"
            sigs.append(f"{r['fvg_type']}{active} [{r['fvg_bottom']:.2f}-{r['fvg_top']:.2f}]")
        if r.get("vi_type"):
            sigs.append(f"{r['vi_type']} [{r['vi_bottom']:.2f}-{r['vi_top']:.2f}]")
        if r.get("displacement"):
            confirmed = " [VOL CONFIRMED]" if r.get("vol_confirmed") else ""
            sigs.append(f"{r['displacement']}{rvol_tag}{confirmed}")
        if r.get("vol_divergence"):
            sigs.append(r["vol_divergence"])
        if r.get("liq_type"):
            sigs.append(f"{r['liq_type']} liq @ {r['liq_level']:.2f} ({r['liq_touches']}x)")
        if not np.isnan(r.get("nwog_top", np.nan)):
            sigs.append(f"NWOG [{r['nwog_bottom']:.2f}-{r['nwog_top']:.2f}] CE={r['nwog_ce']:.2f}")
        if not np.isnan(r.get("bpr_top", np.nan)):
            sigs.append(f"BPR [{r['bpr_bottom']:.2f}-{r['bpr_top']:.2f}]")

        if sigs:
            rows.append({"date": r["date"], "close": r["close"],
                         "signals": " | ".join(sigs)})

    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "close", "signals"])


# ---------------------------------------------------------------------------
# Trade setup quality — bakes in backtest learnings
# ---------------------------------------------------------------------------

@dataclass
class TradeSetup:
    """A scored trade idea based on backtest-validated filters."""
    direction: str          # "long" or "short"
    entry_zone: str         # description of the zone
    entry_top: float
    entry_btm: float
    stop: float
    target: float
    risk_pct: float
    reward_pct: float
    conviction: int         # 0-5 score
    reasons: list           # list of why this setup scores
    warnings: list          # reasons to be cautious
    max_hold_days: int = 5  # backtest-validated: exit by day 5


def evaluate_setups(df: pd.DataFrame, max_dist_pct: float = 5.0) -> list[TradeSetup]:
    """
    Evaluate current price against all available zones and return scored setups.

    Backtest-validated rules (500 days, 22 selective trades, 73% WR, 2.87 PF):
      1. Only trade with BOS-confirmed bias (not CHoCH alone — 22% 5d accuracy)
      2. Structure retests are the primary entry (73.7% WR)
      3. No FVG-only entries (14.3% WR historically)
      4. Volume divergence against trade = skip
      5. Confluence of 3+ levels = 80% WR (sweet spot)
      6. Winners resolve in 1-2 days; 5-day max hold
      7. Entry bar volume confirmation (rvol >= 1.5) = 80% WR when present
    """
    n = len(df)
    if n < 60:
        return []

    price = df["close"].iloc[-1]
    last = df.iloc[-1]

    # --- Determine bias quality ---
    struct_events = df[df["structure_event"].notna()]
    if struct_events.empty:
        return []

    latest_evt = struct_events.iloc[-1]
    event = latest_evt["structure_event"]
    bias = "bullish" if "bullish" in event else "bearish"
    bias_is_bos = "bos" in event

    # Check if BOS has confirmed after a CHoCH
    if not bias_is_bos:
        # Look for a subsequent BOS in the same direction
        for idx in range(len(struct_events) - 2, max(0, len(struct_events) - 6), -1):
            evt = struct_events.iloc[idx]["structure_event"]
            if "bos" in evt and (("bullish" in evt) == (bias == "bullish")):
                bias_is_bos = True
                break

    # Bias volume confirmation: was the BOS that set the bias vol confirmed?
    bias_vol = bool(latest_evt.get("vol_confirmed", False))

    # Current bar volume state
    cur_rvol = last.get("rvol", 0.0)
    if np.isnan(cur_rvol):
        cur_rvol = 0.0
    cur_vol_div = last.get("vol_divergence", None)

    # ATR for stop calculation
    atr_series = (df["high"] - df["low"]).rolling(14, min_periods=5).mean()
    cur_atr = atr_series.iloc[-1]
    if np.isnan(cur_atr):
        cur_atr = df["high"].iloc[-1] - df["low"].iloc[-1]

    # --- Gather all nearby zones ---
    obs = active_order_blocks(df)
    fvgs = active_fvgs(df)

    # Structure levels from recent events
    struct_levels = []
    for _, r in struct_events.tail(10).iterrows():
        struct_levels.append(r["structure_level"])

    setups: list[TradeSetup] = []

    if bias == "bearish":
        setups.extend(_evaluate_bear_setups(
            df, price, cur_atr, obs, fvgs, struct_levels,
            bias_is_bos, bias_vol, cur_rvol, cur_vol_div, max_dist_pct))
    else:
        setups.extend(_evaluate_bull_setups(
            df, price, cur_atr, obs, fvgs, struct_levels,
            bias_is_bos, bias_vol, cur_rvol, cur_vol_div, max_dist_pct))

    # Sort by conviction descending
    setups.sort(key=lambda s: -s.conviction)
    return setups


def _evaluate_bear_setups(df, price, atr, obs, fvgs, struct_levels,
                           bias_is_bos, bias_vol, cur_rvol, cur_vol_div, max_dist):
    last = df.iloc[-1]
    setups = []

    # Candidate resistance zones above price (within max_dist%)
    candidates = []

    # Structure levels
    for level in struct_levels:
        dist = (level - price) / price * 100
        if 0 < dist <= max_dist:
            candidates.append(("struct", level, level, level, dist))
        elif -0.5 <= dist <= 0:
            candidates.append(("struct", level, level, level, dist))

    # PDH
    pdh = last.get("pdh", np.nan)
    if not np.isnan(pdh):
        dist = (pdh - price) / price * 100
        if -0.5 <= dist <= max_dist:
            candidates.append(("pdh", pdh, pdh, pdh, dist))

    # PWL (broken support = resistance)
    pwl = last.get("pwl", np.nan)
    if not np.isnan(pwl):
        dist = (pwl - price) / price * 100
        if -0.5 <= dist <= max_dist:
            candidates.append(("pwl", pwl, pwl, pwl, dist))

    # Bearish OBs above
    bear_obs = obs[(obs["type"].str.contains("bearish")) &
                   (~obs["type"].str.contains("breaker"))]
    for _, r in bear_obs.iterrows():
        dist = r["distance_%"]
        if -0.5 <= dist <= max_dist:
            candidates.append(("ob", r["top"], r["bottom"], r["top"], dist))

    # Group candidates by proximity (within 1% of each other) to find confluence
    if not candidates:
        return []

    # Score each candidate zone
    for ctype, top, btm, level, dist in candidates:
        reasons = []
        warnings = []
        confluence = 0

        # Count how many other zones are within 1% of this one
        for otype, otop, obtm, olevel, odist in candidates:
            if abs(level - olevel) / price < 0.01:
                confluence += 1

        # --- Conviction scoring ---
        conviction = 0

        # 1. BOS-confirmed bias (+1)
        if bias_is_bos:
            conviction += 1
            reasons.append("BOS-confirmed bias")
        else:
            warnings.append("CHoCH-only bias (22% 5d accuracy — wait for BOS)")

        # 2. Structure retest entry (+1)
        if ctype == "struct":
            conviction += 1
            reasons.append("Structure level retest (73.7% WR)")
        elif ctype in ("pdh", "pwl"):
            conviction += 1
            reasons.append(f"{ctype.upper()} level")
        elif ctype == "ob":
            reasons.append("Order block zone")
        # FVG-only would score 0 here — but we don't add FVG candidates for shorts

        # 3. Volume on bias event (+1)
        if bias_vol:
            conviction += 1
            reasons.append("Bias BOS was volume-confirmed")

        # 4. Entry bar rvol (+1)
        if cur_rvol >= 1.5:
            conviction += 1
            reasons.append(f"Current rvol={cur_rvol:.1f}x (elevated)")

        # 5. Confluence (+1 if 3+)
        if confluence >= 3:
            conviction += 1
            reasons.append(f"{confluence} levels stacking (80% WR at 3+)")
        elif confluence >= 2:
            reasons.append(f"{confluence} levels stacking")

        # --- Warnings ---
        if cur_vol_div == "bullish_div":
            warnings.append("Bullish volume divergence — seller exhaustion risk")
            conviction = max(0, conviction - 1)

        if not bias_is_bos:
            conviction = max(0, conviction - 1)

        # --- Calculate stop/target ---
        stop = top + atr * 0.5
        risk = stop - price if dist <= 0 else stop - level  # entry at zone
        entry_price = price if dist <= 0 else level
        target = entry_price - risk * 2.0  # 2R reward

        if risk <= 0:
            continue

        risk_pct = risk / entry_price * 100
        reward_pct = risk_pct * 2.0

        setups.append(TradeSetup(
            direction="short",
            entry_zone=f"{ctype.upper()} @ {btm:.2f}-{top:.2f}" if btm != top else f"{ctype.upper()} @ {level:.2f}",
            entry_top=top, entry_btm=btm,
            stop=round(stop, 2),
            target=round(target, 2),
            risk_pct=round(risk_pct, 2),
            reward_pct=round(reward_pct, 2),
            conviction=conviction,
            reasons=reasons,
            warnings=warnings,
        ))

    # Deduplicate: keep the highest-conviction setup per unique level
    seen = {}
    for s in setups:
        key = round(s.entry_top, 2)
        if key not in seen or s.conviction > seen[key].conviction:
            seen[key] = s
    return list(seen.values())


def _evaluate_bull_setups(df, price, atr, obs, fvgs, struct_levels,
                           bias_is_bos, bias_vol, cur_rvol, cur_vol_div, max_dist):
    last = df.iloc[-1]
    setups = []

    candidates = []

    for level in struct_levels:
        dist = (level - price) / price * 100
        if -max_dist <= dist < 0:
            candidates.append(("struct", level, level, level, dist))
        elif 0 <= dist <= 0.5:
            candidates.append(("struct", level, level, level, dist))

    pdl = last.get("pdl", np.nan)
    if not np.isnan(pdl):
        dist = (pdl - price) / price * 100
        if -max_dist <= dist <= 0.5:
            candidates.append(("pdl", pdl, pdl, pdl, dist))

    pwh = last.get("pwh", np.nan)
    if not np.isnan(pwh):
        dist = (pwh - price) / price * 100
        if -max_dist <= dist <= 0.5:
            candidates.append(("pwh", pwh, pwh, pwh, dist))

    bull_obs = obs[(obs["type"].str.contains("bullish")) &
                   (~obs["type"].str.contains("breaker"))]
    for _, r in bull_obs.iterrows():
        dist = r["distance_%"]
        if -max_dist <= dist <= 0.5:
            candidates.append(("ob", r["top"], r["bottom"], r["bottom"], dist))

    if not candidates:
        return []

    for ctype, top, btm, level, dist in candidates:
        reasons = []
        warnings = []
        confluence = 0

        for otype, otop, obtm, olevel, odist in candidates:
            if abs(level - olevel) / price < 0.01:
                confluence += 1

        conviction = 0

        if bias_is_bos:
            conviction += 1
            reasons.append("BOS-confirmed bias")
        else:
            warnings.append("CHoCH-only bias (22% 5d accuracy — wait for BOS)")

        if ctype == "struct":
            conviction += 1
            reasons.append("Structure level retest (73.7% WR)")
        elif ctype in ("pdl", "pwh"):
            conviction += 1
            reasons.append(f"{ctype.upper()} level")
        elif ctype == "ob":
            reasons.append("Order block zone")

        if bias_vol:
            conviction += 1
            reasons.append("Bias BOS was volume-confirmed")

        if cur_rvol >= 1.5:
            conviction += 1
            reasons.append(f"Current rvol={cur_rvol:.1f}x (elevated)")

        if confluence >= 3:
            conviction += 1
            reasons.append(f"{confluence} levels stacking (80% WR at 3+)")
        elif confluence >= 2:
            reasons.append(f"{confluence} levels stacking")

        if cur_vol_div == "bearish_div":
            warnings.append("Bearish volume divergence — buyer exhaustion risk")
            conviction = max(0, conviction - 1)

        if not bias_is_bos:
            conviction = max(0, conviction - 1)

        stop = btm - atr * 0.5
        entry_price = price if dist >= 0 else level
        risk = entry_price - stop
        target = entry_price + risk * 2.0

        if risk <= 0:
            continue

        risk_pct = risk / entry_price * 100
        reward_pct = risk_pct * 2.0

        setups.append(TradeSetup(
            direction="long",
            entry_zone=f"{ctype.upper()} @ {btm:.2f}-{top:.2f}" if btm != top else f"{ctype.upper()} @ {level:.2f}",
            entry_top=top, entry_btm=btm,
            stop=round(stop, 2),
            target=round(target, 2),
            risk_pct=round(risk_pct, 2),
            reward_pct=round(reward_pct, 2),
            conviction=conviction,
            reasons=reasons,
            warnings=warnings,
        ))

    return setups


# ---------------------------------------------------------------------------
# IWM-specific setup evaluation — recalibrated from 500-day IWM backtest
# ---------------------------------------------------------------------------

def evaluate_setups_iwm(df: pd.DataFrame, max_dist_pct: float = 5.0) -> list[TradeSetup]:
    """
    IWM-specific setup evaluation with recalibrated scoring.

    IWM backtest findings (500 days, struct + conv>=4):
      - 14 trades, 64% WR, PF 1.80 (same-day, hold<=3d, 1.5R target)
      - Struct-only entries: 50% WR overall, but 70% at conv>=4
      - PDL/PDH/FVG entries: all negative — hard skip
      - Entry rvol >= 1.5: 100% WR (3/3) — strongest single factor
      - Bias vol confirmed: marginal (53% vs 52%) — not scored
      - Confluence >= 3: helps (57% WR), >= 2 doesn't (50%)
      - Winners resolve in 1d median; 3-day max hold optimal
      - Same-day entry: 64% WR vs next-day 45%

    IWM scoring (differs from QQQ):
      +1 BOS-confirmed bias
      +1 Structure level retest (hard filter — no other entry types)
      +1 Entry bar rvol >= 1.5 (100% WR when present)
      +1 Confluence >= 3 levels within 1% (raised from 2)
      +1 Short direction during bearish bias (shorts 75% WR at conv>=4)
      Penalty: -1 CHoCH-only bias, -1 vol divergence against trade
    """
    n = len(df)
    if n < 60:
        return []

    price = df["close"].iloc[-1]
    last = df.iloc[-1]

    struct_events = df[df["structure_event"].notna()]
    if struct_events.empty:
        return []

    latest_evt = struct_events.iloc[-1]
    event = latest_evt["structure_event"]
    bias = "bullish" if "bullish" in event else "bearish"
    bias_is_bos = "bos" in event

    if not bias_is_bos:
        for idx in range(len(struct_events) - 2, max(0, len(struct_events) - 6), -1):
            evt = struct_events.iloc[idx]["structure_event"]
            if "bos" in evt and (("bullish" in evt) == (bias == "bullish")):
                bias_is_bos = True
                break

    cur_rvol = last.get("rvol", 0.0)
    if np.isnan(cur_rvol):
        cur_rvol = 0.0
    cur_vol_div = last.get("vol_divergence", None)

    atr_series = (df["high"] - df["low"]).rolling(14, min_periods=5).mean()
    cur_atr = atr_series.iloc[-1]
    if np.isnan(cur_atr):
        cur_atr = df["high"].iloc[-1] - df["low"].iloc[-1]

    struct_levels = []
    for _, r in struct_events.tail(10).iterrows():
        struct_levels.append(r["structure_level"])

    pdh = last.get("pdh", np.nan)
    pdl = last.get("pdl", np.nan)
    pwh = last.get("pwh", np.nan)
    pwl = last.get("pwl", np.nan)

    setups: list[TradeSetup] = []

    for level in struct_levels:
        if bias == "bearish":
            dist = (level - price) / price * 100
            if not (-0.5 <= dist <= max_dist_pct):
                continue
            setup = _score_iwm_short(
                price, level, cur_atr, struct_levels,
                pdh, pdl, pwh, pwl,
                bias_is_bos, cur_rvol, cur_vol_div)
        else:
            dist = (level - price) / price * 100
            if not (-max_dist_pct <= dist <= 0.5):
                continue
            setup = _score_iwm_long(
                price, level, cur_atr, struct_levels,
                pdh, pdl, pwh, pwl,
                bias_is_bos, cur_rvol, cur_vol_div)

        if setup is not None:
            setups.append(setup)

    # Deduplicate by level
    seen = {}
    for s in setups:
        key = round(s.entry_top, 2)
        if key not in seen or s.conviction > seen[key].conviction:
            seen[key] = s
    setups = list(seen.values())
    setups.sort(key=lambda s: -s.conviction)
    return setups


def _iwm_confluence(price, level, struct_levels, pdh, pdl, pwh, pwl):
    """Count levels stacking within 1% of the target level."""
    count = 0
    for sl in struct_levels:
        if abs(sl - level) / price < 0.01:
            count += 1
    for ref in [pdh, pdl, pwh, pwl]:
        if ref is not None and not np.isnan(ref):
            if abs(ref - level) / price < 0.01:
                count += 1
    return count


def _score_iwm_short(price, level, atr, struct_levels,
                      pdh, pdl, pwh, pwl,
                      bias_is_bos, cur_rvol, cur_vol_div):
    reasons = []
    warnings = []
    conviction = 0

    confluence = _iwm_confluence(price, level, struct_levels, pdh, pdl, pwh, pwl)

    if bias_is_bos:
        conviction += 1
        reasons.append("BOS-confirmed bias")
    else:
        warnings.append("CHoCH-only bias (IWM BOS 5d accuracy 51%)")

    conviction += 1
    reasons.append("Structure level retest (IWM: 70% WR at conv>=4)")

    if cur_rvol >= 1.5:
        conviction += 1
        reasons.append(f"Entry rvol={cur_rvol:.1f}x (IWM: 100% WR)")

    if confluence >= 3:
        conviction += 1
        reasons.append(f"{confluence} levels stacking (IWM: 57% WR at 3+)")
    elif confluence >= 2:
        reasons.append(f"{confluence} levels nearby")

    # Shorts get directional bonus in bearish bias (75% WR at conv>=4)
    conviction += 1
    reasons.append("Short w/ bearish bias (IWM: 75% WR at conv>=4)")

    if cur_vol_div == "bullish_div":
        warnings.append("Bullish volume divergence — seller exhaustion risk")
        conviction = max(0, conviction - 1)
    if not bias_is_bos:
        conviction = max(0, conviction - 1)

    dist = (level - price) / price * 100
    stop = level + atr * 0.5
    entry_price = price if dist <= 0 else level
    risk = stop - entry_price
    if risk <= 0:
        return None
    target = entry_price - risk * 1.5

    return TradeSetup(
        direction="short",
        entry_zone=f"STRUCT @ {level:.2f}",
        entry_top=level, entry_btm=level,
        stop=round(stop, 2), target=round(target, 2),
        risk_pct=round(risk / entry_price * 100, 2),
        reward_pct=round(risk / entry_price * 100 * 1.5, 2),
        conviction=conviction, reasons=reasons, warnings=warnings,
        max_hold_days=3,
    )


def _score_iwm_long(price, level, atr, struct_levels,
                     pdh, pdl, pwh, pwl,
                     bias_is_bos, cur_rvol, cur_vol_div):
    reasons = []
    warnings = []
    conviction = 0

    confluence = _iwm_confluence(price, level, struct_levels, pdh, pdl, pwh, pwl)

    if bias_is_bos:
        conviction += 1
        reasons.append("BOS-confirmed bias")
    else:
        warnings.append("CHoCH-only bias (IWM BOS 5d accuracy 51%)")

    conviction += 1
    reasons.append("Structure level retest (IWM: 70% WR at conv>=4)")

    if cur_rvol >= 1.5:
        conviction += 1
        reasons.append(f"Entry rvol={cur_rvol:.1f}x (IWM: 100% WR)")

    if confluence >= 3:
        conviction += 1
        reasons.append(f"{confluence} levels stacking (IWM: 57% WR at 3+)")
    elif confluence >= 2:
        reasons.append(f"{confluence} levels nearby")

    # Longs don't get directional bonus (shorts stronger for IWM)

    if cur_vol_div == "bearish_div":
        warnings.append("Bearish volume divergence — buyer exhaustion risk")
        conviction = max(0, conviction - 1)
    if not bias_is_bos:
        conviction = max(0, conviction - 1)

    dist = (level - price) / price * 100
    stop = level - atr * 0.5
    entry_price = price if dist >= 0 else level
    risk = entry_price - stop
    if risk <= 0:
        return None
    target = entry_price + risk * 1.5

    return TradeSetup(
        direction="long",
        entry_zone=f"STRUCT @ {level:.2f}",
        entry_top=level, entry_btm=level,
        stop=round(stop, 2), target=round(target, 2),
        risk_pct=round(risk / entry_price * 100, 2),
        reward_pct=round(risk / entry_price * 100 * 1.5, 2),
        conviction=conviction, reasons=reasons, warnings=warnings,
        max_hold_days=3,
    )
