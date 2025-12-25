import os
import pandas as pd
from data.akshare_loader import load_ashare_daily
from data.calendar.trade_calendar import get_trade_days

CACHE_DIR = "data_cache"


def load_daily_df_with_cache(
    code: str,
    start="2019-01-01",
    end=None,
):
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{code}.csv")

    # === 1. 读取缓存 ===
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    else:
        df = pd.DataFrame()

    # === 2. 判断是否需要补数据 ===
    need_fetch = df.empty
    fetch_start = start

    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")

    if need_fetch or fetch_start <= end:
        new_df = load_ashare_daily(code, fetch_start, end)
        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated()].sort_index()
        df.to_csv(cache_file)

    # === 3. 交易日对齐 ===
    trade_days = get_trade_days(start, end)
    df = df.reindex(trade_days).ffill()

    # === 4. ⭐ 补充元信息（非常关键） ===
    df.attrs["code"] = code

    return df
