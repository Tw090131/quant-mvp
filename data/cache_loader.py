import os
import pandas as pd
from data.akshare_loader import load_ashare_daily, load_ashare_minute

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def load_daily_df_with_cache(code: str, start="2019-01-01", end=None):
    # 日线缓存逻辑
    cache_file = os.path.join(CACHE_DIR, f"{code}.csv")
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    else:
        df = pd.DataFrame()
    fetch_start = start
    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    if df.empty or fetch_start <= end:
        new_df = load_ashare_daily(code, fetch_start, end)
        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated()].sort_index()
        df.to_csv(cache_file)
    return df

def load_minute_df_with_cache(code: str, period="1min", start="2025-01-01", end=None):
    # 分钟线缓存逻辑
    filename = f"{code}_{period}.csv"
    cache_file = os.path.join(CACHE_DIR, filename)
    if os.path.exists(cache_file):
        df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
    else:
        df = pd.DataFrame()
    fetch_start = start
    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    if df.empty or fetch_start <= end:
        new_df = load_ashare_minute(code, period, fetch_start, end)
        df = pd.concat([df, new_df])
        df = df[~df.index.duplicated()].sort_index()
        df.to_csv(cache_file)
    return df
