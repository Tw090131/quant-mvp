# data/akshare_loader.py
"""
A 股数据加载
"""
import akshare as ak
import pandas as pd


def load_ashare_daily(code: str, start: str, end: str = None) -> pd.DataFrame:
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start.replace("-", ""),
        end_date=None if end is None else end.replace("-", ""),
        adjust="qfq",
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # === 统一字段 ===
    df = df.rename(
        columns={
            "日期": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        }
    )

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime").sort_index()

    return df
