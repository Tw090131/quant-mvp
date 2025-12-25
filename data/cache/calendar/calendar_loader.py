 # 拉 + 缓存
import os
import pandas as pd
import akshare as ak

CALENDAR_DIR = os.path.dirname(__file__)
# 本地缓存
CALENDAR_PATH = os.path.join(CALENDAR_DIR, "sse_calendar.csv")

def load_trade_calendar(refresh=False) -> pd.DataFrame:
    """
    加载 A 股交易日历（带本地缓存）
    """
    if os.path.exists(CALENDAR_PATH) and not refresh:
        df = pd.read_csv(CALENDAR_PATH, parse_dates=["trade_date"])
        return df

    df = ak.tool_trade_date_hist_sina()
    df = df.rename(columns={"trade_date": "trade_date"})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)

    df.to_csv(CALENDAR_PATH, index=False)
    return df
