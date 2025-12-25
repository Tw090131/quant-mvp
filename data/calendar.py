'''
交易日历层
A 股交易日历（可缓存、可复用）
'''
import os
import pandas as pd
import akshare as ak

BASE_DIR = os.path.dirname(__file__)
CALENDAR_DIR = os.path.join(BASE_DIR, "cache", "calendar")
CALENDAR_FILE = os.path.join(CALENDAR_DIR, "trade_days.csv")


def get_trade_calendar(start="2000-01-01", end="2030-12-31"):
    """
    获取 A 股交易日历（带本地缓存）
    """
    os.makedirs(CALENDAR_DIR, exist_ok=True)

    # ===== 1. 本地缓存 =====
    if os.path.exists(CALENDAR_FILE):
        df = pd.read_csv(CALENDAR_FILE, parse_dates=["trade_date"])
        return df["trade_date"]

    # ===== 2. 首次拉取 =====
    print("[CALENDAR] 拉取 A 股交易日历")

    df = ak.tool_trade_date_hist_sina()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)]

    df[["trade_date"]].to_csv(CALENDAR_FILE, index=False)
    print(f"[CALENDAR SAVE] {CALENDAR_FILE}")

    return df["trade_date"]
