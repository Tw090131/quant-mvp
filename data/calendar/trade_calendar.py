
import pandas as pd
from functools import lru_cache
from .calendar_loader import load_trade_calendar

@lru_cache(maxsize=1)
def _calendar_df():
    return load_trade_calendar()

def is_trade_day(date) -> bool:
    date = pd.to_datetime(date)
    return date in set(_calendar_df()["trade_date"].values)

def get_trade_days(start, end):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    df = _calendar_df()
    return df[(df.trade_date >= start) & (df.trade_date <= end)]["trade_date"].tolist()

def next_trade_day(date):
    date = pd.to_datetime(date)
    days = _calendar_df()[_calendar_df().trade_date > date]["trade_date"]
    return days.iloc[0] if not days.empty else None

def prev_trade_day(date):
    date = pd.to_datetime(date)
    days = _calendar_df()[_calendar_df().trade_date < date]["trade_date"]
    return days.iloc[-1] if not days.empty else None
