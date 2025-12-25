'''
数据对齐层
'''
import pandas as pd
from data.calendar import get_trade_calendar


def align_daily_data(dfs, start, end, method="ffill"):
    """
    基于交易日历的多股票数据对齐
    """

    trade_days = get_trade_calendar(start=start, end=end)

    aligned = {}

    for code, df in dfs.items():
        # 以交易日历为基准重建索引
        df = df.reindex(trade_days)

        if method == "ffill":
            df[["open", "high", "low", "close"]] = df[
                ["open", "high", "low", "close"]
            ].ffill()

            df["volume"] = df["volume"].fillna(0)

        elif method == "drop":
            df = df.dropna()

        aligned[code] = df

    return aligned
