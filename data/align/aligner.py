import pandas as pd
from typing import Dict
from data.calendar.trade_calendar import get_trade_days


def align_dfs_by_trade_days(
    dfs: Dict[str, pd.DataFrame],
    start: str,
    end: str,
    method: str = "ffill",
) -> Dict[str, pd.DataFrame]:
    """
    按 A 股交易日历对齐多股票数据

    :param dfs: {code: DataFrame}, 必须包含 datetime 列
    :param start: 回测开始日期
    :param end: 回测结束日期
    :param method: 缺失值处理方式（ffill / None）
    :return: 对齐后的 dfs
    """
    # 1️⃣ 全局交易日轴
    trade_days = pd.to_datetime(get_trade_days(start, end))
    if len(trade_days) == 0:
        raise RuntimeError("交易日为空，无法对齐")

    aligned = {}

    for code, df in dfs.items():
        if "datetime" not in df.columns:
            raise ValueError(f"{code} 缺少 datetime 列")

        # 2️⃣ 设 index
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime").sort_index()

        # 3️⃣ reindex 到统一交易日
        df = df.reindex(trade_days)

        # 4️⃣ 缺失处理
        if method == "ffill":
            df = df.ffill()
        elif method is None:
            pass
        else:
            raise ValueError(f"未知 method: {method}")

        # 5️⃣ 过滤上市前的 NaN
        df = df.dropna(how="all")

        # 6️⃣ 还原 datetime
        df["datetime"] = df.index
        df = df.reset_index(drop=True)

        aligned[code] = df

    return aligned
