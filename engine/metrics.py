# engine/metrics.py
import numpy as np


# engine/metrics.py
def calc_drawdown(equity_curve):
    """
    equity_curve: list of dicts [{'date':..., 'total':...}, ...] 
    或者 pd.DataFrame，必须包含 'total' 列
    返回: dict 最大回撤信息
    """
    import pandas as pd

    if isinstance(equity_curve, pd.DataFrame):
        if equity_curve.empty:
            return {"max_drawdown": 0, "start": None, "end": None, "duration": 0}
        total_values = equity_curve["total"].values
        dates = equity_curve["date"].values
    else:
        if not equity_curve:
            return {"max_drawdown": 0, "start": None, "end": None, "duration": 0}
        total_values = [x["total"] for x in equity_curve]
        dates = [x["date"] for x in equity_curve]

    max_dd = 0
    peak = total_values[0]
    peak_idx = 0
    dd_start = dd_end = None

    for i, val in enumerate(total_values):
        if val > peak:
            peak = val
            peak_idx = i
        dd = (peak - val) / peak
        if dd > max_dd:
            max_dd = dd
            dd_start = dates[peak_idx]
            dd_end = dates[i]

    duration = (pd.to_datetime(dd_end) - pd.to_datetime(dd_start)).days if dd_start and dd_end else 0
    return {
        "max_drawdown": max_dd,
        "start": dd_start,
        "end": dd_end,
        "duration": duration,
    }
