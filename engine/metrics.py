"""
性能指标计算模块
提供回测结果分析相关的指标计算功能
"""
from typing import Dict, List, Union
import pandas as pd


def calc_drawdown(
    equity_curve: Union[pd.DataFrame, List[Dict]]
) -> Dict:
    """
    计算最大回撤
    
    Args:
        equity_curve: 资产曲线，可以是：
            - pd.DataFrame，必须包含 'total' 和 'date' 列
            - List[Dict]，每个字典包含 'total' 和 'date' 键
            
    Returns:
        包含最大回撤信息的字典：
        - max_drawdown: 最大回撤比例
        - start: 回撤开始日期
        - end: 回撤结束日期
        - duration: 回撤持续天数
    """
    if isinstance(equity_curve, pd.DataFrame):
        if equity_curve.empty:
            return {"max_drawdown": 0.0, "start": None, "end": None, "duration": 0}
        if "total" not in equity_curve.columns:
            raise ValueError("equity_curve DataFrame 必须包含 'total' 列")
        if "date" not in equity_curve.columns:
            raise ValueError("equity_curve DataFrame 必须包含 'date' 列")
        total_values = equity_curve["total"].values
        dates = equity_curve["date"].values
    else:
        if not equity_curve:
            return {"max_drawdown": 0.0, "start": None, "end": None, "duration": 0}
        if not all("total" in x and "date" in x for x in equity_curve):
            raise ValueError("equity_curve 列表中的字典必须包含 'total' 和 'date' 键")
        total_values = [x["total"] for x in equity_curve]
        dates = [x["date"] for x in equity_curve]

    if len(total_values) == 0:
        return {"max_drawdown": 0.0, "start": None, "end": None, "duration": 0}

    max_dd = 0.0
    peak = total_values[0]
    peak_idx = 0
    dd_start = dd_end = None

    for i, val in enumerate(total_values):
        if val > peak:
            peak = val
            peak_idx = i
        dd = (peak - val) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            dd_start = dates[peak_idx]
            dd_end = dates[i]

    duration = 0
    if dd_start and dd_end:
        try:
            duration = (pd.to_datetime(dd_end) - pd.to_datetime(dd_start)).days
        except Exception:
            duration = 0
    
    return {
        "max_drawdown": max_dd,
        "start": dd_start,
        "end": dd_end,
        "duration": duration,
    }
