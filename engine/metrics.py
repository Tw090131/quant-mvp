# engine/metrics.py
import numpy as np


def calc_drawdown(equity_curve):
    """
    equity_curve: List[{"date", "total", ...}]
    """
    if not equity_curve:
        return {}

    equity = np.array([x["total"] for x in equity_curve])

    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak

    max_dd = drawdown.min()
    end_idx = drawdown.argmin()
    start_idx = peak[: end_idx + 1].argmax()

    return {
        "max": {
            "drawdown": float(max_dd),
            "start": equity_curve[start_idx]["date"],
            "end": equity_curve[end_idx]["date"],
        }
    }
