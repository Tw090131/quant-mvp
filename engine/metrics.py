# engine/metrics.py
import numpy as np

def calc_drawdown(equity_curve):
    if not equity_curve:
        return {}
    equity = np.array([x["total_value"] for x in equity_curve])
    peak = np.maximum.accumulate(equity)
    drawdown = (equity - peak) / peak
    max_dd = drawdown.min()
    max_dd_end = drawdown.argmin()
    max_dd_start = equity[:max_dd_end].argmax() if max_dd_end > 0 else 0
    return {
        "max": {
            "drawdown": float(max_dd),
            "start": int(max_dd_start),
            "end": int(max_dd_end),
        }
    }
