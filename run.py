from data.cache_loader import load_daily_df_with_cache, load_minute_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross
import akshare as ak
codes = ["600519"]

# ===== 日线回测 =====
# datas = {code: load_daily_df_with_cache(code, start="2025-08-01") for code in codes}

# ===== 分钟线回测（可选） =====
datas = {code: load_minute_df_with_cache(code, period="1min",start="2025-08-01") for code in codes}

result = run_backtest(
    datas,
    MaCross,
    fee_rate=0.001,
    trade_log_csv="daily_trades.csv",
    equity_csv="equity_curve.csv",
    pnl_csv="daily_pnl.csv",
)

print("最终资产:", result["final_value"])
print("最大回撤:", result["drawdown"]["max_drawdown"])
