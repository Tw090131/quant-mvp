from data.cache_loader import load_daily_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross

codes = ["300678"]

datas = {}
for code in codes:
    datas[code] = load_daily_df_with_cache(code, start="2025-08-01")

result = run_backtest(
    datas,
    MaCross,
    fee_rate=0.001,
    trade_log_csv="daily_trades.csv",
    equity_csv="equity_curve.csv",
    pnl_csv="daily_pnl.csv",
)

print("最终资产:", result["final_value"])
print("最大回撤:", result["drawdown"]["max"]["drawdown"])

