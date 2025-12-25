from data.cache_loader import load_daily_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross

codes = ["603296"]
datas = []

for code in codes:
    try:
        df = load_daily_df_with_cache(code, start="2025-01-01")
        df.attrs["code"] = code
        datas.append(df)
    except Exception as e:
        print(f"跳过股票 {code}: {e}")

if not datas:
    raise RuntimeError("没有有效股票数据")

result = run_backtest(datas, MaCross, fee_rate=0.001, trade_log_csv="daily_trades.csv")

print("最终资产:", result["final_value"])
if "max" in result["drawdown"]:
    print("最大回撤: {:.2%}".format(result["drawdown"]["max"]["drawdown"]))

# 保存每日净值和每日盈亏
result["equity"].to_csv("equity_curve.csv", index=False)
result["daily_pnl"].to_csv("daily_pnl.csv", index=False)

print("交易明细、每日净值和每日盈亏已导出 CSV")
