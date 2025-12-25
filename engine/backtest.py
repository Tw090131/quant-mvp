# engine/backtest.py
import pandas as pd
import os
from engine.portfolio import Portfolio
from engine.metrics import calc_drawdown
from engine.risk import RiskManager


def run_backtest(
    datas: dict,
    StrategyClass,
    fee_rate=0.001,
    trade_log_csv="daily_trades.csv",
    equity_csv="equity_curve.csv",
    pnl_csv="daily_pnl.csv",
    init_cash=1_000_000,
):
    portfolio = Portfolio(init_cash=init_cash)
    strategy = StrategyClass(datas)
    risk_mgr = RiskManager()

    # 获取交易日
    trade_days = datas[next(iter(datas))].index

    for dt in trade_days:
        # T+1 买入转持仓
        portfolio.on_new_day()

        # 更新每个股票价格
        for code, df in datas.items():
            portfolio.update_price(code, df.loc[dt, "close"])

        # 获取策略信号
        target_weights = strategy.on_bar(dt)

        # 调仓，返回当天交易
        trades_today = portfolio.rebalance(
            date=dt,
            target_weights=target_weights,
            risk_mgr=risk_mgr,
            fee_rate=fee_rate,
        )

        # 日结，计算总资产和每日盈亏
        portfolio.record_daily(dt, trades_today)

    # ===== 导出 CSV =====
    trades_df = portfolio.get_trades_df()
    equity_df = portfolio.get_equity_df()
    pnl_df = portfolio.get_daily_pnl_df()

    if not trades_df.empty:
        trades_df.to_csv(trade_log_csv, index=False)
        print(f"交易明细已写入: {os.path.abspath(trade_log_csv)}")

    if not equity_df.empty:
        equity_df.to_csv(equity_csv, index=False)
        print(f"每日资产已写入: {os.path.abspath(equity_csv)}")

    if not pnl_df.empty:
        pnl_df.to_csv(pnl_csv, index=False)
        print(f"每日盈亏已写入: {os.path.abspath(pnl_csv)}")

    # 计算最大回撤
    drawdown = calc_drawdown(equity_df)

    return {
        "final_value": portfolio.total_value(),
        "equity": equity_df,
        "daily_pnl": pnl_df,
        "drawdown": drawdown,
        "trades": trades_df,
    }
