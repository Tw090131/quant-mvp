# engine/backtest.py
from engine.portfolio import Portfolio
from engine.metrics import calc_drawdown
from engine.trade_log import record_trade, get_daily_trades_df
import pandas as pd
import os

def run_backtest(datas, StrategyClass, fee_rate=0.001, trade_log_csv="daily_trades.csv"):
    strategy = StrategyClass(datas)
    portfolio = Portfolio()
    trade_days = datas[0].index  # 假设第一个股票的日期作为回测日历
    daily_trades = []

    for dt in trade_days:
        # --- 更新价格 ---
        for df in datas:
            code = getattr(df.attrs, "code", df.attrs.get("code", None))
            if dt in df.index:
                price = df.loc[dt, "close"]
                portfolio.update_price(code, price)

        # --- 获取目标仓位 ---
        target_weights = strategy.on_bar(dt)

        # --- 调仓 & 记录交易 ---
        for code, w in target_weights.items():
            price = portfolio.prices.get(code)
            if not price or price <= 0:
                continue

            current_pos = portfolio.positions.get(code, 0)
            target_value = portfolio.total_value() * w
            target_volume = int(target_value / price) - current_pos

            if target_volume > 0:
                action = "BUY"
            elif target_volume < 0:
                action = "SELL"
                target_volume = abs(target_volume)
            else:
                action = "HOLD"

            new_pos = current_pos + (target_volume if action=="BUY" else -target_volume)
            portfolio.positions[code] = new_pos

            if action != "HOLD":
                record_trade(daily_trades, dt, code, action, price, target_volume, fee_rate, new_pos)

        # --- 记录每日资产和盈亏 ---
        portfolio.record_daily(dt)

        # --- 风控：最大回撤 ---
        if portfolio.max_drawdown_triggered():
            print(f"[风控] 触发最大回撤 {portfolio.max_drawdown_pct*100:.2f}%")
            break

    # --- 导出交易明细 CSV ---
    trades_df = get_daily_trades_df(daily_trades)
    if not trades_df.empty:
        trades_df.to_csv(trade_log_csv, index=False)
        print(f"交易明细已写入: {os.path.abspath(trade_log_csv)}")

    return {
        "final_value": portfolio.total_value(),
        "equity": portfolio.get_equity_df(),
        "daily_pnl": portfolio.get_daily_pnl_df(),
        "drawdown": calc_drawdown(portfolio.equity_curve),
        "trade": trades_df
    }
