"""
回测引擎模块
提供完整的回测功能，包括策略执行、组合管理、风险控制和结果输出
"""
import os
import logging
from typing import Dict, Type, Optional
import pandas as pd
from engine.portfolio import Portfolio
from engine.metrics import calc_drawdown
from engine.risk import RiskManager
from strategy.base import StrategyBase

# 配置日志
logger = logging.getLogger(__name__)


def run_backtest(
    datas: Dict[str, pd.DataFrame],
    StrategyClass: Type[StrategyBase],
    fee_rate: float = 0.001,
    trade_log_csv: str = "daily_trades.csv",
    equity_csv: str = "equity_curve.csv",
    pnl_csv: str = "daily_pnl.csv",
    init_cash: float = 1_000_000,
    risk_mgr: Optional[RiskManager] = None,
) -> Dict:
    """
    运行回测
    
    Args:
        datas: 股票数据字典，{code: DataFrame}，DataFrame 索引为 datetime
        StrategyClass: 策略类（继承自 StrategyBase）
        fee_rate: 手续费率
        trade_log_csv: 交易明细输出文件路径
        equity_csv: 资产曲线输出文件路径
        pnl_csv: 每日盈亏输出文件路径
        init_cash: 初始资金
        risk_mgr: 风险管理器，None 则使用默认配置
        
    Returns:
        包含回测结果的字典：
        - final_value: 最终资产
        - equity: 资产曲线 DataFrame
        - daily_pnl: 每日盈亏 DataFrame
        - drawdown: 最大回撤信息
        - trades: 交易明细 DataFrame
    """
    if not datas:
        raise ValueError("数据字典为空")
    
    # 初始化组件
    portfolio = Portfolio(init_cash=init_cash)
    strategy = StrategyClass(datas)
    if risk_mgr is None:
        risk_mgr = RiskManager()
    
    # 获取交易日（使用第一个股票的时间轴）
    first_code = next(iter(datas))
    trade_days = datas[first_code].index
    
    if len(trade_days) == 0:
        raise ValueError(f"股票 {first_code} 没有交易日数据")
    
    logger.info(f"开始回测，共 {len(trade_days)} 个交易日，初始资金: {init_cash:,.0f}")
    
    # 回测主循环
    for i, dt in enumerate(trade_days):
        try:
            # T+1 买入转持仓
            portfolio.on_new_day()

            # 更新每个股票价格
            for code, df in datas.items():
                if dt not in df.index:
                    logger.warning(f"{code} 在 {dt} 无数据，跳过")
                    continue
                try:
                    price = df.loc[dt, "close"]
                    if pd.isna(price) or price <= 0:
                        logger.warning(f"{code} 在 {dt} 价格无效: {price}")
                        continue
                    portfolio.update_price(code, price)
                except KeyError:
                    logger.warning(f"{code} 在 {dt} 缺少 'close' 列")
                    continue

            # 获取策略信号
            target_weights = strategy.on_bar(dt)

            # 调仓，返回当天交易
            trades_today = portfolio.rebalance(
                date=dt,
                target_weights=target_weights,
                risk_mgr=risk_mgr,
                fee_rate=fee_rate,
            )

            # 检查风控
            current_value = portfolio.total_value()
            if not risk_mgr.check_portfolio(current_value):
                logger.warning(f"在 {dt} 触发风控，停止回测")
                break

            # 日结，计算总资产和每日盈亏
            portfolio.record_daily(dt, trades_today)
            
            # 进度提示
            if (i + 1) % 50 == 0 or i == len(trade_days) - 1:
                logger.info(f"回测进度: {i+1}/{len(trade_days)}, 当前资产: {current_value:,.0f}")
                
        except Exception as e:
            logger.error(f"回测在 {dt} 发生错误: {e}", exc_info=True)
            raise

    # ===== 导出 CSV =====
    trades_df = portfolio.get_trades_df()
    equity_df = portfolio.get_equity_df()
    pnl_df = portfolio.get_daily_pnl_df()

    if not trades_df.empty:
        trades_df.to_csv(trade_log_csv, index=False)
        logger.info(f"交易明细已写入: {os.path.abspath(trade_log_csv)}")

    if not equity_df.empty:
        equity_df.to_csv(equity_csv, index=False)
        logger.info(f"每日资产已写入: {os.path.abspath(equity_csv)}")

    if not pnl_df.empty:
        pnl_df.to_csv(pnl_csv, index=False)
        logger.info(f"每日盈亏已写入: {os.path.abspath(pnl_csv)}")

    # 计算最大回撤
    drawdown = calc_drawdown(equity_df)
    
    final_value = portfolio.total_value()
    total_return = (final_value - init_cash) / init_cash
    
    logger.info(f"回测完成，最终资产: {final_value:,.0f}, 总收益率: {total_return:.2%}, 最大回撤: {drawdown['max_drawdown']:.2%}")

    return {
        "final_value": final_value,
        "total_return": total_return,
        "equity": equity_df,
        "daily_pnl": pnl_df,
        "drawdown": drawdown,
        "trades": trades_df,
    }
