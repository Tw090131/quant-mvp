"""
回测引擎模块
提供完整的回测功能，包括策略执行、组合管理、风险控制和结果输出
"""
import os
import logging
from typing import Any, Dict, Type, Optional
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
    
    # 设置策略的上下文对象
    from engine.context import Context
    strategy.context = Context(portfolio=portfolio, data=datas)
    
    if risk_mgr is None:
        risk_mgr = RiskManager()
    
    # 获取交易日（使用第一个股票的时间轴）
    first_code = next(iter(datas))
    trade_days = datas[first_code].index
    
    if len(trade_days) == 0:
        raise ValueError(f"股票 {first_code} 没有交易日数据")
    
    # 判断是日线还是分钟线数据
    # 检查前几个时间点，如果有小时或分钟信息，则是分钟线
    is_minute_data = False
    sample_dts = trade_days[:min(10, len(trade_days))]
    for dt in sample_dts:
        if isinstance(dt, pd.Timestamp):
            # 如果有小时或分钟信息（不是00:00），则是分钟线
            if hasattr(dt, 'hour') and hasattr(dt, 'minute'):
                if dt.hour != 0 or dt.minute != 0:
                    is_minute_data = True
                    break
        elif ':' in str(dt):
            # 字符串中包含冒号，可能是时间格式
            is_minute_data = True
            break
    
    logger.info(f"开始回测，共 {len(trade_days)} 个时间点，初始资金: {init_cash:,.0f}")
    if is_minute_data:
        logger.info("检测到分钟线数据，支持定时任务功能")
    
    # 回测主循环
    for i, dt in enumerate(trade_days):
        try:
            # T+1 买入转持仓（仅在日期变化时执行）
            if i == 0 or (hasattr(dt, 'date') and hasattr(trade_days[i-1], 'date') and dt.date() != trade_days[i-1].date()):
                portfolio.on_new_day()

            # 更新每个股票价格
            for code, df in datas.items():
                if dt not in df.index:
                    continue
                try:
                    price = df.loc[dt, "close"]
                    if pd.isna(price) or price <= 0:
                        continue
                    portfolio.update_price(code, price)
                except (KeyError, IndexError):
                    continue

            # 执行定时任务（如果使用分钟线数据）
            if is_minute_data and strategy.context:
                # 更新 context 的当前日期
                strategy.context.current_date = dt
                strategy.scheduler.on_bar(dt, strategy.context)

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
            if not is_minute_data:
                # 日线数据，每天都记录
                portfolio.record_daily(dt, trades_today)
            else:
                # 分钟线数据，只在日期变化时记录（新的一天开始时，记录前一天）
                is_new_day = False
                if i == 0:
                    is_new_day = True
                elif hasattr(dt, 'date') and hasattr(trade_days[i-1], 'date'):
                    if dt.date() != trade_days[i-1].date():
                        is_new_day = True
                        # 记录前一天的日结
                        portfolio.record_daily(trade_days[i-1], [])
                
                # 最后一天，记录当天
                if i == len(trade_days) - 1:
                    portfolio.record_daily(dt, trades_today)
            
            # 进度提示
            progress_interval = 50 if not is_minute_data else 1000
            if (i + 1) % progress_interval == 0 or i == len(trade_days) - 1:
                logger.info(f"回测进度: {i+1}/{len(trade_days)}, 当前资产: {current_value:,.0f}")
                
        except Exception as e:
            logger.error(f"回测在 {dt} 发生错误: {e}", exc_info=True)
            raise

    # ===== 导出 CSV =====
    trades_df = portfolio.get_trades_df()
    equity_df = portfolio.get_equity_df()
    pnl_df = portfolio.get_daily_pnl_df()

    if not trades_df.empty:
        # 格式化日期列，保留时分秒信息
        trades_df = trades_df.copy()
        if 'date' in trades_df.columns:
            # 转换为 datetime 类型
            date_series = pd.to_datetime(trades_df['date'])
            
            # 检查是否包含时分秒信息（基于 is_minute_data 或检查实际数据）
            has_time_info = is_minute_data
            if not has_time_info:
                # 检查实际数据是否包含时分秒
                sample_dates = date_series.head(10)
                has_time_info = any(
                    isinstance(dt, pd.Timestamp) and 
                    (dt.hour != 0 or dt.minute != 0 or dt.second != 0)
                    for dt in sample_dates
                )
            
            # 根据是否有时分秒信息格式化
            if has_time_info:
                trades_df['date'] = date_series.dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                trades_df['date'] = date_series.dt.strftime('%Y-%m-%d')
        
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
