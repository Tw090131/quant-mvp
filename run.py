"""
回测运行脚本
"""
import logging
from typing import Optional
import pandas as pd
from data.cache_loader import load_daily_df_with_cache, load_minute_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross
from strategy.daily_strategy import DailyStrategy  # 使用 run_daily 的示例策略
from config import Config

# 初始化日志
Config.setup_logging()
logger = logging.getLogger(__name__)

# ===== 配置 =====
codes = ["300059"]
start_date = "2025-08-01"
# end_date = None  # None 表示到今天，也可以设置为 "2025-12-31" 这样的具体日期
# 回测到指定日期
end_date = "2025-11-20"
# ===== 加载数据 =====
date_range_str = f"{start_date} 至 {end_date if end_date else '今天'}"
logger.info(f"开始加载数据，股票: {codes}, 日期范围: {date_range_str}")

# 日线回测
datas = {}
for code in codes:
    try:
        df = load_daily_df_with_cache(code, start=start_date, end=end_date)
        if not df.empty:
            # 如果指定了结束日期，过滤数据
            if end_date:
                df = df[df.index <= pd.to_datetime(end_date)]
            # 确保数据从开始日期开始
            df = df[df.index >= pd.to_datetime(start_date)]
            if not df.empty:
                datas[code] = df
                logger.info(f"{code} 加载成功，共 {len(df)} 条数据，日期范围: {df.index.min()} 至 {df.index.max()}")
            else:
                logger.warning(f"{code} 在指定日期范围内数据为空")
        else:
            logger.warning(f"{code} 数据为空")
    except Exception as e:
        logger.error(f"{code} 加载失败: {e}")

if not datas:
    logger.error("没有可用的数据，退出")
    exit(1)

# ===== 分钟线回测（支持 run_daily 定时任务） =====
# 如果要使用 run_daily 功能，需要加载分钟线数据
# datas = {}
# for code in codes:
#     try:
#         df = load_minute_df_with_cache(code, period="1min", start=start_date, end=end_date)
#         if not df.empty:
#             # 如果指定了结束日期，过滤数据
#             if end_date:
#                 df = df[df.index <= pd.to_datetime(end_date)]
#             # 确保数据从开始日期开始
#             df = df[df.index >= pd.to_datetime(start_date)]
#             if not df.empty:
#                 datas[code] = df
#                 logger.info(f"{code} 分钟线加载成功，共 {len(df)} 条数据，日期范围: {df.index.min()} 至 {df.index.max()}")
#     except Exception as e:
#         logger.error(f"{code} 分钟线加载失败: {e}")
#
# # 使用支持 run_daily 的策略
# # result = run_backtest(
# #     datas,
# #     DailyStrategy,  # 使用 DailyStrategy 替代 MaCross
# #     fee_rate=Config.DEFAULT_FEE_RATE,
# #     trade_log_csv=Config.DEFAULT_TRADE_LOG_CSV,
# #     equity_csv=Config.DEFAULT_EQUITY_CSV,
# #     pnl_csv=Config.DEFAULT_PNL_CSV,
# #     init_cash=Config.DEFAULT_INIT_CASH,
# # )

# ===== 运行回测 =====
logger.info("开始回测...")
result = run_backtest(
    datas,
    MaCross,
    fee_rate=Config.DEFAULT_FEE_RATE,
    trade_log_csv=Config.DEFAULT_TRADE_LOG_CSV,
    equity_csv=Config.DEFAULT_EQUITY_CSV,
    pnl_csv=Config.DEFAULT_PNL_CSV,
    init_cash=Config.DEFAULT_INIT_CASH,
)

# ===== 输出结果 =====
logger.info("=" * 50)
logger.info("回测结果汇总")
logger.info("=" * 50)
logger.info(f"回测日期范围: {date_range_str}")
if not result['equity'].empty:
    actual_start = result['equity']['date'].iloc[0]
    actual_end = result['equity']['date'].iloc[-1]
    logger.info(f"实际回测日期: {actual_start} 至 {actual_end}")
logger.info(f"最终资产: {result['final_value']:,.2f}")
logger.info(f"总收益率: {result['total_return']:.2%}")
logger.info(f"最大回撤: {result['drawdown']['max_drawdown']:.2%}")
if result['drawdown']['start']:
    logger.info(f"回撤期间: {result['drawdown']['start']} 至 {result['drawdown']['end']}")
    logger.info(f"回撤持续: {result['drawdown']['duration']} 天")
logger.info("=" * 50)
