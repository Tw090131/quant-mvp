"""
回测运行脚本
"""
import logging
from typing import Optional
import pandas as pd
from data.cache_loader import load_daily_df_with_cache, load_minute_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross
from strategy.platform_breakout import PlatformBreakout
from strategy.daily_strategy import DailyStrategy  # 使用 run_daily 的示例策略
from config import Config
import akshare as ak
import tushare as ts
from config import Config

# 设置 tushare token
ts.set_token('87db6ebbe821bf474e7835abf0dd9ec2be7f9dbbe9a06210f17183ad')
# 初始化日志
Config.setup_logging()
logger = logging.getLogger(__name__)

# ===== 配置 =====
codes = ["000423"]
start_date = "2025-03-01"
# end_date = None  # None 表示到今天，也可以设置为 "2025-12-31" 这样的具体日期
# 回测到指定日期
end_date = "2025-12-20"

# 数据模式选择：'daily' 或 'minute'
# 'daily': 日线数据，回测速度快，但 run_daily 定时任务不会触发
# 'minute': 分钟线数据，支持 run_daily 定时任务，但回测速度较慢
data_mode = "daily"  # 改为 "minute" 以启用 run_daily 功能

# 策略选择：'ma_cross' 或 'platform_breakout'
strategy_name = "platform_breakout"  # 选择使用的策略

# ===== 策略参数配置 =====
# 根据选择的策略，配置相应的参数
strategy_kwargs = {}

if strategy_name == "ma_cross":
    # 双均线策略参数
    strategy_kwargs = {
        "short": 5,      # 短期均线周期
        "long": 15,      # 长期均线周期
        "weight": 0.5,   # 单只股票的最大权重
        "stop_loss": None,      # 止损比例（None 表示不启用）
        "take_profit": None,    # 止盈比例（None 表示不启用）
    }
elif strategy_name == "platform_breakout":
    # 平台突破策略参数
    strategy_kwargs = {
        "period": 20,              # 平台周期（交易日数）
        "weight": 0.5,             # 单只股票的最大权重
        "stop_loss": 0.05,         # 止损比例（5%）
        "take_profit": 0.15,       # 止盈比例（15%）
        "breakout_threshold": 0.01, # 突破阈值（1%，降低阈值以便更容易产生信号）
        "use_volume_filter": False, # 是否使用成交量过滤
        "min_volume_ratio": 1.2,   # 最小成交量比例（相对于均量）
    }

# ===== 加载数据 =====
date_range_str = f"{start_date} 至 {end_date if end_date else '今天'}"
logger.info(f"开始加载数据，股票: {codes}, 日期范围: {date_range_str}, 数据模式: {data_mode}")

datas = {}

if data_mode == "daily":
    # ===== 日线回测 =====
    logger.info("使用日线数据进行回测（run_daily 定时任务不会触发）")
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
                    logger.info(f"{code} 日线加载成功，共 {len(df)} 条数据，日期范围: {df.index.min()} 至 {df.index.max()}")
                else:
                    logger.warning(f"{code} 在指定日期范围内数据为空")
            else:
                logger.warning(f"{code} 数据为空")
        except Exception as e:
            logger.error(f"{code} 加载失败: {e}")

elif data_mode == "minute":
    # ===== 分钟线回测（支持 run_daily 定时任务） =====
    logger.info("使用分钟线数据进行回测（支持 run_daily 定时任务）")
    for code in codes:
        try:
            df = load_minute_df_with_cache(code, period="1min", start=start_date, end=end_date)
            if not df.empty:
                # 对于分钟线数据，需要按日期过滤（包含当天的所有分钟）
                start_dt = pd.to_datetime(start_date)
                if end_date:
                    # 结束日期包含当天的所有时间，所以应该是 end_date 23:59:59
                    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                else:
                    end_dt = pd.Timestamp.now()
                
                # 记录原始数据范围
                original_count = len(df)
                original_min = df.index.min()
                original_max = df.index.max()
                
                # 过滤数据：确保在日期范围内
                df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                
                if not df.empty:
                    datas[code] = df
                    logger.info(f"{code} 分钟线加载成功，共 {len(df)} 条数据，日期范围: {df.index.min()} 至 {df.index.max()}")
                else:
                    logger.warning(f"{code} 在指定日期范围内数据为空")
                    logger.warning(f"  原始数据范围: {original_min} 至 {original_max}，共 {original_count} 条")
                    logger.warning(f"  请求日期范围: {start_dt} 至 {end_dt}")
            else:
                logger.warning(f"{code} 数据为空")
        except Exception as e:
            logger.error(f"{code} 分钟线加载失败: {e}", exc_info=True)
else:
    logger.error(f"不支持的数据模式: {data_mode}，请使用 'daily' 或 'minute'")
    exit(1)

if not datas:
    logger.error("没有可用的数据，退出")
    exit(1)

# ===== 选择策略 =====
if strategy_name == "ma_cross":
    StrategyClass = MaCross
    logger.info("使用策略: 双均线策略 (MaCross)")
elif strategy_name == "platform_breakout":
    StrategyClass = PlatformBreakout
    logger.info("使用策略: 平台突破策略 (PlatformBreakout)")
else:
    logger.error(f"不支持的策略: {strategy_name}，请使用 'ma_cross' 或 'platform_breakout'")
    exit(1)

# ===== 运行回测 =====
logger.info("开始回测...")
result = run_backtest(
    datas,
    StrategyClass,
    fee_rate=Config.DEFAULT_FEE_RATE,
    fee_mode=Config.DEFAULT_FEE_MODE,
    fee_rate_pct=Config.DEFAULT_FEE_RATE_PCT,
    fee_fixed=Config.DEFAULT_FEE_FIXED,
    trade_log_csv=Config.DEFAULT_TRADE_LOG_CSV,
    equity_csv=Config.DEFAULT_EQUITY_CSV,
    pnl_csv=Config.DEFAULT_PNL_CSV,
    init_cash=Config.DEFAULT_INIT_CASH,
    strategy_kwargs=strategy_kwargs,  # 传递策略参数
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
