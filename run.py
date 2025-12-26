"""
回测运行脚本
"""
import logging
from data.cache_loader import load_daily_df_with_cache, load_minute_df_with_cache
from engine.backtest import run_backtest
from strategy.ma_cross import MaCross
from config import Config

# 初始化日志
Config.setup_logging()
logger = logging.getLogger(__name__)

# ===== 配置 =====
codes = ["300059"]
start_date = "2025-08-01"

# ===== 加载数据 =====
logger.info(f"开始加载数据，股票: {codes}, 开始日期: {start_date}")

# 日线回测
datas = {}
for code in codes:
    try:
        df = load_daily_df_with_cache(code, start=start_date)
        if not df.empty:
            datas[code] = df
            logger.info(f"{code} 加载成功，共 {len(df)} 条数据")
        else:
            logger.warning(f"{code} 数据为空")
    except Exception as e:
        logger.error(f"{code} 加载失败: {e}")

if not datas:
    logger.error("没有可用的数据，退出")
    exit(1)

# ===== 分钟线回测（可选） =====
# datas = {}
# for code in codes:
#     try:
#         df = load_minute_df_with_cache(code, period="1min", start=start_date)
#         if not df.empty:
#             datas[code] = df
#             logger.info(f"{code} 分钟线加载成功，共 {len(df)} 条数据")
#     except Exception as e:
#         logger.error(f"{code} 分钟线加载失败: {e}")

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
logger.info(f"最终资产: {result['final_value']:,.2f}")
logger.info(f"总收益率: {result['total_return']:.2%}")
logger.info(f"最大回撤: {result['drawdown']['max_drawdown']:.2%}")
if result['drawdown']['start']:
    logger.info(f"回撤期间: {result['drawdown']['start']} 至 {result['drawdown']['end']}")
    logger.info(f"回撤持续: {result['drawdown']['duration']} 天")
logger.info("=" * 50)
