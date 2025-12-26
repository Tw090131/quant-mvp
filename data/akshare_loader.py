"""
akshare 数据加载模块
提供从 akshare 获取股票数据的功能
"""
import logging
from typing import Optional
import akshare as ak
import pandas as pd

# 配置日志
logger = logging.getLogger(__name__)

# === 日线 ===
def load_ashare_daily(code: str, start: str, end: Optional[str] = None) -> pd.DataFrame:
    """
    加载 A 股日线数据
    
    Args:
        code: 股票代码
        start: 开始日期，格式 "YYYY-MM-DD"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含日线数据的 DataFrame，索引为 datetime
    """
    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start.replace("-", ""),
            end_date=None if end is None else end.replace("-", ""),
            adjust="qfq",
        )
        if df is None or df.empty:
            logger.warning(f"{code} 日线数据为空")
            return pd.DataFrame()

        df = df.rename(columns={
            "日期": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
        })
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime").sort_index()
        logger.debug(f"{code} 日线数据获取成功，共 {len(df)} 条")
        return df
    except Exception as e:
        logger.error(f"获取 {code} 日线数据失败: {e}")
        raise

# === 分钟线 ===
def load_ashare_minute(
    code: str, 
    period: str = "1min", 
    start: Optional[str] = None, 
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    加载 A 股分钟线数据
    
    Args:
        code: 股票代码
        period: 周期，支持 "1min", "5min", "15min", "30min", "60min"
        start: 开始日期时间，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含分钟线数据的 DataFrame，索引为 datetime
    """
    period_map = {
        "1min": "1",
        "5min": "5",
        "15min": "15",
        "30min": "30",
        "60min": "60"
    }

    if period not in period_map:
        raise ValueError(f"不支持的 period: {period}, 支持 {list(period_map.keys())}")

    ak_period = period_map[period]

    try:
        df = ak.stock_zh_a_hist_min_em(
            symbol=code,
            start_date=start, 
            period=ak_period, 
            adjust="qfq"
        )
    except Exception as e:
        logger.warning(f"{code} {period}分钟线获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty or "时间" not in df.columns:
        logger.warning(f"{code} {period}分钟线数据为空或缺失 '时间' 列")
        return pd.DataFrame()

    df = df.rename(columns={
        "时间": "datetime",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume"
    })

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime").sort_index()
    
    logger.debug(f"{code} {period}分钟线数据获取成功，共 {len(df)} 条")
    return df
