"""
数据缓存加载模块
提供带缓存的数据加载功能，支持日线和分钟线数据
"""
import os
import logging
from typing import Optional
import pandas as pd
from data.akshare_loader import load_ashare_daily, load_ashare_minute

# 配置日志
logger = logging.getLogger(__name__)

CACHE_DIR = "data_cache"
os.makedirs(CACHE_DIR, exist_ok=True)


def load_daily_df_with_cache(
    code: str, 
    start: str = "2019-01-01", 
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    加载日线数据（带缓存）
    
    Args:
        code: 股票代码
        start: 开始日期，格式 "YYYY-MM-DD"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含日线数据的 DataFrame，索引为 datetime
    """
    cache_file = os.path.join(CACHE_DIR, f"{code}.csv")
    
    # 读取缓存
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            logger.debug(f"从缓存加载 {code} 日线数据，共 {len(df)} 条")
        except Exception as e:
            logger.warning(f"读取缓存文件失败 {cache_file}: {e}，将重新获取")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    
    # 确定需要获取的日期范围
    fetch_start = start
    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    
    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    
    # 如果需要获取新数据
    if df.empty or fetch_start <= end:
        logger.info(f"获取 {code} 日线数据: {fetch_start} 至 {end}")
        try:
            new_df = load_ashare_daily(code, fetch_start, end)
            if not new_df.empty:
                # 合并数据并去重
                df = pd.concat([df, new_df])
                df = df[~df.index.duplicated()].sort_index()
                # 保存缓存
                df.to_csv(cache_file)
                logger.info(f"已更新 {code} 日线缓存，共 {len(df)} 条")
            elif df.empty:
                logger.warning(f"{code} 未获取到数据")
        except Exception as e:
            logger.error(f"获取 {code} 日线数据失败: {e}")
            if df.empty:
                raise
    
    return df


def load_minute_df_with_cache(
    code: str, 
    period: str = "1min", 
    start: str = "2025-01-01", 
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    加载分钟线数据（带缓存）
    
    Args:
        code: 股票代码
        period: 周期，支持 "1min", "5min", "15min", "30min", "60min"
        start: 开始日期时间，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含分钟线数据的 DataFrame，索引为 datetime
    """
    filename = f"{code}_{period}.csv"
    cache_file = os.path.join(CACHE_DIR, filename)
    
    # 读取缓存
    if os.path.exists(cache_file):
        try:
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            logger.debug(f"从缓存加载 {code} {period} 分钟线数据，共 {len(df)} 条")
        except Exception as e:
            logger.warning(f"读取缓存文件失败 {cache_file}: {e}，将重新获取")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    
    # 确定需要获取的时间范围
    fetch_start = start
    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    
    # 如果需要获取新数据
    if df.empty or fetch_start <= end:
        logger.info(f"获取 {code} {period} 分钟线数据: {fetch_start} 至 {end}")
        try:
            new_df = load_ashare_minute(code, period, fetch_start, end)
            if not new_df.empty:
                # 合并数据并去重
                df = pd.concat([df, new_df])
                df = df[~df.index.duplicated()].sort_index()
                # 保存缓存
                df.to_csv(cache_file)
                logger.info(f"已更新 {code} {period} 分钟线缓存，共 {len(df)} 条")
            elif df.empty:
                logger.warning(f"{code} {period} 未获取到数据")
        except Exception as e:
            logger.error(f"获取 {code} {period} 分钟线数据失败: {e}")
            if df.empty:
                raise
    
    return df
