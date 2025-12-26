"""
数据缓存加载模块
提供带缓存的数据加载功能，支持日线和分钟线数据
支持多种数据源：akshare、tushare
"""
import os
import logging
from typing import Optional
import pandas as pd
from config import Config

# 配置日志
logger = logging.getLogger(__name__)

# 根据配置选择数据源
if Config.DATA_SOURCE == "tushare":
    try:
        from data.tushare_loader import load_ashare_daily, load_ashare_minute
        logger.info("使用 tushare 作为数据源")
    except ImportError:
        logger.warning("tushare 未安装或导入失败，回退到 akshare")
        from data.akshare_loader import load_ashare_daily, load_ashare_minute
        Config.DATA_SOURCE = "akshare"
else:
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
    start_dt = pd.to_datetime(start)
    if end is None:
        end_dt = pd.Timestamp.today()
        end = end_dt.strftime("%Y-%m-%d")
    else:
        end_dt = pd.to_datetime(end)
    
    fetch_start = start
    need_fetch = False
    
    if df.empty:
        # 缓存为空，需要获取
        need_fetch = True
        fetch_start = start
    else:
        # 检查缓存数据的日期范围
        cache_min = df.index.min()
        cache_max = df.index.max()
        
        # 如果请求的开始日期早于缓存的最早数据，需要获取更早的数据
        if start_dt < cache_min:
            need_fetch = True
            fetch_start = start
        # 如果请求的结束日期晚于缓存的最新数据，需要获取更新的数据
        elif end_dt > cache_max:
            need_fetch = True
            fetch_start = (cache_max + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        # 如果请求的日期范围完全在缓存范围内，不需要获取
        else:
            need_fetch = False
            logger.debug(f"{code} {period} 分钟线数据已在缓存中，日期范围: {cache_min} 至 {cache_max}")
    
    # 如果需要获取新数据
    if need_fetch:
        logger.info(f"获取 {code} {period} 分钟线数据: {fetch_start} 至 {end}")
        try:
            new_df = load_ashare_minute(code, period, fetch_start, end)
            if not new_df.empty:
                # 合并数据并去重
                if not df.empty:
                    df = pd.concat([df, new_df])
                else:
                    df = new_df
                df = df[~df.index.duplicated()].sort_index()
                
                # 过滤到请求的日期范围（避免返回超出范围的数据）
                df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                
                # 保存缓存（只保存请求范围内的数据）
                if not df.empty:
                    df.to_csv(cache_file)
                    logger.info(f"已更新 {code} {period} 分钟线缓存，共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
                else:
                    logger.warning(f"{code} {period} 获取的数据不在请求的日期范围内")
            else:
                # 获取的数据为空
                if df.empty:
                    logger.warning(f"{code} {period} 未获取到数据，且缓存也为空")
                else:
                    # 有缓存数据，但不在请求范围内
                    cache_in_range = df[(df.index >= start_dt) & (df.index <= end_dt)]
                    if cache_in_range.empty:
                        logger.warning(f"{code} {period} 未获取到新数据，且缓存数据不在请求范围内（缓存: {df.index.min()} 至 {df.index.max()}，请求: {start_dt} 至 {end_dt}）")
                        logger.warning(f"{code} {period} 尝试重新获取数据...")
                        # 清空缓存，强制重新获取
                        df = pd.DataFrame()
                        try:
                            new_df = load_ashare_minute(code, period, start, end)
                            if not new_df.empty:
                                df = new_df
                                df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                                if not df.empty:
                                    df.to_csv(cache_file)
                                    logger.info(f"已重新获取并更新 {code} {period} 分钟线缓存，共 {len(df)} 条")
                                else:
                                    logger.error(f"{code} {period} 重新获取的数据仍不在请求范围内")
                            else:
                                logger.error(f"{code} {period} 重新获取数据失败，返回空数据")
                        except Exception as e2:
                            logger.error(f"{code} {period} 重新获取数据时发生错误: {e2}")
                    else:
                        logger.info(f"{code} {period} 未获取到新数据，使用缓存数据（部分在请求范围内）")
                        df = cache_in_range
        except Exception as e:
            logger.error(f"获取 {code} {period} 分钟线数据失败: {e}", exc_info=True)
            if df.empty:
                raise
    
    # 如果使用缓存数据（没有触发 need_fetch），也需要过滤到请求的日期范围
    if not df.empty and not need_fetch:
        # 检查缓存数据是否在请求范围内
        df_filtered = df[(df.index >= start_dt) & (df.index <= end_dt)]
        if df_filtered.empty:
            logger.warning(f"{code} {period} 缓存数据不在请求的日期范围内，需要重新获取")
            logger.warning(f"  缓存数据范围: {df.index.min()} 至 {df.index.max()}")
            logger.warning(f"  请求数据范围: {start_dt} 至 {end_dt}")
            # 重新获取数据
            try:
                new_df = load_ashare_minute(code, period, start, end)
                if not new_df.empty:
                    df = new_df
                    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                    if not df.empty:
                        df.to_csv(cache_file)
                        logger.info(f"已重新获取并更新 {code} {period} 分钟线缓存，共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
                    else:
                        logger.error(f"{code} {period} 重新获取的数据不在请求范围内")
                else:
                    logger.error(f"{code} {period} 重新获取数据失败，返回空数据")
                    df = pd.DataFrame()
            except Exception as e:
                logger.error(f"重新获取 {code} {period} 分钟线数据失败: {e}", exc_info=True)
                df = pd.DataFrame()
        else:
            df = df_filtered
    
    return df
