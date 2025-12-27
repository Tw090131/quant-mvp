"""
数据缓存加载模块
提供带缓存的数据加载功能，支持日线和分钟线数据
支持多种数据源：akshare、tushare
支持多种缓存格式：CSV、Parquet
"""
import os
import logging
from typing import Optional, Tuple
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


def _code_to_ts_code(code: str) -> str:
    """
    将股票代码转换为 tushare 格式
    
    Args:
        code: 股票代码，如 "300548" 或 "000001"
        
    Returns:
        tushare 格式代码，如 "300548.SZ" 或 "000001.SZ"
    """
    # 判断是上交所还是深交所
    if code.startswith('6'):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


def _find_cache_file(code: str, period: Optional[str] = None) -> Tuple[Optional[str], str, str]:
    """
    查找缓存文件（支持 CSV 和 Parquet 格式）
    支持从配置的外部 parquet 路径加载
    
    Args:
        code: 股票代码
        period: 周期，None 表示日线，否则为分钟线周期（如 "1min"）
        
    Returns:
        (缓存文件路径, 文件格式, 数据来源类型) 如果找到，否则 (None, "csv", "api")
        数据来源类型: "external_parquet", "local_parquet", "local_csv", "api"
    """
    # 1. 首先检查配置的外部 parquet 路径
    if period:
        # 分钟线数据：从配置的目录中查找
        if Config.PARQUET_MINUTE_DIR and os.path.exists(Config.PARQUET_MINUTE_DIR):
            ts_code = _code_to_ts_code(code)
            parquet_file = os.path.join(Config.PARQUET_MINUTE_DIR, f"{ts_code}.parquet")
            if os.path.exists(parquet_file):
                logger.debug(f"找到外部 parquet 文件: {parquet_file}")
                return parquet_file, "parquet", "external_parquet"
    else:
        # 日线数据：检查配置的路径
        if Config.PARQUET_DAILY_PATH and os.path.exists(Config.PARQUET_DAILY_PATH):
            # 如果是单个文件，直接返回
            if os.path.isfile(Config.PARQUET_DAILY_PATH):
                logger.debug(f"找到外部日线 parquet 文件: {Config.PARQUET_DAILY_PATH}")
                return Config.PARQUET_DAILY_PATH, "parquet", "external_parquet"
    
    # 2. 检查本地缓存目录
    if period:
        filename_base = f"{code}_{period}"
    else:
        filename_base = code
    
    # 根据配置偏好选择格式
    preference = Config.CACHE_FORMAT_PREFERENCE
    
    if preference == "parquet":
        # 优先 parquet
        parquet_file = os.path.join(CACHE_DIR, f"{filename_base}.parquet")
        csv_file = os.path.join(CACHE_DIR, f"{filename_base}.csv")
        if os.path.exists(parquet_file):
            return parquet_file, "parquet", "local_parquet"
        elif os.path.exists(csv_file):
            return csv_file, "csv", "local_csv"
    elif preference == "csv":
        # 优先 csv
        csv_file = os.path.join(CACHE_DIR, f"{filename_base}.csv")
        parquet_file = os.path.join(CACHE_DIR, f"{filename_base}.parquet")
        if os.path.exists(csv_file):
            return csv_file, "csv", "local_csv"
        elif os.path.exists(parquet_file):
            return parquet_file, "parquet", "local_parquet"
    else:
        # auto: 优先 parquet，如果不存在则使用 csv
        parquet_file = os.path.join(CACHE_DIR, f"{filename_base}.parquet")
        csv_file = os.path.join(CACHE_DIR, f"{filename_base}.csv")
        if os.path.exists(parquet_file):
            return parquet_file, "parquet", "local_parquet"
        elif os.path.exists(csv_file):
            return csv_file, "csv", "local_csv"
    
    return None, "csv", "api"


def _load_cache_file(file_path: str, file_format: str, code: Optional[str] = None) -> pd.DataFrame:
    """
    加载缓存文件
    
    Args:
        file_path: 文件路径
        file_format: 文件格式 "csv" 或 "parquet"
        code: 股票代码（用于从包含多股票的文件中筛选）
        
    Returns:
        DataFrame
    """
    try:
        if file_format == "parquet":
            try:
                df = pd.read_parquet(file_path)
            except ImportError:
                logger.warning("pyarrow 或 fastparquet 未安装，无法读取 parquet 文件，请安装: pip install pyarrow")
                return pd.DataFrame()
            except Exception as e:
                logger.warning(f"读取 parquet 文件失败 {file_path}: {e}")
                return pd.DataFrame()
            
            # 处理 MultiIndex（分钟线数据可能是 (trade_date, trade_time) 的 MultiIndex）
            if isinstance(df.index, pd.MultiIndex):
                # 先重置索引，将 MultiIndex 转换为列
                df = df.reset_index()
                logger.debug(f"检测到 MultiIndex，索引层级: {df.index.names if hasattr(df.index, 'names') else 'N/A'}")
                
                # 如果文件包含多股票数据（有 ts_code 列），需要筛选
                if code and "ts_code" in df.columns:
                    ts_code = _code_to_ts_code(code)
                    df = df[df["ts_code"] == ts_code].copy()
                    # 删除 ts_code 列（如果存在）
                    if "ts_code" in df.columns:
                        df = df.drop(columns=["ts_code"])
                    logger.debug(f"从多股票文件中筛选 {code} ({ts_code}) 的数据，共 {len(df)} 条")
                
                # 检查是否有 trade_time 层级（分钟线数据）
                if "trade_time" in df.columns:
                    # 使用 trade_time 作为索引（分钟线数据）
                    df["trade_time"] = pd.to_datetime(df["trade_time"])
                    df = df.set_index("trade_time")
                    logger.debug("使用 trade_time 作为索引（分钟线数据）")
                elif "trade_date" in df.columns:
                    # 使用 trade_date 作为索引（日线数据）
                    df["trade_date"] = pd.to_datetime(df["trade_date"])
                    df = df.set_index("trade_date")
                    logger.debug("使用 trade_date 作为索引（日线数据）")
                else:
                    # 使用第一层作为索引
                    first_col = df.columns[0]
                    df[first_col] = pd.to_datetime(df[first_col])
                    df = df.set_index(first_col)
                    logger.debug(f"使用第一列 {first_col} 作为索引")
            else:
                # 如果文件包含多股票数据（有 ts_code 列），需要筛选
                if code and "ts_code" in df.columns:
                    ts_code = _code_to_ts_code(code)
                    df = df[df["ts_code"] == ts_code].copy()
                    # 删除 ts_code 列（如果存在）
                    if "ts_code" in df.columns:
                        df = df.drop(columns=["ts_code"])
                    logger.debug(f"从多股票文件中筛选 {code} ({ts_code}) 的数据，共 {len(df)} 条")
            
            # 处理日期列（tushare 格式可能是 trade_date 或 trade_time）
            if "trade_time" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
                # 分钟线数据：使用 trade_time
                df["trade_time"] = pd.to_datetime(df["trade_time"])
                df = df.set_index("trade_time")
                logger.debug("使用 trade_time 列作为索引")
            elif "trade_date" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
                # 日线数据：使用 trade_date
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                df = df.set_index("trade_date")
                logger.debug("使用 trade_date 列作为索引")
            elif "datetime" in df.columns and not isinstance(df.index, pd.DatetimeIndex):
                df = df.set_index("datetime")
                logger.debug("使用 datetime 列作为索引")
            
            # 确保索引是 datetime 类型
            if not isinstance(df.index, pd.DatetimeIndex):
                try:
                    if df.index.name == "datetime" or df.index.name is None or df.index.name == "trade_time" or df.index.name == "trade_date":
                        df.index = pd.to_datetime(df.index)
                    else:
                        # 尝试转换索引
                        df.index = pd.to_datetime(df.index)
                except (ValueError, TypeError) as e:
                    logger.warning(f"无法将索引转换为 datetime: {e}，尝试重置索引")
                    # 如果转换失败，尝试重置索引
                    df = df.reset_index()
                    if "trade_time" in df.columns:
                        df = df.set_index(pd.to_datetime(df["trade_time"]))
                    elif "trade_date" in df.columns:
                        df = df.set_index(pd.to_datetime(df["trade_date"]))
                    elif "datetime" in df.columns:
                        df = df.set_index(pd.to_datetime(df["datetime"]))
            
            # 确保列名正确（tushare 可能使用不同的列名）
            column_mapping = {
                "vol": "volume",  # tushare 使用 vol 表示成交量
            }
            df = df.rename(columns=column_mapping)
            
            # 删除不需要的列（如果存在）
            # trade_date 列在分钟线数据中通常不需要（因为索引已经是 trade_time）
            # 但保留它以便需要时使用
            
        else:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        return df
    except Exception as e:
        logger.warning(f"读取缓存文件失败 {file_path}: {e}")
        return pd.DataFrame()


def _save_cache_file(df: pd.DataFrame, file_path: str, file_format: str) -> None:
    """
    保存缓存文件
    
    Args:
        df: 要保存的 DataFrame
        file_path: 文件路径
        file_format: 文件格式 "csv" 或 "parquet"
    """
    try:
        if file_format == "parquet":
            try:
                df.to_parquet(file_path, index=True)
            except ImportError:
                logger.warning("pyarrow 或 fastparquet 未安装，无法保存 parquet 文件，回退到 CSV 格式")
                # 回退到 CSV
                csv_path = file_path.replace(".parquet", ".csv")
                df.to_csv(csv_path)
            except Exception as e:
                logger.error(f"保存 parquet 文件失败 {file_path}: {e}，尝试保存为 CSV")
                # 回退到 CSV
                csv_path = file_path.replace(".parquet", ".csv")
                df.to_csv(csv_path)
        else:
            df.to_csv(file_path)
    except Exception as e:
        logger.error(f"保存缓存文件失败 {file_path}: {e}")


def load_daily_df_with_cache(
    code: str, 
    start: str = "2019-01-01", 
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    加载日线数据（带缓存）
    
    支持从 Parquet 或 CSV 格式的缓存文件加载数据
    优先使用 Parquet 格式（如果存在）
    
    Args:
        code: 股票代码
        start: 开始日期，格式 "YYYY-MM-DD"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含日线数据的 DataFrame，索引为 datetime
    """
    # 查找缓存文件（支持 parquet 和 csv）
    cache_file, file_format, data_source = _find_cache_file(code, period=None)
    
    # 读取缓存
    if cache_file:
        df = _load_cache_file(cache_file, file_format, code=code)
        if not df.empty:
            source_desc = {
                "external_parquet": "外部 Parquet",
                "local_parquet": "本地 Parquet",
                "local_csv": "本地 CSV"
            }.get(data_source, data_source)
            logger.info(f"[{code} 日线] 数据来源: {source_desc} | 文件: {cache_file} | 共 {len(df)} 条")
    else:
        df = pd.DataFrame()
    
    # 如果是从外部 parquet 加载的，直接返回（不需要缓存和更新）
    if data_source == "external_parquet" and not df.empty:
        # 过滤到请求的日期范围
        start_dt = pd.to_datetime(start)
        if end is None:
            end_dt = pd.Timestamp.today()
        else:
            end_dt = pd.to_datetime(end)
        df = df[(df.index >= start_dt) & (df.index <= end_dt)]
        return df
    
    # 确定需要获取的日期范围
    fetch_start = start
    if not df.empty:
        last_dt = df.index.max()
        fetch_start = (last_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    
    if end is None:
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    
    # 如果需要获取新数据
    if df.empty or fetch_start <= end:
        logger.info(f"[{code} 日线] 数据来源: API ({Config.DATA_SOURCE}) | 获取日期范围: {fetch_start} 至 {end}")
        try:
            new_df = load_ashare_daily(code, fetch_start, end)
            if not new_df.empty:
                # 合并数据并去重
                df = pd.concat([df, new_df])
                df = df[~df.index.duplicated()].sort_index()
                
                # 保存缓存（使用配置的格式偏好）
                # 注意：如果原始数据来源是外部 parquet，不保存缓存
                if data_source != "external_parquet":
                    save_format = Config.CACHE_FORMAT_PREFERENCE
                    if save_format == "auto":
                        save_format = "parquet"  # 默认使用 parquet
                    
                    if save_format == "parquet":
                        cache_file = os.path.join(CACHE_DIR, f"{code}.parquet")
                    else:
                        cache_file = os.path.join(CACHE_DIR, f"{code}.csv")
                    
                    _save_cache_file(df, cache_file, save_format)
                    logger.info(f"已更新 {code} 日线缓存（{save_format.upper()}格式），共 {len(df)} 条")
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
    
    支持从 Parquet 或 CSV 格式的缓存文件加载数据
    优先使用 Parquet 格式（如果存在）
    
    Args:
        code: 股票代码
        period: 周期，支持 "1min", "5min", "15min", "30min", "60min"
        start: 开始日期时间，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含分钟线数据的 DataFrame，索引为 datetime
    """
    # 查找缓存文件（支持 parquet 和 csv）
    cache_file, file_format, data_source = _find_cache_file(code, period=period)
    
    # 读取缓存
    if cache_file:
        df = _load_cache_file(cache_file, file_format, code=code)
        if not df.empty:
            source_desc = {
                "external_parquet": "外部 Parquet",
                "local_parquet": "本地 Parquet",
                "local_csv": "本地 CSV"
            }.get(data_source, data_source)
            logger.info(f"[{code} {period}] 数据来源: {source_desc} | 文件: {cache_file} | 共 {len(df)} 条")
    else:
        df = pd.DataFrame()
    
    # 确定需要获取的时间范围
    start_dt = pd.to_datetime(start)
    if end is None:
        end_dt = pd.Timestamp.today()
        end = end_dt.strftime("%Y-%m-%d")
    else:
        end_dt = pd.to_datetime(end)
    
    # 如果是从外部 parquet 加载的，直接返回（不需要缓存和更新）
    if data_source == "external_parquet" and not df.empty:
        # 过滤到请求的日期范围
        df = df[(df.index >= start_dt) & (df.index <= end_dt)]
        return df
    
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
        logger.info(f"[{code} {period}] 数据来源: API ({Config.DATA_SOURCE}) | 获取日期范围: {fetch_start} 至 {end}")
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
                
                # 保存缓存（使用配置的格式偏好）
                # 注意：如果原始数据来源是外部 parquet，不保存缓存
                if not df.empty and data_source != "external_parquet":
                    save_format = Config.CACHE_FORMAT_PREFERENCE
                    if save_format == "auto":
                        save_format = "parquet"  # 默认使用 parquet
                    
                    if save_format == "parquet":
                        cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.parquet")
                    else:
                        cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.csv")
                    
                    _save_cache_file(df, cache_file, save_format)
                    logger.info(f"已更新 {code} {period} 分钟线缓存（{save_format.upper()}格式），共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
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
                            logger.info(f"[{code} {period}] 数据来源: API ({Config.DATA_SOURCE}) | 强制重新获取日期范围: {start} 至 {end}")
                            new_df = load_ashare_minute(code, period, start, end)
                            if not new_df.empty:
                                df = new_df
                                df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                                if not df.empty:
                                    # 保存缓存（使用配置的格式偏好）
                                    # 注意：如果原始数据来源是外部 parquet，不保存缓存
                                    if data_source != "external_parquet":
                                        save_format = Config.CACHE_FORMAT_PREFERENCE
                                        if save_format == "auto":
                                            save_format = "parquet"
                                        
                                        if save_format == "parquet":
                                            cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.parquet")
                                        else:
                                            cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.csv")
                                        
                                        _save_cache_file(df, cache_file, save_format)
                                        logger.info(f"已重新获取并更新 {code} {period} 分钟线缓存（{save_format.upper()}格式），共 {len(df)} 条")
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
                logger.info(f"[{code} {period}] 数据来源: API ({Config.DATA_SOURCE}) | 重新获取日期范围: {start} 至 {end}")
                new_df = load_ashare_minute(code, period, start, end)
                if not new_df.empty:
                    df = new_df
                    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                    if not df.empty:
                        # 保存缓存（使用配置的格式偏好）
                        # 注意：如果原始数据来源是外部 parquet，不保存缓存
                        if data_source != "external_parquet":
                            save_format = Config.CACHE_FORMAT_PREFERENCE
                            if save_format == "auto":
                                save_format = "parquet"
                            
                            if save_format == "parquet":
                                cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.parquet")
                            else:
                                cache_file = os.path.join(CACHE_DIR, f"{code}_{period}.csv")
                            
                            _save_cache_file(df, cache_file, save_format)
                            logger.info(f"已重新获取并更新 {code} {period} 分钟线缓存（{save_format.upper()}格式），共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
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
