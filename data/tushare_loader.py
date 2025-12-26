"""
tusharePro 数据加载模块
提供从 tusharePro 获取股票数据的功能

使用前需要：
1. pip install tushare
2. 设置 tushare token: import tushare as ts; ts.set_token('your_token')
"""
import logging
from typing import Optional
import pandas as pd

# 配置日志
logger = logging.getLogger(__name__)

# 延迟导入 tushare，避免未安装时出错
try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    logger.warning("tushare 未安装，请使用: pip install tushare")


def _check_tushare():
    """检查 tushare 是否可用"""
    if not TUSHARE_AVAILABLE:
        raise ImportError("tushare 未安装，请使用: pip install tushare")
    try:
        # 检查是否设置了 token
        pro = ts.pro_api()
        return pro
    except Exception as e:
        raise RuntimeError(f"tushare 初始化失败，请先设置 token: ts.set_token('your_token'), 错误: {e}")


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


# === 日线 ===
def load_ashare_daily(code: str, start: str, end: Optional[str] = None) -> pd.DataFrame:
    """
    加载 A 股日线数据（使用 tusharePro）
    
    Args:
        code: 股票代码
        start: 开始日期，格式 "YYYY-MM-DD"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含日线数据的 DataFrame，索引为 datetime
    """
    pro = _check_tushare()
    ts_code = _code_to_ts_code(code)
    
    try:
        # 转换日期格式：YYYY-MM-DD -> YYYYMMDD
        start_date = start.replace("-", "")
        end_date = end.replace("-", "") if end else None
        
        if end_date is None:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")
        
        logger.info(f"{code} 从 tushare 获取日线数据: {start_date} 至 {end_date}")
        
        # 调用 tushare API
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            adj='qfq'  # 前复权
        )
        
        if df is None or df.empty:
            logger.warning(f"{code} 日线数据为空")
            return pd.DataFrame()
        
        # 重命名列以匹配标准格式
        df = df.rename(columns={
            "trade_date": "datetime",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "vol": "volume",  # tushare 使用 vol 表示成交量
        })
        
        # 转换日期格式
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d")
        df = df.set_index("datetime").sort_index()
        
        # 只保留需要的列
        required_cols = ["open", "close", "high", "low", "volume"]
        df = df[required_cols]
        
        logger.info(f"{code} 日线数据获取成功，共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
        return df
        
    except Exception as e:
        logger.error(f"获取 {code} 日线数据失败: {e}", exc_info=True)
        raise


# === 分钟线 ===
def load_ashare_minute(
    code: str, 
    period: str = "1min", 
    start: Optional[str] = None, 
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    加载 A 股分钟线数据（使用 tusharePro）
    
    Args:
        code: 股票代码
        period: 周期，支持 "1min", "5min", "15min", "30min", "60min"
        start: 开始日期时间，格式 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
        end: 结束日期，格式 "YYYY-MM-DD"，None 表示今天
        
    Returns:
        包含分钟线数据的 DataFrame，索引为 datetime
    """
    pro = _check_tushare()
    ts_code = _code_to_ts_code(code)
    
    # tushare 的周期映射
    period_map = {
        "1min": "1",
        "5min": "5",
        "15min": "15",
        "30min": "30",
        "60min": "60"
    }
    
    if period not in period_map:
        raise ValueError(f"不支持的 period: {period}, 支持 {list(period_map.keys())}")
    
    freq = period_map[period]
    
    try:
        # 处理日期时间格式
        # tushare 的 stk_mins 需要 start_time 和 end_time，格式为 "YYYYMMDDHHMMSS"
        from datetime import datetime, timedelta
        
        if start:
            if " " in start:
                # 包含时间，转换为 tushare 格式
                start_dt = pd.to_datetime(start)
                start_time = start_dt.strftime("%Y%m%d%H%M%S")
            else:
                # 只有日期，使用当天开盘时间 09:30:00
                start_dt = pd.to_datetime(start)
                start_time = start_dt.strftime("%Y%m%d") + "093000"
        else:
            # 默认最近30天
            start_dt = datetime.now() - timedelta(days=30)
            start_time = start_dt.strftime("%Y%m%d") + "093000"
        
        if end:
            # 结束时间使用当天收盘时间 15:00:00
            end_dt = pd.to_datetime(end)
            end_time = end_dt.strftime("%Y%m%d") + "150000"
        else:
            # 默认今天收盘时间
            end_dt = datetime.now()
            end_time = end_dt.strftime("%Y%m%d") + "150000"
        
        logger.info(f"{code} 从 tushare 获取 {period} 分钟线数据: {start_time} 至 {end_time}")
        
        # 调用 tushare API
        # 注意：tushare 的 stk_mins 使用 start_time 和 end_time，不是 start_date 和 end_date
        df = pro.stk_mins(
            ts_code=ts_code,
            freq=freq,
            start_time=start_time,
            end_time=end_time,
            adj='qfq'  # 前复权
        )
        
        if df is None or df.empty:
            logger.warning(f"{code} {period}分钟线数据为空")
            return pd.DataFrame()
        
        # 重命名列
        df = df.rename(columns={
            "trade_time": "datetime",
            "open": "open",
            "close": "close",
            "high": "high",
            "low": "low",
            "vol": "volume",
        })
        
        # 转换日期格式
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime").sort_index()
        
        # 只保留需要的列
        required_cols = ["open", "close", "high", "low", "volume"]
        df = df[required_cols]
        
        # 如果指定了结束日期，过滤数据到指定日期范围
        if start:
            start_dt = pd.to_datetime(start)
            df = df[df.index >= start_dt]
        if end:
            end_dt = pd.to_datetime(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            df = df[df.index <= end_dt]
        
        if not df.empty:
            logger.info(f"{code} {period}分钟线数据获取成功，共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
        else:
            logger.warning(f"{code} {period}分钟线数据获取后为空（可能被日期过滤）")
        return df
        
    except Exception as e:
        logger.error(f"获取 {code} {period}分钟线数据失败: {e}", exc_info=True)
        return pd.DataFrame()

