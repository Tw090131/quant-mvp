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
        logger.info(f"{code} 日线数据获取成功，共 {len(df)} 条，日期范围: {df.index.min()} 至 {df.index.max()}")
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

    # akshare 的 start_date 和 end_date 格式应该是 "YYYY-MM-DD HH:MM:SS"
    # 如果只提供了日期，需要添加时间部分
    ak_start_date = start
    if start:
        if " " not in start:
            # 只有日期，添加默认时间 09:30:00（开盘时间）
            ak_start_date = f"{start} 09:30:00"
            logger.info(f"{code} {period} 添加时间部分: {start} -> {ak_start_date}")
        else:
            ak_start_date = start
    
    # 处理 end_date
    ak_end_date = end
    if end:
        if " " not in end:
            # 只有日期，添加默认时间 15:00:00（收盘时间）
            ak_end_date = f"{end} 15:00:00"
            logger.info(f"{code} {period} 添加结束时间部分: {end} -> {ak_end_date}")
        else:
            ak_end_date = end
    else:
        # 如果没有指定 end_date，使用默认值（今天收盘时间）
        from datetime import datetime
        ak_end_date = datetime.now().strftime("%Y-%m-%d 15:00:00")

    try:
        logger.info(f"{code} {period} 调用 akshare API: symbol={code}, start_date={ak_start_date}, end_date={ak_end_date}, period={ak_period}")
        df = ak.stock_zh_a_hist_min_em(
            symbol=code,
            start_date=ak_start_date,
            end_date=ak_end_date,
            period=ak_period, 
            adjust="qfq"
        )
        data_count = len(df) if df is not None and not df.empty else 0
        logger.info(f"{code} {period} akshare API 返回数据: {data_count} 条")
        
        # 如果返回数据为空，尝试不指定 end_date（使用默认值）
        if data_count == 0 and end:
            logger.warning(f"{code} {period} 指定 end_date 后返回空数据，尝试不指定 end_date...")
            try:
                df = ak.stock_zh_a_hist_min_em(
                    symbol=code,
                    start_date=ak_start_date,
                    end_date="2222-01-01 09:32:00",  # 使用默认的最大值
                    period=ak_period, 
                    adjust="qfq"
                )
                data_count = len(df) if df is not None and not df.empty else 0
                logger.info(f"{code} {period} 不指定 end_date 后返回数据: {data_count} 条")
            except Exception as e2:
                logger.warning(f"{code} {period} 不指定 end_date 后仍然失败: {e2}")
     
    except Exception as e:
        logger.error(f"{code} {period}分钟线获取失败: {e}", exc_info=True)
        return pd.DataFrame()

    if df is None or df.empty:
        logger.warning(f"{code} {period}分钟线数据为空（start_date={ak_start_date}, end_date={ak_end_date}）")
        logger.warning(f"{code} {period} 可能的原因：")
        logger.warning(f"  1. akshare 的分钟线数据可能不支持获取过久远的历史数据（建议使用最近1-3个月的数据）")
        logger.warning(f"  2. 该股票在指定时间段可能没有交易数据")
        logger.warning(f"  3. 建议：尝试更近的日期范围，或使用日线数据进行回测")
        return pd.DataFrame()
    
    if "时间" not in df.columns:
        logger.warning(f"{code} {period}分钟线数据缺失 '时间' 列，可用列: {list(df.columns)}")
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
