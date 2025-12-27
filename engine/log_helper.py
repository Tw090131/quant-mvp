"""
日志辅助模块
提供统一的日志格式化功能，自动添加回测日期
"""
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def format_log_msg(msg: str, dt: Optional[pd.Timestamp] = None) -> str:
    """
    格式化日志消息，自动添加日期信息
    
    Args:
        msg: 原始日志消息
        dt: 日期时间戳，如果为 None 则不添加日期
        
    Returns:
        格式化后的日志消息
    """
    if dt is not None:
        if isinstance(dt, pd.Timestamp):
            # 如果是分钟线数据，显示完整时间；如果是日线数据，只显示日期
            if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
                date_str = dt.strftime("%Y-%m-%d")
            else:
                date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            return f"[{date_str}] {msg}"
    return msg


def log_info(msg: str, dt: Optional[pd.Timestamp] = None):
    """记录 INFO 级别日志，自动添加日期"""
    logger.info(format_log_msg(msg, dt))


def log_warning(msg: str, dt: Optional[pd.Timestamp] = None):
    """记录 WARNING 级别日志，自动添加日期"""
    logger.warning(format_log_msg(msg, dt))


def log_error(msg: str, dt: Optional[pd.Timestamp] = None):
    """记录 ERROR 级别日志，自动添加日期"""
    logger.error(format_log_msg(msg, dt))


def log_debug(msg: str, dt: Optional[pd.Timestamp] = None):
    """记录 DEBUG 级别日志，自动添加日期"""
    logger.debug(format_log_msg(msg, dt))

