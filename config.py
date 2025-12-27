"""
配置管理模块
提供统一的配置管理和日志初始化
"""
import os
import logging
from typing import Optional
from pathlib import Path


class Config:
    """全局配置类"""
    
    # 数据相关
    CACHE_DIR = "data_cache"
    DATA_DIR = "data"
    
    # 数据源选择：'akshare' 或 'tushare'
    DATA_SOURCE = "akshare"  # 默认使用 akshare
    
    # 回测默认参数
    DEFAULT_INIT_CASH = 1_000_000
    # 手续费配置
    # 方式1：固定费率模式（旧模式，兼容）
    DEFAULT_FEE_RATE = 0.001  # 0.1%
    # 方式2：费率+固定费用模式（新模式，如"万1+5"）
    DEFAULT_FEE_RATE_PCT = 0.0001  # 万1，即0.01%（成交金额的万分之一）
    DEFAULT_FEE_FIXED = 5.0  # 每笔固定费用5元
    # 手续费模式：'rate'（仅费率）或 'rate+fixed'（费率+固定费用）
    DEFAULT_FEE_MODE = 'rate+fixed'  # 默认使用"万1+5"模式
    
    # 风控默认参数
    DEFAULT_MAX_POSITION_PCT = 0.3
    DEFAULT_MAX_DRAWDOWN = 0.2
    DEFAULT_STOP_LOSS_PCT = None
    
    # 输出文件
    DEFAULT_TRADE_LOG_CSV = "daily_trades.csv"
    DEFAULT_EQUITY_CSV = "equity_curve.csv"
    DEFAULT_PNL_CSV = "daily_pnl.csv"
    
    # 日志配置
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE: Optional[str] = None  # None 表示只输出到控制台
    
    @classmethod
    def setup_logging(
        cls,
        level: Optional[int] = None,
        log_file: Optional[str] = None,
        format_str: Optional[str] = None
    ) -> None:
        """
        配置日志系统
        
        Args:
            level: 日志级别，None 则使用默认值
            log_file: 日志文件路径，None 表示只输出到控制台
            format_str: 日志格式，None 则使用默认格式
        """
        level = level or cls.LOG_LEVEL
        format_str = format_str or cls.LOG_FORMAT
        log_file = log_file or cls.LOG_FILE
        
        # 配置根日志器
        logging.basicConfig(
            level=level,
            format=format_str,
            handlers=[
                logging.StreamHandler(),
                *([logging.FileHandler(log_file)] if log_file else [])
            ],
            force=True  # 覆盖已有配置
        )
        
        # 设置第三方库日志级别
        logging.getLogger("akshare").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    @classmethod
    def ensure_dirs(cls) -> None:
        """确保必要的目录存在"""
        Path(cls.CACHE_DIR).mkdir(parents=True, exist_ok=True)
        Path(cls.DATA_DIR).mkdir(parents=True, exist_ok=True)


# 初始化配置
Config.ensure_dirs()

