"""
使用 run_daily 定时任务的示例策略
演示如何在每天指定时间执行策略逻辑
"""
import logging
from typing import Dict
import pandas as pd
from strategy.base import StrategyBase

logger = logging.getLogger(__name__)


class DailyStrategy(StrategyBase):
    """
    每日定时策略示例
    
    在每天9:58执行 market_open 函数
    """
    
    def __init__(self, datas: Dict[str, pd.DataFrame]):
        super().__init__(datas)
        
        # 注册定时任务：每天9:58执行
        self.run_daily(self.market_open, time='09:58')
        
        # 可以注册多个定时任务
        # self.run_daily(self.market_close, time='14:55')
    
    def market_open(self, context):
        """
        开盘前执行的逻辑（每天9:58执行）
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        logger.info(f"执行 market_open，当前资产: {context.portfolio.total_value():,.2f}")
        # 在这里可以执行开盘前的逻辑，比如：
        # - 获取市场数据
        # - 计算信号
        # - 准备交易计划
        # 注意：这里不能直接交易，交易需要在 on_bar 中返回权重
    
    def market_close(self, context):
        """
        收盘前执行的逻辑（每天14:55执行）
        
        Args:
            context: 上下文对象
        """
        logger.info(f"执行 market_close，当前资产: {context.portfolio.total_value():,.2f}")
        # 收盘前的逻辑
    
    def on_bar(self, dt: pd.Timestamp) -> Dict[str, float]:
        """
        每个 bar 调用一次，生成交易信号
        
        Args:
            dt: 当前时间点
            
        Returns:
            目标权重字典
        """
        targets = {}
        
        # 示例：简单的买入逻辑
        # 在实际使用中，可以根据 market_open 中计算的信号来决定权重
        
        for code, df in self.datas.items():
            if dt not in df.index:
                continue
            
            try:
                row = df.loc[dt]
                # 简单的策略：如果当前价格高于开盘价，买入
                if row["close"] > row["open"]:
                    targets[code] = 0.5  # 50% 仓位
            except Exception as e:
                logger.warning(f"{code} 在 {dt} 处理失败: {e}")
        
        return targets

