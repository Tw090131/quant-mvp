"""
上下文模块
提供策略执行时的上下文信息
"""
from typing import Dict
import pandas as pd
from engine.portfolio import Portfolio


class Context:
    """
    上下文类
    
    在策略执行时提供组合和数据信息
    """
    
    def __init__(self, portfolio: Portfolio, data: Dict[str, pd.DataFrame]):
        """
        初始化上下文
        
        Args:
            portfolio: 组合对象
            data: 股票数据字典
        """
        self.portfolio = portfolio
        self.data = data
