"""
策略基类模块
所有策略必须继承自 StrategyBase
"""
from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class StrategyBase(ABC):
    """
    策略基类
    
    所有策略必须继承此类并实现 on_bar 方法
    """
    
    def __init__(self, datas: Dict[str, pd.DataFrame]):
        """
        初始化策略
        
        Args:
            datas: 股票数据字典，{code: DataFrame}，DataFrame 索引为 datetime
        """
        if not datas:
            raise ValueError("数据字典为空")
        
        self.datas = datas
        self._validate_data()

    def _validate_data(self) -> None:
        """
        验证数据格式
        """
        required_columns = ["open", "close", "high", "low", "volume"]
        for code, df in self.datas.items():
            if df.empty:
                raise ValueError(f"{code} 数据为空")
            missing = [col for col in required_columns if col not in df.columns]
            if missing:
                raise ValueError(f"{code} 缺少必需的列: {missing}")

    @abstractmethod
    def on_bar(self, dt: pd.Timestamp) -> Dict[str, float]:
        """
        每个 bar 调用一次，生成交易信号
        
        Args:
            dt: 当前时间点
            
        Returns:
            目标权重字典 {code: weight}，weight 为 0-1 之间的浮点数
            所有股票的权重之和可以超过 1（表示加杠杆），也可以小于 1（表示部分资金闲置）
        """
        raise NotImplementedError
