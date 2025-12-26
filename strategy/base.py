"""
策略基类模块
所有策略必须继承自 StrategyBase
"""
from abc import ABC, abstractmethod
from typing import Dict, Callable, Optional
import pandas as pd
from engine.scheduler import Scheduler
from engine.context import Context


class StrategyBase(ABC):
    """
    策略基类
    
    所有策略必须继承此类并实现 on_bar 方法
    支持 run_daily 定时任务功能
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
        self.scheduler = Scheduler()  # 定时任务调度器
        self.context: Optional[Context] = None  # 上下文对象，由回测引擎设置
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

    def run_daily(
        self, 
        func: Callable, 
        time: str = "09:30", 
        reference_security: Optional[str] = None
    ) -> None:
        """
        注册每日定时任务（类似聚宽风格）
        
        Args:
            func: 要执行的函数，函数签名应为 func(context)
            time: 执行时间，格式 'HH:MM'，例如 '09:58'
            reference_security: 参考证券（保留参数，用于兼容聚宽接口，当前版本未使用）
            
        示例:
            def market_open(context):
                # 在每天9:58执行的逻辑
                pass
            
            run_daily(market_open, time='09:58')
        """
        self.scheduler.run_daily(func, time)

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
