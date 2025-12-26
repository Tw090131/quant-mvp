"""
调度器模块
提供定时任务调度功能
"""
from typing import Callable, List, Tuple
import pandas as pd


class Scheduler:
    """
    调度器类
    
    用于在特定时间执行任务，主要用于实盘交易场景
    """
    
    def __init__(self):
        """初始化调度器"""
        self.daily_tasks: List[Tuple[Callable, str]] = []  # 普通定时任务
        self.after_close_tasks: List[Callable] = []  # 收盘后任务

    def run_daily(self, func: Callable, time: str) -> None:
        """
        注册每日定时任务
        
        Args:
            func: 要执行的函数
            time: 执行时间，格式 'HH:MM' 或特殊值 'after_close'（收盘后）
        """
        if time == "after_close":
            # 收盘后任务
            self.after_close_tasks.append(func)
        elif isinstance(time, str) and len(time) == 5 and time[2] == ":":
            # 普通定时任务
            self.daily_tasks.append((func, time))
        else:
            raise ValueError(f"时间格式错误，应为 'HH:MM' 或 'after_close'，当前值: {time}")

    def on_bar(self, dt: pd.Timestamp, context) -> None:
        """
        在每个 bar 调用，检查是否需要执行定时任务
        
        Args:
            dt: 当前时间
            context: 上下文对象
        """
        # 执行普通定时任务
        for func, time_str in self.daily_tasks:
            if self._match_time(dt, time_str):
                try:
                    func(context)
                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"执行定时任务失败 {time_str}: {e}", exc_info=True)
    
    def on_after_close(self, context) -> None:
        """
        收盘后执行任务
        
        Args:
            context: 上下文对象
        """
        for func in self.after_close_tasks:
            try:
                func(context)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"执行收盘后任务失败: {e}", exc_info=True)

    def _match_time(self, dt: pd.Timestamp, time_str: str) -> bool:
        """
        检查时间是否匹配
        
        Args:
            dt: 当前时间
            time_str: 目标时间字符串 'HH:MM'
            
        Returns:
            True 表示匹配
        """
        if not isinstance(dt, pd.Timestamp):
            return False
        
        try:
            # 提取小时和分钟
            current_time = dt.strftime("%H:%M")
            return current_time == time_str
        except (AttributeError, ValueError):
            return False
