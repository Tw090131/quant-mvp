"""
双均线策略
支持 run_daily 定时任务功能
"""
import logging
from typing import Dict
import pandas as pd
from strategy.base import StrategyBase

logger = logging.getLogger(__name__)


class MaCross(StrategyBase):
    """
    多股票双均线策略
    每只股票独立判断，返回权重
    支持 run_daily 定时任务，可在每天指定时间执行策略逻辑
    """

    def __init__(self, datas: Dict[str, pd.DataFrame], short: int = 5, long: int = 20, weight: float = 0.5):
        super().__init__(datas)
        assert short < long
        self.short = short
        self.long = long
        self.weight = weight
        
        # 用于存储定时任务中计算的信号
        self.daily_signals = {}

        # === 注册定时任务：每天9:58执行 ===
        # 注意：需要分钟线数据才能触发定时任务
        self.run_daily(self.market_open, time='09:58')

        # === 预计算均线 ===
        valid_codes = []
        for code, df in self.datas.items():
            if df.empty or "close" not in df.columns:
                logger.warning(f"{code} 数据为空或缺失 'close'，跳过")
                continue

            df["ma_short"] = df["close"].rolling(self.short).mean()
            df["ma_long"] = df["close"].rolling(self.long).mean()
            valid_codes.append(code)

        # 只保留有效数据
        self.datas = {code: self.datas[code] for code in valid_codes}
        
        logger.info(f"MaCross 策略初始化完成，股票数量: {len(valid_codes)}, 短期均线: {short}, 长期均线: {long}")

    def market_open(self, context):
        """
        开盘前执行的逻辑（每天9:58执行）
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        logger.info(f"执行 market_open，当前资产: {context.portfolio.total_value():,.2f}")
        
        # 计算当天的交易信号
        current_date = pd.Timestamp.now().date() if hasattr(context, 'current_date') else None
        signals = {}
        
        for code, df in self.datas.items():
            if df.empty:
                continue
            
            # 获取最新的均线数据
            latest_idx = df.index[-1]
            if pd.isna(df.loc[latest_idx, "ma_short"]) or pd.isna(df.loc[latest_idx, "ma_long"]):
                continue
            
            ma_short = df.loc[latest_idx, "ma_short"]
            ma_long = df.loc[latest_idx, "ma_long"]
            
            # 判断信号
            if ma_short > ma_long:
                signals[code] = {
                    "signal": "BUY",
                    "ma_short": ma_short,
                    "ma_long": ma_long,
                    "strength": (ma_short - ma_long) / ma_long  # 信号强度
                }
                logger.debug(f"{code} 产生买入信号，短期均线: {ma_short:.2f}, 长期均线: {ma_long:.2f}")
        
        # 保存信号供 on_bar 使用
        self.daily_signals = signals
        
        if signals:
            logger.info(f"market_open 计算完成，产生 {len(signals)} 个买入信号")

    def on_bar(self, dt: pd.Timestamp) -> Dict[str, float]:
        """
        每个 bar 调用一次，生成交易信号
        
        Args:
            dt: 当前时间点
            
        Returns:
            目标权重字典 {code: weight}
        """
        targets = {}
        
        for code, df in self.datas.items():
            if dt not in df.index:
                continue

            row = df.loc[dt]
            if pd.isna(row["ma_short"]) or pd.isna(row["ma_long"]):
                continue

            # 双均线策略：短期均线上穿长期均线时买入
            if row["ma_short"] > row["ma_long"]:
                targets[code] = self.weight

        return targets
