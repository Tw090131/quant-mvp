"""
双均线策略（金叉死叉策略）
支持 run_daily 定时任务功能
完善版本：包含买入卖出信号、止损止盈、持仓管理等功能
"""
import logging
from typing import Dict, Optional
import pandas as pd
import numpy as np
from strategy.base import StrategyBase

logger = logging.getLogger(__name__)


class MaCross(StrategyBase):
    """
    多股票双均线策略（金叉死叉策略）
    
    策略逻辑：
    1. 金叉（买入信号）：短期均线上穿长期均线
    2. 死叉（卖出信号）：短期均线下穿长期均线
    3. 支持止损止盈
    4. 支持持仓管理
    
    每只股票独立判断，返回权重
    支持 run_daily 定时任务，可在每天指定时间执行策略逻辑
    """

    def __init__(
        self, 
        datas: Dict[str, pd.DataFrame], 
        short: int = 5, 
        long: int = 20, 
        weight: float = 0.5,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        use_volume_filter: bool = False,
        min_volume_ratio: float = 1.0
    ):
        """
        初始化双均线策略
        
        Args:
            datas: 股票数据字典
            short: 短期均线周期
            long: 长期均线周期
            weight: 单只股票的最大权重（0-1之间）
            stop_loss: 止损比例（例如 0.05 表示亏损5%时止损），None 表示不启用
            take_profit: 止盈比例（例如 0.15 表示盈利15%时止盈），None 表示不启用
            use_volume_filter: 是否使用成交量过滤（成交量需要大于平均值）
            min_volume_ratio: 最小成交量比例（相对于均量）
        """
        super().__init__(datas)
        assert short < long, "短期均线周期必须小于长期均线周期"
        assert 0 < weight <= 1, "权重必须在 (0, 1] 之间"
        
        self.short = short
        self.long = long
        self.weight = weight
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.use_volume_filter = use_volume_filter
        self.min_volume_ratio = min_volume_ratio
        
        # 用于存储定时任务中计算的信号
        self.daily_signals = {}
        
        # 用于存储每只股票的买入价格（用于止损止盈判断）
        self.entry_prices: Dict[str, float] = {}

        # === 注册定时任务：每天9:58执行 ===
        # 注意：需要分钟线数据才能触发定时任务
        # generate_signal=True 表示在该时间点产生交易信号
        self.run_daily(self.market_open, time='09:58', generate_signal=True)
        
        # === 注册收盘后任务 ===
        # 收盘后执行，用于收盘后的数据处理、统计等
        self.run_daily(self.after_market_close, time='after_close')

        # === 预计算均线和指标 ===
        valid_codes = []
        for code, df in self.datas.items():
            if df.empty or "close" not in df.columns:
                logger.warning(f"{code} 数据为空或缺失 'close'，跳过")
                continue

            # 计算均线
            df["ma_short"] = df["close"].rolling(self.short).mean()
            df["ma_long"] = df["close"].rolling(self.long).mean()
            
            # 计算均线交叉状态（用于判断金叉死叉）
            # 1 表示短期均线在长期均线上方，0 表示在下方
            df["ma_cross_state"] = (df["ma_short"] > df["ma_long"]).astype(int)
            
            # 计算金叉和死叉信号
            # 金叉：从0变为1（短期均线从下方穿越到上方）
            # 死叉：从1变为0（短期均线从上方穿越到下方）
            df["golden_cross"] = (df["ma_cross_state"] == 1) & (df["ma_cross_state"].shift(1) == 0)
            df["death_cross"] = (df["ma_cross_state"] == 0) & (df["ma_cross_state"].shift(1) == 1)
            
            # 计算成交量相关指标（如果启用）
            if self.use_volume_filter and "volume" in df.columns:
                df["volume_ma"] = df["volume"].rolling(20).mean()
                df["volume_ratio"] = df["volume"] / df["volume_ma"]
            
            valid_codes.append(code)

        # 只保留有效数据
        self.datas = {code: self.datas[code] for code in valid_codes}
        
        logger.info(
            f"MaCross 策略初始化完成 | "
            f"股票数量: {len(valid_codes)} | "
            f"短期均线: {short} | "
            f"长期均线: {long} | "
            f"单股权重: {weight} | "
            f"止损: {stop_loss*100 if stop_loss else '未启用'}% | "
            f"止盈: {take_profit*100 if take_profit else '未启用'}% | "
            f"成交量过滤: {'启用' if use_volume_filter else '未启用'}"
        )

    def market_open(self, context):
        """
        开盘前执行的逻辑（每天9:58执行）
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        # 获取当前交易日
        if context.current_date is not None:
            trade_date = context.current_date.strftime("%Y-%m-%d")
            trade_time = context.current_date.strftime("%H:%M:%S")
            logger.info(
                f"执行 market_open [交易日: {trade_date} {trade_time}] | "
                f"总资产: {context.portfolio.total_value():,.2f} | "
                f"现金: {context.portfolio.cash:,.2f} | "
                f"持仓数量: {len(context.portfolio.positions_hold)}"
            )
        else:
            logger.info(
                f"执行 market_open | "
                f"总资产: {context.portfolio.total_value():,.2f} | "
                f"现金: {context.portfolio.cash:,.2f}"
            )
        
        # 计算当天的交易信号
        signals = {}
        current_price = context.portfolio.prices
        
        for code, df in self.datas.items():
            if df.empty:
                continue
            
            # 获取当前时间点的数据
            if context.current_date is not None and context.current_date in df.index:
                current_idx = context.current_date
            else:
                # 如果没有精确匹配，使用最新的数据
                current_idx = df.index[-1]
            
            row = df.loc[current_idx]
            
            # 检查数据是否有效
            if pd.isna(row["ma_short"]) or pd.isna(row["ma_long"]):
                continue
            
            ma_short = row["ma_short"]
            ma_long = row["ma_long"]
            current_price_val = current_price.get(code, row["close"])
            
            # 检查是否有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            # 1. 检查止损止盈（如果有持仓）
            if has_position and code in self.entry_prices:
                entry_price = self.entry_prices[code]
                pnl_ratio = (current_price_val - entry_price) / entry_price
                
                # 止损
                if self.stop_loss and pnl_ratio <= -self.stop_loss:
                    signals[code] = {
                        "signal": "SELL",
                        "reason": "止损",
                        "entry_price": entry_price,
                        "current_price": current_price_val,
                        "pnl_ratio": pnl_ratio,
                        "ma_short": ma_short,
                        "ma_long": ma_long
                    }
                    logger.info(
                        f"{code} 触发止损 | "
                        f"买入价: {entry_price:.2f} | "
                        f"当前价: {current_price_val:.2f} | "
                        f"亏损: {pnl_ratio*100:.2f}%"
                    )
                    continue
                
                # 止盈
                if self.take_profit and pnl_ratio >= self.take_profit:
                    signals[code] = {
                        "signal": "SELL",
                        "reason": "止盈",
                        "entry_price": entry_price,
                        "current_price": current_price_val,
                        "pnl_ratio": pnl_ratio,
                        "ma_short": ma_short,
                        "ma_long": ma_long
                    }
                    logger.info(
                        f"{code} 触发止盈 | "
                        f"买入价: {entry_price:.2f} | "
                        f"当前价: {current_price_val:.2f} | "
                        f"盈利: {pnl_ratio*100:.2f}%"
                    )
                    continue
            
            # 2. 检查死叉（卖出信号）
            if has_position and row.get("death_cross", False):
                signals[code] = {
                    "signal": "SELL",
                    "reason": "死叉",
                    "ma_short": ma_short,
                    "ma_long": ma_long,
                    "strength": (ma_long - ma_short) / ma_long
                }
                logger.info(
                    f"{code} 产生卖出信号（死叉） | "
                    f"短期均线: {ma_short:.2f} | "
                    f"长期均线: {ma_long:.2f}"
                )
                continue
            
            # 3. 检查金叉（买入信号）
            if not has_position and row.get("golden_cross", False):
                # 成交量过滤（如果启用）
                if self.use_volume_filter:
                    volume_ratio = row.get("volume_ratio", 1.0)
                    if pd.isna(volume_ratio) or volume_ratio < self.min_volume_ratio:
                        logger.debug(f"{code} 金叉信号被成交量过滤（成交量比例: {volume_ratio:.2f}）")
                        continue
                
                signals[code] = {
                    "signal": "BUY",
                    "reason": "金叉",
                    "ma_short": ma_short,
                    "ma_long": ma_long,
                    "strength": (ma_short - ma_long) / ma_long,
                    "volume_ratio": row.get("volume_ratio", None)
                }
                logger.info(
                    f"{code} 产生买入信号（金叉） | "
                    f"短期均线: {ma_short:.2f} | "
                    f"长期均线: {ma_long:.2f} | "
                    f"信号强度: {(ma_short - ma_long) / ma_long * 100:.2f}%"
                )
        
        # 保存信号供 on_bar 使用
        self.daily_signals = signals
        
        buy_count = sum(1 for s in signals.values() if s["signal"] == "BUY")
        sell_count = sum(1 for s in signals.values() if s["signal"] == "SELL")
        if signals:
            logger.info(f"market_open 计算完成 | 买入信号: {buy_count} | 卖出信号: {sell_count}")

    def after_market_close(self, context):
        """
        收盘后执行的逻辑
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        # 获取当前交易日
        if context.current_date is not None:
            trade_date = context.current_date.strftime("%Y-%m-%d")
            logger.info(f"执行 after_market_close [交易日: {trade_date}]，当前资产: {context.portfolio.total_value():,.2f}")
        else:
            logger.info(f"执行 after_market_close，当前资产: {context.portfolio.total_value():,.2f}")
        
        # 收盘后可以执行的操作：
        # - 统计当天的交易情况
        # - 计算当天的收益
        # - 准备明天的交易计划
        # - 记录日志等
        
        # 示例：记录当天的持仓情况
        total_value = context.portfolio.total_value()
        cash = context.portfolio.cash
        positions_value = total_value - cash
        logger.info(f"收盘后统计 - 总资产: {total_value:,.2f}, 现金: {cash:,.2f}, 持仓市值: {positions_value:,.2f}")

    def on_bar(self, dt: pd.Timestamp) -> Dict[str, float]:
        """
        每个 bar 调用一次，生成交易信号
        
        注意：如果使用 run_daily 并设置 generate_signal=True，
        交易信号只在指定的时间点产生，其他时间返回空字典
        
        Args:
            dt: 当前时间点
            
        Returns:
            目标权重字典 {code: weight}
            权重为 0 表示卖出，权重 > 0 表示买入
        """
        targets = {}
        
        # 检查当前时间是否应该产生交易信号（从 run_daily 注册的时间中获取）
        if not self.is_trade_time(dt):
            # 不是交易时间，返回空字典
            return targets
        
        # 使用 market_open 中计算的信号
        # 这里直接使用 daily_signals，因为 market_open 已经在 scheduler 中执行
        for code, signal_info in self.daily_signals.items():
            signal = signal_info["signal"]
            
            if signal == "BUY":
                # 买入信号：设置目标权重
                targets[code] = self.weight
                # 记录买入价格（用于止损止盈判断）
                if self.context and code in self.context.portfolio.prices:
                    self.entry_prices[code] = self.context.portfolio.prices[code]
                elif code in self.datas and dt in self.datas[code].index:
                    self.entry_prices[code] = self.datas[code].loc[dt, "close"]
                    
            elif signal == "SELL":
                # 卖出信号：设置权重为 0
                targets[code] = 0.0
                # 清除买入价格记录
                if code in self.entry_prices:
                    del self.entry_prices[code]
        
        return targets
