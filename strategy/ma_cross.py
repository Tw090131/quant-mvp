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
from engine.log_helper import format_log_msg

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
        long: int = 15, 
        weight: float = 0.5,
        stop_loss: Optional[float] = 0.05,
        take_profit: Optional[float] = 0.15,
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
        
        # 用于存储昨天收盘检测到的金叉信号（今天9:58买入）
        self.pending_buy_signals: Dict[str, Dict] = {}

        # === 注册定时任务：每天9:58执行买入 ===
        # 注意：需要分钟线数据才能触发定时任务
        # generate_signal=True 表示在该时间点产生交易信号
        self.run_daily(self.market_open, time='09:58', generate_signal=True)
        
        # === 注册收盘后任务：检测金叉信号 ===
        # 收盘后执行，检测当天是否有金叉，如果有则记录，第二天9:58买入
        self.run_daily(self.after_market_close, time='after_close')

        # === 预计算均线和指标 ===
        valid_codes = []
        for code, df in self.datas.items():
            if df.empty or "close" not in df.columns:
                logger.warning(f"{code} 数据为空或缺失 'close'，跳过")
                continue

            # 判断是日线还是分钟线数据
            # 检查索引是否有时间信息（小时、分钟）
            is_minute_data = False
            if isinstance(df.index, pd.DatetimeIndex):
                sample_idx = df.index[0] if len(df.index) > 0 else None
                if sample_idx is not None:
                    # 如果有小时或分钟信息（不是00:00），则是分钟线数据
                    if hasattr(sample_idx, 'hour') and hasattr(sample_idx, 'minute'):
                        if sample_idx.hour != 0 or sample_idx.minute != 0:
                            is_minute_data = True
            
            # 计算均线
            if is_minute_data:
                # 分钟线数据：需要先按日期分组，计算每日收盘价，再计算均线
                # 方法：使用 resample 按日分组，取每日最后一个收盘价，然后计算均线
                # 注意：resample('D') 会包含所有日期，但实际数据中只有交易日有数据
                # 所以 daily_close 只包含有交易数据的日期（即交易日）
                daily_close = df["close"].resample('D').last().dropna()  # 每日最后一个收盘价，只保留有数据的日期（交易日）
                
                # 计算均线（按交易日计算，不是按自然日）
                daily_ma_short = daily_close.rolling(window=self.short, min_periods=1).mean()  # 短期均线（按交易日）
                daily_ma_long = daily_close.rolling(window=self.long, min_periods=1).mean()  # 长期均线（按交易日）
                
                # 将日线均线值填充回分钟线数据
                # 方法：为每个分钟线数据点匹配对应日期的均线值
                df["ma_short"] = None
                df["ma_long"] = None
                
                # 将日线均线值按日期匹配到分钟线数据
                for date, ma_short_val in daily_ma_short.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "ma_short"] = ma_short_val
                
                for date, ma_long_val in daily_ma_long.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "ma_long"] = ma_long_val
                
                # 使用 ffill 填充缺失值（如果某天没有数据）
                df["ma_short"] = df["ma_short"].ffill()
                df["ma_long"] = df["ma_long"].ffill()
                
                # 调试：输出均线计算验证信息
                if len(daily_close) > 0:
                    # 输出前几个和后几个交易日的均线值用于验证
                    logger.debug(f"{code} 均线计算验证（前5个交易日）:")
                    for i, date in enumerate(daily_close.index[:5]):
                        close_val = daily_close.loc[date]
                        ma5_val = daily_ma_short.loc[date] if date in daily_ma_short.index else None
                        ma15_val = daily_ma_long.loc[date] if date in daily_ma_long.index else None
                        ma5_str = f"{ma5_val:.2f}" if ma5_val is not None else "NaN"
                        ma15_str = f"{ma15_val:.2f}" if ma15_val is not None else "NaN"
                        logger.debug(f"  {date.strftime('%Y-%m-%d')}: 收盘={close_val:.2f}, MA{self.short}={ma5_str}, MA{self.long}={ma15_str}")
                    
                    # 如果数据足够，输出8月12日附近的数据（用于与同花顺对比）
                    target_date = pd.to_datetime("2025-08-12")
                    if target_date.date() in [d.date() for d in daily_close.index]:
                        logger.info(f"{code} 均线计算验证（8月12日附近）:")
                        # 找到8月12日及其前后各2个交易日
                        date_list = list(daily_close.index)
                        try:
                            target_idx = next(i for i, d in enumerate(date_list) if d.date() == target_date.date())
                            start_idx = max(0, target_idx - 2)
                            end_idx = min(len(date_list), target_idx + 3)
                            for date in date_list[start_idx:end_idx]:
                                close_val = daily_close.loc[date]
                                ma5_val = daily_ma_short.loc[date] if date in daily_ma_short.index else None
                                ma15_val = daily_ma_long.loc[date] if date in daily_ma_long.index else None
                                ma5_str = f"{ma5_val:.2f}" if ma5_val is not None else "NaN"
                                ma15_str = f"{ma15_val:.2f}" if ma15_val is not None else "NaN"
                                marker = " <-- 目标日期" if date.date() == target_date.date() else ""
                                logger.info(f"  {date.strftime('%Y-%m-%d')}: 收盘={close_val:.2f}, MA{self.short}={ma5_str}, MA{self.long}={ma15_str}{marker}")
                        except StopIteration:
                            pass
            else:
                # 日线数据：直接计算
                df["ma_short"] = df["close"].rolling(self.short, min_periods=1).mean()
                df["ma_long"] = df["close"].rolling(self.long, min_periods=1).mean()
            
            # 计算均线交叉状态（用于判断金叉死叉）
            # 1 表示短期均线在长期均线上方，0 表示在下方
            df["ma_cross_state"] = (df["ma_short"] > df["ma_long"]).astype(int)
            
            # 计算金叉和死叉信号
            # 对于分钟线数据，只在每天的第一个时间点判断是否发生金叉死叉
            # 金叉：从0变为1（短期均线从下方穿越到上方）
            # 死叉：从1变为0（短期均线从上方穿越到下方）
            if is_minute_data:
                # 分钟线数据：只在每天的第一个时间点判断交叉
                # 先按日期分组，每天只判断第一个时间点
                daily_first = df.groupby(df.index.date).first()
                daily_first["ma_cross_state"] = (daily_first["ma_short"] > daily_first["ma_long"]).astype(int)
                daily_first["prev_state"] = daily_first["ma_cross_state"].shift(1)
                
                # 计算每日的金叉死叉
                daily_golden_cross = (daily_first["ma_cross_state"] == 1) & (daily_first["prev_state"] == 0)
                daily_death_cross = (daily_first["ma_cross_state"] == 0) & (daily_first["prev_state"] == 1)
                
                # 将每日的金叉死叉信号映射回分钟线数据
                # 如果当天有金叉，则当天的所有分钟都标记为 True
                df["golden_cross"] = False
                df["death_cross"] = False
                
                for date, has_golden in daily_golden_cross.items():
                    if has_golden:
                        day_mask = df.index.date == date
                        df.loc[day_mask, "golden_cross"] = True
                
                for date, has_death in daily_death_cross.items():
                    if has_death:
                        day_mask = df.index.date == date
                        df.loc[day_mask, "death_cross"] = True
            else:
                # 日线数据：直接计算
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
        执行昨天收盘检测到的金叉买入信号
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        # 获取当前交易日
        dt = context.current_date
        logger.info(format_log_msg(
            f"执行 market_open | "
            f"总资产: {context.portfolio.total_value():,.2f} | "
            f"现金: {context.portfolio.cash:,.2f} | "
            f"持仓数量: {len(context.portfolio.positions_hold)}",
            dt
        ))
        
        # 执行昨天收盘检测到的金叉买入信号
        signals = {}
        current_price = context.portfolio.prices
        
        # 处理待买入信号（昨天收盘检测到的金叉）
        for code, signal_info in self.pending_buy_signals.items():
            # 检查是否已有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            if not has_position:
                # 没有持仓，执行买入
                signals[code] = {
                    "signal": "BUY",
                    "reason": "昨天收盘金叉",
                    "yesterday_date": signal_info.get("date", "未知"),
                    "ma_short": signal_info.get("ma_short"),
                    "ma_long": signal_info.get("ma_long"),
                    "strength": signal_info.get("strength")
                }
                logger.info(format_log_msg(
                    f"{code} 执行买入（昨天收盘金叉） | "
                    f"日期: {signal_info.get('date', '未知')} | "
                    f"短期均线: {signal_info.get('ma_short', 0):.2f} | "
                    f"长期均线: {signal_info.get('ma_long', 0):.2f}",
                    dt
                ))
        
        # 清空待买入信号（已处理）
        self.pending_buy_signals.clear()
        
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
                logger.info(format_log_msg(
                    f"{code} 触发止损 | "
                    f"买入价: {entry_price:.2f} | "
                    f"当前价: {current_price_val:.2f} | "
                    f"亏损: {pnl_ratio*100:.2f}%",
                    dt
                ))
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
                    logger.info(format_log_msg(
                        f"{code} 触发止盈 | "
                        f"买入价: {entry_price:.2f} | "
                        f"当前价: {current_price_val:.2f} | "
                        f"盈利: {pnl_ratio*100:.2f}%",
                        dt
                    ))
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
            
        
        # 保存信号供 on_bar 使用
        self.daily_signals = signals
        
        buy_count = sum(1 for s in signals.values() if s["signal"] == "BUY")
        sell_count = sum(1 for s in signals.values() if s["signal"] == "SELL")
        if signals:
            logger.info(format_log_msg(
                f"market_open 计算完成 | 买入信号: {buy_count} | 卖出信号: {sell_count}",
                dt
            ))

    def after_market_close(self, context):
        """
        收盘后执行的逻辑
        检测当天是否有金叉信号，如果有则记录，第二天9:58买入
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        # 获取当前交易日
        dt = context.current_date
        logger.info(format_log_msg(
            f"执行 after_market_close | "
            f"总资产: {context.portfolio.total_value():,.2f} | "
            f"现金: {context.portfolio.cash:,.2f}",
            dt
        ))
        
        # 检测当天收盘是否有金叉信号
        current_price = context.portfolio.prices
        golden_cross_count = 0
        signals = {}  # 用于存储卖出信号（死叉、止损、止盈）
        
        for code, df in self.datas.items():
            if df.empty:
                continue
            
            # 获取当前时间点的数据（收盘数据）
            if context.current_date is not None:
                # 尝试找到当天的收盘数据
                # 对于分钟线数据，找当天最后一个时间点
                day_data = df[df.index.date == context.current_date.date()]
                if day_data.empty:
                    continue
                current_idx = day_data.index[-1]
            else:
                # 如果没有当前日期，使用最新数据
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
            
            # 检查是否有死叉（卖出信号）
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
            
            # 检查是否有金叉（买入信号）- 记录到待买入列表，明天9:58买入
            if not has_position and row.get("golden_cross", False):
                # 成交量过滤（如果启用）
                if self.use_volume_filter:
                    volume_ratio = row.get("volume_ratio", 1.0)
                    if pd.isna(volume_ratio) or volume_ratio < self.min_volume_ratio:
                        logger.debug(format_log_msg(
                            f"{code} 金叉信号被成交量过滤（成交量比例: {volume_ratio:.2f}）",
                            dt
                        ))
                        continue
                
                # 记录金叉信号，明天9:58买入
                trade_date = dt.strftime("%Y-%m-%d") if dt is not None else "未知"
                self.pending_buy_signals[code] = {
                    "date": trade_date,
                    "ma_short": ma_short,
                    "ma_long": ma_long,
                    "strength": (ma_short - ma_long) / ma_long,
                    "volume_ratio": row.get("volume_ratio", None),
                    "close_price": current_price_val
                }
                golden_cross_count += 1
                logger.info(format_log_msg(
                    f"{code} 检测到金叉信号（明天9:58买入） | "
                    f"短期均线: {ma_short:.2f} | "
                    f"长期均线: {ma_long:.2f} | "
                    f"信号强度: {(ma_short - ma_long) / ma_long * 100:.2f}%",
                    dt
                ))
        
        # 保存卖出信号供 on_bar 使用（如果有）
        if signals:
            self.daily_signals.update(signals)
        
        # 收盘后统计
        total_value = context.portfolio.total_value()
        cash = context.portfolio.cash
        positions_value = total_value - cash
        sell_count = len(signals)
        logger.info(format_log_msg(
            f"收盘后统计 | "
            f"总资产: {total_value:,.2f} | "
            f"现金: {cash:,.2f} | "
            f"持仓市值: {positions_value:,.2f} | "
            f"检测到 {golden_cross_count} 个金叉信号（明天9:58买入） | "
            f"检测到 {sell_count} 个卖出信号（明天9:58卖出）",
            dt
        ))

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
