"""
平台突破策略
支持 run_daily 定时任务功能

策略逻辑：
1. 计算最近N天的最高价和最低价（平台的上沿和下沿）
2. 当收盘价突破平台上沿时买入
3. 当收盘价跌破平台下沿时卖出
4. 支持止损止盈
"""
import logging
from typing import Dict, Optional
import pandas as pd
import numpy as np
from strategy.base import StrategyBase
from engine.log_helper import format_log_msg

logger = logging.getLogger(__name__)


class PlatformBreakout(StrategyBase):
    """
    平台突破策略
    
    策略逻辑：
    1. 平台定义：最近 N 个交易日的最高价（上沿）和最低价（下沿）
    2. 买入信号：收盘价突破平台上沿
    3. 卖出信号：收盘价跌破平台下沿
    4. 支持止损止盈
    
    每只股票独立判断，返回权重
    支持 run_daily 定时任务，可在每天指定时间执行策略逻辑
    """

    def __init__(
        self, 
        datas: Dict[str, pd.DataFrame], 
        period: int = 20,  # 平台周期（交易日数）
        weight: float = 0.5,  # 单只股票的最大权重
        stop_loss: Optional[float] = None,  # 止损比例
        take_profit: Optional[float] = None,  # 止盈比例
        breakout_threshold: float = 0.02,  # 突破阈值（2%），突破幅度需要超过这个值才算有效突破
        use_volume_filter: bool = False,  # 是否使用成交量过滤
        min_volume_ratio: float = 1.2  # 最小成交量比例（相对于均量）
    ):
        """
        初始化平台突破策略
        
        Args:
            datas: 股票数据字典
            period: 平台周期（交易日数），用于计算平台的上沿和下沿
            weight: 单只股票的最大权重（0-1之间）
            stop_loss: 止损比例（例如 0.05 表示亏损5%时止损），None 表示不启用
            take_profit: 止盈比例（例如 0.15 表示盈利15%时止盈），None 表示不启用
            breakout_threshold: 突破阈值，突破幅度需要超过这个值才算有效突破（例如 0.02 表示2%）
            use_volume_filter: 是否使用成交量过滤（成交量需要大于平均值）
            min_volume_ratio: 最小成交量比例（相对于均量）
        """
        super().__init__(datas)
        assert period > 0, "平台周期必须大于0"
        assert 0 < weight <= 1, "权重必须在 (0, 1] 之间"
        assert 0 <= breakout_threshold < 1, "突破阈值必须在 [0, 1) 之间"
        
        self.period = period
        self.weight = weight
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.breakout_threshold = breakout_threshold
        self.use_volume_filter = use_volume_filter
        self.min_volume_ratio = min_volume_ratio
        
        # 用于存储定时任务中计算的信号
        self.daily_signals = {}
        
        # 用于存储每只股票的买入价格（用于止损止盈判断）
        self.entry_prices: Dict[str, float] = {}
        
        # 用于存储昨天收盘检测到的突破信号（今天9:58买入）
        self.pending_buy_signals: Dict[str, Dict] = {}

        # === 注册定时任务：每天9:58执行买入 ===
        self.run_daily(self.market_open, time='09:58', generate_signal=True)
        
        # === 注册收盘后任务：检测突破信号 ===
        self.run_daily(self.after_market_close, time='after_close')

        # === 预计算平台指标 ===
        valid_codes = []
        for code, df in self.datas.items():
            if df.empty or "close" not in df.columns:
                logger.warning(f"{code} 数据为空或缺失 'close'，跳过")
                continue

            # 判断是日线还是分钟线数据
            is_minute_data = False
            if isinstance(df.index, pd.DatetimeIndex):
                sample_idx = df.index[0] if len(df.index) > 0 else None
                if sample_idx is not None:
                    if hasattr(sample_idx, 'hour') and hasattr(sample_idx, 'minute'):
                        if sample_idx.hour != 0 or sample_idx.minute != 0:
                            is_minute_data = True

            # 计算平台指标
            if is_minute_data:
                # 分钟线数据：先按日期分组，计算每日的最高价和最低价
                daily_high = df["high"].resample('D').max().dropna()  # 每日最高价
                daily_low = df["low"].resample('D').min().dropna()  # 每日最低价
                daily_close = df["close"].resample('D').last().dropna()  # 每日收盘价
                
                # 计算平台的上沿（最近N个交易日的最高价）和下沿（最近N个交易日的最低价）
                platform_upper = daily_high.rolling(window=self.period, min_periods=1).max()  # 平台上沿
                platform_lower = daily_low.rolling(window=self.period, min_periods=1).min()  # 平台下沿
                
                # 将日线平台值填充回分钟线数据
                df["platform_upper"] = None
                df["platform_lower"] = None
                df["daily_close"] = None
                
                for date, upper_val in platform_upper.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "platform_upper"] = upper_val
                
                for date, lower_val in platform_lower.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "platform_lower"] = lower_val
                
                for date, close_val in daily_close.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "daily_close"] = close_val
                
                # 使用 ffill 填充缺失值
                df["platform_upper"] = df["platform_upper"].ffill()
                df["platform_lower"] = df["platform_lower"].ffill()
                df["daily_close"] = df["daily_close"].ffill()
            else:
                # 日线数据：直接计算
                df["platform_upper"] = df["high"].rolling(window=self.period, min_periods=1).max()  # 平台上沿
                df["platform_lower"] = df["low"].rolling(window=self.period, min_periods=1).min()  # 平台下沿
                df["daily_close"] = df["close"]  # 收盘价
            
            # 计算成交量相关指标（如果启用）
            if self.use_volume_filter and "volume" in df.columns:
                if is_minute_data:
                    daily_volume = df["volume"].resample('D').sum().dropna()  # 每日成交量
                    daily_volume_ma = daily_volume.rolling(20).mean()
                    daily_volume_ratio = daily_volume / daily_volume_ma
                    
                    df["volume_ratio"] = None
                    for date, ratio_val in daily_volume_ratio.items():
                        day_mask = df.index.date == date.date()
                        df.loc[day_mask, "volume_ratio"] = ratio_val
                    df["volume_ratio"] = df["volume_ratio"].ffill()
                else:
                    df["volume_ma"] = df["volume"].rolling(20).mean()
                    df["volume_ratio"] = df["volume"] / df["volume_ma"]
            
            valid_codes.append(code)

        # 只保留有效数据
        self.datas = {code: self.datas[code] for code in valid_codes}
        
        # 输出每只股票的平台值示例（前几个交易日）
        for code in valid_codes[:3]:  # 只输出前3只股票，避免日志过多
            df = self.datas[code]
            if not df.empty:
                # 获取前5个交易日的数据
                sample_dates = df.index[:5] if len(df.index) >= 5 else df.index
                logger.info(f"{code} 平台值示例（前{len(sample_dates)}个交易日）:")
                for date in sample_dates:
                    row = df.loc[date]
                    if "platform_upper" in row and "platform_lower" in row:
                        upper = row["platform_upper"]
                        lower = row["platform_lower"]
                        close = row.get("daily_close", row.get("close", 0))
                        logger.info(f"  {date.strftime('%Y-%m-%d')}: 收盘={close:.2f}, 上沿={upper:.2f}, 下沿={lower:.2f}")
        
        logger.info(
            f"PlatformBreakout 策略初始化完成 | "
            f"股票数量: {len(valid_codes)} | "
            f"平台周期: {period}交易日 | "
            f"单股权重: {weight} | "
            f"突破阈值: {breakout_threshold*100:.1f}% | "
            f"止损: {stop_loss*100 if stop_loss else '未启用'}% | "
            f"止盈: {take_profit*100 if take_profit else '未启用'}% | "
            f"成交量过滤: {'启用' if use_volume_filter else '未启用'}"
        )

    def market_open(self, context):
        """
        开盘前执行的逻辑（每天9:58执行）
        执行昨天收盘检测到的突破买入信号
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        dt = context.current_date
        logger.info(format_log_msg(
            f"执行 market_open | "
            f"总资产: {context.portfolio.total_value():,.2f} | "
            f"现金: {context.portfolio.cash:,.2f} | "
            f"持仓数量: {len(context.portfolio.positions_hold)}",
            dt
        ))
        
        # 执行昨天收盘检测到的突破买入信号
        signals = {}
        current_price = context.portfolio.prices
        
        # 处理待买入信号（昨天收盘检测到的突破）
        for code, signal_info in self.pending_buy_signals.items():
            # 检查是否已有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            if not has_position:
                # 没有持仓，执行买入
                signals[code] = {
                    "signal": "BUY",
                    "reason": "昨天收盘平台突破",
                    "yesterday_date": signal_info.get("date", "未知"),
                    "platform_upper": signal_info.get("platform_upper"),
                    "breakout_price": signal_info.get("breakout_price")
                }
                logger.info(format_log_msg(
                    f"{code} 执行买入（昨天收盘平台突破） | "
                    f"日期: {signal_info.get('date', '未知')} | "
                    f"平台上沿: {signal_info.get('platform_upper', 0):.2f} | "
                    f"突破价格: {signal_info.get('breakout_price', 0):.2f}",
                    dt
                ))
        
        # 清空待买入信号（已处理）
        self.pending_buy_signals.clear()
        
        # 检查止损止盈（如果有持仓）
        for code, df in self.datas.items():
            if df.empty:
                continue
            
            # 获取当前时间点的数据
            if dt is not None and dt in df.index:
                current_idx = dt
            else:
                current_idx = df.index[-1]
            
            row = df.loc[current_idx]
            current_price_val = current_price.get(code, row.get("close", 0))
            
            # 检查是否有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            # 检查止损止盈
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
                        "pnl_ratio": pnl_ratio
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
                        "pnl_ratio": pnl_ratio
                    }
                    logger.info(format_log_msg(
                        f"{code} 触发止盈 | "
                        f"买入价: {entry_price:.2f} | "
                        f"当前价: {current_price_val:.2f} | "
                        f"盈利: {pnl_ratio*100:.2f}%",
                        dt
                    ))
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
        检测当天是否有平台突破信号，如果有则记录，第二天9:58买入
        
        Args:
            context: 上下文对象，包含 portfolio 和 data
        """
        dt = context.current_date
        logger.info(format_log_msg(
            f"执行 after_market_close | "
            f"总资产: {context.portfolio.total_value():,.2f} | "
            f"现金: {context.portfolio.cash:,.2f}",
            dt
        ))
        
        # 检测当天收盘是否有平台突破信号
        current_price = context.portfolio.prices
        breakout_count = 0
        signals = {}  # 用于存储卖出信号（跌破下沿、止损、止盈）
        
        for code, df in self.datas.items():
            if df.empty:
                continue
            
            # 获取当前时间点的数据（收盘数据）
            if dt is not None:
                # 尝试找到当天的收盘数据
                day_data = df[df.index.date == dt.date()]
                if day_data.empty:
                    continue
                current_idx = day_data.index[-1]
            else:
                current_idx = df.index[-1]
            
            row = df.loc[current_idx]
            
            # 检查数据是否有效
            if pd.isna(row.get("platform_upper")) or pd.isna(row.get("platform_lower")):
                continue
            
            platform_upper = row["platform_upper"]
            platform_lower = row["platform_lower"]
            current_close = row.get("daily_close", row.get("close", 0))
            current_price_val = current_price.get(code, current_close)
            
            # 调试日志：显示每天的平台值和收盘价（每5天输出一次，避免日志过多）
            breakout_ratio = (current_close - platform_upper) / platform_upper if platform_upper > 0 else 0
            # 只在接近突破或已经突破时输出详细信息
            if current_close >= platform_upper * 0.98:  # 收盘价接近或超过平台上沿的98%时输出
                logger.info(format_log_msg(
                    f"{code} 平台检测 | "
                    f"收盘价: {current_close:.2f} | "
                    f"平台上沿: {platform_upper:.2f} | "
                    f"平台下沿: {platform_lower:.2f} | "
                    f"突破幅度: {breakout_ratio*100:.2f}% | "
                    f"需要突破: {self.breakout_threshold*100:.1f}%",
                    dt
                ))
            
            # 检查是否有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            # 检查是否跌破平台下沿（卖出信号）
            if has_position and current_close < platform_lower:
                signals[code] = {
                    "signal": "SELL",
                    "reason": "跌破平台下沿",
                    "platform_lower": platform_lower,
                    "current_price": current_price_val
                }
                logger.info(format_log_msg(
                    f"{code} 产生卖出信号（跌破平台下沿） | "
                    f"平台下沿: {platform_lower:.2f} | "
                    f"当前价: {current_price_val:.2f}",
                    dt
                ))
                continue
            
            # 检查是否突破平台上沿（买入信号）- 记录到待买入列表，明天9:58买入
            if not has_position:
                # 计算突破幅度（已在上面计算，这里直接使用）
                
                # 检查是否有效突破（突破幅度需要超过阈值）
                if current_close > platform_upper:
                    # 检查突破幅度是否满足阈值
                    if breakout_ratio < self.breakout_threshold:
                        logger.info(format_log_msg(
                            f"{code} 突破幅度不足 | "
                            f"收盘价: {current_close:.2f} > 平台上沿: {platform_upper:.2f} | "
                            f"但突破幅度 {breakout_ratio*100:.2f}% < 阈值 {self.breakout_threshold*100:.1f}%",
                            dt
                        ))
                        continue
                    # 成交量过滤（如果启用）
                    if self.use_volume_filter:
                        volume_ratio = row.get("volume_ratio", 1.0)
                        if pd.isna(volume_ratio) or volume_ratio < self.min_volume_ratio:
                            logger.debug(format_log_msg(
                                f"{code} 突破信号被成交量过滤（成交量比例: {volume_ratio:.2f}）",
                                dt
                            ))
                            continue
                    
                    # 记录突破信号，明天9:58买入
                    trade_date = dt.strftime("%Y-%m-%d") if dt is not None else "未知"
                    self.pending_buy_signals[code] = {
                        "date": trade_date,
                        "platform_upper": platform_upper,
                        "platform_lower": platform_lower,
                        "breakout_price": current_close,
                        "breakout_ratio": breakout_ratio,
                        "volume_ratio": row.get("volume_ratio", None)
                    }
                    breakout_count += 1
                    logger.info(format_log_msg(
                        f"{code} 检测到平台突破信号（明天9:58买入） | "
                        f"平台上沿: {platform_upper:.2f} | "
                        f"突破价格: {current_close:.2f} | "
                        f"突破幅度: {breakout_ratio*100:.2f}%",
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
            f"检测到 {breakout_count} 个突破信号（明天9:58买入） | "
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

