"""
RSRS 策略（阻力支撑相对强度策略）
支持 run_daily 定时任务功能

策略逻辑：
1. 计算每日的最高价和最低价
2. 使用线性回归计算阻力位和支撑位的斜率（RSRS 指标）
3. 根据 RSRS 指标判断市场趋势
4. 当 RSRS 大于买入阈值时买入，小于卖出阈值时卖出
5. 支持止损止盈
"""
import logging
from typing import Dict, Optional
import pandas as pd
import numpy as np
from scipy import stats
from strategy.base import StrategyBase
from engine.log_helper import format_log_msg

logger = logging.getLogger(__name__)


class RSRS(StrategyBase):
    """
    RSRS 策略（阻力支撑相对强度策略）
    
    策略逻辑：
    1. RSRS 指标计算：
       - 使用最近 N 天的最高价和最低价
       - 对最高价和最低价进行线性回归，得到斜率（RSRS 值）
       - RSRS = 回归斜率，反映阻力位和支撑位的相对强度
    2. 买入信号：RSRS > 买入阈值（默认 0.7）
    3. 卖出信号：RSRS < 卖出阈值（默认 -0.7）
    4. 支持止损止盈
    
    每只股票独立判断，返回权重
    支持 run_daily 定时任务，可在每天指定时间执行策略逻辑
    """

    def __init__(
        self, 
        datas: Dict[str, pd.DataFrame], 
        period: int = 18,  # RSRS 计算周期（交易日数）
        buy_threshold: float = 0.7,  # 买入阈值
        sell_threshold: float = -0.7,  # 卖出阈值
        weight: float = 0.5,  # 单只股票的最大权重
        stop_loss: Optional[float] = None,  # 止损比例
        take_profit: Optional[float] = None,  # 止盈比例
        use_rsrs_zscore: bool = False,  # 是否使用 RSRS 标准化分数（Z-score）
        zscore_period: int = 300  # Z-score 计算周期（用于标准化）
    ):
        """
        初始化 RSRS 策略
        
        Args:
            datas: 股票数据字典
            period: RSRS 计算周期（交易日数），用于线性回归的窗口大小
            buy_threshold: 买入阈值，RSRS 大于此值时买入
            sell_threshold: 卖出阈值，RSRS 小于此值时卖出
            weight: 单只股票的最大权重（0-1之间）
            stop_loss: 止损比例（例如 0.05 表示亏损5%时止损），None 表示不启用
            take_profit: 止盈比例（例如 0.15 表示盈利15%时止盈），None 表示不启用
            use_rsrs_zscore: 是否使用 RSRS 标准化分数（Z-score），如果启用，会使用标准化后的 RSRS 值
            zscore_period: Z-score 计算周期（用于标准化 RSRS 值）
        """
        super().__init__(datas)
        assert period > 0, "RSRS 计算周期必须大于0"
        assert 0 < weight <= 1, "权重必须在 (0, 1] 之间"
        assert buy_threshold > sell_threshold, "买入阈值必须大于卖出阈值"
        
        self.period = period
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.weight = weight
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.use_rsrs_zscore = use_rsrs_zscore
        self.zscore_period = zscore_period
        
        # 用于存储定时任务中计算的信号
        self.daily_signals = {}
        
        # 用于存储每只股票的买入价格（用于止损止盈判断）
        self.entry_prices: Dict[str, float] = {}
        
        # 用于存储昨天收盘检测到的买入信号（今天9:58买入）
        self.pending_buy_signals: Dict[str, Dict] = {}

        # === 注册定时任务：每天9:58执行买入 ===
        self.run_daily(self.market_open, time='09:58', generate_signal=True)
        
        # === 注册收盘后任务：检测 RSRS 信号 ===
        self.run_daily(self.after_market_close, time='after_close')

        # === 预计算 RSRS 指标 ===
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

            # 计算 RSRS 指标
            if is_minute_data:
                # 分钟线数据：先按日期分组，计算每日的最高价和最低价
                daily_high = df["high"].resample('D').max().dropna()  # 每日最高价
                daily_low = df["low"].resample('D').min().dropna()  # 每日最低价
                
                # 计算 RSRS：对最高价和最低价进行线性回归
                rsrs_values = []
                rsrs_dates = []
                
                for i in range(len(daily_high)):
                    if i < self.period - 1:
                        rsrs_values.append(np.nan)
                        rsrs_dates.append(daily_high.index[i])
                        continue
                    
                    # 获取最近 period 天的数据
                    high_window = daily_high.iloc[i - self.period + 1:i + 1].values
                    low_window = daily_low.iloc[i - self.period + 1:i + 1].values
                    
                    # 线性回归：low = slope * high + intercept
                    # RSRS 值就是斜率
                    if len(high_window) == self.period and len(low_window) == self.period:
                        # 使用 scipy.stats.linregress 进行线性回归
                        slope, intercept, r_value, p_value, std_err = stats.linregress(high_window, low_window)
                        rsrs_values.append(slope)
                    else:
                        rsrs_values.append(np.nan)
                    
                    rsrs_dates.append(daily_high.index[i])
                
                # 创建 RSRS Series
                rsrs_series = pd.Series(rsrs_values, index=rsrs_dates)
                
                # 如果启用 Z-score，计算标准化分数
                if self.use_rsrs_zscore:
                    rsrs_zscore = []
                    for i in range(len(rsrs_series)):
                        if i < self.zscore_period - 1:
                            rsrs_zscore.append(np.nan)
                        else:
                            # 计算过去 zscore_period 天的均值和标准差
                            window = rsrs_series.iloc[i - self.zscore_period + 1:i + 1]
                            mean_val = window.mean()
                            std_val = window.std()
                            if std_val > 0:
                                zscore = (rsrs_series.iloc[i] - mean_val) / std_val
                            else:
                                zscore = 0.0
                            rsrs_zscore.append(zscore)
                    
                    rsrs_series = pd.Series(rsrs_zscore, index=rsrs_dates)
                
                # 将日线 RSRS 值填充回分钟线数据
                df["rsrs"] = None
                for date, rsrs_val in rsrs_series.items():
                    day_mask = df.index.date == date.date()
                    df.loc[day_mask, "rsrs"] = rsrs_val
                
                # 使用 ffill 填充缺失值
                df["rsrs"] = df["rsrs"].ffill()
            else:
                # 日线数据：直接计算
                rsrs_values = []
                for i in range(len(df)):
                    if i < self.period - 1:
                        rsrs_values.append(np.nan)
                        continue
                    
                    # 获取最近 period 天的数据
                    high_window = df["high"].iloc[i - self.period + 1:i + 1].values
                    low_window = df["low"].iloc[i - self.period + 1:i + 1].values
                    
                    # 线性回归
                    if len(high_window) == self.period and len(low_window) == self.period:
                        slope, intercept, r_value, p_value, std_err = stats.linregress(high_window, low_window)
                        rsrs_values.append(slope)
                    else:
                        rsrs_values.append(np.nan)
                
                df["rsrs"] = rsrs_values
                
                # 如果启用 Z-score，计算标准化分数
                if self.use_rsrs_zscore:
                    rsrs_zscore = []
                    for i in range(len(df)):
                        if i < self.zscore_period - 1:
                            rsrs_zscore.append(np.nan)
                        else:
                            window = df["rsrs"].iloc[i - self.zscore_period + 1:i + 1]
                            mean_val = window.mean()
                            std_val = window.std()
                            if std_val > 0:
                                zscore = (df["rsrs"].iloc[i] - mean_val) / std_val
                            else:
                                zscore = 0.0
                            rsrs_zscore.append(zscore)
                    
                    df["rsrs"] = rsrs_zscore
            
            valid_codes.append(code)

        # 只保留有效数据
        self.datas = {code: self.datas[code] for code in valid_codes}
        
        logger.info(
            f"RSRS 策略初始化完成 | "
            f"股票数量: {len(valid_codes)} | "
            f"计算周期: {period}交易日 | "
            f"买入阈值: {buy_threshold} | "
            f"卖出阈值: {sell_threshold} | "
            f"单股权重: {weight} | "
            f"止损: {stop_loss*100 if stop_loss else '未启用'}% | "
            f"止盈: {take_profit*100 if take_profit else '未启用'}% | "
            f"使用Z-score: {'是' if use_rsrs_zscore else '否'}"
        )

    def market_open(self, context):
        """
        开盘前执行的逻辑（每天9:58执行）
        执行昨天收盘检测到的买入信号
        
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
        
        # 执行昨天收盘检测到的买入信号
        signals = {}
        current_price = context.portfolio.prices
        
        # 处理待买入信号（昨天收盘检测到的 RSRS 买入信号）
        for code, signal_info in self.pending_buy_signals.items():
            # 检查是否已有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            if not has_position:
                # 没有持仓，执行买入
                signals[code] = {
                    "signal": "BUY",
                    "reason": "昨天收盘RSRS买入信号",
                    "yesterday_date": signal_info.get("date", "未知"),
                    "rsrs_value": signal_info.get("rsrs_value"),
                }
                logger.info(format_log_msg(
                    f"{code} 执行买入（昨天收盘RSRS信号） | "
                    f"日期: {signal_info.get('date', '未知')} | "
                    f"RSRS值: {signal_info.get('rsrs_value', 0):.4f}",
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
        检测当天是否有 RSRS 买入/卖出信号，如果有则记录，第二天9:58执行
        
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
        
        # 检测当天收盘是否有 RSRS 信号
        current_price = context.portfolio.prices
        buy_signal_count = 0
        sell_signal_count = 0
        signals = {}  # 用于存储卖出信号（RSRS卖出、止损、止盈）
        
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
            
            # 检查 RSRS 值是否有效
            if pd.isna(row.get("rsrs")):
                continue
            
            rsrs_value = row["rsrs"]
            current_close = row.get("close", 0)
            current_price_val = current_price.get(code, current_close)
            
            # 检查是否有持仓
            has_position = code in context.portfolio.positions_hold and context.portfolio.positions_hold[code] > 0
            
            # 检查卖出信号（RSRS < 卖出阈值）
            if has_position and rsrs_value < self.sell_threshold:
                signals[code] = {
                    "signal": "SELL",
                    "reason": "RSRS卖出信号",
                    "rsrs_value": rsrs_value,
                    "current_price": current_price_val
                }
                sell_signal_count += 1
                logger.info(format_log_msg(
                    f"{code} 产生卖出信号（RSRS卖出） | "
                    f"RSRS值: {rsrs_value:.4f} | "
                    f"卖出阈值: {self.sell_threshold:.2f} | "
                    f"当前价: {current_price_val:.2f}",
                    dt
                ))
                continue
            
            # 检查买入信号（RSRS > 买入阈值）- 记录到待买入列表，明天9:58买入
            if not has_position and rsrs_value > self.buy_threshold:
                # 记录买入信号，明天9:58买入
                trade_date = dt.strftime("%Y-%m-%d") if dt is not None else "未知"
                self.pending_buy_signals[code] = {
                    "date": trade_date,
                    "rsrs_value": rsrs_value,
                    "current_price": current_price_val
                }
                buy_signal_count += 1
                logger.info(format_log_msg(
                    f"{code} 检测到RSRS买入信号（明天9:58买入） | "
                    f"RSRS值: {rsrs_value:.4f} | "
                    f"买入阈值: {self.buy_threshold:.2f} | "
                    f"当前价: {current_price_val:.2f}",
                    dt
                ))
        
        # 保存卖出信号供 on_bar 使用（如果有）
        if signals:
            self.daily_signals.update(signals)
        
        # 收盘后统计
        total_value = context.portfolio.total_value()
        cash = context.portfolio.cash
        positions_value = total_value - cash
        logger.info(format_log_msg(
            f"收盘后统计 | "
            f"总资产: {total_value:,.2f} | "
            f"现金: {cash:,.2f} | "
            f"持仓市值: {positions_value:,.2f} | "
            f"检测到 {buy_signal_count} 个买入信号（明天9:58买入） | "
            f"检测到 {sell_signal_count} 个卖出信号（明天9:58卖出）",
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

