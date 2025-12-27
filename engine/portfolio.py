"""
组合管理模块
负责持仓管理、资金管理、交易执行和记录
"""
import logging
from typing import Dict, List, Optional
import pandas as pd
from engine.risk import RiskManager
from engine.log_helper import format_log_msg

# 配置日志
logger = logging.getLogger(__name__)


class Portfolio:
    """
    组合管理类
    
    管理持仓、资金和交易记录，支持 T+1 交易制度
    """
    
    def __init__(self, init_cash: float = 1_000_000):
        """
        初始化组合
        
        Args:
            init_cash: 初始资金
        """
        self.init_cash = init_cash
        self.cash = init_cash

        self.positions_hold: Dict[str, int] = {}    # 可卖持仓 {code: shares}
        self.positions_today: Dict[str, int] = {}   # T+1 买入冻结 {code: shares}

        self.prices: Dict[str, float] = {}          # 最新价格 {code: price}

        self.equity_curve: List[Dict] = []          # 每日总资产
        self.daily_pnl: List[Dict] = []             # 每日盈亏
        self.daily_trades: List[Dict] = []          # 交易明细

        self._last_total = init_cash

    def on_new_day(self) -> None:
        """
        T+1 买入转持仓
        将昨日买入的股票转为可卖持仓
        """
        for code, shares in self.positions_today.items():
            self.positions_hold[code] = self.positions_hold.get(code, 0) + shares
        self.positions_today.clear()

    def update_price(self, code: str, price: float, dt: Optional[pd.Timestamp] = None) -> None:
        """
        更新股票价格
        
        Args:
            code: 股票代码
            price: 最新价格
            dt: 当前日期（用于日志）
        """
        if price <= 0:
            logger.warning(format_log_msg(f"{code} 价格无效: {price}", dt))
            return
        self.prices[code] = price

    def total_value(self) -> float:
        """
        计算总资产
        
        Returns:
            总资产 = 现金 + 持仓市值
        """
        total = self.cash
        for pos in (self.positions_hold, self.positions_today):
            for code, shares in pos.items():
                price = self.prices.get(code, 0)
                if price > 0:
                    total += shares * price
        return total

    def rebalance(
        self, 
        date: pd.Timestamp, 
        target_weights: Dict[str, float], 
        risk_mgr: RiskManager, 
        fee_rate: float = 0.001,
        fee_mode: str = 'rate',
        fee_rate_pct: float = 0.0001,
        fee_fixed: float = 5.0
    ) -> List[Dict]:
        """
        调仓，根据目标权重调整持仓
        
        Args:
            date: 交易日期
            target_weights: 目标权重字典 {code: weight}
            risk_mgr: 风险管理器
            fee_rate: 手续费率（旧模式，当 fee_mode='rate' 时使用）
            fee_mode: 手续费模式，'rate'（仅费率）或 'rate+fixed'（费率+固定费用）
            fee_rate_pct: 费率百分比（当 fee_mode='rate+fixed' 时使用），如 0.0001 表示万1
            fee_fixed: 每笔固定费用（当 fee_mode='rate+fixed' 时使用），单位：元
            
        Returns:
            当天交易列表
        """
        total_value = self.total_value()
        trades_today = []

        for code, weight in target_weights.items():
            # 风控限制仓位
            weight = risk_mgr.cap_position(weight)
            price = self.prices.get(code)
            if not price or price <= 0:
                logger.warning(format_log_msg(f"{code} 价格无效，跳过调仓", date))
                continue

            target_value = total_value * weight
            target_shares = int(target_value / price)

            current_shares = (
                self.positions_hold.get(code, 0)
                + self.positions_today.get(code, 0)
            )

            diff = target_shares - current_shares

            # === 卖出（只能卖 hold）===
            if diff < 0:
                sellable = self.positions_hold.get(code, 0)
                sell_qty = min(-diff, sellable)
                if sell_qty <= 0:
                    continue

                proceeds = sell_qty * price
                # 计算手续费
                if fee_mode == 'rate+fixed':
                    fee = proceeds * fee_rate_pct + fee_fixed
                else:
                    fee = proceeds * fee_rate

                self.cash += proceeds - fee
                self.positions_hold[code] -= sell_qty
                
                if self.positions_hold[code] == 0:
                    del self.positions_hold[code]

                trades_today.append({
                    "date": date, # 交易日期时间
                    "code": code, # 股票代码
                    "side": "SELL", # 交易方向
                    "price": price, # 成交价格
                    "shares": sell_qty, # 成交数量
                    "amount": proceeds,  # 成交金额
                    "fee": fee, # 手续费
                })

            # === 买入（冻结）===
            elif diff > 0:
                cost = diff * price
                # 计算手续费
                if fee_mode == 'rate+fixed':
                    fee = cost * fee_rate_pct + fee_fixed
                else:
                    fee = cost * fee_rate
                if self.cash < cost + fee:
                    logger.warning(f"{code} 资金不足，无法买入 {diff} 股，需要 {cost + fee:.2f}，可用 {self.cash:.2f}")
                    continue

                self.cash -= cost + fee
                self.positions_today[code] = self.positions_today.get(code, 0) + diff

                trades_today.append({
                    "date": date,
                    "code": code,
                    "side": "BUY",
                    "price": price,
                    "shares": diff,
                    "amount": cost,  # 成交金额
                    "fee": fee,
                })

        # 保存交易
        self.daily_trades.extend(trades_today)
        return trades_today

    def record_daily(self, date: pd.Timestamp, trades_today: Optional[List[Dict]] = None) -> None:
        """
        日结，记录每日资产和盈亏
        
        Args:
            date: 日期
            trades_today: 当天交易列表
        """
        total = self.total_value()

        # 计算当日现金流：买入负，卖出正
        cash_flow = 0.0
        if trades_today:
            for t in trades_today:
                amt = t["price"] * t["shares"]
                fee = t.get("fee", 0)
                if t["side"] == "BUY":
                    cash_flow -= amt + fee
                else:
                    cash_flow += amt - fee

        # 当日盈亏 = 总资产变化 - 当日现金流
        pnl = total - self._last_total - cash_flow
        ret = pnl / self._last_total if self._last_total > 0 else 0.0

        self.equity_curve.append({
            "date": date,
            "total": total,
            "cash": self.cash,
        })

        self.daily_pnl.append({
            "date": date,
            "pnl": pnl,
            "return": ret,
            "total": total,
        })

        self._last_total = total

    def get_equity_df(self) -> pd.DataFrame:
        """
        获取资产曲线 DataFrame
        
        Returns:
            包含 date, total, cash 列的 DataFrame
        """
        return pd.DataFrame(self.equity_curve)

    def get_daily_pnl_df(self) -> pd.DataFrame:
        """
        获取每日盈亏 DataFrame
        
        Returns:
            包含 date, pnl, return, total 列的 DataFrame
        """
        return pd.DataFrame(self.daily_pnl)

    def get_trades_df(self) -> pd.DataFrame:
        """
        获取交易明细 DataFrame
        
        Returns:
            包含 date, code, side, price, shares, amount, fee 列的 DataFrame
            - date: 交易日期时间
            - code: 股票代码
            - side: 交易方向（BUY/SELL）
            - price: 成交价格
            - shares: 成交数量（股）
            - amount: 成交金额（price * shares）
            - fee: 手续费
        """
        return pd.DataFrame(self.daily_trades)
