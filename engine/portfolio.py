# engine/portfolio.py
import pandas as pd

class Portfolio:
    def __init__(self, init_cash=1_000_000, max_drawdown_pct=0.3):
        self.init_cash = init_cash
        self.cash = init_cash
        self.positions = {}  # code -> shares
        self.prices = {}     # code -> last_price
        self.max_drawdown_pct = max_drawdown_pct

        self.equity_curve = []  # 每日总资产
        self.daily_pnl = []     # 每日盈亏

    def update_price(self, code, price):
        self.prices[code] = price

    def total_value(self):
        value = self.cash
        for code, shares in self.positions.items():
            value += shares * self.prices.get(code, 0)
        return value

    def rebalance(self, target_weights: dict, risk_mgr=None):
        total_value = self.total_value()
        for code, weight in target_weights.items():
            if risk_mgr:
                weight = risk_mgr.cap_position(weight)
            target_value = total_value * weight
            price = self.prices.get(code)
            if not price or price <= 0:
                continue

            current_shares = self.positions.get(code, 0)
            target_shares = int(target_value / price)
            diff = target_shares - current_shares
            cost = diff * price

            if self.cash - cost < 0:
                continue

            self.cash -= cost
            self.positions[code] = target_shares

    def record_daily(self, date):
        current_value = self.total_value()
        self.equity_curve.append({"date": pd.to_datetime(date), "total_value": current_value})

        if len(self.equity_curve) == 1:
            pnl = current_value - self.init_cash
        else:
            pnl = current_value - self.equity_curve[-2]["total_value"]

        self.daily_pnl.append({"date": pd.to_datetime(date), "pnl": pnl})

    def max_drawdown_triggered(self):
        if not self.equity_curve:
            return False
        equity = [x["total_value"] for x in self.equity_curve]
        peak = max(equity)
        dd = (equity[-1] - peak) / peak
        return dd < -self.max_drawdown_pct

    def get_equity_df(self):
        return pd.DataFrame(self.equity_curve)

    def get_daily_pnl_df(self):
        return pd.DataFrame(self.daily_pnl)
