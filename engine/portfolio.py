# engine/portfolio.py
import pandas as pd

class Portfolio:
    def __init__(self, init_cash=1_000_000):
        self.init_cash = init_cash
        self.cash = init_cash

        self.positions_hold = {}    # 可卖持仓
        self.positions_today = {}   # 当日买入冻结，T+1 可卖
        self.prices = {}            # 最新价格
        self._prev_prices = {}      # 昨收价，用于计算未实现盈亏
        self._cost_basis = {}       # 成本价

        self.equity_curve = []      # 每日总资产
        self.daily_pnl = []         # 每日盈亏
        self.daily_trades = []      # 交易明细

    # ===== T+1 =====
    def on_new_day(self):
        # 当日买入变为可卖持仓
        for code, shares in self.positions_today.items():
            self.positions_hold[code] = self.positions_hold.get(code, 0) + shares
        self.positions_today.clear()
        # 更新昨日价格
        self._prev_prices = self.prices.copy()

    # ===== 更新价格 =====
    def update_price(self, code, price):
        self.prices[code] = price

    # ===== 总资产 =====
    def total_value(self):
        total = self.cash
        for pos in (self.positions_hold, self.positions_today):
            for code, shares in pos.items():
                total += shares * self.prices.get(code, 0)
        return total

    # ===== 调仓 =====
    def rebalance(self, date, target_weights, risk_mgr, fee_rate=0.001):
        """
        target_weights: code -> weight
        返回 trades_today 方便 record_daily 计算 pnl
        """
        total_value = self.total_value()
        trades_today = []

        for code, weight in target_weights.items():
            weight = risk_mgr.cap_position(weight)
            price = self.prices.get(code)
            if not price or price <= 0:
                continue

            target_value = total_value * weight
            target_shares = int(target_value / price)

            current_shares = (
                self.positions_hold.get(code, 0)
                + self.positions_today.get(code, 0)
            )

            diff = target_shares - current_shares

            # === SELL（只能卖 hold）===
            if diff < 0:
                sellable = self.positions_hold.get(code, 0)
                sell_qty = min(-diff, sellable)
                if sell_qty <= 0:
                    continue

                proceeds = sell_qty * price
                fee = proceeds * fee_rate

                self.cash += proceeds - fee
                self.positions_hold[code] -= sell_qty

                trades_today.append({
                    "date": date,
                    "code": code,
                    "side": "SELL",
                    "price": price,
                    "shares": sell_qty,
                    "fee": fee,
                })

            # === BUY（进冻结）===
            elif diff > 0:
                cost = diff * price
                fee = cost * fee_rate
                if self.cash < cost + fee:
                    continue

                self.cash -= cost + fee
                self.positions_today[code] = self.positions_today.get(code, 0) + diff

                trades_today.append({
                    "date": date,
                    "code": code,
                    "side": "BUY",
                    "price": price,
                    "shares": diff,
                    "fee": fee,
                })

        # 写入每日交易
        self.daily_trades.extend(trades_today)
        return trades_today

    # ===== 日结 =====
    def record_daily(self, date, trades_today=None):
        """
        计算每日盈亏：
        - 卖出产生的实现盈亏
        - 持仓未实现盈亏
        """
        # 未实现盈亏（昨日持仓变化）
        unrealized_pnl = 0
        for code, shares in self.positions_hold.items():
            if code in self._prev_prices and code in self.prices:
                unrealized_pnl += shares * (self.prices[code] - self._prev_prices[code])

        # 实现盈亏（卖出盈亏）
        realized_pnl = 0
        if trades_today:
            for t in trades_today:
                code = t["code"]
                price = t["price"]
                shares = t["shares"]
                fee = t.get("fee", 0)

                if t["side"] == "SELL":
                    cost_price = self._cost_basis.get(code, price)
                    realized_pnl += (price - cost_price) * shares - fee

                elif t["side"] == "BUY":
                    # 更新成本价（加权平均）
                    old_shares = self.positions_hold.get(code, 0)
                    old_cost = self._cost_basis.get(code, 0)
                    new_shares = old_shares + shares
                    self._cost_basis[code] = (old_cost * old_shares + price * shares) / new_shares

        total_pnl = realized_pnl + unrealized_pnl
        total_value = self.total_value()

        self.equity_curve.append({
            "date": date,
            "total": total_value,
            "cash": self.cash,
        })

        self.daily_pnl.append({
            "date": date,
            "pnl": total_pnl,
            "total": total_value,
        })

    # ===== 输出 DataFrame =====
    def get_equity_df(self):
        return pd.DataFrame(self.equity_curve)

    def get_daily_pnl_df(self):
        return pd.DataFrame(self.daily_pnl)

    def get_trades_df(self):
        return pd.DataFrame(self.daily_trades)
