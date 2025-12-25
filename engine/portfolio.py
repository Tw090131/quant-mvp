# engine/portfolio.py
class Portfolio:
    def __init__(self, init_cash=1_000_000):
        self.init_cash = init_cash
        self.cash = init_cash

        self.positions_hold = {}    # 可卖
        self.positions_today = {}   # T+1 冻结

        self.prices = {}

        self.equity_curve = []      # 每日资产
        self.daily_pnl = []         # 每日盈亏
        self.daily_trades = []      # 交易明细

        self._last_total = init_cash

    # ===== T+1 =====
    def on_new_day(self):
        for code, shares in self.positions_today.items():
            self.positions_hold[code] = self.positions_hold.get(code, 0) + shares
        self.positions_today.clear()

    # =================
    def update_price(self, code, price):
        self.prices[code] = price

    def total_value(self):
        total = self.cash
        for pos in (self.positions_hold, self.positions_today):
            for code, shares in pos.items():
                total += shares * self.prices.get(code, 0)
        return total

    def rebalance(self, date, target_weights, risk_mgr, fee_rate=0.001):
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

        return trades_today  # 方便 record_daily 计算 pnl

    # ===== 日结（关键）=====
    def record_daily(self, date, trades_today=None):
        """
        trades_today: 当天买卖列表
        """
        total = self.total_value()

        # 当天现金流（买入负，卖出正） + 手续费
        cash_flow = 0
        if trades_today:
            for t in trades_today:
                amt = t["price"] * t["shares"]
                fee = t.get("fee", 0)
                if t["side"] == "BUY":
                    cash_flow -= (amt + fee)
                else:
                    cash_flow += (amt - fee)

        # 当日盈亏 = 总资产变化 - 当日现金流
        pnl = total - self._last_total - cash_flow
        ret = pnl / self._last_total if self._last_total > 0 else 0

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

    # ===== DataFrame 输出 =====
    def get_equity_df(self):
        import pandas as pd
        return pd.DataFrame(self.equity_curve)

    def get_daily_pnl_df(self):
        import pandas as pd
        return pd.DataFrame(self.daily_pnl)

    def get_trades_df(self):
        import pandas as pd
        return pd.DataFrame(self.daily_trades)
