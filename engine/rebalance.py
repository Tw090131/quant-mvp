# engine/rebalance.py
# 调仓周期模块
# daily
# weekly
# monthly
# N日调仓
import pandas as pd

class RebalanceController:
    def __init__(self, freq="monthly", n_days=5):
        """
        freq: daily | weekly | monthly | ndays
        """
        self.freq = freq
        self.n_days = n_days
        self.last_rebalance_date = None

    def should_rebalance(self, current_date: pd.Timestamp) -> bool:
        if self.last_rebalance_date is None:
            return True

        if self.freq == "daily":
            return True

        if self.freq == "weekly":
            return current_date.weekday() == 0  # 周一

        if self.freq == "monthly":
            return current_date.day <= 3  # 月初前 3 个交易日

        if self.freq == "ndays":
            delta = (current_date - self.last_rebalance_date).days
            return delta >= self.n_days

        return False

    def mark_rebalanced(self, current_date: pd.Timestamp):
        self.last_rebalance_date = current_date
