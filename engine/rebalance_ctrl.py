# engine/rebalance_ctrl.py
class RebalanceController:
    def __init__(self, period='1d', risk_mgr=None):
        self.period = period
        self.risk_mgr = risk_mgr
        self.last_rebalance_date = None

    def should_rebalance(self, date):
        if self.last_rebalance_date is None:
            self.last_rebalance_date = date
            return True

        # 按天
        if self.period.endswith('d'):
            n = int(self.period[:-1])
            delta = (date - self.last_rebalance_date).days
            if delta >= n:
                self.last_rebalance_date = date
                return True
        # 按周
        elif self.period.endswith('w'):
            n = int(self.period[:-1])
            delta = (date - self.last_rebalance_date).days // 7
            if delta >= n:
                self.last_rebalance_date = date
                return True
        # 按月
        elif self.period.endswith('m'):
            delta = (date.year - self.last_rebalance_date.year) * 12 + (date.month - self.last_rebalance_date.month)
            if delta >= int(self.period[:-1]):
                self.last_rebalance_date = date
                return True

        return False
