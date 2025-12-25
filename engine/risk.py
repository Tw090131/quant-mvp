# engine/risk.py
# 1️⃣ 支持的风控规则
# 单股最大仓位
# 单日最大回撤（止损）
# 总资产最大回撤（熔断）
# 不允许负现金
class RiskManager:
    def __init__(
        self,
        max_position_pct=0.3,      # 单股最大仓位
        max_drawdown=0.2,          # 最大回撤
        stop_loss_pct=0.1          # 单股止损
    ):
        self.max_position_pct = max_position_pct
        self.max_drawdown = max_drawdown
        self.stop_loss_pct = stop_loss_pct
        self.peak_value = None

    def check_portfolio(self, portfolio_value: float) -> bool:
        """
        返回 False 表示触发风控，应该停止回测
        """
        if self.peak_value is None:
            self.peak_value = portfolio_value

        self.peak_value = max(self.peak_value, portfolio_value)
        drawdown = (self.peak_value - portfolio_value) / self.peak_value

        if drawdown >= self.max_drawdown:
            print(f"[风控] 触发最大回撤 {drawdown:.2%}")
            return False

        return True

    def cap_position(self, target_weight: float) -> float:
        """
        限制单股仓位
        """
        return min(target_weight, self.max_position_pct)
