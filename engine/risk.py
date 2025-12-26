"""
风险管理模块
提供仓位限制、回撤控制等风控功能
"""
import logging
from typing import Optional

# 配置日志
logger = logging.getLogger(__name__)


class RiskManager:
    """
    风险管理器
    
    支持的风控规则：
    - 单股最大仓位限制
    - 总资产最大回撤（熔断）
    - 单股止损（可选）
    """
    
    def __init__(
        self,
        max_position_pct: float = 0.3,      # 单股最大仓位
        max_drawdown: float = 0.2,           # 最大回撤
        stop_loss_pct: Optional[float] = None  # 单股止损，None 表示不启用
    ):
        """
        初始化风险管理器
        
        Args:
            max_position_pct: 单股最大仓位比例
            max_drawdown: 总资产最大回撤比例
            stop_loss_pct: 单股止损比例，None 表示不启用
        """
        if not 0 < max_position_pct <= 1:
            raise ValueError(f"max_position_pct 必须在 (0, 1] 之间，当前值: {max_position_pct}")
        if not 0 < max_drawdown <= 1:
            raise ValueError(f"max_drawdown 必须在 (0, 1] 之间，当前值: {max_drawdown}")
        if stop_loss_pct is not None and not 0 < stop_loss_pct <= 1:
            raise ValueError(f"stop_loss_pct 必须在 (0, 1] 之间，当前值: {stop_loss_pct}")
        
        self.max_position_pct = max_position_pct
        self.max_drawdown = max_drawdown
        self.stop_loss_pct = stop_loss_pct
        self.peak_value: Optional[float] = None

    def check_portfolio(self, portfolio_value: float) -> bool:
        """
        检查组合是否触发风控
        
        Args:
            portfolio_value: 当前组合总资产
            
        Returns:
            True 表示正常，False 表示触发风控，应该停止回测
        """
        if portfolio_value < 0:
            logger.error(f"组合资产为负: {portfolio_value}")
            return False
        
        if self.peak_value is None:
            self.peak_value = portfolio_value

        self.peak_value = max(self.peak_value, portfolio_value)
        drawdown = (self.peak_value - portfolio_value) / self.peak_value

        if drawdown >= self.max_drawdown:
            logger.warning(f"[风控] 触发最大回撤 {drawdown:.2%}，峰值: {self.peak_value:.2f}，当前: {portfolio_value:.2f}")
            return False

        return True

    def cap_position(self, target_weight: float) -> float:
        """
        限制单股仓位
        
        Args:
            target_weight: 目标权重
            
        Returns:
            限制后的权重
        """
        if target_weight < 0:
            return 0.0
        return min(target_weight, self.max_position_pct)
    
    def check_stop_loss(self, entry_price: float, current_price: float) -> bool:
        """
        检查单股是否触发止损
        
        Args:
            entry_price: 买入价格
            current_price: 当前价格
            
        Returns:
            True 表示触发止损，False 表示正常
        """
        if self.stop_loss_pct is None:
            return False
        
        if entry_price <= 0:
            return False
        
        loss_pct = (entry_price - current_price) / entry_price
        return loss_pct >= self.stop_loss_pct
