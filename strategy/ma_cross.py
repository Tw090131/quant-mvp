# strategy/ma_cross.py
import pandas as pd
from strategy.base import StrategyBase


class MaCross(StrategyBase):
    """
    多股票双均线策略
    每只股票独立判断，返回权重
    """

    def __init__(self, datas: dict, short: int = 5, long: int = 20, weight: float = 0.5):
        super().__init__(datas)
        assert short < long
        self.short = short
        self.long = long
        self.weight = weight

        # === 预计算均线 ===
        valid_codes = []
        for code, df in self.datas.items():
            if df.empty or "close" not in df.columns:
                print(f"[WARN] {code} 数据为空或缺失 'close'，跳过")
                continue

            df["ma_short"] = df["close"].rolling(self.short).mean()
            df["ma_long"] = df["close"].rolling(self.long).mean()
            valid_codes.append(code)

        # 只保留有效数据
        self.datas = {code: self.datas[code] for code in valid_codes}

    def on_bar(self, dt):
        targets = {}
        for code, df in self.datas.items():
            if dt not in df.index:
                continue

            row = df.loc[dt]
            if pd.isna(row["ma_short"]) or pd.isna(row["ma_long"]):
                continue

            if row["ma_short"] > row["ma_long"]:
                targets[code] = self.weight

        return targets
