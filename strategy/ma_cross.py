# strategy/ma_cross.py
import pandas as pd
from strategy.base import StrategyBase

class MaCross(StrategyBase):
    """
    多股票双均线策略
    """
    def __init__(self, datas, short=5, long=20, weight=0.5):
        super().__init__(datas)
        assert short < long, "short MA 必须小于 long MA"
        self.short = short
        self.long = long
        self.weight = weight

        # 预计算均线
        for code, df in self.datas.items():
            df["ma_short"] = df["close"].rolling(self.short).mean()
            df["ma_long"] = df["close"].rolling(self.long).mean()

    def on_bar(self, dt):
        signals = {}
        for code, df in self.datas.items():
            if dt not in df.index:
                continue
            row = df.loc[dt]
            if pd.isna(row["ma_short"]) or pd.isna(row["ma_long"]):
                continue
            if row["ma_short"] > row["ma_long"]:
                signals[code] = self.weight
        return signals
