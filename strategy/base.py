# strategy/base.py
class StrategyBase:
    def __init__(self, datas):
        """
        datas: list[pd.DataFrame]
        """
        self.datas = {}
        for i, df in enumerate(datas):
            code = getattr(df, "attrs", {}).get("code", f"code_{i}")
            self.datas[code] = df

    def on_bar(self, dt):
        raise NotImplementedError
