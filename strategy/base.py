# strategy/base.py
class StrategyBase:
    def __init__(self, datas: dict):
        """
        datas: code -> DataFrame
        """
        self.datas = datas

    def on_bar(self, dt):
        """
        返回目标权重:
        { code: weight }
        """
        raise NotImplementedError
