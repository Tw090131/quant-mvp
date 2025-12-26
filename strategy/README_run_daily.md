# run_daily 定时任务使用说明

## 功能说明

`run_daily` 功能允许你在每天指定时间执行策略逻辑，类似于聚宽（JoinQuant）的 `run_daily` 接口。

## 使用方法

### 1. 在策略中注册定时任务

在策略的 `__init__` 方法中，使用 `self.run_daily()` 注册定时任务：

```python
from strategy.base import StrategyBase

class MyStrategy(StrategyBase):
    def __init__(self, datas):
        super().__init__(datas)
        
        # 注册定时任务：每天9:58执行 market_open 函数
        self.run_daily(self.market_open, time='09:58')
        
        # 可以注册多个定时任务
        self.run_daily(self.market_close, time='14:55')
    
    def market_open(self, context):
        """每天9:58执行的逻辑"""
        # 在这里可以：
        # - 获取市场数据
        # - 计算信号
        # - 准备交易计划
        # 注意：不能直接交易，交易需要在 on_bar 中返回权重
        pass
    
    def market_close(self, context):
        """每天14:55执行的逻辑"""
        pass
    
    def on_bar(self, dt):
        """每个 bar 调用，返回目标权重"""
        return {}
```

### 2. 使用分钟线数据进行回测

**重要：`run_daily` 功能需要分钟线数据才能工作！**

在 `run.py` 中加载分钟线数据：

```python
# 加载分钟线数据（而不是日线数据）
datas = {}
for code in codes:
    df = load_minute_df_with_cache(code, period="1min", start=start_date, end=end_date)
    if not df.empty:
        datas[code] = df

# 使用支持 run_daily 的策略
result = run_backtest(
    datas,
    MyStrategy,  # 使用你的策略类
    ...
)
```

### 3. 参数说明

- `func`: 要执行的函数，函数签名应为 `func(context)`
  - `context` 包含 `portfolio` 和 `data` 属性
- `time`: 执行时间，格式 `'HH:MM'`，例如 `'09:58'`、`'14:55'`
- `reference_security`: 参考证券（保留参数，当前版本未使用）

### 4. 注意事项

1. **必须使用分钟线数据**：日线数据无法精确到分钟，无法触发定时任务
2. **定时任务不能直接交易**：在定时任务函数中，你可以计算信号、准备数据，但实际的交易决策需要在 `on_bar` 方法中返回权重
3. **时间格式**：时间必须使用 24 小时制，格式为 `'HH:MM'`，例如 `'09:58'` 而不是 `'9:58'`
4. **交易日限制**：定时任务只在交易日执行，非交易日不会触发

### 5. 完整示例

参考 `strategy/daily_strategy.py` 文件，查看完整的使用示例。

