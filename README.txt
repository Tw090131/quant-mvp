Strategy      →  只产出 buy / sell 信号
Portfolio     →  管仓位 / 资金 / 权重
Backtest      →  推时间轴


data
 ├─ cache_loader        数据获取 + 缓存
 ├─ calendar            交易日历
 ├─ align               数据对齐
engine
 ├─ backtest            回测主循环
 ├─ portfolio           组合管理
 ├─ rebalance           调仓控制
 ├─ risk                风控模块
strategy
 ├─ base                策略基类
 ├─ ma_cross            示例策略
run.py

data/
├── loader/
│   ├── minute_hist.py      # 用 hist_min_em（回测）
│   ├── minute_realtime.py  # 用 stock_zh_a_minute（实盘）
│
├── cache/
│   ├── 300678_1min.parquet
│
backtest/
├── engine.py
├── strategy.py


回测 → 用 stock_zh_a_hist_min_em
实盘 / 准实时 → 用 stock_zh_a_minute