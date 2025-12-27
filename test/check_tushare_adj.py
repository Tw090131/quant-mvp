"""
检查 Tushare 的复权逻辑
直接调用 Tushare API 获取2025-04-01的数据，对比复权前后的价格
"""
import tushare as ts
import pandas as pd
from datetime import datetime

# 设置 token
ts.set_token('87db6ebbe821bf474e7835abf0dd9ec2be7f9dbbe9a06210f17183ad')
pro = ts.pro_api()

code = "000423"
ts_code = "000423.SZ"
target_date = "20250401"

print("=" * 60)
print("检查 Tushare 复权逻辑")
print("=" * 60)

# 方法1：获取未复权数据 + 复权因子
print("\n【方法1】获取未复权数据 + 复权因子:")
try:
    # 获取未复权分钟数据
    df_raw = pro.stk_mins(
        ts_code=ts_code,
        freq='1',
        start_time=target_date + "093000",
        end_time=target_date + "150000",
        adj=None  # 未复权
    )
    
    if df_raw is not None and not df_raw.empty:
        # 获取复权因子
        adj_factor_data = pro.adj_factor(
            ts_code=ts_code,
            start_date=target_date,
            end_date=target_date
        )
        
        print(f"未复权数据条数: {len(df_raw)}")
        if len(df_raw) > 0:
            last_row = df_raw.iloc[-1]
            print(f"最后一条未复权收盘价: {last_row['close']:.2f}")
        
        if adj_factor_data is not None and not adj_factor_data.empty:
            adj_factor = adj_factor_data.iloc[0]['adj_factor']
            print(f"复权因子: {adj_factor:.4f}")
            
            # 计算前复权价格（使用复权因子）
            if len(df_raw) > 0:
                adj_price = last_row['close'] * adj_factor
                print(f"使用复权因子计算的前复权价格: {last_row['close']:.2f} * {adj_factor:.4f} = {adj_price:.2f}")
except Exception as e:
    print(f"获取失败: {e}")

# 方法2：直接获取前复权数据
print("\n【方法2】直接获取前复权数据（adj='qfq'）:")
try:
    df_adj = pro.stk_mins(
        ts_code=ts_code,
        freq='1',
        start_time=target_date + "093000",
        end_time=target_date + "150000",
        adj='qfq'  # 前复权
    )
    
    if df_adj is not None and not df_adj.empty:
        print(f"前复权数据条数: {len(df_adj)}")
        if len(df_adj) > 0:
            last_row = df_adj.iloc[-1]
            print(f"最后一条前复权收盘价: {last_row['close']:.2f}")
            print(f"这就是用户期望的59.27吗？")
except Exception as e:
    print(f"获取失败: {e}")

# 方法3：获取日线数据对比
print("\n【方法3】获取日线数据对比:")
try:
    # 未复权日线
    daily_raw = pro.daily(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date,
        adj=None
    )
    
    # 前复权日线
    daily_adj = pro.daily(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date,
        adj='qfq'
    )
    
    if daily_raw is not None and not daily_raw.empty:
        print(f"未复权日线收盘价: {daily_raw.iloc[0]['close']:.2f}")
    
    if daily_adj is not None and not daily_adj.empty:
        print(f"前复权日线收盘价: {daily_adj.iloc[0]['close']:.2f}")
        
    # 获取复权因子
    adj_factor_daily = pro.adj_factor(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date
    )
    if adj_factor_daily is not None and not adj_factor_daily.empty:
        adj_factor = adj_factor_daily.iloc[0]['adj_factor']
        print(f"复权因子: {adj_factor:.4f}")
        if daily_raw is not None and not daily_raw.empty:
            calculated = daily_raw.iloc[0]['close'] * adj_factor
            print(f"计算的前复权价格: {daily_raw.iloc[0]['close']:.2f} * {adj_factor:.4f} = {calculated:.2f}")
except Exception as e:
    print(f"获取失败: {e}")

print("\n" + "=" * 60)

