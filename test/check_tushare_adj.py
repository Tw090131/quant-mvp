"""
检查 Tushare 的复权逻辑 - 使用日线数据
直接调用 Tushare API 获取日线数据，对比复权前后的价格和复权因子
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
start_date = "20250401"
end_date = "20250520"  # 获取一段时间的数据用于对比

print("=" * 60)
print("检查 Tushare 复权逻辑（日线数据）")
print("=" * 60)

# 方法1：获取未复权日线数据 + 复权因子
print("\n【方法1】获取未复权日线数据 + 复权因子:")
try:
    # 获取未复权日线数据
    daily_raw = pro.daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        adj=None  # 未复权
    )
    
    # 获取复权因子
    adj_factor_data = pro.adj_factor(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date
    )
    
    if daily_raw is not None and not daily_raw.empty:
        print(f"未复权日线数据条数: {len(daily_raw)}")
        
        # 找到目标日期的数据
        target_data = daily_raw[daily_raw['trade_date'] == target_date]
        if len(target_data) > 0:
            target_row = target_data.iloc[0]
            print(f"\n{target_date} 未复权收盘价: {target_row['close']:.2f}")
            print(f"  开盘价: {target_row['open']:.2f}")
            print(f"  最高价: {target_row['high']:.2f}")
            print(f"  最低价: {target_row['low']:.2f}")
    
    if adj_factor_data is not None and not adj_factor_data.empty:
        print(f"\n复权因子数据条数: {len(adj_factor_data)}")
        
        # 找到目标日期的复权因子
        target_adj = adj_factor_data[adj_factor_data['trade_date'] == target_date]
        if len(target_adj) > 0:
            adj_factor = target_adj.iloc[0]['adj_factor']
            print(f"{target_date} 复权因子: {adj_factor:.4f}")
            
            # 获取最新日期的复权因子（作为基准）
            latest_adj = adj_factor_data.iloc[-1]['adj_factor']
            latest_date = adj_factor_data.iloc[-1]['trade_date']
            print(f"最新日期({latest_date})复权因子: {latest_adj:.4f}")
            
            # 计算前复权价格（使用复权因子）
            if len(target_data) > 0:
                target_row = target_data.iloc[0]
                raw_close = target_row['close']
                
                # 方法1：当前复权因子 / 最新复权因子（前复权）
                ratio1 = adj_factor / latest_adj
                adj_price1 = raw_close * ratio1
                print(f"\n方法1（当前复权因子/最新复权因子）:")
                print(f"  复权比例: {ratio1:.4f}")
                print(f"  复权后价格: {adj_price1:.2f}")
                
                # 方法2：直接使用复权因子（后复权）
                adj_price2 = raw_close * adj_factor
                print(f"\n方法2（直接乘以复权因子，后复权）:")
                print(f"  复权后价格: {adj_price2:.2f}")
                
except Exception as e:
    print(f"获取失败: {e}")
    import traceback
    traceback.print_exc()

# 方法2：直接获取前复权日线数据
print("\n【方法2】直接获取前复权日线数据（adj='qfq'）:")
try:
    daily_adj = pro.daily(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        adj='qfq'  # 前复权
    )
    
    if daily_adj is not None and not daily_adj.empty:
        print(f"前复权日线数据条数: {len(daily_adj)}")
        
        # 找到目标日期的数据
        target_data = daily_adj[daily_adj['trade_date'] == target_date]
        if len(target_data) > 0:
            target_row = target_data.iloc[0]
            print(f"\n{target_date} Tushare前复权收盘价: {target_row['close']:.2f}")
            print(f"  开盘价: {target_row['open']:.2f}")
            print(f"  最高价: {target_row['high']:.2f}")
            print(f"  最低价: {target_row['low']:.2f}")
            print(f"\n用户期望价格: 59.27")
            print(f"差异: {abs(target_row['close'] - 59.27):.2f}")
            
            # 显示前后几天的数据用于对比
            print(f"\n前后几天的前复权收盘价:")
            date_list = daily_adj['trade_date'].tolist()
            if target_date in date_list:
                idx = date_list.index(target_date)
                start_idx = max(0, idx - 2)
                end_idx = min(len(date_list), idx + 3)
                for i in range(start_idx, end_idx):
                    date = date_list[i]
                    row = daily_adj[daily_adj['trade_date'] == date].iloc[0]
                    marker = " <-- 目标日期" if date == target_date else ""
                    print(f"  {date}: {row['close']:.2f}{marker}")
                    
except Exception as e:
    print(f"获取失败: {e}")
    import traceback
    traceback.print_exc()

# 方法3：对比分析
print("\n【方法3】对比分析:")
try:
    # 重新获取数据用于对比
    daily_raw = pro.daily(ts_code=ts_code, start_date=target_date, end_date=target_date, adj=None)
    daily_adj = pro.daily(ts_code=ts_code, start_date=target_date, end_date=target_date, adj='qfq')
    adj_factor_data = pro.adj_factor(ts_code=ts_code, start_date=target_date, end_date=target_date)
    
    if (daily_raw is not None and not daily_raw.empty and 
        daily_adj is not None and not daily_adj.empty and
        adj_factor_data is not None and not adj_factor_data.empty):
        
        raw_close = daily_raw.iloc[0]['close']
        adj_close = daily_adj.iloc[0]['close']
        adj_factor = adj_factor_data.iloc[0]['adj_factor']
        
        print(f"未复权收盘价: {raw_close:.2f}")
        print(f"Tushare前复权收盘价: {adj_close:.2f}")
        print(f"复权因子: {adj_factor:.4f}")
        print(f"用户期望价格: 59.27")
        
        # 计算差异
        diff1 = abs(adj_close - 59.27)
        print(f"\nTushare前复权价格与期望价格差异: {diff1:.2f}")
        
        # 如果Tushare返回的价格和未复权价格相同，说明该日期没有除权除息
        if abs(raw_close - adj_close) < 0.01:
            print(f"\n注意：Tushare返回的前复权价格({adj_close:.2f})和未复权价格({raw_close:.2f})相同")
            print(f"这说明该日期可能没有除权除息事件")
            print(f"但用户期望的59.27与61.81不同，可能需要检查：")
            print(f"  1. 数据源是否一致（分钟线 vs 日线）")
            print(f"  2. 复权基准日期是否不同")
            print(f"  3. 是否有其他复权方法")
            
except Exception as e:
    print(f"获取失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)

