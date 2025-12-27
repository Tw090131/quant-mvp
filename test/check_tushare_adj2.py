"""
检查 Tushare 的复权逻辑 - 使用日线数据
"""
import tushare as ts
import pandas as pd

# 设置 token
ts.set_token('87db6ebbe821bf474e7835abf0dd9ec2be7f9dbbe9a06210f17183ad')
pro = ts.pro_api()

code = "000423"
ts_code = "000423.SZ"
target_date = "20250401"

print("=" * 60)
print("检查 Tushare 复权逻辑（日线数据）")
print("=" * 60)

# 获取2025-04-01的日线数据
try:
    # 未复权
    daily_raw = pro.daily(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date,
        adj=None
    )
    
    # 前复权
    daily_adj = pro.daily(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date,
        adj='qfq'
    )
    
    # 获取复权因子
    adj_factor_data = pro.adj_factor(
        ts_code=ts_code,
        start_date=target_date,
        end_date=target_date
    )
    
    # 获取最新日期的复权因子（作为基准）
    latest_adj = pro.adj_factor(
        ts_code=ts_code,
        start_date="20251128",  # 最新日期
        end_date="20251128"
    )
    
    if daily_raw is not None and not daily_raw.empty:
        raw_close = daily_raw.iloc[0]['close']
        print(f"\n未复权收盘价: {raw_close:.2f}")
    
    if daily_adj is not None and not daily_adj.empty:
        adj_close = daily_adj.iloc[0]['close']
        print(f"Tushare前复权收盘价: {adj_close:.2f}")
        print(f"用户期望价格: 59.27")
        print(f"差异: {abs(adj_close - 59.27):.2f}")
    
    if adj_factor_data is not None and not adj_factor_data.empty:
        adj_factor = adj_factor_data.iloc[0]['adj_factor']
        print(f"\n2025-04-01 复权因子: {adj_factor:.4f}")
    
    if latest_adj is not None and not latest_adj.empty:
        latest_adj_factor = latest_adj.iloc[0]['adj_factor']
        print(f"最新日期复权因子: {latest_adj_factor:.4f}")
        
        # 计算前复权价格
        if daily_raw is not None and not daily_raw.empty and adj_factor_data is not None:
            # 方法1：使用最新复权因子作为基准
            ratio1 = adj_factor / latest_adj_factor
            price1 = raw_close * ratio1
            print(f"\n方法1（当前复权因子/最新复权因子）:")
            print(f"  复权比例: {ratio1:.4f}")
            print(f"  复权价格: {price1:.2f}")
            
            # 方法2：使用1.0作为基准（如果复权因子是相对于某个基准的）
            ratio2 = adj_factor / 1.0
            price2 = raw_close * ratio2
            print(f"\n方法2（当前复权因子/1.0）:")
            print(f"  复权比例: {ratio2:.4f}")
            print(f"  复权价格: {price2:.2f}")
            
            # 方法3：反向计算（如果数据已经是复权后的）
            if daily_adj is not None and not daily_adj.empty:
                ratio3 = latest_adj_factor / adj_factor
                price3 = raw_close * ratio3
                print(f"\n方法3（最新复权因子/当前复权因子）:")
                print(f"  复权比例: {ratio3:.4f}")
                print(f"  复权价格: {price3:.2f}")
                
                # 检查：如果Tushare返回的前复权价格和未复权价格相同，说明什么？
                if abs(adj_close - raw_close) < 0.01:
                    print(f"\n注意：Tushare返回的前复权价格({adj_close:.2f})和未复权价格({raw_close:.2f})相同！")
                    print(f"这可能意味着：")
                    print(f"  1. 该日期没有除权除息，所以价格相同")
                    print(f"  2. 或者需要使用分钟线数据来验证")
    
    # 检查分钟线数据（如果可能）
    print(f"\n尝试获取分钟线数据（可能需要更高权限）...")
    try:
        # 获取2025-04-01 15:00的分钟数据
        min_data = pro.stk_mins(
            ts_code=ts_code,
            freq='1',
            start_time=target_date + "145900",
            end_time=target_date + "150100",
            adj='qfq'
        )
        if min_data is not None and not min_data.empty:
            print(f"分钟线前复权收盘价（15:00）: {min_data.iloc[-1]['close']:.2f}")
    except Exception as e:
        print(f"无法获取分钟线数据（权限限制）: {str(e)[:50]}")
        
except Exception as e:
    print(f"获取失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)

