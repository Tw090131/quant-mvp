import pandas as pd

df = pd.read_parquet('E:\\BaiduNetdiskDownload\\data\\stock_1min\\000423.SZ.parquet')
target_date = pd.Timestamp('2025-04-01')
date_data = df[df.index.get_level_values('trade_date') == target_date]

print('2025-04-01 15:00:00 数据:')
last_row = date_data.iloc[-1]
close_price = last_row['close']
adj_factor = last_row['adj_factor']
print(f'原始收盘价: {close_price:.2f}')
print(f'复权因子: {adj_factor:.4f}')

latest_adj = df['adj_factor'].iloc[-1]
print(f'最新复权因子: {latest_adj:.4f}')

# 方法1：当前使用的方法（前复权）
adj_ratio_1 = adj_factor / latest_adj
adj_price_1 = close_price * adj_ratio_1
print(f'\n方法1（前复权，当前使用）:')
print(f'  复权比例: {adj_ratio_1:.4f}')
print(f'  复权后价格: {adj_price_1:.2f}')

# 方法2：反向计算（如果数据已经是复权后的）
adj_ratio_2 = latest_adj / adj_factor
adj_price_2 = close_price * adj_ratio_2
print(f'\n方法2（反向复权）:')
print(f'  复权比例: {adj_ratio_2:.4f}')
print(f'  复权后价格: {adj_price_2:.2f}')

# 方法3：直接使用复权因子（后复权）
adj_price_3 = close_price * adj_factor
print(f'\n方法3（后复权，直接乘以复权因子）:')
print(f'  复权后价格: {adj_price_3:.2f}')

# 检查用户期望的59.27
target_price = 59.27
print(f'\n用户期望价格: {target_price:.2f}')
print(f'需要复权比例: {target_price / close_price:.4f}')
print(f'如果使用复权因子计算: {close_price * (target_price / close_price):.2f}')

# 检查是否有其他日期的数据用于参考
print(f'\n检查2025-04-02的数据（如果存在）:')
next_date = pd.Timestamp('2025-04-02')
next_data = df[df.index.get_level_values('trade_date') == next_date]
if len(next_data) > 0:
    next_row = next_data.iloc[0]
    print(f'  开盘价: {next_row["open"]:.2f}')
    print(f'  复权因子: {next_row["adj_factor"]:.4f}')

