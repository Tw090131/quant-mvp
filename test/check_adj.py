import pandas as pd

df = pd.read_parquet('E:\\BaiduNetdiskDownload\\data\\stock_1min\\000423.SZ.parquet')
target_date = pd.Timestamp('2025-04-01')
date_data = df[df.index.get_level_values('trade_date') == target_date]

print('2025-04-01 原始数据（最后一条）:')
last_row = date_data.iloc[-1]
close_price = last_row['close']
adj_factor = last_row['adj_factor']
print(f'收盘价: {close_price:.2f}')
print(f'复权因子: {adj_factor:.4f}')

latest_adj = df['adj_factor'].iloc[-1]
print(f'最新复权因子: {latest_adj:.4f}')

adj_ratio = adj_factor / latest_adj
print(f'复权比例: {adj_ratio:.4f}')
print(f'复权后价格: {close_price * adj_ratio:.2f}')

# 检查是否有其他日期的数据用于对比
print('\n对比：2025-04-01 15:00:00 的数据')
if len(date_data) > 0:
    close_1500 = date_data[date_data.index.get_level_values('trade_time').hour == 15]
    if len(close_1500) > 0:
        last_1500 = close_1500.iloc[-1]
        print(f'收盘价: {last_1500["close"]:.2f}')
        print(f'复权因子: {last_1500["adj_factor"]:.4f}')
        adj_ratio_1500 = last_1500["adj_factor"] / latest_adj
        print(f'复权比例: {adj_ratio_1500:.4f}')
        print(f'复权后价格: {last_1500["close"] * adj_ratio_1500:.2f}')

