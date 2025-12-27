"""
测试读取 Parquet 文件
"""
import pandas as pd
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.cache_loader import _load_cache_file, _code_to_ts_code

def test_read_parquet(file_path: str, code: str = None):
    """
    测试读取 Parquet 文件
    
    Args:
        file_path: Parquet 文件路径
        code: 股票代码（可选，用于从多股票文件中筛选）
    """
    print(f"=" * 60)
    print(f"测试读取 Parquet 文件: {file_path}")
    print(f"=" * 60)
    
    # 方法1：直接使用 pandas 读取
    print("\n【方法1】直接使用 pandas.read_parquet:")
    try:
        df_raw = pd.read_parquet(file_path)
        print(f"[OK] 读取成功，共 {len(df_raw)} 条数据")
        print(f"  列名: {df_raw.columns.tolist()}")
        print(f"  索引类型: {type(df_raw.index)}")
        if isinstance(df_raw.index, pd.MultiIndex):
            print(f"  MultiIndex 层级: {df_raw.index.names}")
        else:
            print(f"  索引名称: {df_raw.index.name}")
        
        # 显示前几行
        print(f"\n  前5行数据:")
        print(df_raw.head())
        
        # 显示数据类型
        print(f"\n  数据类型:")
        print(df_raw.dtypes)
        
        # 如果有复权因子列，显示复权因子信息
        if "adj_factor" in df_raw.columns:
            adj_values = df_raw["adj_factor"].dropna()
            if len(adj_values) > 0:
                print(f"\n  复权因子信息:")
                print(f"    最小值: {adj_values.min():.4f}")
                print(f"    最大值: {adj_values.max():.4f}")
                print(f"    最新值: {adj_values.iloc[-1]:.4f}")
                print(f"    初始值: {adj_values.iloc[0]:.4f}")
        
        # 如果有价格列，显示价格信息
        if "close" in df_raw.columns:
            close_values = df_raw["close"].dropna()
            if len(close_values) > 0:
                print(f"\n  收盘价信息:")
                print(f"    最小值: {close_values.min():.2f}")
                print(f"    最大值: {close_values.max():.2f}")
                print(f"    最新值: {close_values.iloc[-1]:.2f}")
                print(f"    初始值: {close_values.iloc[0]:.2f}")
        
    except Exception as e:
        print(f"[ERROR] 读取失败: {e}")
        return
    
    # 方法2：使用 cache_loader 的 _load_cache_file 函数（会处理复权等）
    print("\n【方法2】使用 cache_loader._load_cache_file（处理复权等）:")
    try:
        df_processed = _load_cache_file(file_path, "parquet", code=code)
        print(f"[OK] 处理成功，共 {len(df_processed)} 条数据")
        print(f"  列名: {df_processed.columns.tolist()}")
        print(f"  索引类型: {type(df_processed.index)}")
        print(f"  索引名称: {df_processed.index.name}")
        
        # 显示前几行
        print(f"\n  前5行数据:")
        print(df_processed.head())
        
        # 如果有价格列，显示价格信息（复权后）
        if "close" in df_processed.columns:
            close_values = df_processed["close"].dropna()
            if len(close_values) > 0:
                print(f"\n  复权后收盘价信息:")
                print(f"    最小值: {close_values.min():.2f}")
                print(f"    最大值: {close_values.max():.2f}")
                print(f"    最新值: {close_values.iloc[-1]:.2f}")
                print(f"    初始值: {close_values.iloc[0]:.2f}")
                
                # 对比复权前后的价格（如果有复权因子）
                if "adj_factor" in df_raw.columns and len(df_raw) == len(df_processed):
                    print(f"\n  复权前后价格对比（最后一条记录）:")
                    raw_close = df_raw["close"].iloc[-1] if len(df_raw) > 0 else None
                    processed_close = df_processed["close"].iloc[-1] if len(df_processed) > 0 else None
                    if raw_close is not None and processed_close is not None:
                        print(f"    复权前: {raw_close:.2f}")
                        print(f"    复权后: {processed_close:.2f}")
                        print(f"    变化: {processed_close - raw_close:.2f} ({((processed_close / raw_close - 1) * 100):.2f}%)")
        
        # 检查特定日期的数据
        if len(df_processed) > 0:
            sample_date = df_processed.index[-1]
            if isinstance(sample_date, pd.Timestamp):
                print(f"\n  最后一条记录日期: {sample_date}")
                if sample_date in df_processed.index:
                    row = df_processed.loc[sample_date]
                    if "close" in row:
                        print(f"  收盘价: {row['close']:.2f}")
        
        # 检查2025-04-01的数据（如果存在）
        if len(df_processed) > 0 and "trade_date" in df_processed.columns:
            target_date = pd.Timestamp("2025-04-01")
            date_data = df_processed[df_processed["trade_date"] == target_date]
            if len(date_data) > 0:
                print(f"\n  2025-04-01 数据检查:")
                last_row = date_data.iloc[-1]
                print(f"    收盘价: {last_row['close']:.2f}")
                print(f"    开盘价: {last_row['open']:.2f}")
                print(f"    最高价: {last_row['high']:.2f}")
                print(f"    最低价: {last_row['low']:.2f}")
                
                # 对比原始数据
                if len(df_raw) > 0:
                    # 找到原始数据中2025-04-01的数据
                    if isinstance(df_raw.index, pd.MultiIndex):
                        raw_date_data = df_raw[df_raw.index.get_level_values('trade_date') == target_date]
                    else:
                        raw_date_data = df_raw[df_raw.index.date == target_date.date()]
                    
                    if len(raw_date_data) > 0:
                        raw_last_row = raw_date_data.iloc[-1]
                        print(f"\n    原始数据（未复权）:")
                        print(f"      收盘价: {raw_last_row['close']:.2f}")
                        if "adj_factor" in raw_last_row:
                            print(f"      复权因子: {raw_last_row['adj_factor']:.4f}")
                            # 计算复权比例
                            if len(df_raw) > 0:
                                latest_adj = df_raw["adj_factor"].iloc[-1] if "adj_factor" in df_raw.columns else 1.0
                                adj_ratio = raw_last_row['adj_factor'] / latest_adj
                                print(f"      复权比例: {adj_ratio:.4f}")
                                print(f"      复权后价格应该是: {raw_last_row['close'] * adj_ratio:.2f}")
        
    except Exception as e:
        print(f"[ERROR] 处理失败: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'=' * 60}\n")


if __name__ == "__main__":
    # 测试用例1：读取单个股票的分钟线数据
    test_file_1 = "E:\\BaiduNetdiskDownload\\data\\stock_1min\\000423.SZ.parquet"
    if os.path.exists(test_file_1):
        test_read_parquet(test_file_1, code="000423")
    else:
        print(f"文件不存在: {test_file_1}")
    
    # 测试用例2：读取日线数据（如果存在）
    test_file_2 = "E:\\BaiduNetdiskDownload\\data\\stock_daily.parquet"
    if os.path.exists(test_file_2):
        print("\n" + "=" * 60)
        print("测试读取日线数据（多股票文件）")
        print("=" * 60)
        test_read_parquet(test_file_2, code="000423")
    else:
        print(f"文件不存在: {test_file_2}")
    
    # 测试用例3：用户指定的文件
    if len(sys.argv) > 1:
        user_file = sys.argv[1]
        user_code = sys.argv[2] if len(sys.argv) > 2 else None
        if os.path.exists(user_file):
            test_read_parquet(user_file, code=user_code)
        else:
            print(f"文件不存在: {user_file}")
    else:
        print("\n提示: 可以传入文件路径作为参数，例如:")
        print("  python test/test.py D:/stock_data/000001.SZ.parquet 000001")
