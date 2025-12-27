"""
检查复权计算，找出正确的复权方法
"""
raw_price = 61.81
target_price = 59.27
adj_factor = 11.2037
latest_adj = 11.7564

print("=" * 60)
print("复权计算检查")
print("=" * 60)

print(f"\n原始价格: {raw_price:.2f}")
print(f"目标价格（akshare）: {target_price:.2f}")
print(f"2025-04-01 复权因子: {adj_factor:.4f}")
print(f"最新复权因子: {latest_adj:.4f}")

# 方法1：当前复权因子 / 最新复权因子
ratio1 = adj_factor / latest_adj
result1 = raw_price * ratio1
print(f"\n方法1（当前复权因子/最新复权因子）:")
print(f"  复权比例: {ratio1:.4f}")
print(f"  复权后价格: {result1:.2f}")
print(f"  与目标差异: {abs(result1 - target_price):.2f}")

# 方法2：计算需要的复权比例
needed_ratio = target_price / raw_price
print(f"\n需要的复权比例: {needed_ratio:.4f}")

# 方法3：如果使用不同的基准
# 假设基准复权因子应该是某个值
base_adj_candidate = adj_factor / needed_ratio
print(f"\n如果复权比例是 {needed_ratio:.4f}，基准复权因子应该是: {base_adj_candidate:.4f}")

# 方法4：检查是否应该使用1.0作为基准（后复权）
result4 = raw_price * adj_factor
print(f"\n方法4（直接乘以复权因子，后复权）:")
print(f"  复权后价格: {result4:.2f}")

# 方法5：检查是否应该使用当日复权因子作为基准
# 如果当日复权因子就是基准，那么复权比例应该是 1.0
if abs(adj_factor - latest_adj) < 0.001:
    print(f"\n注意：当日复权因子({adj_factor:.4f})和最新复权因子({latest_adj:.4f})相同")
    print(f"这说明该日期没有除权除息，复权比例应该是 1.0")
    print(f"但原始价格({raw_price:.2f})和目标价格({target_price:.2f})不同")
    print(f"这可能意味着：")
    print(f"  1. 数据源不同（akshare vs tushare）")
    print(f"  2. 复权基准日期不同")
    print(f"  3. 需要使用不同的复权方法")

print("\n" + "=" * 60)

