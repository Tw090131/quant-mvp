# 使用 tusharePro 数据源

## 安装和配置

### 1. 安装 tushare

```bash
pip install tushare
```

### 2. 设置 tushare token

在使用前，需要先设置 tushare token（需要注册 tushare 账号获取）：

```python
import tushare as ts
ts.set_token('your_token_here')  # 替换为你的 token
```

可以在 `run.py` 开头添加：

```python
import tushare as ts
ts.set_token('your_token_here')
```

### 3. 切换数据源

在 `config.py` 中修改：

```python
# 数据源选择：'akshare' 或 'tushare'
DATA_SOURCE = "tushare"  # 改为 tushare
```

或者在 `run.py` 中动态设置：

```python
from config import Config
Config.DATA_SOURCE = "tushare"
```

## 使用方式

切换数据源后，其他代码无需修改，`cache_loader` 会自动使用 tushare 数据源：

```python
from data.cache_loader import load_daily_df_with_cache, load_minute_df_with_cache

# 使用方式与 akshare 完全相同
df = load_daily_df_with_cache("300548", start="2025-08-01", end="2025-11-20")
```

## 注意事项

1. **Token 设置**：必须在导入 tushare 相关模块前设置 token
2. **数据格式**：tushare 返回的数据格式会自动转换为标准格式，与 akshare 保持一致
3. **股票代码**：代码会自动转换格式（如 "300548" -> "300548.SZ"）
4. **数据限制**：tusharePro 有积分限制，不同积分等级可获取的数据范围不同
5. **回退机制**：如果 tushare 未安装或导入失败，会自动回退到 akshare

## 数据源对比

| 特性 | akshare | tusharePro |
|------|---------|------------|
| 免费 | ✅ 完全免费 | ⚠️ 需要积分 |
| 历史数据 | ✅ 支持 | ✅ 支持（需积分） |
| 分钟线数据 | ✅ 支持 | ✅ 支持（需积分） |
| 数据质量 | 良好 | 优秀 |
| 稳定性 | 良好 | 优秀 |
| 速度 | 中等 | 快 |

## 示例

```python
# run.py
import tushare as ts
from config import Config

# 设置 tushare token
ts.set_token('your_token_here')

# 切换数据源
Config.DATA_SOURCE = "tushare"

# 其他代码保持不变
from data.cache_loader import load_daily_df_with_cache
df = load_daily_df_with_cache("300548", start="2025-08-01")
```

