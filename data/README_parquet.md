# Parquet 缓存格式支持

## 功能说明

框架现在支持使用 Parquet 格式存储和加载缓存数据，相比 CSV 格式有以下优势：
- **更快的读写速度**：Parquet 是列式存储格式，读取速度更快
- **更小的文件大小**：Parquet 使用压缩，文件更小
- **更好的数据类型支持**：自动保留数据类型信息

## 使用方法

### 1. 配置外部 Parquet 路径（推荐）

如果你有从 tushare 下载的 parquet 数据，可以在 `config.py` 中配置外部路径：

```python
# 日线数据路径（单个文件，包含所有股票，需要有 ts_code 列）
PARQUET_DAILY_PATH = r"E:\BaiduNetdiskDownload\data\stock_daily.parquet"

# 分钟线数据目录（每个股票一个文件，文件名格式：{ts_code}.parquet）
PARQUET_MINUTE_DIR = r"E:\BaiduNetdiskDownload\data\stock_1min"
```

**文件命名规则**：
- 日线：单个文件包含所有股票，需要有 `ts_code` 列用于筛选
- 分钟线：每个股票一个文件，文件名格式为 `{ts_code}.parquet`，例如：
  - `000001.SZ.parquet`
  - `300548.SZ.parquet`
  - `600519.SH.parquet`

### 2. 配置缓存格式偏好

在 `config.py` 中设置：

```python
# 缓存格式偏好：'parquet'（优先使用parquet）、'csv'（优先使用csv）、'auto'（自动选择，parquet优先）
CACHE_FORMAT_PREFERENCE = "auto"  # 默认自动选择，优先使用 parquet
```

选项说明：
- `"auto"`：自动选择，优先使用 parquet（如果存在），否则使用 csv
- `"parquet"`：优先使用 parquet 格式
- `"csv"`：优先使用 csv 格式

### 3. 使用本地缓存目录的 Parquet 文件

如果你将 parquet 文件放在 `data_cache` 目录下，按照以下命名规则：

**日线数据**：
- `{code}.parquet`，例如：`300548.parquet`

**分钟线数据**：
- `{code}_{period}.parquet`，例如：`300548_1min.parquet`

**数据格式要求**：
- DataFrame 索引必须是 datetime 类型（或包含 `datetime` 或 `trade_date` 列）
- 必须包含以下列：`open`, `close`, `high`, `low`, `volume`
- 如果是多股票文件（日线），需要有 `ts_code` 列用于筛选
- 列名会自动转换：`vol` -> `volume`，`trade_date` -> `datetime`

### 4. 自动识别和加载

框架会按以下优先级查找数据：
1. **外部 parquet 路径**（如果配置了 `PARQUET_DAILY_PATH` 或 `PARQUET_MINUTE_DIR`）
2. **本地缓存目录的 parquet 文件**（`data_cache` 目录）
3. **本地缓存目录的 csv 文件**（`data_cache` 目录）
4. **从 API 获取数据**（如果以上都不存在）

保存时使用配置的格式偏好。

### 5. 示例

```python
# 配置外部路径（在 config.py 中）
PARQUET_DAILY_PATH = r"E:\BaiduNetdiskDownload\data\stock_daily.parquet"
PARQUET_MINUTE_DIR = r"E:\BaiduNetdiskDownload\data\stock_1min"

# 使用框架加载数据
from data.cache_loader import load_minute_df_with_cache, load_daily_df_with_cache

# 自动从外部 parquet 文件加载（如果存在）
df_minute = load_minute_df_with_cache("300548", period="1min", start="2025-08-01")
df_daily = load_daily_df_with_cache("300548", start="2025-08-01")

# 框架会自动：
# 1. 查找 E:\BaiduNetdiskDownload\data\stock_1min\300548.SZ.parquet
# 2. 如果不存在，查找 data_cache/300548_1min.parquet
# 3. 如果不存在，查找 data_cache/300548_1min.csv
# 4. 如果都不存在，从 API 获取
```

## 数据格式示例

Parquet 文件中的 DataFrame 应该类似这样：

```
                    open    close   high    low     volume
datetime
2025-08-01 09:30:00 146.04  146.04  146.04  146.04  5123
2025-08-01 09:31:00 146.04  149.60  149.68  146.58  19293
...
```

索引必须是 datetime 类型，列名必须匹配。

## 注意事项

1. **依赖库**：使用 parquet 需要安装 `pyarrow` 或 `fastparquet`：
   ```bash
   pip install pyarrow
   # 或
   pip install fastparquet
   ```

2. **兼容性**：框架同时支持 CSV 和 Parquet，可以混合使用

3. **迁移**：如果要从 CSV 迁移到 Parquet，只需：
   - 将 CSV 文件转换为 Parquet 格式
   - 放在 `data_cache` 目录下
   - 框架会自动识别并使用

4. **保存格式**：新获取的数据会按照 `CACHE_FORMAT_PREFERENCE` 配置保存

