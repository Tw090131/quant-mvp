import akshare as ak
import pandas as pd

# === 日线 ===
def load_ashare_daily(code: str, start: str, end: str = None) -> pd.DataFrame:
    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start.replace("-", ""),
        end_date=None if end is None else end.replace("-", ""),
        adjust="qfq",
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={
        "日期": "datetime",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
    })
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime").sort_index()
    return df

# === 分钟线 ===
def load_ashare_minute(code: str, period: str = "1min", start=None, end=None) -> pd.DataFrame:
    """
    period 支持 "1min", "5min", "15min", "30min", "60min"
    内部会转换成 akshare 要求的 "1", "5", "15", "30", "60"
    """
    period_map = {
        "1min": "1",
        "5min": "5",
        "15min": "15",
        "30min": "30",
        "60min": "60"
    }

    if period not in period_map:
        raise ValueError(f"不支持的 period: {period}, 支持 {list(period_map.keys())}")

    ak_period = period_map[period]

    try:
        df = ak.stock_zh_a_hist_min_em(symbol=code,start_date=start, period=ak_period, adjust="qfq")
    except Exception as e:
        print(f"[WARN] {code} {period}分钟线获取失败: {e}")
        return pd.DataFrame()

    if df is None or df.empty or "时间" not in df.columns:
        print(f"[WARN] {code} 数据为空或缺失 '时间'，跳过")
        return pd.DataFrame()

    df = df.rename(columns={
        "时间": "datetime",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume"
    })

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.set_index("datetime").sort_index()

    return df
