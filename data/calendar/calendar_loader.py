
import os
import pandas as pd
import akshare as ak

CALENDAR_DIR = os.path.dirname(__file__)
CALENDAR_PATH = os.path.join(CALENDAR_DIR, "sse_calendar.csv")

def load_trade_calendar(refresh=False) -> pd.DataFrame:
    if os.path.exists(CALENDAR_PATH) and not refresh:
        return pd.read_csv(CALENDAR_PATH, parse_dates=["trade_date"])

    df = ak.tool_trade_date_hist_sina()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df.to_csv(CALENDAR_PATH, index=False)
    return df
