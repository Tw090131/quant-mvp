# engine/trade_log.py
import pandas as pd

def record_trade(daily_trades, date, code, action, price, volume, fee_rate=0.001, position=0):
    amount = price * volume
    fee = amount * fee_rate
    daily_trades.append({
        "date": pd.to_datetime(date),
        "code": code,
        "action": action,
        "price": price,
        "volume": volume,
        "amount": amount,
        "fee": fee,
        "position": position
    })

def get_daily_trades_df(daily_trades):
    return pd.DataFrame(daily_trades)
