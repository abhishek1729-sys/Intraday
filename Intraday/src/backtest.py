# src/backtest.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

CAPITAL = 60_000
DAILY_STOP = 0.01   # 1% daily stop = ₹600
TCOST_BPS = 3       # round-trip ~3 bps placeholder

def backtest_symbol(df: pd.DataFrame, qty_per_trade: int = 15) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy().reset_index(drop=True)
    df["ret"] = df["close"].pct_change().fillna(0.0)

    sig = df["signal"].fillna(0.0)
    pos = np.where(sig != 0, sig, np.nan)
    pos = pd.Series(pos).ffill().fillna(0.0)

    trade_on_bar = pos.diff().fillna(pos).ne(0).astype(int)

    gross_pnl = (pos.shift(1).fillna(0.0) * df["ret"]) * df["close"] * qty_per_trade
    turns = trade_on_bar.abs()
    tcost = (TCOST_BPS / 1e4) * df["close"] * qty_per_trade * turns

    df["pnl"] = gross_pnl - tcost

    # apply daily stop
    d = df["datetime"].dt.date
    out = []
    for _, g in df.groupby(d):
        pnl = g["pnl"].to_numpy().copy()
        cum = pnl.cumsum()
        if cum.size:
            hit = np.argmax(cum <= -(CAPITAL * DAILY_STOP))
            if cum[hit] <= -(CAPITAL * DAILY_STOP):
                pnl[hit+1:] = 0.0
        gg = g.copy()
        gg["pnl"] = pnl
        out.append(gg)
    df = pd.concat(out, ignore_index=True)

    df["eq"] = df["pnl"].cumsum()
    return df

def metrics(df: pd.DataFrame) -> dict:
    if df.empty: 
        return {"trades": 0, "net_pnl": 0.0, "daily_sharpe": 0.0, "max_drawdown": 0.0}
    trades = (df["signal"].fillna(0).astype(float) != 0).sum()
    daily = df.groupby(df["datetime"].dt.date)["pnl"].sum()
    sharpe = 0.0 if daily.std(ddof=1) == 0 else (daily.mean() / daily.std(ddof=1)) * np.sqrt(252)
    mdd = (df["eq"].cummax() - df["eq"]).max()
    return {
        "trades": int(trades),
        "net_pnl": round(df["eq"].iloc[-1], 2),
        "daily_sharpe": round(float(sharpe), 2),
        "max_drawdown": round(float(mdd), 2),
    }
