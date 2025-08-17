from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np

PROC = Path("data/processed")
UNIV = Path("data/active_symbols.csv")

CAPITAL = 60_000       # you said you’ll deploy 60k
DAILY_STOP = 0.01      # 1% daily stop -> ₹600
TCOST_BPS = 3          # round-trip ~3 bps as a placeholder

def load_signals(sym: str) -> pd.DataFrame:
    p = PROC / f"{sym}_signals.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, parse_dates=["datetime"])
    return df.sort_values("datetime")

def backtest_symbol(df: pd.DataFrame, qty_per_trade: int = 15) -> pd.DataFrame:
    if df.empty: 
        return df
    df = df.copy().reset_index(drop=True)

    # simple execution: enter at next bar close when signal != 0; flat when signal returns to 0
    df["ret"] = df["close"].pct_change().fillna(0.0)
    sig = df["signal"].fillna(0.0)

    # position is last nonzero signal until it flips back to 0
    pos = np.where(sig != 0, sig, np.nan)
    pos = pd.Series(pos).ffill().fillna(0.0)

    # trade detection
    trade_on_bar = pos.diff().fillna(pos).ne(0).astype(int)

    # PnL in rupees with quantity
    gross_pnl = (pos.shift(1).fillna(0.0) * df["ret"]) * df["close"] * qty_per_trade

    # transaction costs when position changes (both entry + exit counted via |Δpos|)
    turns = trade_on_bar.abs()
    tcost = (TCOST_BPS / 1e4) * df["close"] * qty_per_trade * turns

    df["pnl"] = gross_pnl - tcost

    # daily stop at ₹600 loss (1% of 60k): once hit, zero out remainder of day
    d = df["datetime"].dt.date
    out = []
    for _, g in df.groupby(d):
        pnl = g["pnl"].to_numpy().copy()
        cum = pnl.cumsum()
        hit = np.argmax(cum <= -CAPITAL * DAILY_STOP)
        if cum.size and cum[hit] <= -CAPITAL * DAILY_STOP:
            pnl[hit+1:] = 0.0
        gg = g.copy()
        gg["pnl"] = pnl
        out.append(gg)
    df = pd.concat(out, ignore_index=True)

    df["eq"] = df["pnl"].cumsum()
    return df

def metrics(df: pd.DataFrame) -> dict:
    if df.empty: return {"trades": 0}
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

def main():
    if not UNIV.exists():
        print("[WARN] run universe builder first"); return
    syms = pd.read_csv(UNIV)["symbol"].tolist()
    if not syms:
        print("[WARN] empty universe"); return

    summary = []
    OUT = Path("data/processed/equity_curves")
    OUT.mkdir(parents=True, exist_ok=True)

    for s in syms:
        sig = load_signals(s)
        if sig.empty:
            print(f"[WARN] no signals for {s}"); continue
        bt = backtest_symbol(sig, qty_per_trade=15)
        m = metrics(bt); m["symbol"] = s
        summary.append(m)
        bt[["datetime","eq","pnl"]].to_csv(OUT / f"{s}_equity.csv", index=False)
        print(f"[OK] {s}: {m}")

    if summary:
        pd.DataFrame(summary).to_csv(OUT / "summary.csv", index=False)
        print(f"\n[OK] wrote {OUT/'summary.csv'}")
    else:
        print("[INFO] nothing to report")

if __name__ == "__main__":
    main()
