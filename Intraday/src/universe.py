# src/universe.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
OUT = Path("data/active_symbols.csv")

def load_recent_raw(days: int = 2) -> list[pd.DataFrame]:
    # simple: read all raw CSVs (you can filter by modified time later)
    dfs = []
    for p in RAW_DIR.glob("*.csv"):
        try:
            df = pd.read_csv(p, parse_dates=["datetime"])
            df["symbol"] = df["symbol"].iloc[0] if "symbol" in df.columns else p.stem.split("_")[0]
            df["fname"] = p.name
            dfs.append(df)
        except Exception:
            pass
    return dfs

def metrics(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series({"turnover": 0.0, "range_pct": 0.0})
    # last trading day in the file
    d = df["datetime"].dt.date.max()
    day = df[df["datetime"].dt.date == d].copy()
    if day.empty:
        return pd.Series({"turnover": 0.0, "range_pct": 0.0})
    turnover = (day["close"] * day["volume"]).sum()
    rng = (day["high"].max() - day["low"].min()) / max(day["close"].iloc[0], 1e-9)
    return pd.Series({"turnover": turnover, "range_pct": rng * 100})

def build_universe(top_n: int = 6) -> pd.DataFrame:
    rows = []
    for df in load_recent_raw():
        sym = df["symbol"].iloc[0]
        s = metrics(df)
        s["symbol"] = sym
        rows.append(s)
    u = pd.DataFrame(rows)
    if u.empty:
        return u
    u = u.sort_values(["turnover", "range_pct"], ascending=[False, False]).head(top_n)
    u = u[["symbol", "turnover", "range_pct"]]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    u.to_csv(OUT, index=False)
    return u
