# src/signals.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
from joblib import dump, load

from .utils import zscore

PROC_DIR = Path("data/processed")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

def load_3min(symbol: str) -> pd.DataFrame:
    # choose latest 3-min file for the symbol (today or *_YDAY_3min.csv if you make those)
    # for now, resample on the fly from raw if needed; here we assume processed exists
    cands = sorted(PROC_DIR.glob(f"{symbol}*_3min.csv")) or list(PROC_DIR.glob(f"{symbol}.csv"))
    if not cands:
        return pd.DataFrame()
    return pd.read_csv(cands[-1], parse_dates=["datetime"])

def signal_meanrev(df: pd.DataFrame, win: int = 20, z: float = 1.5) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy().sort_values("datetime")
    ret = df["close"].pct_change().fillna(0.0)
    zs = zscore(ret, win=win).fillna(0.0)
    df["signal"] = 0.0
    df.loc[zs < -z, "signal"] = +1.0   # go long on down-move
    df.loc[zs > +z, "signal"] = -1.0   # go short on up-move
    df["z"] = zs
    return df

def save_model(symbol: str, params: dict):
    dump(params, MODEL_DIR / f"{symbol}_meanrev.joblib")

def load_model(symbol: str) -> dict | None:
    p = MODEL_DIR / f"{symbol}_meanrev.joblib"
    if p.exists():
        return load(p)
    return None
