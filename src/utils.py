from __future__ import annotations
import numpy as np
import pandas as pd

RANDOM_STATE = 42

def set_seed(seed:int = RANDOM_STATE):
    import random
    random.seed(seed); np.random.seed(seed)

def ensure_dt_index(df: pd.DataFrame, ts_col: str = "datetime") -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df[ts_col] = pd.to_datetime(df[ts_col])
        df = df.set_index(ts_col).sort_index()
    return df

def zscore(s: pd.Series, win: int) -> pd.Series:
    r = s.rolling(win)
    return (s - r.mean()) / (r.std(ddof=0) + 1e-9)
