from __future__ import annotations
from pathlib import Path
import pandas as pd
from .utils import ensure_dt_index

COLUMNS = ["symbol","datetime","open","high","low","close","volume"]

class CSVMinuteLoader:
    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)

    def load_symbol(self, symbol: str) -> pd.DataFrame:
        f = self.data_dir / f"{symbol}.csv"
        df = pd.read_csv(f)
        df = df[COLUMNS]
        return ensure_dt_index(df)

    def load_universe(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        return {s: self.load_symbol(s) for s in symbols}
