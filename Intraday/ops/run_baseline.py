# ops/run_baseline.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.signals import load_3min, signal_meanrev, save_model
from src.universe import OUT as UNIVERSE_CSV

def main():
    if not Path(UNIVERSE_CSV).exists():
        print("[WARN] active_symbols.csv not found. Run ops/build_universe.py")
        return

    u = pd.read_csv(UNIVERSE_CSV)["symbol"].tolist()
    if not u:
        print("[WARN] Universe empty.")
        return

    results = []
    for sym in u:
        df = load_3min(sym)
        if df.empty:
            print(f"[WARN] no 3-min for {sym}")
            continue
        # model params (could be tuned later)
        params = {"win": 20, "z": 1.5}
        df_sig = signal_meanrev(df, **params)
        # persist "model"
        save_model(sym, params)
        # write signals next to processed file
        out = Path("data/processed") / f"{sym}_signals.csv"
        df_sig.to_csv(out, index=False)
        results.append((sym, len(df_sig)))
        print(f"[OK] {sym}: signals={len(df_sig)} -> {out}")

    if not results:
        print("[INFO] nothing produced.")

if __name__ == "__main__":
    main()
