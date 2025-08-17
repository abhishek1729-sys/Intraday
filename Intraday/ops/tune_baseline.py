# ops/tune_baseline.py
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pathlib import Path
import pandas as pd
from src.signals import signal_meanrev, load_model, save_model, load_3min
from src.backtest import backtest_symbol, metrics

UNIVERSE_CSV = Path("data/active_symbols.csv")
OUT = Path("data/processed/tuning")
OUT.mkdir(parents=True, exist_ok=True)

WINS = [10, 20, 30, 40]
ZTHS = [1.0, 1.5, 2.0]

def main():
    if not UNIVERSE_CSV.exists():
        print("[WARN] run universe builder first"); return
    syms = pd.read_csv(UNIVERSE_CSV)["symbol"].tolist()
    if not syms:
        print("[WARN] empty universe"); return

    summary_rows = []
    for sym in syms:
        df = load_3min(sym)
        if df.empty:
            print(f"[WARN] no 3-min for {sym}"); 
            continue

        best = None
        best_row = None

        for w in WINS:
            for z in ZTHS:
                df_sig = signal_meanrev(df, win=w, z=z)
                bt = backtest_symbol(df_sig, qty_per_trade=15)
                m = metrics(bt)
                row = {"symbol": sym, "win": w, "z": z, **m}
                summary_rows.append(row)
                # choose by net pnl then sharpe
                key = (m["net_pnl"], m["daily_sharpe"])
                if (best is None) or (key > best):
                    best = key
                    best_row = row

        # persist best params
        if best_row:
            save_model(sym, {"win": int(best_row["win"]), "z": float(best_row["z"])})
            print(f"[OK] {sym}: best -> win={best_row['win']}, z={best_row['z']}, pnl={best_row['net_pnl']}")

    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(OUT / "tuning_results.csv", index=False)
        print(f"\n[OK] wrote {OUT/'tuning_results.csv'}")

if __name__ == "__main__":
    main()
