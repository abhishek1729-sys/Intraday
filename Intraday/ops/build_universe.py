# ops/build_universe.py
from src.universe import build_universe

if __name__ == "__main__":
    u = build_universe(top_n=6)
    if u.empty:
        print("[WARN] No raw data found. Run fetch first.")
    else:
        print(u)
        print("\n[OK] Wrote data/active_symbols.csv")
