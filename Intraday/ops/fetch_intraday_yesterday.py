# ops/fetch_intraday_yesterday.py
from __future__ import annotations
import os, yaml, pandas as pd
from datetime import time
from kiteconnect import KiteConnect

RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

# --- auth / setup ---
def load_kite() -> KiteConnect:
    with open("config/secrets.yaml") as f:
        sec = yaml.safe_load(f)
    kite = KiteConnect(api_key=sec["kite"]["api_key"])
    kite.set_access_token(sec["kite"]["access_token"])
    return kite

def instruments_df(kite: KiteConnect) -> pd.DataFrame:
    return pd.DataFrame(kite.instruments())

# --- instrument resolvers ---
def resolve_equity(df: pd.DataFrame, symbol_dotns: str) -> int:
    # "RELIANCE.NS" -> NSE equity token
    sym = symbol_dotns.replace(".NS", "")
    row = df[(df.exchange == "NSE") & (df.tradingsymbol == sym)]
    if row.empty: raise ValueError(f"Equity not found: {symbol_dotns}")
    return int(row.iloc[0].instrument_token)

def resolve_frontmonth_future(df: pd.DataFrame, index_name: str) -> tuple[int, str]:
    # index_name: "NIFTY" or "BANKNIFTY"
    nfo = df[(df.exchange == "NFO") & (df.segment == "NFO-FUT") & (df.name == index_name)].copy()
    if nfo.empty: raise ValueError(f"No futures rows for {index_name}")
    nfo["expiry"] = pd.to_datetime(nfo["expiry"])
    today = pd.Timestamp.today().normalize()
    row = nfo[nfo.expiry >= today].sort_values("expiry").iloc[0]
    return int(row.instrument_token), str(row.tradingsymbol)

# --- time windows ---
def ist_yesterday_window():
    tz = "Asia/Kolkata"
    d = (pd.Timestamp.now(tz=tz) - pd.Timedelta(days=1)).date()
    start = pd.Timestamp.combine(d, time(9, 15)).tz_localize(tz).tz_localize(None)
    end   = pd.Timestamp.combine(d, time(15, 30)).tz_localize(tz).tz_localize(None)
    return start, end

# --- data fetch / resample ---
def fetch_1min(kite: KiteConnect, token: int, start, end) -> pd.DataFrame:
    try:
        candles = kite.historical_data(token, start, end, interval="minute", continuous=False, oi=False)
        if not candles:
            print(f"[DEBUG] 0 rows for token {token} {start} -> {end}")
            return pd.DataFrame()
        df = pd.DataFrame(candles).rename(columns={"date": "datetime"})
        df["datetime"] = pd.to_datetime(df["datetime"])
        return df[["datetime","open","high","low","close","volume"]]
    except Exception as e:
        print(f"[DEBUG] historical_data error token {token}: {e}")
        return pd.DataFrame()

def to_3min(df: pd.DataFrame) -> pd.DataFrame:
    o = (df.set_index("datetime").sort_index()
           .resample("3min")
           .agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
           .dropna())
    return o.reset_index()

def main():
    # read your universe from config
    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)
    universe = cfg["universe"]

    kite = load_kite()
    inst = instruments_df(kite)
    start, end = ist_yesterday_window()
    print(f"[INFO] Fetching {start} -> {end} (IST yesterday session)")

    for sym in universe:
        try:
            if sym in ("NIFTY_FUT","BANKNIFTY_FUT"):
                idx = "NIFTY" if sym.startswith("NIFTY") else "BANKNIFTY"
                token, display = resolve_frontmonth_future(inst, idx)
            else:
                token = resolve_equity(inst, sym)
                display = sym.replace(".NS","")

            d1 = fetch_1min(kite, token, start, end)
            if d1.empty:
                print(f"[WARN] no data for {sym}")
                continue

            # write files with _YDAY suffix to avoid overwriting today's files
            d1.assign(symbol=display)[["symbol","datetime","open","high","low","close","volume"]].to_csv(
                os.path.join(RAW_DIR, f"{display}_YDAY.csv"), index=False
            )
            d3 = to_3min(d1).assign(symbol=display)
            d3[["symbol","datetime","open","high","low","close","volume"]].to_csv(
                os.path.join(PROC_DIR, f"{display}_YDAY_3min.csv"), index=False
            )
            print(f"[OK] {display}: 1m={len(d1)}  3m={len(d3)}")

        except Exception as e:
            print(f"[ERR] {sym}: {e}")

if __name__ == "__main__":
    main()
