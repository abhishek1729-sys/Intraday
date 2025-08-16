# ops/fetch_intraday.py
from __future__ import annotations
import os, argparse, yaml, pandas as pd
from datetime import time
from kiteconnect import KiteConnect

RAW_DIR = "data/raw"
PROC_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROC_DIR, exist_ok=True)

# -------- Auth / setup --------
def load_kite() -> KiteConnect:
    with open("config/secrets.yaml") as f:
        sec = yaml.safe_load(f)
    kite = KiteConnect(api_key=sec["kite"]["api_key"])
    kite.set_access_token(sec["kite"]["access_token"])
    return kite

def instruments_df(kite: KiteConnect) -> pd.DataFrame:
    return pd.DataFrame(kite.instruments())

# -------- Instrument resolvers --------
def resolve_equity(df: pd.DataFrame, symbol_dotns: str) -> int:
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

# -------- Time windows (IST) --------
def _ist_day_window(date: pd.Timestamp):
    tz = "Asia/Kolkata"
    d = date.tz_localize(tz).date()
    start = pd.Timestamp.combine(d, time(9, 15)).tz_localize(tz).tz_localize(None)
    end   = pd.Timestamp.combine(d, time(15, 30)).tz_localize(tz).tz_localize(None)
    return start, end

def ist_today_window():
    return _ist_day_window(pd.Timestamp.now(tz="Asia/Kolkata"))

def ist_yesterday_window():
    return _ist_day_window(pd.Timestamp.now(tz="Asia/Kolkata") - pd.Timedelta(days=1))

def ist_date_window(date_str: str):
    # date_str: "YYYY-MM-DD"
    return _ist_day_window(pd.Timestamp(date_str, tz="Asia/Kolkata"))

# -------- Fetch / resample --------
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

# -------- Main --------
def parse_args():
    p = argparse.ArgumentParser(description="Fetch intraday 1m data (Kite) and save CSVs. Default = today.")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--yesterday", action="store_true", help="fetch yesterday (IST trading session)")
    g.add_argument("--date", type=str, help='fetch a specific date YYYY-MM-DD (IST)')
    p.add_argument("--symbols", type=str,
                   help="comma-separated list to override config.yaml universe (e.g. RELIANCE.NS,HDFCBANK.NS)")
    p.add_argument("--no-proc", action="store_true", help="skip writing 3-min processed files")
    return p.parse_args()

def main():
    args = parse_args()

    # Read universe from config unless overridden
    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)
    universe = cfg["universe"]
    if args.symbols:
        universe = [s.strip() for s in args.symbols.split(",") if s.strip()]

    kite = load_kite()
    inst = instruments_df(kite)

    # choose window
    if args.yesterday:
        start, end = ist_yesterday_window()
        suffix = "_YDAY"
    elif args.date:
        start, end = ist_date_window(args.date)
        suffix = f"_{args.date}"
    else:
        start, end = ist_today_window()
        suffix = ""  # today
    print(f"[INFO] Window: {start} -> {end} (IST)")

    for sym in universe:
        try:
            if sym in ("NIFTY_FUT","BANKNIFTY_FUT"):
                idx = "NIFTY" if sym.startswith("NIFTY") else "BANKNIFTY"
                token, display = resolve_frontmonth_future(inst, idx)
            else:
                token = resolve_equity(inst, sym)
                display = sym.replace(".NS", "")

            d1 = fetch_1min(kite, token, start, end)
            if d1.empty:
                print(f"[WARN] no data for {sym}")
                continue

            # write raw 1-min
            out_raw = os.path.join(RAW_DIR, f"{display}{suffix}.csv")
            d1.assign(symbol=display)[["symbol","datetime","open","high","low","close","volume"]].to_csv(
                out_raw, index=False
            )

            if not args.no_proc:
                d3 = to_3min(d1).assign(symbol=display)
                out_proc = os.path.join(PROC_DIR, f"{display}{suffix}_3min.csv")
                d3[["symbol","datetime","open","high","low","close","volume"]].to_csv(out_proc, index=False)

            print(f"[OK] {display}: 1m={len(d1)}" + ("" if args.no_proc else f"  3m={len(d3)}"))

        except Exception as e:
            print(f"[ERR] {sym}: {e}")

if __name__ == "__main__":
    main()
