from kiteconnect import KiteConnect
import yaml

with open("config/secrets.yaml") as f:
    sec = yaml.safe_load(f)
kite = KiteConnect(api_key=sec["kite"]["api_key"])
kite.set_access_token(sec["kite"]["access_token"])

# Try a simple LTP call for RELIANCE on NSE
print(kite.ltp(["NSE:RELIANCE"]))
