from kiteconnect import KiteConnect
import yaml, webbrowser

SECRETS = "config/secrets.yaml"

with open(SECRETS) as f:
    sec = yaml.safe_load(f)
api_key = sec["kite"]["api_key"]
api_secret = sec["kite"]["api_secret"]

kite = KiteConnect(api_key=api_key)
# 1) Open login URL in browser; log in; copy the 'request_token' from the redirect URL
login_url = kite.login_url()
print("Open this in your browser, login, and copy the request_token:\n", login_url)
webbrowser.open(login_url)

request_token = input("\nPaste request_token here: ").strip()

# 2) Exchange for access_token
data = kite.generate_session(request_token, api_secret=api_secret)
access_token = data["access_token"]
print("\nACCESS TOKEN:", access_token)

# 3) Save back to secrets
sec["kite"]["access_token"] = access_token
with open(SECRETS, "w") as f:
    yaml.safe_dump(sec, f)
print("Saved access_token to", SECRETS)
