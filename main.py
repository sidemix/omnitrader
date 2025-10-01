import os, time, hmac, hashlib, requests, json
from datetime import datetime, timezone

APEX_KEY    = os.environ["APEX_KEY"]
APEX_SECRET = os.environ["APEX_SECRET"].encode()
BASE_URL    = "https://api.pro.apex.exchange"  # Omni/Pro env per your account
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

def sign(path, method, body=""):
    # ApeX uses timestamp + sign; check exact doc fields for your env
    ts = str(int(time.time() * 1000))
    payload = ts + method + path + body
    sig = hmac.new(APEX_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig

def get_open_positions():
    path = "/v1/account/positions"   # verify the exact path in the docs for your env
    ts, sig = sign(path, "GET")
    headers = {
        "APEX-KEY": APEX_KEY,
        "APEX-SIGN": sig,
        "APEX-TS": ts,
        "Content-Type": "application/json",
    }
    r = requests.get(BASE_URL + path, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def post_to_discord(positions):
    if not positions:
        return
    lines = []
    for p in positions:
        sym   = p.get("symbol")
        side  = p.get("side")
        sz    = p.get("size")
        entry = p.get("entryPrice")
        mark  = p.get("markPrice") or p.get("exitPrice")
        upnl  = p.get("unrealizedPnl")
        lev   = p.get("leverage")
        lines.append(f"**{sym}** — {side}  | size: {sz} | entry: {entry} | mark: {mark} | lev: {lev} | uPnL: {upnl}")
    content = f"🟢 **Active ApeX Positions** ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}):\n" + "\n".join(lines)
    requests.post(DISCORD_WEBHOOK, json={"content": content}, timeout=10)

if __name__ == "__main__":
    last_snapshot = None
    while True:
        data = get_open_positions()
        positions = data.get("positions", data)  # depends on response shape
        # Optional: only post when something changed
        snap = json.dumps(positions, sort_keys=True)
        if snap != last_snapshot:
            post_to_discord(positions)
            last_snapshot = snap
        time.sleep(10)  # adjust cadence
