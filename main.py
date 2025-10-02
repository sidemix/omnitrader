# main.py — ApeX Omni → Discord (Render worker)
# Polls /api/v3/user and posts openPositions to Discord.
# Uses correct Omni base URL, v3 routes, headers, and signature.

import os, time, json, hmac, hashlib, base64, logging, socket
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === Env ===
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"]              # raw secret string
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# Use Omni mainnet by default; swap to testnet by changing this env in Render
BASE_URL = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))

# === Logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# === HTTP session with retries ===
session = requests.Session()
retry = Retry(total=6, connect=6, read=6, backoff_factor=1.5,
              status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
session.mount("https://", HTTPAdapter(max_retries=retry))

# === Helpers ===
def _chunks(s: str, n: int):
    for i in range(0, len(s), n):
        yield s[i:i+n]

def post_to_discord(text_or_lines):
    if isinstance(text_or_lines, str):
        content = text_or_lines
    else:
        content = "\n".join(text_or_lines)
    for chunk in _chunks(content, 1900):
        try:
            session.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=(10, 20))
        except Exception as e:
            logging.error("Discord post failed: %s", e)

def sign_v3(path: str, method: str, data: dict | None = None):
    """
    Omni v3 signature: Base64(HMAC_SHA256(Base64(secret), ts+METHOD+path+dataString))
    GET: dataString is ''
    POST (form-encoded): sorted k=v joined by &
    """
    ts = str(int(time.time() * 1000))
    method = method.upper()
    data_string = ""
    if data:
        items = sorted((k, v) for k, v in data.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)

    message = ts + method + path + data_string
    key = base64.standard_b64encode(APEX_SECRET.encode("utf-8"))
    digest = hmac.new(key, msg=message.encode("utf-8"), digestmod=hashlib.sha256).digest()
    signature = base64.standard_b64encode(digest).decode()
    return ts, signature

def headers_v3(ts: str, sig: str):
    return {
        "APEX-SIGNATURE": sig,
        "APEX-TIMESTAMP": ts,
        "APEX-API-KEY": APEX_KEY,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "Content-Type": "application/json",
    }

def get_user():
    """
    GET /api/v3/user  -> contains 'openPositions'
    Path for signing is '/v3/user' (no '/api' in the message). 
    """
    route = "/v3/user"
    ts, sig = sign_v3(route, "GET")
    r = session.get(f"{BASE_URL}{route}", headers=headers_v3(ts, sig), timeout=(20, 30))
    r.raise_for_status()
    return r.json()

def build_position_lines(user_json):
    # 'openPositions' appears inside the user response
    positions = []
    if isinstance(user_json, dict):
        # Two places it appears in docs; try both
        op1 = user_json.get("openPositions")
        op2 = user_json.get("positions") or user_json.get("contractAccount", {}).get("openPositions")
        items = op1 or op2 or []
    else:
        items = []

    lines = []
    for p in items:
        sym   = p.get("symbol") or "?"
        side  = p.get("side") or "?"
        size  = p.get("size") or "?"
        entry = p.get("entryPrice") or "?"
        mark  = p.get("markPrice") or p.get("exitPrice") or "?"
        upnl  = p.get("unrealizedPnl") or p.get("realizedPnl") or "?"
        lines.append(f"**{sym}** — {side} | size: {size} | entry: {entry} | mark: {mark} | PnL: {upnl}")
    if not lines:
        lines.append("_No open positions_")
    lines.insert(0, f"🟢 **Active ApeX Positions** ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')})")
    return lines

def diag():
    host = BASE_URL.split("://",1)[1].split("/",1)[0]
    lines = [f"🔧 **Diagnostics**  BASE_URL=`{BASE_URL}`  route=`/v3/user`"]
    # egress IP
    try:
        ip = session.get("https://api.ipify.org", timeout=(5,10)).text
        lines.append(f"Egress IP: `{ip}`")
    except Exception as e:
        lines.append(f"Egress IP check failed: `{e}`")
    # DNS
    try:
        addrs = list({a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})
        lines.append(f"DNS {host} → {addrs}")
    except Exception as e:
        lines.append(f"DNS failed: `{e}`")
    # Omni time endpoint to confirm reachability
    try:
        r = session.get(f"{BASE_URL}/v3/time", timeout=(10,10))
        lines.append(f"GET /v3/time → {r.status_code}")
    except Exception as e:
        lines.append(f"/v3/time FAILED: `{e}`")
    post_to_discord(lines)

# === Main loop ===
if __name__ == "__main__":
    logging.info("Starting worker. BASE_URL=%s", BASE_URL)
    try:
        diag()
    except Exception as e:
        logging.warning("Diagnostics failed: %s", e)

    last_snapshot = None
    while True:
        try:
            user = get_user()
            snap = json.dumps(user.get("openPositions") or user, sort_keys=True, default=str)
            if snap != last_snapshot:
                post_to_discord(build_position_lines(user))
                last_snapshot = snap
        except requests.exceptions.RequestException as e:
            logging.error("HTTP error: %s", e)
        except Exception as e:
            logging.exception("Unexpected error")
        time.sleep(POLL_SECS)
