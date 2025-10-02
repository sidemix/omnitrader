# main.py — ApeX Omni → Discord (Render worker)
# Uses /api/v3/account to fetch positions and posts to Discord embeds.

import os, time, json, hmac, hashlib, base64, logging, socket
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== ENV =====
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"]              # raw secret string
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# Omni mainnet by default. For testnet: https://testnet.omni.apex.exchange/api
BASE_URL  = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))
MIN_POST_INTERVAL_SECS = int(os.environ.get("MIN_POST_INTERVAL_SECS", "60"))  # throttle updates
POST_EMPTY_EVERY_SECS  = int(os.environ.get("POST_EMPTY_EVERY_SECS", "0"))    # 0 = only on transition

# ===== Logging =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logging.info("Starting worker. BASE_URL=%s", BASE_URL)

# ===== HTTP session with retries =====
session = requests.Session()
retry = Retry(
    total=6, connect=6, read=6,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry))

# ===== Discord helpers =====
def post_json(payload: dict):
    try:
        session.post(DISCORD_WEBHOOK, json=payload, timeout=(10, 20))
    except Exception as e:
        logging.error("Discord post failed: %s", e)

def code_block_table(rows):
    return "```\n" + "\n".join(rows) + "\n```"

def positions_embed(positions_norm):
    ts = datetime.now(timezone.utc).isoformat()
    if not positions_norm:
        return {"embeds": [{
            "title": "Active ApeX Positions",
            "description": "_No open positions_",
            "timestamp": ts,
            "color": 0x7f8c8d,
            "footer": {"text": "ApeX → Discord"},
        }]}
    rows = [
        "SYMBOL      SIDE  xLEV   SIZE         ENTRY         MARK          uPnL",
        "----------  ----  ----  -----------  ------------  ------------  ------------",
    ]
    total_upnl = 0.0
    for p in positions_norm:
        total_upnl += p["uPnL"]
        rows.append(
            f"{p['symbol']:<10}  {p['side']:<4}  {int(p['lev']) if p['lev'] else 0:>4}  "
            f"{p['size']:<11.6g}  {p['entry']:<12.6g}  {p['mark']:<12.6g}  {p['uPnL']:<12.6g}"
        )
    color = 0x2ecc71 if total_upnl >= 0 else 0xe74c3c
    return {"embeds": [{
        "title": "Active ApeX Positions",
        "description": code_block_table(rows),
        "timestamp": ts,
        "color": color,
        "footer": {"text": "ApeX → Discord"},
    }]}

# ===== Omni v3 signing =====
def sign_v3(path: str, method: str, data: dict | None = None):
    """
    Signature = Base64(HMAC_SHA256(Base64(secret), ts + METHOD + path + dataString))
    For GET requests dataString is ''. For POST it's sorted form 'k=v&k=v'.
    """
    ts = str(int(time.time() * 1000))
    method = method.upper()
    data_string = ""
    if data:
        items = sorted((k, v) for k, v in data.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)
    msg = ts + method + path + data_string
    key = base64.standard_b64encode(APEX_SECRET.encode("utf-8"))
    digest = hmac.new(key, msg=msg.encode("utf-8"), digestmod=hashlib.sha256).digest()
    sig = base64.standard_b64encode(digest).decode()
    return ts, sig

def headers_v3(ts: str, sig: str):
    return {
        "APEX-SIGNATURE": sig,
        "APEX-TIMESTAMP": ts,
        "APEX-API-KEY": APEX_KEY,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "Content-Type": "application/json",
    }

# ===== API calls =====
def get_account():
    """
    GET /api/v3/account  (sign with path '/v3/account')
    Returns account state incl. 'positions' (list of current positions).
    """
    route = "/v3/account"
    ts, sig = sign_v3(route, "GET")
    r = session.get(f"{BASE_URL}{route}", headers=headers_v3(ts, sig), timeout=(20, 30))
    r.raise_for_status()
    return r.json()

# ===== Normalization =====
def to_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def normalize_positions(account_json):
    """
    Keep only fields that matter to avoid noisy diffs.
    Filter out zero-size positions.
    """
    items = []
    if isinstance(account_json, dict):
        items = account_json.get("positions") or []
    norm = []
    for p in items:
        size = to_float(p.get("size"))
        if abs(size) <= 0:
            continue
        norm.append({
            "symbol": str(p.get("symbol") or "").replace("-", "").upper(),  # e.g., 'ASTERUSDT'
            "side":   str(p.get("side") or "").upper(),                     # LONG/SHORT
            "size":   size,
            "entry":  to_float(p.get("entryPrice")),
            "mark":   to_float(p.get("markPrice") or p.get("lastPrice")),
            "lev":    to_float(p.get("leverage")),
            "uPnL":   to_float(p.get("unrealizedPnl") or p.get("realizedPnl")),
        })
    norm.sort(key=lambda x: (x["symbol"], x["side"]))
    return norm

# ===== Diagnostics (one-time post) =====
def diag():
    host = BASE_URL.split("://",1)[1].split("/",1)[0]
    lines = [f"🔧 **Diagnostics**  BASE_URL=`{BASE_URL}`  route=`/v3/account`"]
    try:
        ip = session.get("https://api.ipify.org", timeout=(5,10)).text
        lines.append(f"Egress IP: `{ip}`")
    except Exception as e:
        lines.append(f"Egress IP check failed: `{e}`")
    try:
        addrs = list({a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)})
        lines.append(f"DNS {host} → {addrs}")
    except Exception as e:
        lines.append(f"DNS failed: `{e}`")
    try:
        r = session.get(f"{BASE_URL}/v3/time", timeout=(10,10))
        lines.append(f"GET /v3/time → {r.status_code}")
    except Exception as e:
        lines.append(f"/v3/time FAILED: `{e}`")
    post_json({"content": "\n".join(lines)})

# ===== Main loop =====
if __name__ == "__main__":
    try:
        diag()
    except Exception as e:
        logging.warning("Diagnostics failed: %s", e)

    last_snapshot = None
    last_post_ts = 0.0
    last_empty_post_ts = 0.0
    had_positions_last = False

    while True:
        try:
            account = get_account()
            norm = normalize_positions(account)
            snap = json.dumps(norm, sort_keys=True)
            now = time.time()

            should_post = False
            if norm:
                if snap != last_snapshot and (now - last_post_ts) >= MIN_POST_INTERVAL_SECS:
                    should_post = True
            else:
                if had_positions_last:
                    should_post = True
                elif POST_EMPTY_EVERY_SECS and (now - last_empty_post_ts) >= POST_EMPTY_EVERY_SECS:
                    should_post = True

            if should_post:
                post_json(positions_embed(norm))
                last_post_ts = now
                last_snapshot = snap if norm else "EMPTY"
                if not norm:
                    last_empty_post_ts = now

            had_positions_last = bool(norm)

        except requests.exceptions.HTTPError as e:
            body = ""
            try:
                body = e.response.text
            except Exception:
                pass
            logging.error("HTTP %s: %s", getattr(e.response, "status_code", "?"), body)
        except requests.exceptions.RequestException as e:
            logging.error("HTTP error: %s", e)
        except Exception as e:
            logging.exception("Unexpected error")
        time.sleep(POLL_SECS)
