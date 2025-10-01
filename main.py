import os, time, hmac, hashlib, json, logging
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

APEX_KEY    = os.environ["APEX_KEY"]
APEX_SECRET = os.environ["APEX_SECRET"].encode()
BASE_URL    = os.environ.get("APEX_BASE_URL", "https://api.pro.apex.exchange")  # override in Render if needed
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# requests session with retries & longer connect timeout
session = requests.Session()
retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=1.5,  # 0s, 1.5s, 3s, 4.5s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://", HTTPAdapter(max_retries=retry))

def sign(path, method, body=""):
    ts = str(int(time.time() * 1000))
    payload = ts + method + path + body
    sig = hmac.new(APEX_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig

def get_open_positions():
    path = "/v1/account/positions"  # adjust if your API path differs
    ts, sig = sign(path, "GET")
    headers = {
        "APEX-KEY": APEX_KEY,
        "APEX-SIGN": sig,
        "APEX-TS": ts,
        "Content-Type": "application/json",
    }
    # use longer timeouts: (connect, read)
    r = session.get(BASE_URL + path, headers=headers, timeout=(20, 30))
    r.raise_for_status()
    return r.json()

def post_to_discord(lines):
    if not lines:
        return
    content = "🟢 **Active ApeX Positions** ({})\n{}".format(
        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ'),
        "\n".join(lines)
    )
    try:
        session.post(DISCORD_WEBHOOK, json={"content": content}, timeout=(10, 20))
    except Exception as e:
        logging.error("Discord post failed: %s", e)

def build_lines(positions):
    out = []
    for p in positions:
        sym   = p.get("symbol")
        side  = p.get("side")
        sz    = p.get("size")
        entry = p.get("entryPrice")
        mark  = p.get("markPrice") or p.get("exitPrice")
        upnl  = p.get("unrealizedPnl")
        lev   = p.get("leverage")
        out.append(f"**{sym}** — {side} | size: {sz} | entry: {entry} | mark: {mark} | lev: {lev} | uPnL: {upnl}")
    return out

if __name__ == "__main__":
    logging.info("Starting worker. BASE_URL=%s", BASE_URL)
    last_snapshot = None
    while True:
        try:
            data = get_open_positions()
            positions = data.get("positions", data)
            snap = json.dumps(positions, sort_keys=True)
            if snap != last_snapshot:
                post_to_discord(build_lines(positions))
                last_snapshot = snap
        except requests.exceptions.ConnectTimeout as e:
            logging.warning("Connect timeout: %s", e)
        except requests.exceptions.ReadTimeout as e:
            logging.warning("Read timeout: %s", e)
        except requests.exceptions.RequestException as e:
            logging.error("HTTP error: %s", e)
        except Exception as e:
            logging.exception("Unexpected error")
        # sleep a bit no matter what (avoid tight loop on failure)
        time.sleep(10)
