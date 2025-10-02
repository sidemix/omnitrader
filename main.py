# main.py — ApeX Omni → Discord (Render worker)
# Polls /v3/account and posts open positions to Discord.

import os, time, json, hmac, hashlib, base64, logging
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==== Env ====
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"]           # raw secret from Omni UI
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]
BASE_URL        = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")
POLL_SECS       = int(os.environ.get("POLL_INTERVAL_SECS", "10"))
DEBUG           = os.environ.get("DEBUG", "0") == "1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ==== HTTP session with retries ====
session = requests.Session()
retry = Retry(total=6, connect=6, read=6, backoff_factor=1.5,
              status_forcelist=(429, 500, 502, 503, 504),
              allowed_methods=("GET", "POST"))
session.mount("https://", HTTPAdapter(max_retries=retry))

# ==== Signing (per Omni docs) ====
def _iso_ts() -> str:
    # the docs use an ISO-ish string in examples; this works
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _sign(path: str, method: str, iso_ts: str, data: dict | None = None) -> str:
    data = data or {}
    # canonical dataString (sorted, '&' joined). GETs send params in the path, so this is empty.
    if data:
        items = sorted((k, v) for k, v in data.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)
    else:
        data_string = ""
    message = f"{iso_ts}{method.upper()}{path}{data_string}"
    key = base64.standard_b64encode(APEX_SECRET.encode("utf-8"))
    digest = hmac.new(key, msg=message.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode()

def _headers(path: str, method: str, body: dict | None = None) -> dict:
    ts = _iso_ts()
    sig = _sign(path, method, ts, body)
    return {
        "APEX-API-KEY": APEX_KEY,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "APEX-TIMESTAMP": ts,
        "APEX-SIGNATURE": sig,
    }

# ==== Discord ====
def post_to_discord(text: str):
    try:
        session.post(DISCORD_WEBHOOK, json={"content": text}, timeout=10)
    except Exception as e:
        logging.error("Discord post failed: %s", e)

# ==== API ====
def get_json(path: str, params: dict | None = None):
    url = f"{BASE_URL}{path}"
    hdrs = _headers(path, "GET")
    r = session.get(url, headers=hdrs, params=params, timeout=20)
    return r.status_code, (r.json() if r.headers.get("content-type","").startswith("application/json") else {})

def get_account():
    # primary endpoint for positions
    code, data = get_json("/v3/account")
    return code, data

def extract_open_positions(account: dict):
    # Try top-level positions first
    pos = account.get("positions") or []
    # Also try nested contractAccount.positions (some payloads put it there)
    nested = (account.get("contractAccount") or {}).get("positions") or []
    if len(nested) > len(pos):
        pos = nested

    open_pos = []
    for p in pos:
        try:
            size = float(p.get("size", "0") or "0")
        except Exception:
            continue
        if abs(size) > 0:
            sym = p.get("symbol") or p.get("market") or "?"
            side = "LONG" if size > 0 else "SHORT"
            entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice")
            mark  = p.get("markPrice") or p.get("indexPrice")
            upl   = p.get("unrealizedPnl") or "0"
            open_pos.append({
                "symbol": sym,
                "side": side,
                "size": size,
                "entry": entry,
                "mark": mark,
                "upl": upl,
            })
    return open_pos

def format_lines(positions: list[dict]) -> str:
    if not positions:
        return "No open positions"
    lines = []
    for p in positions:
        sym = p["symbol"].replace("-", "")
        lines.append(f"{sym} {p['side']} size={p['size']} | entry={p['entry']} | mark={p['mark']} | UPL={p['upl']}")
    return "\n".join(lines)

# ==== Optional quick diagnostics ====
def diag():
    # This hits /v3/time (public) and /v3/account (private) and prints a compact report.
    try:
        t = session.get(f"{BASE_URL}/v3/time", timeout=10)
        t_ok = t.status_code
    except Exception as e:
        t_ok = f"err: {e}"

    code, data = get_account()
    pos = data.get("positions") or []
    nested = (data.get("contractAccount") or {}).get("positions") or []
    post_to_discord(
        "🧪 Diagnostics "
        f"BASE_URL={BASE_URL}\n"
        f"GET /v3/time → {t_ok} | GET /v3/account → {code}\n"
        f"positions(top)={len(pos)} | positions(contractAccount)={len(nested)}"
    )

# ==== Main loop ====
_last_payload = None

def loop():
    global _last_payload
    if DEBUG:
        diag()

    code, account = get_account()
    if code != 200:
        logging.error("Account HTTP %s", code)
        post_to_discord(f"⚠️ Account HTTP {code}")
        return

    open_pos = extract_open_positions(account)
    text = format_lines(open_pos)

    # Only post when the text changes to avoid spam
    if text != _last_payload:
        post_to_discord(f"● **Active ApeX Positions** ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')})\n{text}")
        _last_payload = text

if __name__ == "__main__":
    logging.info("Starting worker. BASE_URL=%s", BASE_URL)
    while True:
        try:
            loop()
        except Exception as e:
            logging.exception("Unhandled error: %s", e)
        time.sleep(POLL_SECS)
