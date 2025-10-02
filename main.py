# main.py — ApeX Omni → Discord worker (v0.9 no-datetime)
#
# Env (accepts both legacy and new names):
#   APEX_API_KEY        or APEX_KEY
#   APEX_API_SECRET     or APEX_SECRET
#   APEX_API_PASSPHRASE or APEX_PASSPHRASE
#   DISCORD_WEBHOOK   (required)
#   OMNI_BASE_URL or APEX_BASE_URL (default: https://omni.apex.exchange/api)
#   INTERVAL_SECONDS (default 30)
#   SCAN_SYMBOLS (optional, comma-separated)
#   APEX_LATENCY_CUSHION_MS (default 600)
#   LOG_LEVEL (default INFO)

import os
import sys
import time
import hmac
import json
import base64
import hashlib
import logging
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter, Retry

# ---------------- Logging ----------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("apex-bridge")

def ascii_only(s: str) -> str:
    try:
        return s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        return s

# ---------------- Env ----------------
API_KEY        = os.getenv("APEX_API_KEY")        or os.getenv("APEX_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET")     or os.getenv("APEX_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
BASE_URL = (os.getenv("OMNI_BASE_URL") or os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "30"))
SCAN_SYMBOLS = [s.strip().upper() for s in os.getenv("SCAN_SYMBOLS", "").split(",") if s.strip()]
LATENCY_CUSHION_MS = int(os.getenv("APEX_LATENCY_CUSHION_MS", "600"))

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing envs: APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET, APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------------- HTTP session ----------------
session = requests.Session()
retries = Retry(total=6, connect=6, read=6, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "apex-omni-discord-bridge/0.9"})

# ---------------- Time & signing ----------------
def get_server_time_seconds() -> int:
    """GET /v3/time → epoch seconds (normalize if ms). No datetime used."""
    url = f"{BASE_URL}/v3/time"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    js = {}
    try:
        js = r.json()
    except Exception:
        pass

    sources = []
    if isinstance(js, dict):
        sources.append(js)
        if isinstance(js.get("data"), dict):
            sources.append(js["data"])

    for src in sources:
        for k in ("time", "serverTime", "server_timestamp", "serverTs"):
            v = src.get(k)
            if isinstance(v, (int, float)):
                return int(v if v < 1e12 else v / 1000.0)

    try:
        return int(js)
    except Exception:
        raise RuntimeError(f"Unexpected /v3/time response: {js}")

def iso_from_seconds(sec: int) -> str:
    """Build ISO string via time.gmtime (no datetime)."""
    tm = time.gmtime(sec)
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}.000Z"

def sign_message(ts_str: str, method: str, signed_path: str, body: dict | None) -> str:
    """Omni v3 signature:
       message = timestamp + METHOD + /api/v3/... + dataString
       key = base64(secret-utf8)
       signature = base64( HMAC_SHA256(key, message) )
    """
    method = method.upper()
    body = body or {}
    data_string = ""
    if method == "POST":
        items = sorted((k, v) for k, v in body.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)

    message = f"{ts_str}{method}{signed_path}{data_string}"
    key = base64.standard_b64encode(API_SECRET.encode("utf-8"))
    digest = hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode("utf-8")

def private_request(method: str, path: str, params: dict | None = None, data: dict | None = None) -> requests.Response:
    """
    method: GET/POST
    path: '/v3/...'
    URL = BASE_URL + path
    SIGNED PATH must include '/api': '/api' + path (+ query for GET)
    """
    assert path.startswith("/v3/"), "path must start with '/v3/'"
    url = f"{BASE_URL}{path}"
    query = ""
    if method.upper() == "GET" and params:
        query = "?" + urlencode(sorted(params.items()), doseq=True)
        url = url + query

    server_sec = get_server_time_seconds()
    attempts = [
        ("iso", iso_from_seconds(server_sec)),
        ("ms",  str(int(server_sec * 1000 + LATENCY_CUSHION_MS))),
        ("s",   str(int(server_sec))),
    ]

    last_err = None
    for label, ts in attempts:
        try:
            signed_path = "/api" + path + (query if method.upper() == "GET" else "")
            sig = sign_message(ts, method, signed_path, data if method.upper() == "POST" else {})
            headers = {
                "APEX-SIGNATURE": sig,
                "APEX-TIMESTAMP": ts,
                "APEX-API-KEY": API_KEY,
                "APEX-PASSPHRASE": API_PASSPHRASE,
            }
            if method.upper() == "GET":
                resp = session.get(url, headers=headers, timeout=20)
            else:
                resp = session.post(url, headers=headers, data=data or {}, timeout=20)

            # Omni JSON wrapper (code/msg)
            body = {}
            if resp.headers.get("Content-Type", "").startswith("application/json"):
                try:
                    body = resp.json()
                except Exception:
                    body = {}
                if isinstance(body, dict) and body.get("code") not in (None, 0):
                    code, msg = body.get("code"), str(body.get("msg"))
                    if code in (20002, 20009) or ("timestamp" in msg.lower()):
                        LOG.warning(ascii_only(f"Timestamp rejected on {path} with ts={label}: code={code} msg={msg}"))
                        continue  # try next ts format

            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            LOG.warning(ascii_only(f"Retrying {path} with next timestamp format ({label}) due to: {e}"))
            time.sleep(0.25)

    if last_err:
        raise last_err
    raise RuntimeError("Signing failed with all timestamp formats")

# ---------------- Discord ----------------
def post_discord(text: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": text[:1990]}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        LOG.error(ascii_only(f"Discord post failed: {e}"))

# ---------------- Data helpers ----------------
def fetch_account() -> dict:
    r = private_request("GET", "/v3/account")
    try:
        return r.json()
    except Exception:
        return {}

def extract_positions(js: dict) -> list[dict]:
    out = []
    if not isinstance(js, dict):
        return out

    def take_list(dct: dict, key: str):
        if isinstance(dct.get(key), list):
            out.extend(dct[key])

    # top-level direct
    take_list(js, "positions")
    take_list(js, "openPositions")
    ca = js.get("contractAccount") or {}
    if isinstance(ca, dict):
        take_list(ca, "positions")
        take_list(ca, "openPositions")

    # under data
    data = js.get("data")
    if isinstance(data, dict):
        take_list(data, "positions")
        take_list(data, "openPositions")
        dca = data.get("contractAccount") or {}
        if isinstance(dca, dict):
            take_list(dca, "positions")
            take_list(dca, "openPositions")

    # filter & dedupe (no datetime involved)
    uniq = {}
    for p in out:
        try:
            sym  = str(p.get("symbol") or p.get("contractSymbol") or p.get("market") or "").upper()
            side = str(p.get("side") or p.get("positionSide") or "").upper()
            size = str(p.get("size") or p.get("position") or p.get("quantity") or "")
            entry = str(p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice") or "")
            if SCAN_SYMBOLS and sym not in SCAN_SYMBOLS:
                continue
            uniq[(sym, side, size, entry)] = {"symbol": sym, "side": side, "size": size, "entry": entry}
        except Exception:
            # swallow any weird row; never crash loop
            continue
    return list(uniq.values())

def format_positions(pos: list[dict]) -> str:
    if not pos:
        return "No open positions"
    lines = []
    for p in pos:
        try:
            lines.append(f"- {p['symbol']} {p['side']} size={p['size']} entry={p['entry']}")
        except Exception:
            pass
    return "\n".join(lines)

# ---------------- Loop state ----------------
_last_snapshot = None

def snapshot_of(pos: list[dict]) -> str:
    try:
        key_tuples = [(p.get("symbol",""), p.get("side",""), p.get("size",""), p.get("entry","")) for p in pos]
        key_tuples.sort()
        return json.dumps(key_tuples)
    except Exception:
        return ""

# ---------------- Flow ----------------
def diagnostics_once():
    try:
        srv = get_server_time_seconds()
        acct = fetch_account()
        positions = extract_positions(acct)
        post_discord(
            "Bridge v0.9 (no-datetime) online.\n"
            f"Base={BASE_URL}\n"
            f"ServerTime(sec)={srv}\n"
            f"Positions detected={len(positions)}"
        )
    except Exception as e:
        LOG.error(ascii_only(f"Diagnostics failed: {e}"))

def poll_once():
    global _last_snapshot
    acct = fetch_account()
    positions = extract_positions(acct)
    snap = snapshot_of(positions)
    if snap != _last_snapshot:
        _last_snapshot = snap
        post_discord("Active ApeX Positions\n" + format_positions(positions))
        LOG.info(ascii_only(f"Posted {len(positions)} positions"))
    else:
        LOG.info(ascii_only("No position change"))

def main():
    LOG.info(ascii_only(f"Starting worker. BASE_URL={BASE_URL}"))
    diagnostics_once()
    while True:
        try:
            poll_once()
        except Exception as e:
            LOG.error(ascii_only(f"Poll error: {e}"))
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
