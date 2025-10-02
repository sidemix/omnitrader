# main.py — ApeX Omni → Discord (Render worker)
# Polls /api/v3/account and posts open positions to Discord embeds.
# - Robust signing & retries
# - Recursive detection of positions arrays (handles nesting)
# - Throttled, de-duplicated Discord posts

import os
import time
import json
import hmac
import hashlib
import base64
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= ENV =========
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"]            # raw secret string from Omni
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# Omni mainnet by default. For testnet use: https://testnet.omni.apex.exchange/api
BASE_URL  = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")

# Polling & posting behavior
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))
MIN_POST_INTERVAL_SECS = int(os.environ.get("MIN_POST_INTERVAL_SECS", "60"))  # throttle updates
POST_EMPTY_EVERY_SECS  = int(os.environ.get("POST_EMPTY_EVERY_SECS", "0"))    # 0 = only on transition
DEBUG = os.environ.get("DEBUG", "0") == "1"

# Timestamp style (most setups work with ms). You can flip to ISO if needed: set APEX_TS_STYLE=iso
TS_STYLE = os.environ.get("APEX_TS_STYLE", "ms").lower()  # "ms" or "iso"

# ========= LOGGING =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logging.info("Starting worker. BASE_URL=%s", BASE_URL)

# ========= HTTP SESSION (retries) =========
session = requests.Session()
retry = Retry(
    total=6, connect=6, read=6,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry))

# ========= SIGNING (Omni v3) =========
def _timestamp() -> str:
    if TS_STYLE == "iso":
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    # default milliseconds since epoch
    return str(int(time.time() * 1000))

def _data_string(data: Dict[str, Any] | None) -> str:
    if not data:
        return ""
    items = sorted((k, v) for k, v in data.items() if v is not None)
    return "&".join(f"{k}={v}" for k, v in items)

def _sign(path: str, method: str, ts: str, data: Dict[str, Any] | None = None) -> str:
    # Signature = Base64(HMAC_SHA256(Base64(secret), ts + METHOD + path + dataString))
    message = ts + method.upper() + path + _data_string(data)
    key = base64.standard_b64encode(APEX_SECRET.encode("utf-8"))
    digest = hmac.new(key, msg=message.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode()

def _headers(path: str, method: str, data: Dict[str, Any] | None = None) -> Dict[str, str]:
    ts = _timestamp()
    sig = _sign(path, method, ts, data)
    return {
        "APEX-API-KEY": APEX_KEY,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "APEX-TIMESTAMP": ts,
        "APEX-SIGNATURE": sig,
        # Content-Type not required for GET, but harmless:
        "Content-Type": "application/json",
    }

# ========= DISCORD HELPERS =========
def _post_discord_json(payload: Dict[str, Any]) -> None:
    try:
        session.post(DISCORD_WEBHOOK, json=payload, timeout=(10, 20))
    except Exception as e:
        logging.error("Discord post failed: %s", e)

def _post_text(text: str) -> None:
    _post_discord_json({"content": text})

def _code_block_table(rows: List[str]) -> str:
    return "```\n" + "\n".join(rows) + "\n```"

def _positions_embed(norm: List[Dict[str, Any]]) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    if not norm:
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
    for p in norm:
        total_upnl += float(p["uPnL"])
        rows.append(
            f"{p['symbol']:<10}  {p['side']:<4}  {int(p['lev']) if p['lev'] else 0:>4}  "
            f"{p['size']:<11.6g}  {p['entry']:<12.6g}  {p['mark']:<12.6g}  {p['uPnL']:<12.6g}"
        )
    color = 0x2ecc71 if total_upnl >= 0 else 0xe74c3c
    return {"embeds": [{
        "title": "Active ApeX Positions",
        "description": _code_block_table(rows),
        "timestamp": ts,
        "color": color,
        "footer": {"text": "ApeX → Discord"},
    }]}

# ========= API CALLS =========
def _get(path: str, params: Dict[str, Any] | None = None) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}{path}"
    hdrs = _headers(path, "GET")
    r = session.get(url, headers=hdrs, params=params, timeout=(20, 30))
    data = {}
    try:
        if r.headers.get("content-type", "").startswith("application/json"):
            data = r.json()
    except Exception:
        data = {}
    return r.status_code, data

def get_time_status() -> int:
    code, _ = _get("/v3/time")
    return code

def get_account() -> Tuple[int, Dict[str, Any]]:
    # This is the private endpoint that contains positions
    return _get("/v3/account")

# ========= POSITION EXTRACTION =========
def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _looks_like_position(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    keys = {k.lower() for k in item.keys()}
    return ("size" in keys) and ("symbol" in keys or "market" in keys)

def _walk_positions(obj: Any, path: str = "$"):
    """Yield (json_path, list_of_candidates) for arrays that look like positions."""
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and _looks_like_position(obj[0]):
            yield path, obj
        for i, v in enumerate(obj):
            yield from _walk_positions(v, f"{path}[{i}]")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_positions(v, f"{path}.{k}")

def extract_open_positions(account_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    best_path, best_list = None, []
    for pth, lst in _walk_positions(account_json):
        if len(lst) > len(best_list):
            best_path, best_list = pth, lst
    if DEBUG:
        _post_text(f"🔎 found positions array at: `{best_path or '<none>'}` (len={len(best_list)})")

    open_pos: List[Dict[str, Any]] = []
    for item in best_list:
        size = _to_float(item.get("size"))
        if abs(size) <= 0:
            continue  # ignore zero-size rows
        sym   = (item.get("symbol") or item.get("market") or "?").replace("-", "").upper()
        side  = (item.get("side") or ("LONG" if size > 0 else "SHORT")).upper()
        entry = _to_float(item.get("entryPrice") or item.get("avgEntryPrice") or item.get("avgPrice"))
        mark  = _to_float(item.get("markPrice") or item.get("lastPrice") or item.get("indexPrice"))
        lev   = _to_float(item.get("leverage"))
        upl   = _to_float(item.get("unrealizedPnl") or item.get("realizedPnl"))
        open_pos.append({
            "symbol": sym, "side": side, "size": size,
            "entry": entry, "mark": mark, "lev": lev, "uPnL": upl,
        })
    open_pos.sort(key=lambda x: (x["symbol"], x["side"]))
    return open_pos

# ========= DIAGNOSTICS =========
def diagnostics_once() -> None:
    try:
        time_code = get_time_status()
    except Exception as e:
        time_code = f"err: {e}"

    code, data = get_account()
    # unwrap common envelope
    acct = data.get("data", data) if isinstance(data, dict) else {}
    # pull a few IDs to compare with your UI
    eth = acct.get("ethereumAddress")
    l2  = acct.get("l2Key")
    acc_id = acct.get("id")
    # try the obvious position paths
    top_pos = acct.get("positions") or []
    ca = acct.get("contractAccount") or {}
    ca_pos = ca.get("positions") or []
    # count any array in the tree that "looks like" positions
    any_pos_paths = []
    for pth, arr in _walk_positions(acct, "$"):
        any_pos_paths.append(f"{pth}[{len(arr)}]")

    msg = (
        "🧪 Diagnostics\n"
        f"BASE_URL={BASE_URL}\n"
        f"GET /v3/time → {time_code} | GET /v3/account → {code}\n"
        f"ethereumAddress={eth}\n"
        f"accountId={acc_id} | l2Key={l2}\n"
        f"positions(top)={len(top_pos)} | positions(contractAccount)={len(ca_pos)}\n"
        f"scan:{', '.join(any_pos_paths) or '<none>'}"
    )
    _post_text(msg)


# ========= MAIN LOOP =========
_last_snapshot: str | None = None
_last_post_ts = 0.0
_last_empty_post_ts = 0.0
_had_positions_last = False

def should_post(norm: List[Dict[str, Any]], snap: str, now: float) -> bool:
    global _last_snapshot, _last_post_ts, _last_empty_post_ts, _had_positions_last
    if norm:
        if snap != _last_snapshot and (now - _last_post_ts) >= MIN_POST_INTERVAL_SECS:
            return True
    else:
        if _had_positions_last:
            return True
        if POST_EMPTY_EVERY_SECS and (now - _last_empty_post_ts) >= POST_EMPTY_EVERY_SECS:
            return True
    return False

def record_post_state(norm: List[Dict[str, Any]], snap: str, now: float) -> None:
    global _last_snapshot, _last_post_ts, _last_empty_post_ts, _had_positions_last
    _last_post_ts = now
    _last_snapshot = snap if norm else "EMPTY"
    if not norm:
        _last_empty_post_ts = now
    _had_positions_last = bool(norm)

def loop_once() -> None:
    code, account = get_account()
    if code != 200:
        logging.error("HTTP %s on /v3/account", code)
        if DEBUG:
            _post_text(f"⚠️ /v3/account HTTP {code}")
        return
    norm = extract_open_positions(account)
    snap = json.dumps(norm, sort_keys=True)
    now = time.time()
    if should_post(norm, snap, now):
        _post_discord_json(_positions_embed(norm))
        record_post_state(norm, snap, now)

if __name__ == "__main__":
    # One-time diagnostics at boot
    diagnostics_once()
    while True:
        try:
            loop_once()
        except Exception as e:
            logging.exception("Unhandled error: %s", e)
        time.sleep(POLL_SECS)
