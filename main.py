# main.py — ApeX Omni → Discord (Render worker)
# - Uses server time sync for APEX-TIMESTAMP (solves err APEX-TIMESTAMP)
# - Signs v3 requests; surfaces API "code/msg" errors
# - Recursively finds positions anywhere in payload; filters zero-size
# - Posts tidy Discord embeds only when positions actually change

import os
import time
import json
import hmac
import hashlib
import base64
import logging
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= ENV =========
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"]            # raw secret string from Omni UI
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# Omni mainnet by default. For testnet: https://testnet.omni.apex.exchange/api
BASE_URL  = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")

# Polling & posting behavior
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))
MIN_POST_INTERVAL_SECS = int(os.environ.get("MIN_POST_INTERVAL_SECS", "60"))  # throttle updates
POST_EMPTY_EVERY_SECS  = int(os.environ.get("POST_EMPTY_EVERY_SECS", "0"))    # 0 = only on transition
DEBUG = os.environ.get("DEBUG", "0") == "1"

# Timestamp / secret handling
TS_STYLE = os.environ.get("APEX_TS_STYLE", "ms").lower()        # "ms" or "iso" (ms recommended)
APEX_SECRET_BASE64 = os.environ.get("APEX_SECRET_BASE64", "0") == "1"  # set to "1" if your secret is Base64

# ========= LOGGING =========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logging.info("Starting worker. BASE_URL=%s", BASE_URL)

# ========= HTTP session with retries =========
session = requests.Session()
retry = Retry(
    total=6, connect=6, read=6,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry))

# ========= Server time sync (for APEX-TIMESTAMP) =========
_server_offset_ms = 0  # server_ms - local_ms

def _parse_server_time_to_ms(payload: Dict[str, Any]) -> Optional[int]:
    """
    Try to extract server time (ms since epoch) from /v3/time response.
    Accepts a variety of shapes: {"data":{"serverTime": 169...}}, {"serverTime": "2025-10-02T...Z"}, etc.
    """
    candidate = None
    d = payload or {}
    # common places
    if "data" in d and isinstance(d["data"], dict):
        dd = d["data"]
        candidate = dd.get("serverTime") or dd.get("timestamp") or dd.get("time") or dd.get("now")
    if candidate is None:
        candidate = d.get("serverTime") or d.get("timestamp") or d.get("time") or d.get("now")

    if candidate is None:
        return None

    # numeric seconds or milliseconds
    try:
        val = float(candidate)
        if val > 1e12:  # looks like ms
            return int(val)
        elif val > 1e9:  # looks like seconds
            return int(val * 1000)
    except Exception:
        pass

    # ISO string fallback
    try:
        s = str(candidate)
        # normalize Z → +00:00 for fromisoformat
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def sync_server_time() -> None:
    """Fetch /v3/time and compute offset so that APEX-TIMESTAMP is within tolerance."""
    global _server_offset_ms
    try:
        r = session.get(f"{BASE_URL}/v3/time", timeout=(10, 10))
        r.raise_for_status()
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    except Exception as e:
        logging.warning("Failed to fetch /v3/time: %s", e)
        return

    server_ms = _parse_server_time_to_ms(data)
    if server_ms is None:
        logging.warning("Could not parse server time from /v3/time response: %s", data)
        return

    local_ms = int(time.time() * 1000)
    _server_offset_ms = server_ms - local_ms
    if DEBUG:
        try:
            _post_text(f"⏱ synced server time: server_ms={server_ms} local_ms={local_ms} offset_ms={_server_offset_ms}")
        except Exception:
            pass

# Initial sync at boot, then we’ll re-sync periodically
sync_server_time()
_last_sync = time.time()

def _timestamp() -> str:
    """
    Produce APEX-TIMESTAMP using server offset.
    - "ms": epoch milliseconds (string)
    - "iso": ISO 8601 with Z
    """
    global _last_sync
    now = time.time()
    if now - _last_sync > 300:  # re-sync every 5 minutes
        sync_server_time()
        _last_sync = now

    ms = int(time.time() * 1000) + _server_offset_ms
    if TS_STYLE == "iso":
        # Convert ms to ISO Z
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    else:
        return str(ms)

# ========= Signing (Omni v3) =========
def _data_string(data: Optional[Dict[str, Any]]) -> str:
    if not data:
        return ""
    items = sorted((k, v) for k, v in data.items() if v is not None)
    return "&".join(f"{k}={v}" for k, v in items)

def _sign(path: str, method: str, ts: str, data: Optional[Dict[str, Any]] = None) -> str:
    # Signature = Base64(HMAC_SHA256(secret_bytes, ts + METHOD + path + dataString))
    msg = ts + method.upper() + path + _data_string(data)
    key_bytes = base64.b64decode(APEX_SECRET) if APEX_SECRET_BASE64 else APEX_SECRET.encode("utf-8")
    digest = hmac.new(key_bytes, msg=msg.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode()

def _headers(path: str, method: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    ts = _timestamp()
    sig = _sign(path, method, ts, data)
    return {
        "APEX-API-KEY": APEX_KEY,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "APEX-TIMESTAMP": ts,
        "APEX-SIGNATURE": sig,
        "Content-Type": "application/json",  # harmless for GET
    }

# ========= Discord helpers =========
def _post_discord_json(payload: Dict[str, Any]) -> None:
    try:
        session.post(DISCORD_WEBHOOK, json=payload, timeout=(10, 20))
    except Exception as e:
        logging.error("Discord post failed: %s", e)

def _post_text(text: str) -> None:
    _post_discord_json({"content": text})

def _code_table(rows: List[str]) -> str:
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
        "description": _code_table(rows),
        "timestamp": ts,
        "color": color,
        "footer": {"text": "ApeX → Discord"},
    }]}

# ========= API calls =========
def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE_URL}{path}"
    hdrs = _headers(path, "GET")
    r = session.get(url, headers=hdrs, params=params, timeout=(20, 30))
    try:
        data = r.json()
    except Exception:
        data = {}
    # Surface app-level errors (HTTP 200 with {"code":..., "msg":...})
    api_code = data.get("code")
    api_msg  = data.get("msg")
    if api_code not in (None, 0):
        logging.error("Omni API error on %s: code=%s msg=%s", path, api_code, api_msg)
        if DEBUG:
            _post_text(f"⚠️ Omni error {path}: code={api_code} msg={api_msg}")
    return r.status_code, data

def get_time_status() -> int:
    code, _ = _get("/v3/time")
    return code

def get_user() -> Tuple[int, Dict[str, Any]]:
    return _get("/v3/user")

def get_account() -> Tuple[int, Dict[str, Any]]:
    return _get("/v3/account")

# ========= Position extraction =========
SIZE_KEYS = {"size", "positionSize", "qty", "quantity", "positionQty"}

def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _looks_like_position(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    lk = {k.lower() for k in item.keys()}
    has_sym = ("symbol" in lk) or ("market" in lk)
    has_size = any(k in lk for k in SIZE_KEYS)
    return has_sym and has_size

def _walk_positions(obj: Any, path: str = "$"):
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and _looks_like_position(obj[0]):
            yield path, obj
        for i, v in enumerate(obj):
            yield from _walk_positions(v, f"{path}[{i}]")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield from _walk_positions(v, f"{path}.{k}")

def extract_open_positions(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Normalize if wrapped
    acct = payload.get("data", payload) if isinstance(payload, dict) else payload

    best_path, best_list = None, []
    for pth, lst in _walk_positions(acct):
        if len(lst) > len(best_list):
            best_path, best_list = pth, lst

    if DEBUG:
        _post_text(f"🔎 found positions array at: `{best_path or '<none>'}` (len={len(best_list)})")

    open_pos: List[Dict[str, Any]] = []
    for item in best_list:
        # any recognized size key
        size_val = None
        for k in SIZE_KEYS:
            if k in item:
                size_val = item.get(k)
                break
        size = _to_float(size_val)
        if abs(size) <= 0:
            continue

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

# ========= Diagnostics =========
def _keys_preview(obj: Any) -> str:
    try:
        if isinstance(obj, dict):
            lines = []
            for k, v in list(obj.items())[:30]:
                t = type(v).__name__
                if isinstance(v, dict):
                    lines.append(f"{k}: dict({len(v)})")
                elif isinstance(v, list):
                    inner = type(v[0]).__name__ if v else "empty"
                    lines.append(f"{k}: list[{inner}]({len(v)})")
                else:
                    lines.append(f"{k}: {t}")
            return "; ".join(lines) or "<empty dict>"
        return type(obj).__name__
    except Exception:
        return "<uninspectable>"

def diagnostics_once() -> None:
    try:
        tcode = get_time_status()
    except Exception as e:
        tcode = f"err: {e}"

    ucode, user = get_user()
    acode, acct = get_account()

    def _pick(d: Dict[str, Any], k: str):
        return d.get(k) or d.get("data", {}).get(k) or d.get("user", {}).get(k)

    eth_addr = _pick(user, "ethereumAddress") or _pick(acct, "ethereumAddress")
    l2_key   = _pick(user, "l2Key")           or _pick(acct, "l2Key")
    acc_id   = _pick(user, "id")              or _pick(acct, "id")

    top_pos = (acct.get("positions") or acct.get("data", {}).get("positions") or []) if isinstance(acct, dict) else []
    ca_pos  = ((acct.get("contractAccount") or {}).get("positions") or []) if isinstance(acct, dict) else []

    user_keys = _keys_preview(user)
    acct_keys = _keys_preview(acct)

    _post_text(
        "🧪 Diagnostics\n"
        f"BASE_URL={BASE_URL}\n"
        f"GET /v3/time → {tcode} | /v3/user → {ucode} | /v3/account → {acode}\n"
        f"ethereumAddress={eth_addr}\n"
        f"accountId={acc_id} | l2Key={l2_key}\n"
        f"positions(top)={len(top_pos)} | positions(contractAccount)={len(ca_pos)}\n"
        f"userKeys: {user_keys}\n"
        f"acctKeys: {acct_keys}"
    )

# ========= Main loop =========
_last_snapshot: Optional[str] = None
_last_post_ts = 0.0
_last_empty_post_ts = 0.0
_had_positions_last = False

def _should_post(norm: List[Dict[str, Any]], snap: str, now: float) -> bool:
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

def _record_post_state(norm: List[Dict[str, Any]], snap: str, now: float) -> None:
    global _last_snapshot, _last_post_ts, _last_empty_post_ts, _had_positions_last
    _last_post_ts = now
    _last_snapshot = snap if norm else "EMPTY"
    if not norm:
        _last_empty_post_ts = now
    _had_positions_last = bool(norm)

def loop_once() -> None:
    acode, acct = get_account()
    if acode != 200:
        logging.error("/v3/account HTTP %s", acode)
        if DEBUG:
            _post_text(f"⚠️ /v3/account HTTP {acode}")
        return

    norm = extract_open_positions(acct)
    snap = json.dumps(norm, sort_keys=True)
    now = time.time()

    if _should_post(norm, snap, now):
        _post_discord_json(_positions_embed(norm))
        _record_post_state(norm, snap, now)

if __name__ == "__main__":
    diagnostics_once()
    while True:
        try:
            loop_once()
        except Exception as e:
            logging.exception("Unhandled error: %s", e)
        time.sleep(POLL_SECS)
