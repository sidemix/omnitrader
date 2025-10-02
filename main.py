# main.py  —  ApeX Omni -> Discord background worker
# Notes:
# - Env vars (accepts both legacy and new names):
#     APEX_API_KEY        or APEX_KEY
#     APEX_API_SECRET     or APEX_SECRET
#     APEX_API_PASSPHRASE or APEX_PASSPHRASE
#     DISCORD_WEBHOOK     (required)
#     OMNI_BASE_URL or APEX_BASE_URL (default: https://omni.apex.exchange/api)
#     INTERVAL_SECONDS (default 30)
#     SCAN_SYMBOLS (optional, comma-separated symbols to include)
#     APEX_LATENCY_CUSHION_MS (optional, extra ms for timestamp, default 600)
#
# - Signs: message = timestamp + METHOD + /api/v3/... + dataString
#          key = base64(secret-utf8)
#          signature = base64(HMAC_SHA256(key, message))
#
# - Timestamp negotiation: ISO (from server seconds) -> epoch ms -> epoch s

import os
import sys
import time
import hmac
import json
import base64
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter, Retry

# ---------- logging (ASCII only to avoid latin-1 issues) ----------
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

# ---------- env ----------
API_KEY = os.getenv("APEX_API_KEY") or os.getenv("APEX_KEY", "")
API_SECRET = os.getenv("APEX_API_SECRET") or os.getenv("APEX_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
BASE_URL = (os.getenv("OMNI_BASE_URL") or os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "30"))
SCAN_SYMBOLS = [s.strip().upper() for s in os.getenv("SCAN_SYMBOLS", "").split(",") if s.strip()]
LATENCY_CUSHION_MS = int(os.getenv("APEX_LATENCY_CUSHION_MS", "600"))

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing one or more required envs: APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET, APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------- HTTP session ----------
session = requests.Session()
retries = Retry(total=6, connect=6, read=6, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "apex-omni-discord-bridge/1.0"})

# ---------- time helpers ----------
def get_server_time_seconds() -> int:
    """GET /v3/time and return server epoch seconds."""
    url = f"{BASE_URL}/v3/time"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    js = {}
    try:
        js = r.json()
    except Exception:
        pass
    # accept plain keys or under data
    for src in (js, js.get("data", {}) if isinstance(js, dict) else {}):
        if isinstance(src, dict):
            for k in ("time", "serverTime", "server_timestamp", "serverTs"):
                v = src.get(k)
                if isinstance(v, (int, float)):
                    # seconds or ms, normalize to seconds
                    return int(v if v < 1e12 else v / 1000.0)
    # last resort
    try:
        return int(js)
    except Exception:
        raise RuntimeError(f"Unexpected /v3/time response: {js}")

def iso_from_seconds(sec: int) -> str:
    """ISO8601 with milliseconds and Z from epoch seconds."""
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"

def to_utc_str(ts_like) -> str:
    """Return 'YYYY-MM-DD HH:MM:SS UTC' from ISO string or epoch sec/ms/us."""
    if ts_like in (None, "", 0):
        return ""
    # ISO first
    if isinstance(ts_like, str) and any(c in ts_like for c in ("T", "Z", "+", "-")):
        s = ts_like
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            pass
    # numeric
    try:
        x = float(ts_like)
    except Exception:
        return ""
    # normalize down to seconds
    if x > 1e14:  # microseconds
        x /= 1000.0
    if x > 1e12:  # milliseconds
        x /= 1000.0
    if x > 1e10:  # still too big, clamp again
        x /= 1000.0
    try:
        dt = datetime.fromtimestamp(x, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""

# ---------- signing ----------
def sign_message(ts_str: str, method: str, signed_path: str, body: dict | None) -> str:
    """Build signature per Omni v3 docs."""
    method = method.upper()
    if body is None:
        body = {}
    if method == "GET":
        data_string = ""
    else:
        items = sorted((k, v) for k, v in body.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)
    message = f"{ts_str}{method}{signed_path}{data_string}"
    key = base64.standard_b64encode(API_SECRET.encode("utf-8"))
    digest = hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode("utf-8")

def private_request(method: str, path: str, params: dict | None = None, data: dict | None = None) -> requests.Response:
    """
    method: GET or POST
    path: '/v3/...' (no /api prefix)
    - URL = BASE_URL + path
    - Signed path must include '/api' prefix: '/api' + path
    - For GET with params, query string must be included in the signed path.
    """
    assert path.startswith("/v3/"), "path must start with '/v3/'"
    url = f"{BASE_URL}{path}"
    query = ""
    if method.upper() == "GET" and params:
        # preserve order by sorting for stable signing
        query = "?" + urlencode(sorted(params.items()), doseq=True)
        url = url + query

    server_sec = get_server_time_seconds()
    attempts = [
        ("iso", iso_from_seconds(server_sec)),
        ("ms", str(int(server_sec * 1000 + LATENCY_CUSHION_MS))),
        ("s", str(int(server_sec))),
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

            # Read JSON body (if any) to catch wrapped errors
            body = {}
            if resp.headers.get("Content-Type", "").startswith("application/json"):
                try:
                    body = resp.json()
                except Exception:
                    body = {}

            # Omni-style error wrapper
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

# ---------- discord ----------
def post_discord(text: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": text[:1990]}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        LOG.error(ascii_only(f"Discord post failed: {e}"))

# ---------- data handling ----------
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
    # top-level
    if isinstance(js.get("positions"), list):
        out.extend(js["positions"])
    if isinstance(js.get("openPositions"), list):
        out.extend(js["openPositions"])
    ca = js.get("contractAccount") or {}
    if isinstance(ca.get("positions"), list):
        out.extend(ca["positions"])
    if isinstance(ca.get("openPositions"), list):
        out.extend(ca["openPositions"])
    # under data
    data = js.get("data")
    if isinstance(data, dict):
        if isinstance(data.get("positions"), list):
            out.extend(data["positions"])
        if isinstance(data.get("openPositions"), list):
            out.extend(data["openPositions"])
        dca = data.get("contractAccount") or {}
        if isinstance(dca.get("positions"), list):
            out.extend(dca["positions"])
        if isinstance(dca.get("openPositions"), list):
            out.extend(dca["openPositions"])
    # filter by SCAN_SYMBOLS if provided and dedupe
    uniq = {}
    for p in out:
        sym = str(p.get("symbol") or p.get("contractSymbol") or p.get("market") or "").upper()
        if SCAN_SYMBOLS and sym not in SCAN_SYMBOLS:
            continue
        key = (sym, str(p.get("side")), str(p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice") or ""), str(p.get("size") or p.get("quantity") or p.get("position") or ""))
        uniq[key] = p
    return list(uniq.values())

def format_positions(positions: list[dict]) -> str:
    if not positions:
        return "No open positions"
    lines = []
    for p in positions:
        sym  = str(p.get("symbol") or p.get("contractSymbol") or p.get("market") or "").upper()
        side = str(p.get("side") or p.get("positionSide") or "").upper()
        size = p.get("size") or p.get("position") or p.get("quantity") or "0"
        entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice") or "?"
        upd = p.get("updatedTime") or p.get("updatedAt") or p.get("createdAt") or p.get("time") or p.get("ts")
        when = to_utc_str(upd)
        line = f"- {sym} {side} size={size} entry={entry}"
        if when:
            line += f" updated={when}"
        lines.append(line)
    return "\n".join(lines)

# ---------- state & loop ----------
_last_snapshot = None

def snapshot_of(positions: list[dict]) -> str:
    key_tuples = []
    for p in positions:
        sym  = str(p.get("symbol") or p.get("contractSymbol") or p.get("market") or "").upper()
        side = str(p.get("side") or p.get("positionSide") or "").upper()
        size = str(p.get("size") or p.get("position") or p.get("quantity") or "")
        entry = str(p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice") or "")
        key_tuples.append((sym, side, size, entry))
    key_tuples.sort()
    return json.dumps(key_tuples)

def diagnostics_once():
    try:
        srv = get_server_time_seconds()
        acct = fetch_account()
        positions = extract_positions(acct)
        post_discord(
            "Bridge online.\n"
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
