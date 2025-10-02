import os
import sys
import time
import hmac
import base64
import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter, Retry

# ---------- Config & logging (ASCII-only in stdout) ----------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOG = logging.getLogger("apex-bridge")

def ascii_only(s: str) -> str:
    # Avoid latin-1/ascii codec crashes in Render logs
    try:
        return s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        return s

# ---------- Environment ----------
API_KEY        = os.getenv("APEX_API_KEY")        or os.getenv("APEX_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET")     or os.getenv("APEX_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
BASE_URL = (os.getenv("OMNI_BASE_URL") or os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")

INTERVAL = int(os.getenv("INTERVAL_SECONDS", "30"))
SCAN_SYMBOLS = [s.strip().upper() for s in os.getenv("SCAN_SYMBOLS", "").split(",") if s.strip()]

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing one or more required envs: APEX_API_KEY, APEX_API_SECRET, APEX_API_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------- HTTP session ----------
session = requests.Session()
retries = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=0.5,
    status_forcelist=(429, 500, 502, 503, 504),
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "apex-omni-discord-bridge/1.0"})

# ---------- Helpers from docs ----------
# Signature content: timeStamp + method + path + dataString
# HMAC-SHA256 with base64(secret) as key, then base64-encode the digest.
# Ref: ApeX Omni API docs (ApiKey Signature) and sample sign() method.
# https://api-docs.pro.apex.exchange/  (Account -> GET /v3/account, ApiKey Signature)  # citation in chat text

def _format_iso_from_server_ts(server_ts_seconds: int) -> str:
    # ISO8601 with millisecond precision and 'Z'
    dt = datetime.fromtimestamp(server_ts_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"

def _sign(timestamp_str: str, method: str, request_path: str, data: dict | None) -> str:
    method = method.upper()
    if data is None:
        data = {}
    if method == "GET":
        data_string = ""
    else:
        # Sort body fields alphabetically: key1=v1&key2=v2 ...
        items = sorted((k, v) for k, v in data.items())
        data_string = "&".join(f"{k}={v}" for k, v in items)
    message = f"{timestamp_str}{method}{request_path}{data_string}"
    # IMPORTANT: HMAC key is base64(secret)
    key = base64.standard_b64encode(API_SECRET.encode("utf-8"))
    digest = hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode("utf-8")

def _private_headers(ts_str: str, sig: str) -> dict:
    return {
        "APEX-SIGNATURE": sig,
        "APEX-TIMESTAMP": ts_str,
        "APEX-API-KEY": API_KEY,
        "APEX-PASSPHRASE": API_PASSPHRASE,
    }

def _get_server_time_seconds() -> int:
    url = f"{BASE_URL}/v3/time"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    # Docs show integer system time. Use 'time' or 'serverTime' if present.
    js = r.json()
    # accept any of these keys
    for k in ("time", "serverTime", "server_timestamp", "serverTs"):
        if k in js and isinstance(js[k], (int, float)):
            return int(js[k])
    # Some implementations return data wrapper
    data = js.get("data") if isinstance(js, dict) else None
    if isinstance(data, dict):
        for k in ("time", "serverTime"):
            if k in data and isinstance(data[k], (int, float)):
                return int(data[k])
    # Fallback: parse as int directly
    try:
        return int(js)
    except Exception:
        raise RuntimeError(f"Unexpected /v3/time response: {js}")

def _request_private(method: str, path_v3: str, params: dict | None = None, data: dict | None = None) -> requests.Response:
    """
    path_v3 must start with '/api/v3/...'
    tries ISO -> ms -> s timestamps. Uses server time to avoid skew.
    """
    assert path_v3.startswith("/api/v3/"), "path_v3 must include '/api/v3/...'"
    url = f"{BASE_URL}{path_v3[4:]}" if BASE_URL.endswith("/api") else f"{BASE_URL}{path_v3[4:]}"
    # ^ BASE_URL already ends with /api; path_v3 also contains /api prefix.
    # Construct a GET URL with query (docs: include query in the signed path)
    query = ""
    if method.upper() == "GET" and params:
        query = "?" + urlencode(params, doseq=True)
    server_ts = _get_server_time_seconds()

    attempts = [
        ("iso", _format_iso_from_server_ts(server_ts)),
        ("ms", str(int(server_ts * 1000))),
        ("s", str(int(server_ts))),
    ]

    last_exc = None
    for label, ts in attempts:
        try:
            # Build the request_path including query (for GET)
            request_path = f"/api/v3{path_v3.split('/v3',1)[1]}{query}"
            sig = _sign(ts, method, request_path, data if method.upper() != "GET" else {})
            headers = _private_headers(ts, sig)
            if method.upper() == "GET":
                resp = session.get(url + query, headers=headers, timeout=15)
            else:
                # x-www-form-urlencoded body, as per docs
                resp = session.post(url, headers=headers, data=data or {}, timeout=20)
            # If Omni wraps errors in JSON with 'code', surface it
            if resp.headers.get("Content-Type", "").startswith("application/json"):
                body = {}
                try:
                    body = resp.json()
                except Exception:
                    body = {}
                if isinstance(body, dict) and "code" in body and body.get("code") != 0:
                    code, msg = body.get("code"), str(body.get("msg"))
                    # 20002 -> timestamp error, 20009 -> expired; try next format
                    if code in (20002, 20009) and "timestamp" in msg.lower():
                        LOG.warning(ascii_only(f"Omni API error on {path_v3}: code={code} msg={msg} (ts={label})"))
                        continue  # try next ts format
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            LOG.warning(ascii_only(f"Retrying with different timestamp format ({label}) due to: {e}"))
            time.sleep(0.3)

    # All attempts failed
    if last_exc:
        raise last_exc
    raise RuntimeError("Unknown signing error")

# ---------- Discord ----------
def post_to_discord(text: str):
    try:
        payload = {"content": text}
        r = session.post(DISCORD_WEBHOOK, json=payload, timeout=15)
        r.raise_for_status()
    except Exception as e:
        LOG.error(ascii_only(f"Discord post failed: {e}"))

# ---------- Position fetch & format ----------
def fetch_account() -> dict:
    r = _request_private("GET", "/api/v3/account")
    return r.json()

def extract_positions(account_json: dict) -> list[dict]:
    # Look for positions arrays in several places
    candidates = []
    if isinstance(account_json, dict):
        if isinstance(account_json.get("positions"), list):
            candidates.extend(account_json["positions"])
        if isinstance(account_json.get("openPositions"), list):
            candidates.extend(account_json["openPositions"])
        ca = account_json.get("contractAccount") or {}
        if isinstance(ca.get("openPositions"), list):
            candidates.extend(ca["openPositions"])
        # Some APIs wrap in 'data'
        data = account_json.get("data")
        if isinstance(data, dict):
            if isinstance(data.get("positions"), list):
                candidates.extend(data["positions"])
            if isinstance(data.get("openPositions"), list):
                candidates.extend(data["openPositions"])
            dca = data.get("contractAccount") or {}
            if isinstance(dca.get("openPositions"), list):
                candidates.extend(dca["openPositions"])
    # De-dup naive (by symbol+side+entryPrice)
    uniq = {}
    for p in candidates:
        sym = str(p.get("symbol", "")).upper()
        if SCAN_SYMBOLS and sym not in SCAN_SYMBOLS:
            continue
        key = (sym, p.get("side"), p.get("entryPrice"), p.get("size"))
        uniq[key] = p
    return list(uniq.values())

def format_positions(positions: list[dict]) -> str:
    if not positions:
        return "No open positions"
    lines = []
    for p in positions:
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        size = p.get("size", "0")
        entry = p.get("entryPrice", "") or p.get("entry_price", "")
        upd = p.get("updatedTime") or p.get("updatedAt") or p.get("createdAt")
        when = ""
        if isinstance(upd, (int, float)) and upd > 10**12:
            # epoch ms
            dt = datetime.fromtimestamp(upd/1000, tz=timezone.utc)
            when = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        elif isinstance(upd, (int, float)):
            dt = datetime.fromtimestamp(upd, tz=timezone.utc)
            when = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"- {sym} {side} size={size} entry={entry}"
        if when:
            line += f" updated={when}"
        lines.append(line)
    return "\n".join(lines)

# State to avoid spamming
_last_snapshot = None

def poll_once():
    global _last_snapshot
    acct = fetch_account()
    positions = extract_positions(acct)
    # Build a stable snapshot string for dedupe
    snapshot = "|".join(
        f"{p.get('symbol','')}|{p.get('side','')}|{p.get('size','')}|{p.get('entryPrice') or p.get('entry_price') or ''}"
        for p in sorted(positions, key=lambda x: (str(x.get('symbol','')), str(x.get('side','')), str(x.get('entryPrice') or x.get('entry_price') or '')))
    )
    if snapshot != _last_snapshot:
        _last_snapshot = snapshot
        body = "**Active ApeX Positions**\n" + format_positions(positions)
        post_to_discord(body)
        LOG.info(ascii_only(f"Posted {len(positions)} positions to Discord"))
    else:
        LOG.info(ascii_only("No position change"))

def diagnostics_once():
    try:
        # GET /v3/time (server reachable?)
        t = _get_server_time_seconds()
        # GET /v3/account and /v3/user headers ok?
        acct = fetch_account()
        # Try user endpoint too (optional; ignore failures)
        try:
            resp_user = _request_private("GET", "/api/v3/user")
            uok = resp_user.status_code
        except Exception as ue:
            uok = f"ERR {ue}"
        positions = extract_positions(acct)
        # Print minimal ASCII diag
        LOG.info(ascii_only(f"Diagnostics: base={BASE_URL} serverTime={t} user={uok} positions={len(positions)}"))
        post_to_discord(
            "Bridge online.\n"
            f"Base={BASE_URL}\n"
            f"/v3/time ok, /v3/account ok, /v3/user={uok}\n"
            f"Positions detected: {len(positions)}"
        )
    except Exception as e:
        LOG.error(ascii_only(f"Diagnostics failed: {e}"))

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
