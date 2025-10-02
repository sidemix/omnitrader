# main.py
import os
import time
import hmac
import json
import base64
import hashlib
import logging
from datetime import datetime, timezone

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Environment
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL = os.getenv("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")
API_KEY = os.getenv("APEX_API_KEY", "")
API_SECRET = os.getenv("APEX_API_SECRET", "")
PASSPHRASE = os.getenv("APEX_API_PASSPHRASE", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
# small cushion so our signed timestamp is slightly ahead of server time
LATENCY_MS = int(os.getenv("APEX_LATENCY_MS", "500"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

session = requests.Session()
session.headers.update({"User-Agent": "apex-omni→discord/1.0"})

server_offset_ms = 0  # server_time_ms - local_time_ms


# ──────────────────────────────────────────────────────────────────────────────
# Helpers: time sync & signing (milliseconds timestamp)
# ──────────────────────────────────────────────────────────────────────────────
def _now_ms() -> int:
    return int(time.time() * 1000) + server_offset_ms


def _server_time_ms() -> int:
    """
    GET /v3/time
    Response: {"data":{"time": 1648626529}}  # seconds, per docs
    We normalize to ms.
    """
    url = f"{BASE_URL}/v3/time"
    r = session.get(url, timeout=5)
    r.raise_for_status()
    j = r.json()
    t = (j.get("data") or {}).get("time")
    if t is None:
        raise RuntimeError(f"/v3/time missing 'data.time': {j}")
    # docs show seconds -> convert to ms if needed
    ms = int(float(t) * 1000) if t < 1e12 else int(t)
    return ms


def sync_clock():
    global server_offset_ms
    try:
        srv = _server_time_ms()
        loc = int(time.time() * 1000)
        server_offset_ms = srv - loc
    except Exception as e:
        logging.warning("Clock sync failed; falling back to local clock: %s", e)
        server_offset_ms = 0


def _sign(request_path: str, method: str, body_params: dict | None, ts_ms: int) -> str:
    """
    Signature per Omni v3:
      message = timestamp + method + path + dataString
      key     = base64(secret)
      digest  = HMAC-SHA256(message, key); base64-encode result
    For GET requests, dataString is empty (query params are included in `path` if any).
    """
    if body_params is None:
        body_params = {}
    # Build dataString for POST only (application/x-www-form-urlencoded sorted by key)
    if method.upper() == "POST":
        items = sorted((k, v) for k, v in body_params.items() if v is not None)
        data_string = "&".join(f"{k}={v}" for k, v in items)
    else:
        data_string = ""

    # IMPORTANT: ApeX expects **milliseconds** string in the header and message
    ts_str = str(int(ts_ms))
    message = f"{ts_str}{method.upper()}{request_path}{data_string}"

    key = base64.standard_b64encode(API_SECRET.encode("utf-8"))
    digest = hmac.new(key, msg=message.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode()


def _headers(path: str, method: str = "GET", body_params: dict | None = None) -> dict:
    ts_ms = _now_ms() + LATENCY_MS
    signature = _sign(path, method, body_params, ts_ms)
    return {
        "APEX-API-KEY": API_KEY,
        "APEX-PASSPHRASE": PASSPHRASE,
        "APEX-TIMESTAMP": str(ts_ms),  # <— 13-digit milliseconds
        "APEX-SIGNATURE": signature,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Minimal client
# ──────────────────────────────────────────────────────────────────────────────
def apex_get(path: str, params: dict | None = None, timeout: int = 10) -> dict:
    url = f"{BASE_URL}{path}"
    r = session.get(url, headers=_headers(path, "GET"), params=params, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {path} body={r.text[:300]}")
    j = r.json()
    # Omni wraps errors as code/msg
    if isinstance(j, dict) and j.get("code") not in (None, 0):
        raise RuntimeError(f"Omni API error on {path}: code={j.get('code')} msg={j.get('msg')}")
    return j


def get_user() -> dict:
    return apex_get("/v3/user")


def get_account() -> dict:
    return apex_get("/v3/account")


# ──────────────────────────────────────────────────────────────────────────────
# Position extraction & formatting
# ──────────────────────────────────────────────────────────────────────────────
def extract_positions(account_json: dict) -> list[dict]:
    """
    Account response bundles identifiers and trading state.
    We'll look for 'positions' in the usual places and return a list (possibly empty).
    """
    data = account_json.get("data") or account_json
    candidates = [
        data.get("positions"),
        (data.get("contractAccount") or {}).get("positions"),
        (data.get("spotAccount") or {}).get("positions"),
    ]
    for c in candidates:
        if isinstance(c, list):
            return c
    return []


def format_positions(positions: list[dict]) -> str:
    if not positions:
        return "No open positions"
    lines = []
    for p in positions:
        sym = p.get("symbol") or p.get("contractSymbol") or p.get("market") or "?"
        side = p.get("side") or p.get("positionSide") or "?"
        size = p.get("size") or p.get("position") or p.get("quantity") or "?"
        entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("avgPrice") or "?"
        upl = p.get("unRealizedPnl") or p.get("unrealizedPnl") or p.get("unrealized") or "?"
        liq = p.get("liqPrice") or p.get("liquidationPrice")
        line = f"• {sym} {side}  size={size}  entry={entry}  uPnL={upl}"
        if liq:
            line += f"  liq={liq}"
        lines.append(line)
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Discord
# ──────────────────────────────────────────────────────────────────────────────
def discord_post(content: str | None = None, embeds: list | None = None):
    if not DISCORD_WEBHOOK:
        return
    payload = {}
    if content:
        payload["content"] = content[:1990]
    if embeds:
        payload["embeds"] = embeds
    try:
        rr = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        rr.raise_for_status()
    except Exception as e:
        logging.error("discord_post failed: %s", e)


# ──────────────────────────────────────────────────────────────────────────────
# Diagnostics & loop
# ──────────────────────────────────────────────────────────────────────────────
def diagnostics_once():
    try:
        srv = _server_time_ms()
        drift = srv - int(time.time() * 1000)
        user = get_user()
        acct = get_account()
        pos = extract_positions(acct)
        lines = [
            "🧪 Diagnostics",
            f"BASE_URL={BASE_URL}",
            f"timestamp_mode=ms  drift={drift}ms  latency_pad={LATENCY_MS}ms",
            f"/v3/user → code={user.get('code', 0)}",
            f"/v3/account → code={acct.get('code', 0)}",
            f"positions={len(pos)}",
        ]
        discord_post("\n".join(lines))
    except Exception as e:
        discord_post(f"🧪 Diagnostics failed: {e}")


def main():
    logging.info("Starting worker. BASE_URL=%s", BASE_URL)
    if not (API_KEY and API_SECRET and PASSPHRASE and DISCORD_WEBHOOK):
        logging.error("Missing required env: APEX_API_KEY, APEX_API_SECRET, APEX_API_PASSPHRASE, DISCORD_WEBHOOK")
    sync_clock()
    discord_post("ApeX → Discord bridge online. Using **ms** timestamps.")
    diagnostics_once()

    seen_fingerprints: set[str] = set()
    while True:
        try:
            acct = get_account()
            positions = extract_positions(acct)
            # build a lightweight fingerprint to avoid spam
            fp = json.dumps(sorted([(p.get("symbol"), p.get("side"), str(p.get("size"))) for p in positions]))
            if fp not in seen_fingerprints:
                seen_fingerprints.add(fp)
                msg = f"**Active ApeX Positions**\n{format_positions(positions)}"
                discord_post(msg)
        except Exception as e:
            logging.error("%s", e)
            discord_post(f"⚠️ {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")
    main()
