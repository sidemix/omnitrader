# main.py — ApeX (Omni/Pro) → Discord notifier (Render worker)
# - Robust retries
# - Auto-picks first reachable BASE URL from a candidate list
# - Snapshot diff to avoid spam
# - One-shot diagnostics to Discord on boot

import os
import time
import hmac
import hashlib
import json
import logging
import socket
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------- ENV --------
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"].encode()            # bytes for HMAC
APEX_PASSPHRASE = os.environ.get("APEX_PASSPHRASE", "")
APEX_ACCOUNT_ID = os.environ.get("APEX_ACCOUNT_ID", "").strip()
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# Accept comma-separated candidates; first working one is used.
# You can override in Render → Environment without code changes.
APEX_BASE_URLS  = os.environ.get(
    "APEX_BASE_URLS",
    "https://api.omni.apex.exchange,https://api.pro.apex.exchange,https://api.apex.exchange"
)

POS_PATH  = os.environ.get("APEX_POSITIONS_PATH", "/v1/account/positions")
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))

# -------- Logging --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# -------- HTTP session with retries --------
session = requests.Session()
retry = Retry(
    total=6, connect=6, read=6,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://",  HTTPAdapter(max_retries=retry))

# -------- Utilities --------
def _chunks(s: str, n: int):
    for i in range(0, len(s), n):
        yield s[i:i+n]

def post_to_discord(lines_or_text):
    """Send one or multiple lines to Discord safely."""
    if not lines_or_text:
        return
    if isinstance(lines_or_text, str):
        text = lines_or_text
    else:
        text = "\n".join(lines_or_text)
    content = text
    for chunk in _chunks(content, 1900):
        try:
            session.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=(10, 20))
        except Exception as e:
            logging.error("Discord post failed: %s", e)

def _resolve_host(host: str):
    """Return unique IPs resolved for a host (or [])."""
    try:
        addrs = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        return list({a[4][0] for a in addrs})
    except Exception:
        return []

def pick_working_base_url(candidates):
    """Try each base URL: DNS and then a quick GET to /v1/ping or /v1/time."""
    tried = []
    for raw in candidates:
        base = raw.strip().rstrip("/")
        if not base:
            continue
        host = base.split("://",1)[1].split("/",1)[0]
        ips = _resolve_host(host)
        tried.append(f"{base} DNS={ips or 'N/A'}")
        if not ips:
            continue
        # probe a couple common endpoints quickly
        for probe in ("/v1/ping", "/v1/time", "/"):
            url = base + probe
            try:
                r = session.get(url, timeout=(5, 8))
                # Any 2xx/3xx/4xx means TCP+TLS+HTTP OK; 5xx may still be OK
                if r.status_code >= 200:
                    logging.info("Base URL selected: %s (probe %s -> %s)", base, probe, r.status_code)
                    return base, tried
            except Exception as e:
                logging.warning("Probe failed %s: %s", url, e)
                continue
    return None, tried

def sign(path: str, method: str, body: str = ""):
    """Signature: ts + METHOD + path + body (adjust if ApeX spec differs)."""
    ts = str(int(time.time() * 1000))
    payload = ts + method.upper() + path + body
    sig = hmac.new(APEX_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig

def apex_headers(ts: str, sig: str):
    headers = {
        "APEX-KEY": APEX_KEY,
        "APEX-SIGN": sig,
        "APEX-TS": ts,
        "Content-Type": "application/json",
    }
    if APEX_PASSPHRASE:
        headers["APEX-PASSPHRASE"] = APEX_PASSPHRASE
    if APEX_ACCOUNT_ID:
        headers["APEX-ACCOUNT-ID"] = APEX_ACCOUNT_ID
    return headers

def get_open_positions(base_url: str):
    ts, sig = sign(POS_PATH, "GET")
    headers = apex_headers(ts, sig)
    r = session.get(base_url + POS_PATH, headers=headers, timeout=(20, 30))
    r.raise_for_status()
    return r.json()

def build_lines(positions):
    out = []
    items = positions.get("positions", positions) if isinstance(positions, dict) else positions
    for p in items or []:
        sym   = p.get("symbol") or p.get("market") or "?"
        side  = p.get("side") or p.get("direction") or "?"
        sz    = p.get("size") or p.get("positionSize") or p.get("qty") or "?"
        entry = p.get("entryPrice") or p.get("avgEntryPrice") or "?"
        mark  = p.get("markPrice") or p.get("lastPrice") or p.get("indexPrice") or "?"
        upnl  = p.get("unrealizedPnl") or p.get("uPnL") or "?"
        lev   = p.get("leverage") or "?"
        out.append(f"**{sym}** — {side} | size: {sz} | entry: {entry} | mark: {mark} | lev: {lev} | uPnL: {upnl}")
    if not out:
        out.append("_No open positions_")
    # Prefix with timestamp
    out.insert(0, f"🟢 **Active ApeX Positions** ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')})")
    return out

def diag(selected_base: str, tried_list):
    lines = [f"🔧 **Diagnostics**\nPath: `{POS_PATH}`\nCandidates: {APEX_BASE_URLS}"]
    # Egress IP
    try:
        ip = session.get("https://api.ipify.org", timeout=(5,10)).text
        lines.append(f"Egress IP: `{ip}`")
    except Exception as e:
        lines.append(f"Egress IP check failed: `{e}`")
    # What we tried
    for t in tried_list:
        lines.append(f"Tried: {t}")
    # Result
    if selected_base:
        lines.append(f"✅ Selected BASE_URL: `{selected_base}`")
    else:
        lines.append("❌ No candidate resolved/responded.")
    post_to_discord(lines)

# -------- Main --------
if __name__ == "__main__":
    # Choose a reachable base URL
    candidates = [c for c in APEX_BASE_URLS.split(",")]
    base_url, tried = pick_working_base_url(candidates)
    logging.info("Base URL choice: %s", base_url or "NONE")

    # Post diagnostics once
    try:
        diag(base_url, tried)
    except Exception as e:
        logging.warning("Diagnostics failed: %s", e)

    if not base_url:
        # Sleep & retry periodically so the worker stays up if DNS/egress changes
        while True:
            logging.error("No reachable ApeX base URL from candidates; will retry in 60s.")
            time.sleep(60)
            base_url, tried = pick_working_base_url(candidates)
            if base_url:
                try:
                    diag(base_url, tried)
                except Exception:
                    pass
                break

    last_snapshot = None
    while True:
        try:
            data = get_open_positions(base_url)
            snap = json.dumps(data, sort_keys=True, default=str)
            if snap != last_snapshot:
                post_to_discord(build_lines(data))
                last_snapshot = snap
        except requests.exceptions.ConnectTimeout as e:
            logging.warning("Connect timeout: %s", e)
        except requests.exceptions.ReadTimeout as e:
            logging.warning("Read timeout: %s", e)
        except requests.exceptions.HTTPError as e:
            try:
                logging.error("HTTP %s: %s", e.response.status_code, e.response.text)
            except Exception:
                logging.error("HTTP error: %s", e)
        except requests.exceptions.RequestException as e:
            logging.error("HTTP error: %s", e)
        except Exception as e:
            logging.exception("Unexpected error")
        time.sleep(POLL_SECS)
