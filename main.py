# main.py — ApeX Omni → Discord (24/7 Render worker)
# - Polls active positions and posts to a Discord channel
# - Built-in retries, backoff, diagnostics, and snapshot-diff to avoid spam

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

# ---------- Config via ENV ----------
# REQUIRED
APEX_KEY        = os.environ["APEX_KEY"]
APEX_SECRET     = os.environ["APEX_SECRET"].encode()            # keep bytes for HMAC
APEX_PASSPHRASE = os.environ["APEX_PASSPHRASE"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# OPTIONAL (but nice to have if Omni expects it)
APEX_ACCOUNT_ID = os.environ.get("APEX_ACCOUNT_ID", "").strip()

# You can change these in Render → Environment without redeploying code
BASE_URL  = os.environ.get("APEX_BASE_URL", "https://api.omni.apex.exchange").rstrip("/")
POS_PATH  = os.environ.get("APEX_POSITIONS_PATH", "/v1/account/positions")  # override if docs differ
POLL_SECS = int(os.environ.get("POLL_INTERVAL_SECS", "10"))

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ---------- HTTP session with retries ----------
session = requests.Session()
retry = Retry(
    total=6,
    connect=6,
    read=6,
    backoff_factor=1.5,                     # 0s, 1.5s, 3s, 4.5s, ...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://",  HTTPAdapter(max_retries=retry))

# ---------- Helpers ----------
def sign(path: str, method: str, body: str = ""):
    """
    ApeX signature (typical): ts + method + path + body  -> HMAC-SHA256(secret)
    Adjust here if your Omni docs specify a different payload.
    """
    ts = str(int(time.time() * 1000))
    payload = ts + method.upper() + path + body
    sig = hmac.new(APEX_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return ts, sig

def apex_headers(ts: str, sig: str):
    # Common headers; include passphrase and (optionally) account id if present
    headers = {
        "APEX-KEY": APEX_KEY,
        "APEX-SIGN": sig,
        "APEX-TS": ts,
        "APEX-PASSPHRASE": APEX_PASSPHRASE,
        "Content-Type": "application/json",
    }
    if APEX_ACCOUNT_ID:
        headers["APEX-ACCOUNT-ID"] = APEX_ACCOUNT_ID
    return headers

def get_open_positions():
    path = POS_PATH
    ts, sig = sign(path, "GET")
    headers = apex_headers(ts, sig)
    # Longer (connect, read) timeouts to be resilient on cold networks
    r = session.get(BASE_URL + path, headers=headers, timeout=(20, 30))
    r.raise_for_status()
    return r.json()

def post_to_discord(lines):
    if not lines:
        return
    # Discord message content limit is ~2000 chars; chunk if needed
    content = "🟢 **Active ApeX Positions** ({})\n{}".format(
        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ'),
        "\n".join(lines),
    )
    for chunk in _chunks(content, 1900):
        try:
            session.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=(10, 20))
        except Exception as e:
            logging.error("Discord post failed: %s", e)

def _chunks(s: str, n: int):
    for i in range(0, len(s), n):
        yield s[i:i+n]

def build_lines(positions):
    out = []
    # positions could be under {"positions": [...]} or just a list; normalize:
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
    return out

def diag():
    """One-shot diagnostics posted to Discord so you can confirm connectivity & egress IP."""
    lines = [f"🔧 **Diagnostics** BASE_URL=`{BASE_URL}` PATH=`{POS_PATH}`"]
    # Egress IP
    try:
        ip = session.get("https://api.ipify.org", timeout=(5,10)).text
        lines.append(f"Egress IP: `{ip}`")
    except Exception as e:
        lines.append(f"Egress IP check failed: `{e}`")
    # DNS
    try:
        host = BASE_URL.split("://",1)[1].split("/",1)[0]
        addrs = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        addrs = list({a[4][0] for a in addrs})
        lines.append(f"DNS {host} → {addrs}")
    except Exception as e:
        lines.append(f"DNS failed: `{e}`")
    # Generic HTTPS sanity
    try:
        session.get("https://www.google.com", timeout=(5,10))
        lines.append("HTTPS to google.com: OK")
    except Exception as e:
        lines.append(f"HTTPS to google.com FAILED: `{e}`")
    # ApeX reachability (try a couple of common paths)
    tried = ["/v1/ping", "/v1/time", POS_PATH]
    for p in tried:
        try:
            session.get(BASE_URL + p, timeout=(10,10))
            lines.append(f"ApeX reachability OK: `{p}`")
            break
        except Exception as e:
            lines.append(f"ApeX `{p}` FAILED: `{e}`")
    post_to_discord(lines)

# ---------- Main loop ----------
if __name__ == "__main__":
    logging.info("Starting worker. BASE_URL=%s", BASE_URL)
    # One-shot heartbeat/diagnostic
    try:
        diag()
    except Exception as e:
        logging.warning("Diagnostics failed: %s", e)

    last_snapshot = None
    while True:
        try:
            data = get_open_positions()
            # Only post when positions actually change
            snap = json.dumps(data, sort_keys=True, default=str)
            if snap != last_snapshot:
                post_to_discord(build_lines(data))
                last_snapshot = snap
        except requests.exceptions.ConnectTimeout as e:
            logging.warning("Connect timeout: %s", e)
        except requests.exceptions.ReadTimeout as e:
            logging.warning("Read timeout: %s", e)
        except requests.exceptions.HTTPError as e:
            # Log body for easier debugging
            try:
                logging.error("HTTP %s: %s", e.response.status_code, e.response.text)
            except Exception:
                logging.error("HTTP error: %s", e)
        except requests.exceptions.RequestException as e:
            logging.error("HTTP error: %s", e)
        except Exception as e:
            logging.exception("Unexpected error")
        time.sleep(POLL_SECS)
