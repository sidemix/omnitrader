# ApeX Omni → Discord (forward-only)
# - Polls /v3/account and posts your open positions to Discord.
# - Shows only what the API returns. If PnL/Mark aren’t in the payload,
#   those cells are "–" (no external price calls).

import os, time, hmac, hashlib, base64, json, logging
from datetime import datetime, timezone
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ===== env =====
API_KEY        = os.getenv("APEX_API_KEY") or os.getenv("APEX_KEY")
API_SECRET     = os.getenv("APEX_API_SECRET") or os.getenv("APEX_SECRET")
PASSPHRASE     = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE")
DISCORD_WEBHOOK= os.getenv("DISCORD_WEBHOOK")

BASE_URL       = (os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
POLL_SECS      = int(os.getenv("INTERVAL_SECONDS", "30"))
UPDATE_MODE    = (os.getenv("UPDATE_MODE") or "on-change").lower()  # "on-change" | "always"
TS_STYLE       = (os.getenv("APEX_TS_STYLE") or "iso").lower()       # "iso" | "ms"
SECRET_IS_B64  = (os.getenv("APEX_SECRET_BASE64") or "0").strip() in ("1","true","True")

SIZE_DEC       = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DEC      = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DEC        = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DEC    = int(os.getenv("PNL_PCT_DECIMALS", "2"))

# ===== logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

def _fmt(x, n):
    try:
        f = float(x)
    except Exception:
        return "–"
    return f"{f:.{n}f}"

# ===== HTTP session with retries =====
session = requests.Session()
retry = Retry(total=4, connect=4, read=6, backoff_factor=0.7,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://",  HTTPAdapter(max_retries=retry))

def _timestamp():
    if TS_STYLE == "ms":
        return str(int(time.time() * 1000))
    # ISO8601 with Z
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def _key_bytes():
    if SECRET_IS_B64:
        return base64.b64decode(API_SECRET)
    return API_SECRET.encode()

def _sign(ts, method, path, body=""):
    payload = f"{ts}{method.upper()}{path}{body}"
    sig = hmac.new(_key_bytes(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _headers(path, method="GET", body=""):
    ts = _timestamp()
    return {
        "APEX-KEY": API_KEY,
        "APEX-PASSPHRASE": PASSPHRASE,
        "APEX-TIMESTAMP": ts,
        "APEX-SIGNATURE": _sign(ts, method, path, body),
        "Content-Type": "application/json",
    }

def _req_json(path):
    # Try once; if timestamp error, flip style and retry
    url = f"{BASE_URL}{path}"
    h = _headers(path, "GET", "")
    r = session.get(url, headers=h, timeout=15)
    try:
        j = r.json()
    except Exception:
        r.raise_for_status()
        return {}
    if isinstance(j, dict):
        # Omni error code pattern
        if j.get("code") == 20002 and "APEX-TIMESTAMP" in (j.get("msg") or ""):
            # flip style (ms <-> iso) and retry once
            global TS_STYLE
            TS_STYLE = "ms" if TS_STYLE == "iso" else "iso"
            logging.warning("Timestamp rejected; flipped TS_STYLE to %s and retrying", TS_STYLE)
            h = _headers(path, "GET", "")
            r = session.get(url, headers=h, timeout=15)
            try:
                j = r.json()
            except Exception:
                r.raise_for_status()
                return {}
    return j

def get_positions():
    """
    Fetch account data and extract open positions.
    We look through several commonly used layouts:
    - data.contractAccount.positions
    - account.contractAccount.positions
    - positions
    """
    j = _req_json("/v3/account")
    if not isinstance(j, dict):
        return []

    # Drill down variants
    n = j
    for key in ("data", "account"):
        if isinstance(n, dict) and key in n:
            n = n[key]
    if isinstance(n, dict) and "contractAccount" in n:
        n = n["contractAccount"]
    if isinstance(n, dict):
        positions = n.get("positions") or []
    else:
        positions = j.get("positions") or []

    # Filter to “open size > 0”
    out = []
    for p in positions:
        size = p.get("size") or p.get("positionSize") or p.get("qty") or 0
        try:
            if float(size) <= 0:
                continue
        except Exception:
            continue
        out.append(p)
    return out

def _first_field(d, *names, default="–"):
    for name in names:
        if name in d and d[name] not in (None, "", 0, "0"):
            return d[name]
    return default

def build_table(positions):
    # columns: SYMBOL | SIDE | SIZE | ENTRY | MARK | PNL | PNL%
    lines = []
    head = [
        ("SYMBOL",  12),
        ("SIDE",     6),
        ("SIZE",    10),
        ("ENTRY",   12),
        ("MARK",    12),
        ("PNL",     12),
        ("PNL%",     6),
    ]
    def row(cols):
        return "".join(str(col).ljust(w) for col, w in cols)

    lines.append("Active ApeX Positions")
    lines.append("```")
    lines.append(row([(h,w) for h,w in head]))
    lines.append(row([("".ljust(len(h), "-"), w) for h,w in head]))

    for p in positions:
        symbol = _first_field(p, "symbol", "market", "pair", "instrument", default="–")
        side   = _first_field(p, "side", "positionSide", default="–")
        size   = _fmt(_first_field(p, "size", "positionSize", "qty", default=None), SIZE_DEC)

        entry  = _first_field(p, "entryPrice", "avgEntryPrice", "averageEntryPrice", default=None)
        entry  = _fmt(entry, PRICE_DEC) if entry is not None else "–"

        # These may or may not exist in the payload. If missing, show "–".
        mark   = _first_field(p, "markPrice", "indexPrice", "currentPrice", default=None)
        mark   = _fmt(mark, PRICE_DEC) if mark is not None else "–"

        pnl    = _first_field(p, "unrealizedPnl", "uPnl", "pnl", "unRealizedPnl", default=None)
        pnl    = _fmt(pnl, PNL_DEC) if pnl is not None else "–"

        pnlpct = _first_field(p, "unrealizedPnlPct", "pnlPct", default=None)
        pnlpct = (f"{float(pnlpct):.{PNL_PCT_DEC}f}" if pnlpct not in (None, "–") else "–")

        lines.append(row([
            (symbol, 12),
            (side,    6),
            (size,   10),
            (entry,  12),
            (mark,   12),
            (pnl,    12),
            (pnlpct,  6),
        ]))

    if not positions:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def post_to_discord(text):
    if not DISCORD_WEBHOOK:
        logging.error("DISCORD_WEBHOOK missing")
        return
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": text}, timeout=15)
        if r.status_code >= 300:
            logging.error("Discord post failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:
        logging.error("Discord post error: %s", e)

def hash_snapshot(positions):
    """Create a small signature of what we display to avoid repost spam."""
    key_fields = []
    for p in positions:
        key_fields.append((
            _first_field(p, "symbol", "market", "pair", "instrument", default="–"),
            _first_field(p, "side", "positionSide", default="–"),
            str(_first_field(p, "size", "positionSize", "qty", default="0")),
            str(_first_field(p, "entryPrice", "avgEntryPrice", "averageEntryPrice", default="0")),
            str(_first_field(p, "markPrice", "indexPrice", "currentPrice", default="")),
            str(_first_field(p, "unrealizedPnl", "uPnl", "pnl", "unRealizedPnl", default="")),
            str(_first_field(p, "unrealizedPnlPct", "pnlPct", default="")),
        ))
    payload = json.dumps(key_fields, separators=(",",":"))
    return hashlib.sha1(payload.encode()).hexdigest()

def main():
    if not (API_KEY and API_SECRET and PASSPHRASE and DISCORD_WEBHOOK):
        logging.error("Missing one or more envs: APEX_API_KEY, APEX_API_SECRET, APEX_API_PASSPHRASE, DISCORD_WEBHOOK")
        return

    logging.info("Forward-only bridge online. Base=%s  TS_STYLE=%s", BASE_URL, TS_STYLE)
    last_sig = ""
    while True:
        try:
            positions = get_positions()
            sig = hash_snapshot(positions)
            if UPDATE_MODE == "always" or sig != last_sig:
                msg = build_table(positions)
                post_to_discord(msg)
                last_sig = sig
                logging.info("Posted %d positions", len(positions))
            else:
                logging.info("No position change")
        except Exception as e:
            logging.error("Poll error: %s", e)
        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
