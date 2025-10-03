# main.py — ApeX Omni → Discord (forward-only)
# - Reads /v3/account and posts open positions to Discord.
# - Forwards only what the API returns. If mark/pnl aren’t present,
#   they show as "-" (no external price calls).
# - Robust to Omni variants, timestamp styles, and raw/base64 secrets.

import os
import time
import json
import hmac
import hashlib
import base64
import logging
from typing import Dict, List, Tuple, Any, Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ------------------------- ENV -------------------------

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()

BASE_URL = (os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
API_KEY = os.getenv("APEX_API_KEY", "").strip()
API_SECRET = os.getenv("APEX_API_SECRET", "").strip()
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE", "").strip()

# Timestamp style to try first; will auto-flip on server error 20002 (timestamp)
INIT_TS_STYLE = (os.getenv("APEX_TS_STYLE") or "ms").strip().lower()  # "ms" or "iso"

# Secret handling: auto (default), raw, or base64.
SECRET_MODE = (os.getenv("APEX_SECRET_MODE") or "auto").strip().lower()  # auto|raw|base64

INTERVAL_SECS = int(os.getenv("INTERVAL_SECONDS", "30"))
UPDATE_MODE = (os.getenv("UPDATE_MODE") or "on-change").strip().lower()  # "on-change" | "always"
MIN_POS_SIZE = float(os.getenv("APEX_MIN_POSITION_SIZE", "0"))

SIZE_DEC = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DEC = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DEC = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DEC = int(os.getenv("PNL_PCT_DECIMALS", "2"))

DEBUG_ON = (os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes", "y"))

# Optional: if your account endpoint needs an accountId param
ACCOUNT_ID = os.getenv("APEX_ACCOUNT_ID", "").strip()

# Keep small mutable state (avoids globals)
STATE = {"ts_style": INIT_TS_STYLE}

# ------------------------- LOGGING -------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
LOG = logging.getLogger("apex-forward")

# ------------------------- HTTP -------------------------

session = requests.Session()
retry = Retry(
    total=4,
    connect=4,
    read=6,
    backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://", HTTPAdapter(max_retries=retry))
session.headers.update({"Accept": "application/json", "User-Agent": "apex-forward/1.1"})

# ------------------------- UTILS -------------------------

def secret_bytes() -> bytes:
    """Return HMAC key bytes using SECRET_MODE (auto/raw/base64)."""
    s = (API_SECRET or "").strip()
    if SECRET_MODE == "raw":
        return s.encode("utf-8")
    if SECRET_MODE == "base64":
        return base64.b64decode(s)
    # auto-detect: try strict base64, else raw
    try:
        return base64.b64decode(s, validate=True)
    except Exception:
        return s.encode("utf-8")

def now_ts(ts_style: str) -> str:
    """Return timestamp string per style (ms or ISO8601 Z)."""
    if ts_style == "ms":
        return str(int(time.time() * 1000))
    t = time.gmtime()
    return f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d}T{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}.000Z"

def sign(ts: str, method: str, path: str, body: str = "") -> str:
    payload = f"{ts}{method.upper()}{path}{body}"
    digest = hmac.new(secret_bytes(), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")

def discord_post(text: str):
    if not DISCORD_WEBHOOK:
        LOG.error("DISCORD_WEBHOOK not set")
        return
    try:
        session.post(DISCORD_WEBHOOK, json={"content": text[:1990]}, timeout=15)
    except Exception as e:
        LOG.error("Discord post failed: %s", e)

def safe_float(x: Any) -> float:
    try:
        return float(str(x))
    except Exception:
        return 0.0

def fmt_num(x: Any, dec: int) -> str:
    try:
        f = float(x)
        return f"{f:.{dec}f}"
    except Exception:
        return "-"

# ------------------------- API -------------------------

def request_json(path: str) -> Dict[str, Any]:
    """
    Signed GET. If server returns a timestamp error (code 20002), flip ts_style
    (ms <-> iso) once and retry. Uses STATE['ts_style'] without globals.
    """
    url = f"{BASE_URL}{path}"

    def do_call(ts_style: str) -> Tuple[Optional[Dict[str, Any]], Optional[requests.Response]]:
        ts = now_ts(ts_style)
        hdrs = {
            "APEX-API-KEY": API_KEY,
            "APEX-PASSPHRASE": API_PASSPHRASE,
            "APEX-TIMESTAMP": ts,
            "APEX-SIGNATURE": sign(ts, "GET", path, ""),
            "Content-Type": "application/json",
        }
        r = session.get(url, headers=hdrs, timeout=15)
        try:
            js = r.json()
        except Exception:
            r.raise_for_status()
            return {}, r
        return js, r

    js, r = do_call(STATE["ts_style"])
    if isinstance(js, dict) and js.get("code") == 20002 and "APEX-TIMESTAMP" in str(js.get("msg", "")):
        # flip and retry once
        STATE["ts_style"] = "iso" if STATE["ts_style"] == "ms" else "ms"
        LOG.warning("Timestamp rejected; flipping TS_STYLE to %s and retrying", STATE["ts_style"])
        js, r = do_call(STATE["ts_style"])

    if r is not None:
        r.raise_for_status()
    return js if isinstance(js, dict) else {}

def get_account() -> Dict[str, Any]:
    path = "/v3/account"
    if ACCOUNT_ID:
        connector = "&" if "?" in path else "?"
        path = f"{path}{connector}accountId={ACCOUNT_ID}"
    return request_json(path)

# ------------------------- POSITION EXTRACTION -------------------------

def extract_positions(acct: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Look through common Omni layouts and return (where_found, list_of_positions).
    """
    if not isinstance(acct, dict):
        return "<bad-account>", []

    candidates: List[Tuple[str, Any]] = [
        ("positions", acct.get("positions")),
        ("contractAccount.positions", (acct.get("contractAccount") or {}).get("positions")),
        ("data.positions", (acct.get("data") or {}).get("positions")),
        ("data.contractAccount.positions",
         ((acct.get("data") or {}).get("contractAccount") or {}).get("positions")),
        ("account.contractAccount.positions",
         ((acct.get("account") or {}).get("contractAccount") or {}).get("positions")),
        ("result.positions", (acct.get("result") or {}).get("positions")),
    ]

    for name, arr in candidates:
        if isinstance(arr, list) and len(arr) > 0:
            return name, arr

    # Fallback: deep scan for arrays of dicts that look like positions
    def looks_like_position(d: Dict[str, Any]) -> bool:
        keys = set(d.keys())
        return {"symbol", "size", "entryPrice"} <= keys or \
               (("symbol" in keys or "market" in keys) and ("size" in keys))

    where = "<scan>"
    found: List[Dict[str, Any]] = []

    def walk(x: Any):
        nonlocal found
        if isinstance(x, list):
            if x and isinstance(x[0], dict) and looks_like_position(x[0]):
                found.extend(x)
            else:
                for v in x:
                    walk(v)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)

    walk(acct)
    if found:
        return where, found

    return "<none>", []

# ------------------------- TABLE FORMAT -------------------------

def build_table(positions: List[Dict[str, Any]]) -> str:
    title = "Active ApeX Positions"
    header = [
        ("SYMBOL", 12),
        ("SIDE",    6),
        ("SIZE",   10),
        ("ENTRY",  12),
        ("MARK",   12),
        ("PNL",    12),
        ("PNL%",    7),
    ]

    def row(cols):
        return "".join(str(val).ljust(width) for val, width in cols)

    lines: List[str] = [title, "```", row(header),
                        row([("-" * len(h), w) for h, w in header])]

    for p in positions:
        sym  = p.get("symbol") or p.get("market") or p.get("pair") or "-"
        side = (p.get("side") or p.get("positionSide") or "-").upper()
        size = fmt_num(p.get("size") or p.get("positionSize") or p.get("qty"), SIZE_DEC)

        entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("averageEntryPrice")
        entry = fmt_num(entry, PRICE_DEC) if entry is not None else "-"

        # Optional; many Omni tenants do NOT include these
        mark = p.get("markPrice") or p.get("indexPrice") or p.get("currentPrice")
        mark = fmt_num(mark, PRICE_DEC) if mark is not None else "-"

        pnl = p.get("unrealizedPnl") or p.get("uPnl") or p.get("pnl") or p.get("unRealizedPnl")
        pnl = fmt_num(pnl, PNL_DEC) if pnl is not None else "-"

        pnl_pct = p.get("unrealizedPnlPct") or p.get("unrealizedPnlPercent") or p.get("pnlPct")
        if pnl_pct is not None:
            try:
                pnl_pct = f"{float(pnl_pct):.{PNL_PCT_DEC}f}"
            except Exception:
                pnl_pct = "-"
        else:
            pnl_pct = "-"

        lines.append(row([
            (sym, 12),
            (side, 6),
            (size, 10),
            (entry, 12),
            (mark, 12),
            (pnl, 12),
            (pnl_pct, 7),
        ]))

    if not positions:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def snapshot_signature(positions: List[Dict[str, Any]]) -> str:
    """
    Create a small signature of what we display to avoid repost spam.
    Includes only the forwarded fields we render.
    """
    flat = []
    for p in positions:
        flat.append([
            p.get("symbol") or p.get("market") or p.get("pair") or "",
            (p.get("side") or p.get("positionSide") or "").upper(),
            str(p.get("size") or p.get("positionSize") or p.get("qty") or ""),
            str(p.get("entryPrice") or p.get("avgEntryPrice") or p.get("averageEntryPrice") or ""),
            str(p.get("markPrice") or p.get("indexPrice") or p.get("currentPrice") or ""),
            str(p.get("unrealizedPnl") or p.get("uPnl") or p.get("pnl") or p.get("unRealizedPnl") or ""),
            str(p.get("unrealizedPnlPct") or p.get("unrealizedPnlPercent") or p.get("pnlPct") or ""),
        ])
    payload = json.dumps(flat, separators=(",", ":"))
    return base64.b64encode(hashlib.sha1(payload.encode("utf-8")).digest()).decode("utf-8")

# ------------------------- MAIN LOOP -------------------------

def main():
    if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
        LOG.error("Missing envs: APEX_API_KEY, APEX_API_SECRET, APEX_API_PASSPHRASE, DISCORD_WEBHOOK")
        return

    LOG.info("Forward-only bridge online. Base=%s  TS_STYLE=%s  secret_mode=%s",
             BASE_URL, STATE['ts_style'], SECRET_MODE)

    last_sig = ""
    posted_once = False

    while True:
        try:
            acct = get_account()
            where, raw = extract_positions(acct)

            if DEBUG_ON:
                discord_post(f"debug: found {len(raw)} positions at {where}")

            # Apply min-size filter
            positions = []
            for p in raw:
                sz = safe_float(p.get("size") or p.get("positionSize") or p.get("qty"))
                if sz >= MIN_POS_SIZE:
                    positions.append(p)

            if DEBUG_ON:
                discord_post(f"debug: after min_size={MIN_POS_SIZE}: {len(positions)} remain")

            table = build_table(positions)
            sig = snapshot_signature(positions)

            should_post = (UPDATE_MODE == "always") or (not posted_once) or (sig != last_sig)

            if should_post:
                discord_post(table)
                last_sig = sig
                posted_once = True
                LOG.info("Posted %d positions", len(positions))
            else:
                LOG.info("No position change")

        except requests.HTTPError as e:
            body = ""
            try:
                body = e.response.text[:200]
            except Exception:
                pass
            LOG.error("HTTP error: %s %s", getattr(e.response, "status_code", "?"), body)
        except Exception as e:
            LOG.error("Poll error: %s", e)

        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    main()
