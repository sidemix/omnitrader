# main.py — ApeX Omni → Discord worker (v1.1: mark & pnl columns, rounded, no diagnostics post)
#
# Env (accepts both legacy and new names):
#   APEX_API_KEY        or APEX_KEY
#   APEX_API_SECRET     or APEX_SECRET
#   APEX_API_PASSPHRASE or APEX_PASSPHRASE
#   DISCORD_WEBHOOK   (required)
#   OMNI_BASE_URL or APEX_BASE_URL (default: https://omni.apex.exchange/api)
#   INTERVAL_SECONDS (default 30)
#   SCAN_SYMBOLS (optional, comma-separated)
#   APEX_MIN_POSITION_SIZE (default 1e-12)
#   APEX_LATENCY_CUSHION_MS (default 600)
#   SIZE_DECIMALS / PRICE_DECIMALS / PNL_DECIMALS / PNL_PCT_DECIMALS
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

# ---------- logging (ASCII) ----------
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
API_KEY        = os.getenv("APEX_API_KEY")        or os.getenv("APEX_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET")     or os.getenv("APEX_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "")
BASE_URL = (os.getenv("OMNI_BASE_URL") or os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "30"))
SCAN_SYMBOLS = [s.strip().upper() for s in os.getenv("SCAN_SYMBOLS", "").split(",") if s.strip()]
LATENCY_CUSHION_MS = int(os.getenv("APEX_LATENCY_CUSHION_MS", "600"))
MIN_SIZE = float(os.getenv("APEX_MIN_POSITION_SIZE", "1e-12"))

SIZE_DECIMALS = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DECIMALS = int(os.getenv("PNL_PCT_DECIMALS", "2"))

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing envs: APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET, APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------- http ----------
session = requests.Session()
retries = Retry(total=6, connect=6, read=6, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "apex-omni-discord-bridge/1.1"})

# ---------- time & signing (no datetime) ----------
def get_server_time_seconds() -> int:
    url = f"{BASE_URL}/v3/time"
    r = session.get(url, timeout=10)
    r.raise_for_status()
    js = {}
    try: js = r.json()
    except Exception: pass

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
    tm = time.gmtime(sec)
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}.000Z"

def sign_message(ts_str: str, method: str, signed_path: str, body: dict | None) -> str:
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

            body = {}
            if resp.headers.get("Content-Type", "").startswith("application/json"):
                try: body = resp.json()
                except Exception: body = {}
                if isinstance(body, dict) and body.get("code") not in (None, 0):
                    code, msg = body.get("code"), str(body.get("msg"))
                    if code in (20002, 20009) or ("timestamp" in msg.lower()):
                        LOG.warning(ascii_only(f"Timestamp rejected on {path} with ts={label}: code={code} msg={msg}"))
                        continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            LOG.warning(ascii_only(f"Retrying {path} with next timestamp format ({label}) due to: {e}"))
            time.sleep(0.25)

    if last_err: raise last_err
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

# ---------- utils ----------
def _first(d: dict, *keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _to_float(x):
    if x is None:
        return None
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _norm_side(side_val, size_val) -> str:
    s = (str(side_val or "")).upper()
    if s in ("LONG", "SHORT"):
        return s
    try:
        return "LONG" if float(size_val) >= 0 else "SHORT"
    except Exception:
        return "LONG"

def _fmt_fixed(x, dec) -> str:
    try:
        return f"{float(x):.{dec}f}"
    except Exception:
        return "-"

def _fmt_pct(x, dec) -> str:
    try:
        return f"{float(x):.{dec}f}%"
    except Exception:
        return "-"

# ---------- data ----------
def fetch_account() -> dict:
    r = private_request("GET", "/v3/account")
    try:
        return r.json()
    except Exception:
        return {}

def extract_positions(js: dict) -> list[dict]:
    """
    Normalize positions:
      {symbol:str, side:'LONG'|'SHORT', size:float, entry:float|None, mark:float|None, pnl:float|None, pnl_pct:float|None}
    Filters out abs(size) < MIN_SIZE and applies SCAN_SYMBOLS if set.
    """
    raw = []
    if not isinstance(js, dict):
        return raw

    def take_list(dct: dict, key: str):
        if isinstance(dct.get(key), list):
            raw.extend(dct[key])

    # top-level
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

    out = []
    for p in raw:
        try:
            symbol = str(_first(p, "symbol", "contractSymbol", "market") or "").upper()
            if not symbol:
                continue
            if SCAN_SYMBOLS and symbol not in SCAN_SYMBOLS:
                continue

            size = _to_float(_first(p, "size", "positionSize", "position", "quantity", "qty"))
            if size is None or abs(size) < MIN_SIZE:
                continue

            side = _norm_side(_first(p, "side", "positionSide"), size)
            entry = _to_float(_first(p, "entryPrice", "avgEntryPrice", "avgPrice", "entry_price"))
            mark  = _to_float(_first(p, "markPrice", "mark", "lastPrice", "mark_price"))

            pnl = _to_float(_first(p, "unrealizedPnl", "upnl", "pnl", "unrealizedPnL"))
            pnl_pct = _to_float(_first(p, "unrealizedPnlPercent", "unrealizedPnlRate", "unrealizedPnlRatio"))

            # compute if missing and we have basics
            sign = 1.0 if side == "LONG" else -1.0
            if pnl is None and (entry is not None) and (mark is not None):
                pnl = (mark - entry) * size * sign
            if pnl_pct is None and (entry is not None) and entry != 0 and (mark is not None):
                pnl_pct = ((mark - entry) / entry) * 100.0 * sign

            out.append({
                "symbol": symbol, "side": side,
                "size": float(size),
                "entry": entry if entry is not None else None,
                "mark": mark if mark is not None else None,
                "pnl": pnl if pnl is not None else None,
                "pnl_pct": pnl_pct if pnl_pct is not None else None,
            })
        except Exception:
            continue

    # dedupe by core identity (avoid spamming on mark/pnl changes)
    uniq = {}
    for q in out:
        key = (q["symbol"], q["side"], f"{q['size']:.12g}", f"{(q['entry'] if q['entry'] is not None else 'None')}")
        uniq[key] = q
    out = list(uniq.values())
    out.sort(key=lambda x: (x["symbol"], x["side"] != "LONG"))
    return out

# ---------- formatting ----------
def format_positions(pos: list[dict]) -> str:
    if not pos:
        return "No open positions"
    header = "SYMBOL         SIDE   SIZE         ENTRY         MARK          PNL        PNL%"
    sep    = "-------------  -----  -----------  ------------  ------------  ----------  -----"
    lines = [header, sep]
    for p in pos:
        size_s = _fmt_fixed(p["size"], SIZE_DECIMALS)
        entry_s = _fmt_fixed(p["entry"], PRICE_DECIMALS) if p["entry"] is not None else "-"
        mark_s  = _fmt_fixed(p["mark"],  PRICE_DECIMALS) if p["mark"]  is not None else "-"
        pnl_s   = _fmt_fixed(p["pnl"],   PNL_DECIMALS)   if p["pnl"]   is not None else "-"
        pnlp_s  = _fmt_pct  (p["pnl_pct"], PNL_PCT_DECIMALS) if p["pnl_pct"] is not None else "-"
        lines.append(
            f"{p['symbol']:<13}  {p['side']:<5}  {size_s:>11}  {entry_s:>12}  {mark_s:>12}  {pnl_s:>10}  {pnlp_s:>5}"
        )
    return "```\n" + "\n".join(lines) + "\n```"

# ---------- loop state ----------
_last_snapshot = None

def snapshot_of(pos: list[dict]) -> str:
    try:
        key_tuples = [(p["symbol"], p["side"], f"{p['size']:.10g}", f"{p['entry'] if p['entry'] is not None else 'None'}") for p in pos]
        key_tuples.sort()
        return json.dumps(key_tuples)
    except Exception:
        return ""

# ---------- flow ----------
def poll_once():
    global _last_snapshot
    acct = fetch_account()
    positions = extract_positions(acct)
    snap = snapshot_of(positions)
    if snap != _last_snapshot:
        _last_snapshot = snap
        post_discord("Active ApeX Positions\n" + format_positions(positions))
        LOG.info(ascii_only(f"Posted {len(positions)} active positions"))
    else:
        LOG.info(ascii_only("No position change"))

def main():
    LOG.info(ascii_only(f"Starting worker. BASE_URL={BASE_URL}"))
    # no diagnostics Discord post by request
    while True:
        try:
            poll_once()
        except Exception as e:
            LOG.error(ascii_only(f"Poll error: {e}"))
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
