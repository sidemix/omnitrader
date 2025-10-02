# main.py — ApeX Omni → Discord bridge
# v1.6: account-derived MARK/PNL (no public market dependency), robust field discovery,
#       update-policy, ASCII-safe logs. Public mark lookups kept as best-effort but ignored if 404.

import os, sys, time, hmac, json, base64, hashlib, logging
from urllib.parse import urlencode
import requests
from requests.adapters import HTTPAdapter, Retry

# ---------- logging (ASCII only) ----------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("apex-bridge")
def ascii_only(s: str) -> str:
    try: return s.encode("ascii", "ignore").decode("ascii")
    except Exception: return s

# ---------- env ----------
API_KEY        = os.getenv("APEX_API_KEY")        or os.getenv("APEX_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET")     or os.getenv("APEX_SECRET", "")
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK= os.getenv("DISCORD_WEBHOOK", "")

BASE_URL = (os.getenv("OMNI_BASE_URL") or os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "30"))

SCAN_SYMBOLS = [s.strip().upper() for s in os.getenv("SCAN_SYMBOLS", "").split(",") if s.strip()]
MIN_SIZE  = float(os.getenv("APEX_MIN_POSITION_SIZE", "1e-12"))
LAT_MS    = int(os.getenv("APEX_LATENCY_CUSHION_MS", "600"))

SIZE_DECIMALS     = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS    = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS      = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DECIMALS  = int(os.getenv("PNL_PCT_DECIMALS", "2"))
UPDATE_MODE       = os.getenv("UPDATE_MODE", "positions_only")  # openclose_only | positions_only | any_change
SIZE_CHANGE_THRESHOLD = float(os.getenv("SIZE_CHANGE_THRESHOLD", "0"))

ACCOUNT_DEBUG = os.getenv("ACCOUNT_DEBUG", "0") == "1"
MARK_DEBUG    = os.getenv("MARK_DEBUG", "0") == "1"  # kept; won’t matter if host has no market endpoints

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing envs: APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET, APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------- HTTP session ----------
session = requests.Session()
retries = Retry(total=6, connect=6, read=6, backoff_factor=0.6, status_forcelist=(429,500,502,503,504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://",  HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": "apex-omni-discord-bridge/1.6"})

# ---------- time & signing (no datetime) ----------
def get_server_time_seconds() -> int:
    r = session.get(f"{BASE_URL}/v3/time", timeout=10); r.raise_for_status()
    js = {}
    try: js = r.json()
    except Exception: pass
    sources = []
    if isinstance(js, dict):
        sources.append(js)
        if isinstance(js.get("data"), dict):
            sources.append(js["data"])
    for src in sources:
        for k in ("time","serverTime","server_timestamp","serverTs"):
            v = src.get(k)
            if isinstance(v, (int,float)):
                return int(v if v < 1e12 else v/1000.0)
    try: return int(js)
    except Exception: raise RuntimeError(f"Unexpected /v3/time response: {js}")

def iso_from_seconds(sec: int) -> str:
    tm = time.gmtime(sec)
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}.000Z"

def sign_message(ts_str: str, method: str, signed_path: str, body: dict|None) -> str:
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

def private_request(method: str, path: str, params: dict|None=None, data: dict|None=None) -> requests.Response:
    assert path.startswith("/v3/")
    url = f"{BASE_URL}{path}"
    query = ""
    if method.upper() == "GET" and params:
        query = "?" + urlencode(sorted(params.items()), doseq=True); url += query

    sec = get_server_time_seconds()
    attempts = [("iso", iso_from_seconds(sec)),
                ("ms",  str(int(sec*1000 + LAT_MS))),
                ("s",   str(int(sec)))]

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
            resp = session.get(url, headers=headers, timeout=20) if method.upper()=="GET" else \
                   session.post(url, headers=headers, data=data or {}, timeout=20)

            body = {}
            if resp.headers.get("Content-Type","").startswith("application/json"):
                try: body = resp.json()
                except Exception: body = {}
                if isinstance(body, dict) and body.get("code") not in (None, 0):
                    code, msg = body.get("code"), str(body.get("msg"))
                    if code in (20002, 20009) or ("timestamp" in msg.lower()):
                        # try next timestamp format
                        continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    if last_err: raise last_err
    raise RuntimeError("Signing failed with all timestamp formats")

# ---------- Discord ----------
def post_discord(text: str):
    if not DISCORD_WEBHOOK: return
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": text[:1990]}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        LOG.error(ascii_only(f"Discord post failed: {e}"))

# ---------- helpers ----------
def _first(d: dict, *keys):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None
def _to_float(x):
    if x is None: return None
    try: return float(str(x).replace(",", ""))
    except Exception: return None
def _norm_side(side_val, size_val) -> str:
    s = (str(side_val or "")).upper()
    if s in ("LONG","SHORT"): return s
    try: return "LONG" if float(size_val) >= 0 else "SHORT"
    except Exception: return "LONG"
def _fmt_fixed(x, dec) -> str:
    try: return f"{float(x):.{dec}f}"
    except Exception: return "-"
def _fmt_pct(x, dec) -> str:
    try: return f"{float(x):.{dec}f}%"
    except Exception: return "-"

# ---------- optional: best-effort public mark (ignored if 404) ----------
def best_effort_public_marks(_symbols: list[str]) -> dict[str, float]:
    # On your host these endpoints 404; keep a tiny stub so we don't block.
    return {}

# ---------- account fetch & normalize ----------
def fetch_account() -> dict:
    r = private_request("GET", "/v3/account")
    try: return r.json()
    except Exception: return {}

def _derive_mark_from_account(p: dict) -> float|None:
    """Try to build mark using account fields only."""
    size  = _to_float(_first(p, "size","positionSize","position","quantity","qty","amount"))
    entry = _to_float(_first(p, "entryPrice","avgEntryPrice","avgPrice","entry_price","openPrice"))
    side  = _norm_side(_first(p, "side","positionSide"), size)

    # 1) value/notional -> mark = value/size
    value = _to_float(_first(p, "positionValue","notional","marketValue","value","positionNotional"))
    if value is not None and size not in (None, 0):
        return value/size

    # 2) pnl present -> mark = entry + sign*pnl/size
    pnl = _to_float(_first(p, "unrealizedPnl","upnl","pnl","unrealizedPnL","unrealisedPnl","uPnl"))
    if pnl is not None and entry not in (None, 0) and size not in (None, 0):
        sign = 1.0 if side == "LONG" else -1.0
        return entry + (pnl / size) * sign

    # 3) pnl% present -> mark = entry * (1 + sign * pct/100)
    pnl_pct = _to_float(_first(p, "unrealizedPnlPercent","unrealizedPnlRate","unrealizedPnlRatio","uPnlRate","returnRate"))
    if pnl_pct is not None and entry not in (None, 0):
        sign = 1.0 if side == "LONG" else -1.0
        return entry * (1.0 + sign * pnl_pct/100.0)

    # 4) nothing to do
    return None

def extract_positions(js: dict) -> list[dict]:
    raw = []
    if not isinstance(js, dict): return raw

    def take_list(dct: dict, key: str):
        if isinstance(dct.get(key), list):
            raw.extend(dct[key])

    # top-level
    take_list(js, "positions"); take_list(js, "openPositions")
    ca = js.get("contractAccount") or {}
    if isinstance(ca, dict):
        take_list(ca, "positions"); take_list(ca, "openPositions")

    # under data
    data = js.get("data")
    if isinstance(data, dict):
        take_list(data, "positions"); take_list(data, "openPositions")
        dca = data.get("contractAccount") or {}
        if isinstance(dca, dict):
            take_list(dca, "positions"); take_list(dca, "openPositions")

    if ACCOUNT_DEBUG:
        # sample first raw payload for inspection (trimmed)
        try:
            sample = json.dumps(raw[0], ensure_ascii=True)[:1200] if raw else "{}"
            post_discord("```Account debug\nfirst_position=" + sample + "\n```")
        except Exception:
            pass

    out = []
    for p in raw:
        try:
            symbol = str(_first(p, "symbol","contractSymbol","market") or "").upper()
            if not symbol: continue
            if SCAN_SYMBOLS and symbol not in SCAN_SYMBOLS: continue

            size   = _to_float(_first(p, "size","positionSize","position","quantity","qty","amount"))
            if size is None or abs(size) < MIN_SIZE: continue

            side   = _norm_side(_first(p, "side","positionSide"), size)
            entry  = _to_float(_first(p, "entryPrice","avgEntryPrice","avgPrice","entry_price","openPrice"))
            mark   = _to_float(_first(p, "markPrice","mark","lastPrice","marketPrice","mark_price"))
            pnl    = _to_float(_first(p, "unrealizedPnl","upnl","pnl","unrealizedPnL","unrealisedPnl","uPnl"))
            pnlpct = _to_float(_first(p, "unrealizedPnlPercent","unrealizedPnlRate","unrealizedPnlRatio","uPnlRate","returnRate"))

            # derive mark if missing
            if mark is None:
                mark = _derive_mark_from_account(p)

            # compute pnl/pnl% if missing
            sign = 1.0 if side == "LONG" else -1.0
            if pnl is None and entry not in (None, 0) and mark is not None:
                pnl = (mark - entry) * size * sign
            if pnlpct is None and entry not in (None, 0) and mark is not None:
                pnlpct = ((mark - entry) / entry) * 100.0 * sign

            out.append({
                "symbol": symbol,
                "side": side,
                "size": float(size),
                "entry": entry if entry is not None else None,
                "mark":  mark if mark  is not None else None,
                "pnl":   pnl  if pnl   is not None else None,
                "pnl_pct": pnlpct if pnlpct is not None else None,
            })
        except Exception:
            continue

    # Fallback: best-effort public marks (kept for other deployments; on yours it's 404)
    need = [p["symbol"] for p in out if p.get("mark") is None]
    if need:
        marks = best_effort_public_marks(need)
        for p in out:
            if p["mark"] is None and p["symbol"] in marks:
                p["mark"] = marks[p["symbol"]]
                # recalc pnl/pnl% if still missing
                sign = 1.0 if p["side"] == "LONG" else -1.0
                if p.get("pnl") is None and p.get("entry") not in (None, 0):
                    p["pnl"] = (p["mark"] - p["entry"]) * p["size"] * sign
                if p.get("pnl_pct") is None and p.get("entry") not in (None, 0):
                    p["pnl_pct"] = ((p["mark"] - p["entry"]) / p["entry"]) * 100.0 * sign

    # dedupe by symbol/side/size/entry
    uniq = {}
    for q in out:
        key = (q["symbol"], q["side"], f"{q['size']:.12g}", f"{q['entry'] if q['entry'] is not None else 'None'}")
        uniq[key] = q
    out = list(uniq.values())
    out.sort(key=lambda x: (x["symbol"], x["side"] != "LONG"))
    return out

# ---------- formatting ----------
def format_positions(pos: list[dict]) -> str:
    if not pos: return "No open positions"
    header = "SYMBOL         SIDE   SIZE         ENTRY         MARK          PNL        PNL%"
    sep    = "-------------  -----  -----------  ------------  ------------  ----------  -----"
    lines = [header, sep]
    for p in pos:
        size_s = _fmt_fixed(p["size"],  SIZE_DECIMALS)
        entry_s= _fmt_fixed(p["entry"], PRICE_DECIMALS) if p["entry"] is not None else "-"
        mark_s = _fmt_fixed(p["mark"],  PRICE_DECIMALS) if p["mark"]  is not None else "-"
        pnl_s  = _fmt_fixed(p["pnl"],   PNL_DECIMALS)   if p["pnl"]   is not None else "-"
        pnlp_s = _fmt_pct  (p["pnl_pct"], PNL_PCT_DECIMALS) if p["pnl_pct"] is not None else "-"
        lines.append(f"{p['symbol']:<13}  {p['side']:<5}  {size_s:>11}  {entry_s:>12}  {mark_s:>12}  {pnl_s:>10}  {pnlp_s:>5}")
    return "```\n" + "\n".join(lines) + "\n```"

# ---------- update policy ----------
def _quantize_size(x: float) -> float:
    if SIZE_CHANGE_THRESHOLD > 0:
        steps = round(x / SIZE_CHANGE_THRESHOLD)
        return steps * SIZE_CHANGE_THRESHOLD
    try: return round(float(x), SIZE_DECIMALS)
    except Exception: return x

def snapshot_of(pos: list[dict]) -> str:
    try:
        if UPDATE_MODE == "openclose_only":
            keys = sorted({(p["symbol"], p["side"]) for p in pos})
        elif UPDATE_MODE == "any_change":
            keys = sorted(
                (p["symbol"], p["side"], _quantize_size(p["size"]),
                 None if p.get("entry") is None else round(p["entry"], PRICE_DECIMALS),
                 None if p.get("mark")  is None else round(p["mark"],  PRICE_DECIMALS),
                 None if p.get("pnl")   is None else round(p["pnl"],   PNL_DECIMALS),
                 None if p.get("pnl_pct") is None else round(p["pnl_pct"], PNL_PCT_DECIMALS))
                for p in pos
            )
        else:  # positions_only
            keys = sorted(
                (p["symbol"], p["side"], _quantize_size(p["size"]),
                 None if p.get("entry") is None else round(p["entry"], PRICE_DECIMALS))
                for p in pos
            )
        return json.dumps(keys)
    except Exception:
        return ""

# ---------- loop ----------
_last_snapshot = None
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
    while True:
        try:
            poll_once()
        except Exception as e:
            LOG.error(ascii_only(f"Poll error: {e}"))
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
