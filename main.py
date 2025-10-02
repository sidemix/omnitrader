# main.py — Omni -> Discord with external mark price + PnL
import os, time, hmac, hashlib, base64, json, logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import requests

# ---------- Env ----------
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
BASE_URL         = os.environ.get("APEX_BASE_URL", "https://omni.apex.exchange/api").rstrip("/")
API_KEY          = os.environ.get("APEX_API_KEY", os.environ.get("APEX_KEY","")).strip()
API_SECRET       = os.environ.get("APEX_API_SECRET", os.environ.get("APEX_SECRET","")).strip()
API_PASSPHRASE   = os.environ.get("APEX_API_PASSPHRASE", os.environ.get("APEX_PASSPHRASE","")).strip()

INTERVAL_SECS    = int(os.environ.get("INTERVAL_SECONDS", "20"))
POST_EMPTY_EVERY = int(os.environ.get("POST_EMPTY_EVERY_SECS", "0"))

PRICE_SOURCES    = [s.strip().lower() for s in os.environ.get("PRICE_SOURCES", "binance,gate,gecko").split(",") if s.strip()]
BINANCE_MAP      = os.environ.get("BINANCE_MAP", "")
GATE_MAP         = os.environ.get("GATE_MAP", "")
GECKO_MAP        = os.environ.get("GECKO_MAP", "")

PRICE_DECIMALS   = int(os.environ.get("PRICE_DECIMALS", "4"))
SIZE_DECIMALS    = int(os.environ.get("SIZE_DECIMALS", "4"))
PNL_DECIMALS     = int(os.environ.get("PNL_DECIMALS", "4"))
PNL_PCT_DECIMALS = int(os.environ.get("PNL_PCT_DECIMALS", "2"))

UPDATE_MODE      = os.environ.get("UPDATE_MODE", "any_change")  # any_change | positions_only

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

session = requests.Session()
session.headers.update({"Accept": "application/json"})

# ---------- Helpers ----------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _b64_hmac_sha256(secret: str, msg: str) -> str:
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _sign_v3(secret: str, ts_ms: int, method: str, path: str, query: str, body: str) -> str:
    # ApeX v3: timestamp + method + path(+query) + body
    request_path = path + (f"?{query}" if query else "")
    raw = f"{ts_ms}{method.upper()}{request_path}{body}"
    return _b64_hmac_sha256(secret, raw)

def headers_v3(method: str, path: str, query: str = "", body: str = "") -> Dict[str, str]:
    ts_ms = _now_ms()
    sign = _sign_v3(API_SECRET, ts_ms, method, path, query, body)
    return {
        "Content-Type": "application/json",
        "APEX-KEY": API_KEY,
        "APEX-PASSPHRASE": API_PASSPHRASE,
        "APEX-TIMESTAMP": str(ts_ms),
        "APEX-SIGN": sign,
    }

def post_to_discord(text: str):
    if not DISCORD_WEBHOOK:
        logging.warning("No DISCORD_WEBHOOK set")
        return
    try:
        session.post(DISCORD_WEBHOOK, json={"content": text}, timeout=10)
    except Exception as e:
        logging.warning(f"Discord post failed: {e}")

def chunks(lines: List[str], n: int = 40) -> List[List[str]]:
    return [lines[i:i+n] for i in range(0, len(lines), n)]

def parse_map(env_val: str, sep: str = ";") -> Dict[str, str]:
    out = {}
    for item in filter(None, [p.strip() for p in env_val.replace(",", sep).split(sep)]):
        if "=" in item:
            k, v = item.split("=", 1)
            out[k.strip()] = v.strip()
    return out

BINANCE_SYM_MAP = parse_map(BINANCE_MAP)
GATE_SYM_MAP    = parse_map(GATE_MAP)
GECKO_ID_MAP    = parse_map(GECKO_MAP)

# ---------- Positions ----------
def get_account() -> Dict:
    # v3 account (private)
    path = "/v3/account"
    url  = f"{BASE_URL}{path}"
    hdrs = headers_v3("GET", path)
    r = session.get(url, headers=hdrs, timeout=10)
    r.raise_for_status()
    return r.json()

def extract_positions(obj) -> List[Dict]:
    """
    Walk the account payload and collect any array of dicts
    that look like positions: must have symbol, size, entryPrice
    """
    found: List[Dict] = []

    def walk(x):
        if isinstance(x, dict):
            # looks like a position item
            if {"symbol", "size", "entryPrice"} <= set(x.keys()):
                found.append(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    # filter size>0
    return [p for p in found if safe_float(p.get("size")) > 0]

def safe_float(x) -> float:
    try:
        return float(x)
    except:
        return 0.0

# ---------- Price sources ----------
def fetch_binance(symbols: List[str]) -> Dict[str, float]:
    out = {}
    for sym in symbols:
        q = BINANCE_SYM_MAP.get(sym, sym.replace("-", ""))
        try:
            r = session.get("https://api.binance.com/api/v3/ticker/price",
                            params={"symbol": q}, timeout=6)
            if r.status_code == 200:
                price = safe_float(r.json().get("price"))
                if price > 0:
                    out[sym] = price
        except Exception:
            pass
    return out

def fetch_gate(symbols: List[str]) -> Dict[str, float]:
    out = {}
    for sym in symbols:
        q = GATE_SYM_MAP.get(sym, sym.replace("-", "_"))
        try:
            r = session.get("https://api.gateio.ws/api/v4/spot/tickers",
                            params={"currency_pair": q}, timeout=6)
            if r.status_code == 200:
                arr = r.json()
                if isinstance(arr, list) and arr:
                    last = safe_float(arr[0].get("last"))
                    if last > 0:
                        out[sym] = last
        except Exception:
            pass
    return out

def fetch_gecko(symbols: List[str]) -> Dict[str, float]:
    # build id list from GECKO_MAP
    ids = [GECKO_ID_MAP.get(s) for s in symbols if GECKO_ID_MAP.get(s)]
    if not ids:
        return {}
    try:
        r = session.get("https://api.coingecko.com/api/v3/simple/price",
                        params={"ids": ",".join(ids), "vs_currencies": "usdt"}, timeout=8)
        if r.status_code != 200:
            return {}
        data = r.json()
        rev = {v: k for k, v in GECKO_ID_MAP.items()}
        out = {}
        for gecko_id, obj in data.items():
            usdt = safe_float(obj.get("usdt"))
            if usdt > 0 and gecko_id in rev:
                out[rev[gecko_id]] = usdt
        return out
    except Exception:
        return {}

def get_marks(symbols: List[str]) -> Dict[str, float]:
    remaining = set(symbols)
    marks: Dict[str, float] = {}

    for source in PRICE_SOURCES:
        if not remaining:
            break
        try:
            if source == "binance":
                got = fetch_binance(list(remaining))
            elif source == "gate":
                got = fetch_gate(list(remaining))
            elif source == "gecko":
                got = fetch_gecko(list(remaining))
            else:
                got = {}
        except Exception:
            got = {}
        for k, v in got.items():
            if k in remaining and v > 0:
                marks[k] = v
        remaining -= set(got.keys())

    if remaining:
        logging.info(f"Missing marks for: {', '.join(sorted(remaining))}")
    return marks

# ---------- Formatting ----------
def fmt(x: Optional[float], dec: int) -> str:
    if x is None:
        return "-"
    return f"{x:.{dec}f}"

def build_table(positions: List[Dict], marks: Dict[str, float]) -> str:
    # headers
    rows = []
    rows.append("**Active ApeX Positions**")
    header = f"{'SYMBOL':<10} {'SIDE':<6} {'SIZE':>8} {'ENTRY':>12} {'MARK':>12} {'PNL':>12} {'PNL%':>8}"
    sep    = f"{'-'*10} {'-'*6} {'-'*8} {'-'*12} {'-'*12} {'-'*12} {'-'*8}"
    rows.extend([f"```", header, sep])

    for p in positions:
        sym   = p.get("symbol")
        side  = (p.get("side") or "").upper()
        size  = safe_float(p.get("size"))
        entry = safe_float(p.get("entryPrice"))
        mark  = marks.get(sym)
        pnl = pnl_pct = None
        if mark:
            sgn = 1.0 if side == "LONG" else -1.0
            pnl = sgn * (mark - entry) * size
            pnl_pct = sgn * ((mark - entry) / entry * 100.0) if entry else None

        rows.append(
            f"{sym:<10} "
            f"{(side or '-'): <6} "
            f"{fmt(size, SIZE_DECIMALS):>8} "
            f"{fmt(entry, PRICE_DECIMALS):>12} "
            f"{fmt(mark, PRICE_DECIMALS):>12} "
            f"{fmt(pnl, PNL_DECIMALS):>12} "
            f"{(fmt(pnl_pct, PNL_PCT_DECIMALS) if pnl_pct is not None else '-'):>8}"
        )

    rows.append("```")
    return "\n".join(rows)

# ---------- Main loop ----------
def main():
    last_payload = None
    last_post_empty = 0

    while True:
        try:
            acct = get_account()
            positions = extract_positions(acct)

            # symbols we need marks for
            symbols = sorted({p.get("symbol") for p in positions if safe_float(p.get("size")) > 0})
            marks = get_marks(symbols) if symbols else {}

            table = build_table(positions, marks)

            should_post = False
            if UPDATE_MODE == "positions_only":
                shape = [(p.get("symbol"), p.get("side"), p.get("size"), p.get("entryPrice")) for p in positions]
                if shape != last_payload:
                    should_post = True
                    last_payload = shape
            else:
                if table != last_payload:
                    should_post = True
                    last_payload = table

            if positions or (POST_EMPTY_EVERY > 0 and time.time() - last_post_empty >= POST_EMPTY_EVERY):
                if should_post:
                    post_to_discord(table)
                if not positions:
                    last_post_empty = time.time()

        except requests.HTTPError as e:
            logging.error(f"HTTP error: {e.response.status_code} {e.response.text[:200]}")
        except Exception as e:
            logging.error(f"Poll error: {e}")

        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    logging.info(f"Starting (BASE_URL={BASE_URL}) | price sources={PRICE_SOURCES}")
    main()
