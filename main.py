# main.py — ApeX Omni → Discord (forward-only)
# - Polls /v3/account and posts open positions to Discord.
# - Shows only what the API returns. If Mark/PnL aren’t in the payload,
#   those cells are "–" (no external price calls).
# - Robust to timestamp format and secret format (auto-detect Base64 vs raw).
# - Looks for positions in contractAccount.positions, data.positions, etc.

import os, time, json, hmac, base64, hashlib, logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
import requests
from urllib.parse import urlencode
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ========= Config & ENV =========
API_KEY        = os.getenv("APEX_API_KEY") or os.getenv("APEX_KEY", "")
API_SECRET     = os.getenv("APEX_API_SECRET") or os.getenv("APEX_SECRET", "")
PASSPHRASE     = os.getenv("APEX_API_PASSPHRASE") or os.getenv("APEX_PASSPHRASE", "")
DISCORD_WEBHOOK= os.getenv("DISCORD_WEBHOOK", "")

BASE_URL       = (os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
POLL_SECS      = int(os.getenv("INTERVAL_SECONDS", "30"))
UPDATE_MODE    = (os.getenv("UPDATE_MODE") or "on-change").lower()  # "on-change" | "always"
TS_STYLE       = (os.getenv("APEX_TS_STYLE") or "ms").lower()       # "iso" | "ms" (will auto-flip on 20002)
DEBUG_ON       = os.getenv("DEBUG", "").strip() not in ("", "0", "false", "False")
MIN_SIZE       = float(os.getenv("APEX_MIN_POSITION_SIZE", "0"))
SYMBOL_ALLOW   = {s.strip().upper() for s in (os.getenv("SYMBOL_ALLOWLIST") or "").split(",") if s.strip()}

SIZE_DEC       = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DEC      = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DEC        = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DEC    = int(os.getenv("PNL_PCT_DECIMALS", "2"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("apex-forward")

# ========= HTTP session with retries =========
session = requests.Session()
retry = Retry(total=4, connect=4, read=6, backoff_factor=0.6,
              status_forcelist=[429, 500, 502, 503, 504],
              allowed_methods=["GET", "POST"])
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://",  HTTPAdapter(max_retries=retry))
session.headers.update({"Accept": "application/json", "User-Agent": "apex-forward/1.0"})

# ========= Helpers =========
def env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "y")

def safe_float(x) -> float:
    try: return float(str(x).replace(",", ""))
    except Exception: return 0.0

def fmt_num(x: Optional[float], dec: int) -> str:
    if x is None: return "–"
    try: return f"{float(x):.{dec}f}"
    except Exception: return "–"

def fmt_pct(x: Optional[float], dec: int) -> str:
    if x is None: return "–"
    try: return f"{float(x):.{dec}f}%"
    except Exception: return "–"

def discord_post(content: str):
    if not DISCORD_WEBHOOK:
        LOG.error("DISCORD_WEBHOOK missing")
        return
    try:
        r = session.post(DISCORD_WEBHOOK, json={"content": content[:1990]}, timeout=12)
        if r.status_code >= 300:
            LOG.error("Discord post failed: %s %s", r.status_code, r.text[:200])
    except Exception as e:
        LOG.error("Discord post error: %s", e)

# ---- Secret handling (auto-detect Base64 vs raw) ----
def secret_bytes() -> bytes:
    s = (API_SECRET or "").strip()
    # Try strict base64 first; fall back to raw if it fails.
    try:
        return base64.b64decode(s, validate=True)
    except Exception:
        return s.encode("utf-8")

# ---- Timestamp helpers ----
def ts_string(style: str) -> str:
    if style == "ms":
        return str(int(time.time() * 1000))
    # ISO8601 with Z
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def path_for_sign(path: str, query: str) -> str:
    # Many Omni deployments require '/api' in the signed path if your BASE_URL ends with /api.
    prefix = "/api" if BASE_URL.endswith("/api") else ""
    return prefix + path + (f"?{query}" if query else "")

def sign_headers(method: str, path: str, query: str = "", body: str = "") -> Dict[str, str]:
    global TS_STYLE
    # Try current TS_STYLE first; on APEX-TIMESTAMP error we'll flip inside request.
    ts = ts_string(TS_STYLE)
    message = f"{ts}{method.upper()}{path_for_sign(path, query)}{body}"
    sig = base64.b64encode(hmac.new(secret_bytes(), message.encode("utf-8"), hashlib.sha256).digest()).decode("utf-8")
    return {
        "Content-Type": "application/json",
        "APEX-KEY": API_KEY,
        "APEX-PASSPHRASE": PASSPHRASE,
        "APEX-TIMESTAMP": ts,
        "APEX-SIGNATURE": sig,
    }

# ========= Private GET with auto timestamp fallback =========
def private_get(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    global TS_STYLE
    method = "GET"
    params = params or {}
    query = urlencode(sorted(params.items()), doseq=True)
    url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")

    # First attempt with current TS_STYLE
    hdrs = sign_headers(method, path, query, "")
    r = session.get(url, headers=hdrs, timeout=15)

    # If JSON body has code=20002 (timestamp), flip style and retry once
    try:
        jb = r.json() if r.headers.get("Content-Type","").startswith("application/json") else {}
    except Exception:
        jb = {}
    if isinstance(jb, dict) and jb.get("code") == 20002 and "APEX-TIMESTAMP" in str(jb.get("msg", "")):
        TS_STYLE = "ms" if TS_STYLE == "iso" else "iso"
        LOG.warning("Timestamp rejected (20002). Flipping TS_STYLE to %s and retrying %s", TS_STYLE, path)
        hdrs = sign_headers(method, path, query, "")
        r = session.get(url, headers=hdrs, timeout=15)

    # Raise for non-2xx HTTP
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}

# ========= Account / Positions extraction =========
def extract_positions(acct: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Returns (where_found, positions_list), searching common layouts:
    - positions
    - contractAccount.positions
    - data.positions
    - data.contractAccount.positions
    """
    if not isinstance(acct, dict):
        return "<bad-account>", []

    # gather candidates
    candidates: List[Tuple[str, Any]] = []
    candidates.append(("positions", acct.get("positions")))
    ca = acct.get("contractAccount") or {}
    candidates.append(("contractAccount.positions", ca.get("positions")))
    data = acct.get("data") or {}
    if isinstance(data, dict):
        candidates.append(("data.positions", data.get("positions")))
        dca = data.get("contractAccount") or {}
        if isinstance(dca, dict):
            candidates.append(("data.contractAccount.positions", dca.get("positions")))

    for name, arr in candidates:
        if isinstance(arr, list) and len(arr) > 0:
            return name, arr

    # nothing found; return empty
    return "<none>", []

def filter_positions(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for p in raw:
        size = p.get("size") or p.get("positionSize") or p.get("qty") or 0
        try:
            fsize = float(size)
        except Exception:
            fsize = 0.0
        if fsize < MIN_SIZE:
            continue
        sym = str(p.get("symbol") or p.get("market") or "").upper()
        if SYMBOL_ALLOW and sym and sym not in SYMBOL_ALLOW:
            continue
        out.append(p)
    return out

# ========= Formatting =========
def first_field(d: Dict[str, Any], *names, default=None):
    for n in names:
        if n in d and d[n] not in (None, "", 0, "0"):
            return d[n]
    return default

def table_for(positions: List[Dict[str, Any]]) -> str:
    # Columns: SYMBOL | SIDE | SIZE | ENTRY | MARK | PNL | PNL%
    header = f"{'SYMBOL':<13}{'SIDE':<7}{'SIZE':>12}{'ENTRY':>14}{'MARK':>14}{'PNL':>14}{'PNL%':>8}"
    sep    = f"{'-'*13}{'-'*7}{'-'*12}{'-'*14}{'-'*14}{'-'*14}{'-'*8}"
    lines = ["Active ApeX Positions", "```", header, sep]

    for p in positions:
        symbol = str(first_field(p, "symbol", "market", "pair", "instrument", default="–"))
        side   = str(first_field(p, "side", "positionSide", default="–"))
        size   = first_field(p, "size", "positionSize", "qty", default=None)
        entry  = first_field(p, "entryPrice", "avgEntryPrice", "averageEntryPrice", "openPrice", default=None)
        mark   = first_field(p, "markPrice", "indexPrice", "currentPrice", default=None)
        pnl    = first_field(p, "unrealizedPnl", "uPnl", "pnl", "unRealizedPnl", default=None)
        pnlpct = first_field(p, "unrealizedPnlPct", "unrealizedPnlPercent", "pnlPct", default=None)

        lines.append(
            f"{symbol:<13}"
            f"{(side or '–'):<7}"
            f"{fmt_num(size, SIZE_DEC):>12}"
            f"{fmt_num(entry, PRICE_DEC):>14}"
            f"{fmt_num(mark,  PRICE_DEC):>14}"
            f"{fmt_num(pnl,   PNL_DEC):>14}"
            f"{(fmt_pct(pnlpct, PNL_PCT_DEC) if pnlpct is not None else '–'):>8}"
        )

    if not positions:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def snapshot_signature(positions: List[Dict[str, Any]]) -> str:
    # Build a compact signature of what we display (to avoid spamming)
    view = []
    for p in positions:
        view.append((
            str(first_field(p, "symbol", "market", default="")),
            str(first_field(p, "side", "positionSide", default="")),
            str(first_field(p, "size", "positionSize", "qty", default="")),
            str(first_field(p, "entryPrice", "avgEntryPrice", "averageEntryPrice", "openPrice", default="")),
            str(first_field(p, "markPrice", "indexPrice", "currentPrice", default="")),
            str(first_field(p, "unrealizedPnl", "uPnl", "pnl", "unRealizedPnl", default="")),
            str(first_field(p, "unrealizedPnlPct", "unrealizedPnlPercent", "pnlPct", default="")),
        ))
    payload = json.dumps(view, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()

# ========= Main loop =========
def main():
    if not (API_KEY and API_SECRET and PASSPHRASE and DISCORD_WEBHOOK):
        LOG.error("Missing one or more envs: APEX_API_KEY, APEX_API_SECRET, APEX_API_PASSPHRASE, DISCORD_WEBHOOK")
        return

    LOG.info("Forward-only bridge online. Base=%s  TS_STYLE=%s", BASE_URL, TS_STYLE)
    posted_sig = ""

    # One-time greeting (and where we’ll log first diagnostics if DEBUG_ON)
    if DEBUG_ON:
        discord_post(f"🧪 Forward bridge online\nBase={BASE_URL}\nTS_STYLE={TS_STYLE}\nMIN_SIZE={MIN_SIZE}")

    while True:
        try:
            acct = private_get("/v3/account")
            where, raw = extract_positions(acct)
            positions = filter_positions(raw)

            # Optional debug telemetry
            if DEBUG_ON:
                discord_post(f"debug: found {len(raw)} positions at `{where}` → after min_size={MIN_SIZE}: {len(positions)}")

                # Show a tiny sample of the first position (no secrets)
                if positions:
                    p0 = positions[0].copy()
                    for k in list(p0.keys()):
                        if str(k).lower() in ("signature","sign","apiKey","api_key","passphrase","secret"):
                            p0.pop(k, None)
                    sample = json.dumps(p0, ensure_ascii=True)[:800]
                    discord_post(f"debug: first position sample\n```json\n{sample}\n```")
                # Only once per run
                global DEBUG_ON
                DEBUG_ON = False

            content = table_for(positions)

            should_post = (UPDATE_MODE == "always")
            sig = snapshot_signature(positions)
            if UPDATE_MODE == "on-change" and sig != posted_sig:
                should_post = True

            if should_post:
                discord_post(content)
                posted_sig = sig
                LOG.info("Posted %d positions", len(positions))
            else:
                LOG.info("No position change")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            body   = (e.response.text[:200] if e.response is not None else "")
            LOG.error("HTTP error: %s %s", status, body)
        except Exception as e:
            LOG.error("Poll error: %s", e)

        time.sleep(POLL_SECS)

if __name__ == "__main__":
    main()
