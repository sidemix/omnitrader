# main.py — ApeX Omni → Discord (forward-only, robust discovery)
# - Probes /v3/account AND /v3/user, mines IDs, and tries many ID-specific endpoints.
# - One-time payload maps + endpoint probe report always posted on first run.
# - Optional overrides:
#     APEX_POSITIONS_ENDPOINT=/v3/contractAccount/<id>/positions
#     APEX_POSITIONS_JSONPATH=data.positions
# - No custom PnL math; just forwards whatever Omni returns.

import os
import time
import json
import hmac
import hashlib
import base64
import logging
from typing import Dict, List, Tuple, Any, Optional, Set

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ------------------------- ENV -------------------------

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()

BASE_URL = (os.getenv("APEX_BASE_URL") or "https://omni.apex.exchange/api").rstrip("/")
API_KEY = os.getenv("APEX_API_KEY", "").strip()
API_SECRET = os.getenv("APEX_API_SECRET", "").strip()
API_PASSPHRASE = os.getenv("APEX_API_PASSPHRASE", "").strip()

INIT_TS_STYLE = (os.getenv("APEX_TS_STYLE") or "ms").strip().lower()  # "ms" or "iso"
SECRET_MODE = (os.getenv("APEX_SECRET_MODE") or "auto").strip().lower()  # auto|raw|base64

INTERVAL_SECS = int(os.getenv("INTERVAL_SECONDS", "30"))
UPDATE_MODE = (os.getenv("UPDATE_MODE") or "on-change").strip().lower()  # "on-change" | "always"
MIN_POS_SIZE = float(os.getenv("APEX_MIN_POSITION_SIZE", "0"))

SIZE_DEC = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DEC = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DEC = int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DEC = int(os.getenv("PNL_PCT_DECIMALS", "2"))

DEBUG_ON = (os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes", "y"))
ACCOUNT_ID = os.getenv("APEX_ACCOUNT_ID", "").strip()  # optional

# Optional hard overrides (skip probing)
OVERRIDE_ENDPOINT = (os.getenv("APEX_POSITIONS_ENDPOINT") or "").strip()
OVERRIDE_JSONPATH = (os.getenv("APEX_POSITIONS_JSONPATH") or "").strip()

# Always post startup diagnostics (payload maps + probe results)
FORCE_STARTUP_DIAG = True

STATE = {"ts_style": INIT_TS_STYLE}

# ------------------------- LOGGING -------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("apex-forward")

# ------------------------- HTTP -------------------------

session = requests.Session()
retry = Retry(
    total=4, connect=4, read=6, backoff_factor=0.6,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://", HTTPAdapter(max_retries=retry))
session.headers.update({"Accept": "application/json", "User-Agent": "apex-forward/1.6"})

# ------------------------- HELPERS -------------------------

def discord_post(text: str):
    if not DISCORD_WEBHOOK:
        LOG.error("DISCORD_WEBHOOK not set")
        return
    try:
        session.post(DISCORD_WEBHOOK, json={"content": text[:1990]}, timeout=15)
    except Exception as e:
        LOG.error("Discord post failed: %s", e)

def secret_bytes() -> bytes:
    s = (API_SECRET or "").strip()
    if SECRET_MODE == "raw":
        return s.encode("utf-8")
    if SECRET_MODE == "base64":
        return base64.b64decode(s)
    # auto
    try:
        return base64.b64decode(s, validate=True)
    except Exception:
        return s.encode("utf-8")

def now_ts(ts_style: str) -> str:
    if ts_style == "ms":
        return str(int(time.time() * 1000))
    t = time.gmtime()
    return f"{t.tm_year:04d}-{t.tm_mon:02d}-{t.tm_mday:02d}T{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}.000Z"

def sign(ts: str, method: str, path: str, body: str = "") -> str:
    payload = f"{ts}{method.upper()}{path}{body}"
    digest = hmac.new(secret_bytes(), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")

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

# ------------------------- API CORE -------------------------

def signed_get(path: str) -> Dict[str, Any]:
    url = f"{BASE_URL}{path}"

    def call(ts_style: str) -> Tuple[Dict[str, Any], requests.Response]:
        ts = now_ts(ts_style)
        headers = {
            "APEX-API-KEY": API_KEY,
            "APEX-PASSPHRASE": API_PASSPHRASE,
            "APEX-TIMESTAMP": ts,
            "APEX-SIGNATURE": sign(ts, "GET", path, ""),
            "Content-Type": "application/json",
        }
        r = session.get(url, headers=headers, timeout=15)
        try:
            js = r.json()
        except Exception:
            r.raise_for_status()
            return {}, r
        return (js if isinstance(js, dict) else {}), r

    js, r = call(STATE["ts_style"])
    # Flip ts-style if server says timestamp invalid
    if js.get("code") == 20002 and "APEX-TIMESTAMP" in str(js.get("msg", "")):
        STATE["ts_style"] = "iso" if STATE["ts_style"] == "ms" else "ms"
        LOG.warning("Timestamp rejected; flipping TS_STYLE=%s", STATE["ts_style"])
        js, r = call(STATE["ts_style"])

    if r is not None:
        r.raise_for_status()
    return js

# ------------------------- DISCOVERY -------------------------

def json_path_get(obj: Any, path: str) -> Any:
    """Very small dot-path walker: 'data.positions' or 'result.list'."""
    if not path:
        return obj
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

def looks_like_position(d: Dict[str, Any]) -> bool:
    k = set(d.keys())
    if {"symbol", "size"} <= k:
        return True
    if {"market", "size"} <= k:
        return True
    if ("entryPrice" in k or "avgEntryPrice" in k or "averageEntryPrice" in k) and ("symbol" in k or "market" in k):
        return True
    return False

def extract_positions(acct: Any) -> Tuple[str, List[Dict[str, Any]]]:
    if isinstance(acct, dict):
        candidates = [
            ("positions", acct.get("positions")),
            ("contractAccount.positions", (acct.get("contractAccount") or {}).get("positions")),
            ("data.positions", (acct.get("data") or {}).get("positions")),
            ("data.contractAccount.positions", ((acct.get("data") or {}).get("contractAccount") or {}).get("positions")),
            ("account.contractAccount.positions", ((acct.get("account") or {}).get("contractAccount") or {}).get("positions")),
            ("result.positions", (acct.get("result") or {}).get("positions")),
            ("data", acct.get("data")),
            ("result", acct.get("result")),
        ]
        for name, arr in candidates:
            if isinstance(arr, list) and arr and isinstance(arr[0], dict) and looks_like_position(arr[0]):
                return name, arr
    if isinstance(acct, list) and acct and isinstance(acct[0], dict) and looks_like_position(acct[0]):
        return "<root-list>", acct  # type: ignore
    return "<none>", []

def map_arrays(obj: Any, prefix: str = "") -> List[str]:
    paths: List[str] = []
    if isinstance(obj, list):
        paths.append(f"{prefix or '<root>'}  len={len(obj)}")
        for i, v in enumerate(obj[:3]):
            paths += map_arrays(v, f"{prefix}[{i}]")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            newp = f"{prefix}.{k}" if prefix else k
            if isinstance(v, list):
                paths.append(f"{newp}  len={len(v)}")
                for i, vv in enumerate(v[:2]):
                    if isinstance(vv, dict):
                        paths.append(f"{newp}[{i}].keys={sorted(list(vv.keys()))[:8]}")
            else:
                paths += map_arrays(v, newp)
    return paths

def mine_ids(obj: Any) -> Set[str]:
    found: Set[str] = set()
    def rec(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, (str, int)):
                    ks = k.lower()
                    vs = str(v)
                    if any(t in ks for t in ["accountid", "contractaccountid", "subaccountid", "portfolioid", "contractid"]):
                        if vs.isdigit() or len(vs) > 8:
                            found.add(vs)
                else:
                    rec(v)
        elif isinstance(x, list):
            for v in x: rec(v)
    rec(obj)
    return found

_DIAG_POSTED = False  # one-time diagnostics guard

def post_payload_map(label: str, js: Any):
    arrs = map_arrays(js)
    snippet = "\n".join(arrs[:35]) or "(no arrays found)"
    discord_post(f"diag: {label} payload map\n{snippet}")

def probe_positions() -> Tuple[str, List[Dict[str, Any]]]:
    global _DIAG_POSTED

    # If user supplied override, try it first (no probing).
    if OVERRIDE_ENDPOINT:
        try:
            js = signed_get(OVERRIDE_ENDPOINT)
            node = json_path_get(js, OVERRIDE_JSONPATH) if OVERRIDE_JSONPATH else js
            where, pos = extract_positions(node)
            if pos:
                if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                    discord_post(f"diag: override {OVERRIDE_ENDPOINT} -> {OVERRIDE_JSONPATH or '<root>'} ({len(pos)})")
                    _DIAG_POSTED = True
                return f"{OVERRIDE_ENDPOINT} :: {OVERRIDE_JSONPATH or '<root>'}", pos
            else:
                if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                    discord_post(f"diag: override returned no positions (path={OVERRIDE_JSONPATH or '<root>'})")
                    _DIAG_POSTED = True
        except Exception as e:
            if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                discord_post(f"diag: override error {e}")
                _DIAG_POSTED = True

    # 1) Fetch /v3/account and /v3/user; post payload maps; mine ids.
    ids: Set[str] = set()
    roots: List[Tuple[str, Dict[str, Any]]] = []

    for label, path in [("(/v3/account)", "/v3/account"), ("(/v3/user)", "/v3/user")]:
        try:
            js = signed_get(path)
            roots.append((path, js))
            ids |= mine_ids(js)
            if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                post_payload_map(label, js)
        except requests.HTTPError as e:
            if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                discord_post(f"diag: {path} http={getattr(e.response,'status_code','?')} body={getattr(e.response,'text','')[:140]}")
        except Exception as e:
            if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                discord_post(f"diag: {path} err={e}")

    # Try direct extraction from roots (sometimes positions live under /v3/user)
    for path, js in roots:
        where, pos = extract_positions(js)
        if pos:
            if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                discord_post(f"diag: probe {path} -> {where} ({len(pos)})")
                _DIAG_POSTED = True
            return f"{path} :: {where}", pos

    # Also add explicit ACCOUNT_ID if provided
    if ACCOUNT_ID:
        ids.add(ACCOUNT_ID)

    # 2) Build ID-specific and generic guesses to probe.
    endpoints: List[str] = []

    # With IDs
    for idv in list(ids)[:8]:
        endpoints += [
            f"/v3/account/{idv}",
            f"/v3/account/{idv}/positions",
            f"/v3/contractAccount/{idv}",
            f"/v3/contractAccount/{idv}/positions",
            f"/v3/portfolio/{idv}/positions",
        ]

    # If tenant exposes user- or private-scoped lists
    endpoints += [
        "/v3/user/positions",
        "/v3/user/open-positions",
        "/v3/private/positions",
        "/v3/private/account/positions",
        "/v3/positions/open",
        "/v3/position/list",
        "/v3/position/open",
        "/v1/account/positions",
        "/v3/account/positions",
        "/v3/account/open-positions",
        "/v3/positions",
    ]

    errors: List[str] = []
    for ep in endpoints:
        try:
            js = signed_get(ep)
            # If user gave JSONPATH override, honor it here too
            node = json_path_get(js, OVERRIDE_JSONPATH) if OVERRIDE_JSONPATH else js
            where, pos = extract_positions(node)
            if pos:
                if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
                    discord_post(f"diag: probe {ep} -> {where} ({len(pos)})")
                    _DIAG_POSTED = True
                return f"{ep} :: {where}", pos
        except requests.HTTPError as e:
            errors.append(f"{ep} http={getattr(e.response,'status_code','?')}")
        except Exception as e:
            errors.append(f"{ep} err={e}")

    if FORCE_STARTUP_DIAG and not _DIAG_POSTED:
        msg = "diag: probe errors\n" + ("\n".join(errors[:20]) if errors else "(none)")
        discord_post(msg)
        _DIAG_POSTED = True

    return "<none>", []

# ------------------------- TABLE -------------------------

def build_table(positions: List[Dict[str, Any]]) -> str:
    title = "Active ApeX Positions"
    header = [
        ("SYMBOL", 12), ("SIDE", 6), ("SIZE", 10),
        ("ENTRY", 12), ("MARK", 12), ("PNL", 12), ("PNL%", 7),
    ]
    def row(cols): return "".join(str(v).ljust(w) for v, w in cols)
    lines = [title, "```", row(header), row([("-"*len(h), w) for h,w in header])]

    for p in positions:
        sym  = p.get("symbol") or p.get("market") or p.get("pair") or "-"
        side = (p.get("side") or p.get("positionSide") or "-").upper()
        size = fmt_num(p.get("size") or p.get("positionSize") or p.get("qty"), SIZE_DEC)

        entry = p.get("entryPrice") or p.get("avgEntryPrice") or p.get("averageEntryPrice")
        mark  = p.get("markPrice")  or p.get("indexPrice")    or p.get("currentPrice")

        pnl = p.get("unrealizedPnl") or p.get("uPnl") or p.get("pnl") or p.get("unRealizedPnl")
        pnl_pct = p.get("unrealizedPnlPct") or p.get("unrealizedPnlPercent") or p.get("pnlPct")

        lines.append(row([
            (sym, 12), (side, 6), (size, 10),
            (fmt_num(entry, PRICE_DEC) if entry is not None else "-", 12),
            (fmt_num(mark, PRICE_DEC)  if mark  is not None else "-", 12),
            (fmt_num(pnl, PNL_DEC)     if pnl   is not None else "-", 12),
            (f"{float(pnl_pct):.{PNL_PCT_DEC}f}" if pnl_pct not in (None, "") else "-", 7),
        ]))

    if not positions:
        lines.append("(no open positions)")
    lines.append("```")
    return "\n".join(lines)

def snapshot_signature(positions: List[Dict[str, Any]]) -> str:
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

# ------------------------- LOOP -------------------------

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
            where, raw = probe_positions()

            positions = []
            for p in raw:
                sz = safe_float(p.get("size") or p.get("positionSize") or p.get("qty"))
                if sz >= MIN_POS_SIZE:
                    positions.append(p)

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
            try: body = e.response.text[:200]
            except Exception: pass
            LOG.error("HTTP error: %s %s", getattr(e.response, "status_code", "?"), body)
        except Exception as e:
            LOG.error("Poll error: %s", e)

        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    main()
