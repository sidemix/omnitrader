# main.py — ApeX Omni → Discord (forward-only, deep discovery)
# - Probes multiple endpoints, then deeply inspects /v3/account to discover where positions live.
# - On DEBUG=1 posts a "payload map" of all array paths under /v3/account.
# - Forwards positions (no computed PnL) once discovered.

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
ACCOUNT_ID = os.getenv("APEX_ACCOUNT_ID", "").strip()  # optional; we will also mine ids dynamically

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
session.headers.update({"Accept": "application/json", "User-Agent": "apex-forward/1.3"})

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
    if js.get("code") == 20002 and "APEX-TIMESTAMP" in str(js.get("msg", "")):
        STATE["ts_style"] = "iso" if STATE["ts_style"] == "ms" else "ms"
        LOG.warning("Timestamp rejected; flipping TS_STYLE=%s", STATE["ts_style"])
        js, r = call(STATE["ts_style"])

    if r is not None:
        r.raise_for_status()
    return js

# ------------------------- DISCOVERY -------------------------

def looks_like_position(d: Dict[str, Any]) -> bool:
    k = set(d.keys())
    if {"symbol", "size"} <= k:
        return True
    if {"market", "size"} <= k:
        return True
    if "entryPrice" in k or "avgEntryPrice" in k or "averageEntryPrice" in k:
        if "symbol" in k or "market" in k:
            return True
    return False

def extract_positions(acct: Any) -> Tuple[str, List[Dict[str, Any]]]:
    """Quick pass at common spots + root list detection."""
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
    """Return list of paths to arrays (with size) to understand payload shape."""
    paths: List[str] = []
    if isinstance(obj, list):
        paths.append(f"{prefix or '<root>'}  len={len(obj)}")
        # Recurse a bit to show nested shapes
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
    """Find any string/int fields that look like account/contract IDs."""
    found: Set[str] = set()
    def rec(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(v, (str, int)):
                    ks = k.lower()
                    vs = str(v)
                    if any(t in ks for t in ["accountid", "contractaccountid", "subaccountid", "contractid"]):
                        if vs.isdigit() or len(vs) > 8:
                            found.add(vs)
                else:
                    rec(v)
        elif isinstance(x, list):
            for v in x: rec(v)
    rec(obj)
    return found

def probe_positions() -> Tuple[str, List[Dict[str, Any]]]:
    # First: /v3/account (three variants)
    primary_paths = ["/v3/account"]
    if ACCOUNT_ID:
        primary_paths += [f"/v3/account?accountId={ACCOUNT_ID}",
                          f"/v3/account?account={ACCOUNT_ID}"]

    for p in primary_paths:
        try:
            js = signed_get(p)
            where, pos = extract_positions(js)
            if DEBUG_ON:
                discord_post(f"debug: probe {p} -> {where} ({len(pos)})")
            if pos:
                return f"{p} :: {where}", pos

            # No positions found — dump a compact array map so we can see structure
            if DEBUG_ON:
                arrs = map_arrays(js)
                snippet = "\n".join(arrs[:25]) or "(no arrays found)"
                discord_post("debug: /v3/account payload map\n" + snippet)

            # Mine IDs from this payload and try ID-based endpoints
            ids = list(mine_ids(js))
            if DEBUG_ON and ids:
                discord_post("debug: discovered IDs\n" + "\n".join(ids[:10]))

            # Build ID-shaped endpoints to try if we found IDs
            probe = []
            for idv in ids:
                probe += [
                    f"/v3/account/{idv}/positions",
                    f"/v3/contractAccount/{idv}/positions",
                    f"/v3/contract-account/{idv}/positions",
                    f"/v3/portfolio/{idv}/positions",
                ]

            # Also try generic explicit endpoints
            generic = [
                "/v3/account/positions", "/v3/account/open-positions",
                "/v3/positions", "/v3/position/open",
                "/v1/account/positions",
            ]
            paths = probe + generic
            errors: List[str] = []
            for path in paths:
                try:
                    js2 = signed_get(path)
                    w2, pos2 = extract_positions(js2)
                    if DEBUG_ON:
                        discord_post(f"debug: probe {path} -> {w2} ({len(pos2)})")
                    if pos2:
                        return f"{path} :: {w2}", pos2
                except requests.HTTPError as e:
                    errors.append(f"{path} http={getattr(e.response,'status_code','?')}")
                except Exception as e:
                    errors.append(f"{path} err={e}")

            if DEBUG_ON and errors:
                discord_post("debug: probe errors\n" + "\n".join(errors[:12]))

        except requests.HTTPError as e:
            if DEBUG_ON:
                discord_post(f"debug: {p} http={getattr(e.response,'status_code','?')} body={getattr(e.response,'text','')[:160]}")
        except Exception as e:
            if DEBUG_ON:
                discord_post(f"debug: {p} err={e}")

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

            if DEBUG_ON:
                discord_post(f"debug: found {len(raw)} positions at {where}")

            # min-size filter
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
            try: body = e.response.text[:200]
            except Exception: pass
            LOG.error("HTTP error: %s %s", getattr(e.response, "status_code", "?"), body)
        except Exception as e:
            LOG.error("Poll error: %s", e)

        time.sleep(INTERVAL_SECS)

if __name__ == "__main__":
    main()
