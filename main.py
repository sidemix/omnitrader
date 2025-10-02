# main.py — ApeX Omni → Discord worker
# v1.5: broader mark lookups (tickers w/out params + multiple orderbook shapes),
#       richer MARK_DEBUG (logs “OK but no price keys”), PnL/PNL%, update policy.
#
# Required env (legacy names supported):
#   APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET,
#   APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK
#
# Optional:
#   OMNI_BASE_URL/APEX_BASE_URL (default https://omni.apex.exchange/api)
#   INTERVAL_SECONDS (default 30)
#   SCAN_SYMBOLS (CSV)       APEX_MIN_POSITION_SIZE (default 1e-12)
#   APEX_LATENCY_CUSHION_MS (default 600)
#   SIZE_DECIMALS=4 PRICE_DECIMALS=4 PNL_DECIMALS=4 PNL_PCT_DECIMALS=2
#   UPDATE_MODE=openclose_only|positions_only|any_change (default positions_only)
#   SIZE_CHANGE_THRESHOLD (default 0)
#   MARK_DEBUG=1 to post mark-resolution debug
#   LOG_LEVEL (default INFO)

import os, sys, time, hmac, json, base64, hashlib, logging
from urllib.parse import urlencode
import requests
from requests.adapters import HTTPAdapter, Retry

# ---------- logging ----------
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
LAT_MS  = int(os.getenv("APEX_LATENCY_CUSHION_MS", "600"))
MIN_SIZE= float(os.getenv("APEX_MIN_POSITION_SIZE", "1e-12"))

SIZE_DECIMALS=int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS=int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS=int(os.getenv("PNL_DECIMALS", "4"))
PNL_PCT_DECIMALS=int(os.getenv("PNL_PCT_DECIMALS", "2"))

UPDATE_MODE=os.getenv("UPDATE_MODE", "positions_only")
SIZE_CHANGE_THRESHOLD=float(os.getenv("SIZE_CHANGE_THRESHOLD", "0"))
MARK_DEBUG = os.getenv("MARK_DEBUG", "0") == "1"

if not (API_KEY and API_SECRET and API_PASSPHRASE and DISCORD_WEBHOOK):
    LOG.error(ascii_only("Missing envs: APEX_API_KEY/APEX_KEY, APEX_API_SECRET/APEX_SECRET, APEX_API_PASSPHRASE/APEX_PASSPHRASE, DISCORD_WEBHOOK"))
    sys.exit(1)

# ---------- http ----------
session = requests.Session()
retries = Retry(total=6, connect=6, read=6, backoff_factor=0.6, status_forcelist=(429,500,502,503,504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://",  HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent":"apex-omni-discord-bridge/1.5"})

# ---------- time & signing (no datetime) ----------
def get_server_time_seconds() -> int:
    r = session.get(f"{BASE_URL}/v3/time", timeout=10); r.raise_for_status()
    js = {}
    try: js = r.json()
    except Exception: pass
    sources = []
    if isinstance(js, dict):
        sources.append(js)
        if isinstance(js.get("data"), dict): sources.append(js["data"])
    for src in sources:
        for k in ("time","serverTime","server_timestamp","serverTs"):
            v = src.get(k)
            if isinstance(v,(int,float)): return int(v if v<1e12 else v/1000.0)
    try: return int(js)
    except Exception: raise RuntimeError(f"Unexpected /v3/time response: {js}")

def iso_from_seconds(sec: int) -> str:
    tm = time.gmtime(sec)
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}.000Z"

def sign_message(ts_str: str, method: str, signed_path: str, body: dict|None) -> str:
    body = body or {}; method = method.upper()
    data_string=""
    if method=="POST":
        items=sorted((k,v) for k,v in body.items() if v is not None)
        data_string="&".join(f"{k}={v}" for k,v in items)
    message=f"{ts_str}{method}{signed_path}{data_string}"
    key=base64.standard_b64encode(API_SECRET.encode("utf-8"))
    digest=hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.standard_b64encode(digest).decode("utf-8")

def private_request(method:str, path:str, params:dict|None=None, data:dict|None=None)->requests.Response:
    assert path.startswith("/v3/")
    url=f"{BASE_URL}{path}"; query=""
    if method.upper()=="GET" and params:
        query="?"+urlencode(sorted(params.items()), doseq=True); url+=query
    sec=get_server_time_seconds()
    attempts=[("iso",iso_from_seconds(sec)), ("ms",str(int(sec*1000+LAT_MS))), ("s",str(int(sec)))]
    last=None
    for label,ts in attempts:
        try:
            signed="/api"+path+(query if method.upper()=="GET" else "")
            sig=sign_message(ts,method,signed,data if method.upper()=="POST" else {})
            headers={"APEX-SIGNATURE":sig,"APEX-TIMESTAMP":ts,"APEX-API-KEY":API_KEY,"APEX-PASSPHRASE":API_PASSPHRASE}
            resp=session.get(url,headers=headers,timeout=20) if method.upper()=="GET" else session.post(url,headers=headers,data=data or {},timeout=20)
            body={}
            if resp.headers.get("Content-Type","").startswith("application/json"):
                try: body=resp.json()
                except Exception: body={}
                if isinstance(body,dict) and body.get("code") not in (None,0):
                    code, msg = body.get("code"), str(body.get("msg"))
                    if code in (20002,20009) or ("timestamp" in msg.lower()):
                        LOG.warning(ascii_only(f"Timestamp rejected on {path} with ts={label}: code={code} msg={msg}")); continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last=e; LOG.warning(ascii_only(f"Retrying {path} with next timestamp format ({label}) due to: {e}"))
            time.sleep(0.25)
    if last: raise last
    raise RuntimeError("Signing failed with all timestamp formats")

# ---------- discord ----------
def post_discord(text:str):
    if not DISCORD_WEBHOOK: return
    try:
        r=session.post(DISCORD_WEBHOOK,json={"content":text[:1990]},timeout=15); r.raise_for_status()
    except Exception as e:
        LOG.error(ascii_only(f"Discord post failed: {e}"))

# ---------- utils ----------
def _first(d:dict,*keys):
    for k in keys:
        if k in d and d[k] not in (None,""): return d[k]
    return None
def _to_float(x):
    if x is None: return None
    try: return float(str(x).replace(",",""))
    except Exception: return None
def _norm_side(side_val,size_val)->str:
    s=(str(side_val or "")).upper()
    if s in ("LONG","SHORT"): return s
    try: return "LONG" if float(size_val)>=0 else "SHORT"
    except Exception: return "LONG"
def _fmt_fixed(x,dec)->str:
    try: return f"{float(x):.{dec}f}"
    except Exception: return "-"
def _fmt_pct(x,dec)->str:
    try: return f"{float(x):.{dec}f}%"
    except Exception: return "-"

# ---------- symbol variants & mark lookups ----------
def _symbol_variants(sym:str)->list[str]:
    s=(sym or "").upper()
    c={s, s.replace("-",""), s.replace("/","")}
    if s.endswith("-USDT"): c.add(s[:-5]+"USDT")
    if s.endswith("USDT") and "-" not in s: c.add(s[:-4]+"-USDT")
    return list(c)

def _debug_add(lines, msg): 
    if MARK_DEBUG: lines.append(msg)

def _mid_from_orderbook(js)->float|None:
    """Try several orderbook shapes to get a mid price."""
    d = js.get("data") if isinstance(js, dict) else None
    srcs=[js]
    if isinstance(d, dict): srcs.append(d)
    for src in srcs:
        # bids/asks as arrays of [price, size] or objects
        bids=src.get("bids") if isinstance(src, dict) else None
        asks=src.get("asks") if isinstance(src, dict) else None
        if isinstance(bids, list) and bids and isinstance(asks, list) and asks:
            def head(p):
                if isinstance(p[0], (list,tuple)): return _to_float(p[0][0])
                if isinstance(p[0], dict):
                    return _to_float(_first(p[0], "price","px","p"))
                return _to_float(p[0])
            bp, ap = head(bids), head(asks)
            if bp is not None and ap is not None:
                return (bp+ap)/2.0
        # named bests
        bp=_to_float(_first(src or {}, "bestBidPrice","bidPrice","bid1","bid","bbp"))
        ap=_to_float(_first(src or {}, "bestAskPrice","askPrice","ask1","ask","ap"))
        if bp is not None and ap is not None: return (bp+ap)/2.0
    return None

def fetch_marks(symbols:list[str])->dict[str,float]:
    out:dict[str,float]={}
    if not symbols: return out

    debug=[]
    wanted={disp:_symbol_variants(disp) for disp in symbols}
    all_variants=sorted({v for vs in wanted.values() for v in vs})

    def _maybe_take(js, tag):
        took=[]
        # typical ticker lists live in data.{tickers|list|rows} or top-level {tickers|list|rows}
        buckets=[]
        if isinstance(js,dict):
            if isinstance(js.get("data"),dict):
                for k in ("tickers","list","rows"): 
                    if isinstance(js["data"].get(k),list): buckets.append(js["data"][k])
            for k in ("tickers","list","rows"):
                if isinstance(js.get(k),list): buckets.append(js[k])
        for arr in buckets:
            for row in arr:
                try:
                    sym=str(_first(row,"symbol","contractSymbol","market") or "").upper()
                    px=_to_float(_first(row,"markPrice","lastPrice","price","indexPrice","close"))
                    if sym and px is not None:
                        for disp,vars_ in wanted.items():
                            if sym in vars_:
                                out[disp]=px; took.append((disp,sym,px))
                except Exception:
                    continue
        if took:
            for disp,sym,px in took:
                _debug_add(debug, f"{tag}: {disp} <- {sym} = {px}")
        else:
            # log a hint about keys if nothing matched
            keys=list((js or {}).keys())[:8] if isinstance(js,dict) else type(js).__name__
            _debug_add(debug, f"{tag}: ok but no price keys (top keys: {keys})")

    # A) batch tickers WITH symbols
    try:
        r=session.get(f"{BASE_URL}/v3/market/tickers", params={"symbols": ",".join(all_variants)}, timeout=10)
        if r.ok: _maybe_take(r.json(), "batch/tickers?symbols")
        else:    _debug_add(debug, f"batch/tickers?symbols status={r.status_code}")
    except Exception as e: _debug_add(debug, f"batch/tickers?symbols err={e}")

    # B) batch tickers WITHOUT params (some APIs ignore 'symbols')
    try:
        r=session.get(f"{BASE_URL}/v3/market/tickers", timeout=10)
        if r.ok: _maybe_take(r.json(), "batch/tickers")
        else:    _debug_add(debug, f"batch/tickers status={r.status_code}")
    except Exception as e: _debug_add(debug, f"batch/tickers err={e}")

    # C) per-symbol fallbacks: tickers/price/last/mark-price with symbol|market
    for disp, vars_ in wanted.items():
        if disp in out: continue
        got=False
        for var in vars_:
            for path,param in (
                ("/v3/market/ticker","symbol"),
                ("/v3/market/price","symbol"),
                ("/v3/market/last","symbol"),
                ("/v3/market/mark-price","symbol"),
                ("/v3/market/mark-price","market"),
            ):
                try:
                    r=session.get(f"{BASE_URL}{path}", params={param: var}, timeout=8)
                    if not r.ok:
                        _debug_add(debug, f"{path}?{param}={var} status={r.status_code}"); continue
                    js={}
                    try: js=r.json()
                    except Exception: js={}
                    px=None
                    if isinstance(js,dict) and isinstance(js.get("data"),dict):
                        px=_to_float(_first(js["data"],"markPrice","lastPrice","price","indexPrice","close"))
                    if px is None and isinstance(js,dict):
                        px=_to_float(_first(js,"markPrice","lastPrice","price","indexPrice","close"))
                    if px is not None:
                        out[disp]=px; _debug_add(debug, f"{path}?{param}={var} -> {disp}={px}"); got=True; break
                    else:
                        _debug_add(debug, f"{path}?{param}={var} ok but no price keys")
                except Exception as e:
                    _debug_add(debug, f"{path}?{param}={var} err={e}")
            if got: break

    # D) orderbook mid-price as last resort
    for disp, vars_ in wanted.items():
        if disp in out: continue
        got=False
        for var in vars_:
            for path,param in (
                ("/v3/market/orderbook","symbol"),
                ("/v3/market/orderbook","market"),
                ("/v3/market/depth","symbol"),
                ("/v3/market/depth","market"),
                ("/v3/market/order-book","symbol"),
            ):
                try:
                    r=session.get(f"{BASE_URL}{path}", params={param: var, "limit":1}, timeout=8)
                    if not r.ok:
                        _debug_add(debug, f"{path}?{param}={var} status={r.status_code}"); continue
                    js={}
                    try: js=r.json()
                    except Exception: js={}
                    px=_mid_from_orderbook(js)
                    if px is not None:
                        out[disp]=px; _debug_add(debug, f"{path}?{param}={var} mid -> {disp}={px}"); got=True; break
                    else:
                        _debug_add(debug, f"{path}?{param}={var} ok but no bids/asks")
                except Exception as e:
                    _debug_add(debug, f"{path}?{param}={var} err={e}")
            if got: break

    if MARK_DEBUG:
        if out:
            post_discord("```Mark debug\n" + "\n".join(debug or ["no debug"]) + "\nfound=" + str(out) + "\n```")
        else:
            post_discord("```Mark debug\n" + ("\n".join(debug) if debug else "no marks found") +
                         "\ntried variants=" + ", ".join(all_variants) + "\n```")
    return out

# ---------- account + normalize ----------
def fetch_account()->dict:
    r=private_request("GET","/v3/account")
    try: return r.json()
    except Exception: return {}

def extract_positions(js:dict)->list[dict]:
    raw=[]
    if not isinstance(js,dict): return raw
    def take_list(dct,key):
        if isinstance(dct.get(key),list): raw.extend(dct[key])
    # top-level + contractAccount
    take_list(js,"positions"); take_list(js,"openPositions")
    ca=js.get("contractAccount") or {}
    if isinstance(ca,dict): take_list(ca,"positions"); take_list(ca,"openPositions")
    # under data
    data=js.get("data")
    if isinstance(data,dict):
        take_list(data,"positions"); take_list(data,"openPositions")
        dca=data.get("contractAccount") or {}
        if isinstance(dca,dict): take_list(dca,"positions"); take_list(dca,"openPositions")
    out=[]
    for p in raw:
        try:
            symbol=str(_first(p,"symbol","contractSymbol","market") or "").upper()
            if not symbol: continue
            if SCAN_SYMBOLS and symbol not in SCAN_SYMBOLS: continue
            size=_to_float(_first(p,"size","positionSize","position","quantity","qty"))
            if size is None or abs(size)<MIN_SIZE: continue
            side=_norm_side(_first(p,"side","positionSide"), size)
            entry=_to_float(_first(p,"entryPrice","avgEntryPrice","avgPrice","entry_price"))
            mark =_to_float(_first(p,"markPrice","mark","lastPrice","mark_price"))
            pnl  =_to_float(_first(p,"unrealizedPnl","upnl","pnl","unrealizedPnL","unrealisedPnl","uPnl"))
            pnlp =_to_float(_first(p,"unrealizedPnlPercent","unrealizedPnlRate","unrealizedPnlRatio","uPnlRate","returnRate"))
            out.append({"symbol":symbol,"side":side,"size":float(size),
                        "entry":entry if entry is not None else None,
                        "mark":mark if mark is not None else None,
                        "pnl":pnl if pnl is not None else None,
                        "pnl_pct":pnlp if pnlp is not None else None})
        except Exception: continue
    # fill missing marks
    need=[p["symbol"] for p in out if p.get("mark") is None]
    if need:
        marks=fetch_marks(need)
        for p in out:
            if p["mark"] is None and p["symbol"] in marks:
                p["mark"]=marks[p["symbol"]]
    # compute PnL
    for p in out:
        sign=1.0 if p["side"]=="LONG" else -1.0
        if p.get("pnl") is None and p.get("entry") not in (None,0) and p.get("mark") is not None:
            p["pnl"]=(p["mark"]-p["entry"])*p["size"]*sign
        if p.get("pnl_pct") is None and p.get("entry") not in (None,0) and p.get("mark") is not None:
            p["pnl_pct"]=((p["mark"]-p["entry"])/p["entry"])*100.0*sign
    # de-dupe for posting
    uniq={}
    for q in out:
        key=(q["symbol"], q["side"], f"{q['size']:.12g}", f"{q['entry'] if q['entry'] is not None else 'None'}")
        uniq[key]=q
    out=list(uniq.values())
    out.sort(key=lambda x:(x["symbol"], x["side"]!="LONG"))
    return out

# ---------- formatting ----------
def format_positions(pos:list[dict])->str:
    if not pos: return "No open positions"
    header="SYMBOL         SIDE   SIZE         ENTRY         MARK          PNL        PNL%"
    sep   ="-------------  -----  -----------  ------------  ------------  ----------  -----"
    lines=[header,sep]
    for p in pos:
        size_s=_fmt_fixed(p["size"], SIZE_DECIMALS)
        entry_s=_fmt_fixed(p["entry"], PRICE_DECIMALS) if p["entry"] is not None else "-"
        mark_s =_fmt_fixed(p["mark"],  PRICE_DECIMALS) if p["mark"]  is not None else "-"
        pnl_s  =_fmt_fixed(p["pnl"],   PNL_DECIMALS)   if p["pnl"]   is not None else "-"
        pnlp_s =_fmt_pct  (p["pnl_pct"], PNL_PCT_DECIMALS) if p["pnl_pct"] is not None else "-"
        lines.append(f"{p['symbol']:<13}  {p['side']:<5}  {size_s:>11}  {entry_s:>12}  {mark_s:>12}  {pnl_s:>10}  {pnlp_s:>5}")
    return "```\n" + "\n".join(lines) + "\n```"

# ---------- snapshot policy ----------
def _quantize_size(x:float)->float:
    if SIZE_CHANGE_THRESHOLD>0:
        steps=round(x/SIZE_CHANGE_THRESHOLD); return steps*SIZE_CHANGE_THRESHOLD
    try: return round(float(x), SIZE_DECIMALS)
    except Exception: return x

def snapshot_of(pos:list[dict])->str:
    try:
        if UPDATE_MODE=="openclose_only":
            keys=sorted({(p["symbol"],p["side"]) for p in pos})
        elif UPDATE_MODE=="any_change":
            keys=sorted((p["symbol"],p["side"], _quantize_size(p["size"]),
                         None if p.get("entry") is None else round(p["entry"],PRICE_DECIMALS),
                         None if p.get("mark")  is None else round(p["mark"], PRICE_DECIMALS),
                         None if p.get("pnl")   is None else round(p["pnl"],  PNL_DECIMALS),
                         None if p.get("pnl_pct") is None else round(p["pnl_pct"], PNL_PCT_DECIMALS)) for p in pos)
        else:
            keys=sorted((p["symbol"],p["side"], _quantize_size(p["size"]),
                         None if p.get("entry") is None else round(p["entry"],PRICE_DECIMALS)) for p in pos)
        return json.dumps(keys)
    except Exception: return ""

# ---------- loop ----------
_last_snapshot=None
def poll_once():
    global _last_snapshot
    acct=fetch_account()
    positions=extract_positions(acct)
    snap=snapshot_of(positions)
    if snap!=_last_snapshot:
        _last_snapshot=snap
        post_discord("Active ApeX Positions\n"+format_positions(positions))
        LOG.info(ascii_only(f"Posted {len(positions)} active positions"))
    else:
        LOG.info(ascii_only("No position change"))

def main():
    LOG.info(ascii_only(f"Starting worker. BASE_URL={BASE_URL}"))
    while True:
        try: poll_once()
        except Exception as e: LOG.error(ascii_only(f"Poll error: {e}"))
        time.sleep(INTERVAL)

if __name__=="__main__": main()
