#!/usr/bin/env python3
# ApeX Omni → Discord (WebSocket-first, live PnL)
# - Private WS: account topics (positions)
# - Public WS : generic + per-symbol ticker subscriptions
# PnL:
#   LONG : (mark - entry) * size
#   SHORT: (entry - mark) * size

import os
import json
import time
import hmac
import base64
import asyncio
import logging
import hashlib
import traceback
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Set

import aiohttp

# ---------- logging ----------
DEBUG = (os.getenv("DEBUG") or "").strip().lower() in ("1","true","yes","y","on")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# ---------- env helpers ----------
def _mask(v: Optional[str]) -> str:
    if not v: return "<missing>"
    v = v.strip()
    return f"{v[:2]}…{v[-2:]} (len={len(v)})" if len(v) > 8 else f"<set len={len(v)}>"

def _get_any(*names: str) -> Optional[str]:
    for n in names:
        raw = os.getenv(n)
        logging.info("ENV %s = %s", n, _mask(raw))
        if raw and raw.strip():
            return raw.strip()
    return None

# ---------- credentials & config ----------
APEX_KEY        = _get_any("APEX_KEY","APEX_API_KEY")
APEX_SECRET     = _get_any("APEX_SECRET","APEX_API_SECRET")
APEX_PASSPHRASE = _get_any("APEX_PASSPHRASE","APEX_API_PASSPHRASE","APEX_API_KEY_PASSPHRASE")
APEX_TS_STYLE   = (_get_any("APEX_TS_STYLE") or "ms").lower()   # "ms" or "s"

DISCORD_WEBHOOK = (os.getenv("DISCORD_WEBHOOK") or "").strip()

QUOTE_WS_PRIVATE = (os.getenv("QUOTE_WS_PRIVATE") or "wss://quote.omni.apex.exchange/realtime_private").strip()
QUOTE_WS_PUBLIC  = (os.getenv("QUOTE_WS_PUBLIC")  or "wss://quote.omni.apex.exchange/realtime_public").strip()

PUBLIC_TICKER_TOPIC = (os.getenv("PUBLIC_TICKER_TOPIC") or "ws_omni_tickers_v1").strip()
SUBSCRIBE_PER_SYMBOL = (os.getenv("SUBSCRIBE_PER_SYMBOL") or "1").strip().lower() in ("1","true","yes","y","on")

INTERVAL_SECONDS       = int(os.getenv("INTERVAL_SECONDS", "30"))
POST_EMPTY_EVERY_SECS  = int(os.getenv("POST_EMPTY_EVERY_SECS", "0"))
SIZE_DECIMALS          = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS         = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS           = int(os.getenv("PNL_DECIMALS", "2"))
PNL_PCT_DECIMALS       = int(os.getenv("PNL_PCT_DECIMALS", "2"))

if all([APEX_KEY,APEX_SECRET,APEX_PASSPHRASE]):
    logging.info("API credentials loaded ✓ key=%s pass=%s secret=%s",
                 _mask(APEX_KEY), _mask(APEX_PASSPHRASE), _mask(APEX_SECRET))
else:
    logging.warning("One or more API credentials missing — private WS may not authorize.")

logging.info("WS endpoints: private=%s  public=%s  DEBUG=%s",
             QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, DEBUG)

# ---------- helpers ----------
def _now_ts() -> int:
    return int(time.time()*1000) if APEX_TS_STYLE == "ms" else int(time.time())

def _round(x: Decimal, places: int) -> str:
    q = Decimal(10) ** -places
    return str(x.quantize(q, rounding=ROUND_HALF_UP))

def _fmt_num(x: Optional[Decimal], places: int, dash="—") -> str:
    return dash if x is None else _round(x, places)

def _symbol_variants(sym: str) -> List[str]:
    s = sym.upper().replace("/", "-")
    core = s.replace("-", "")
    return list({s, core, s.replace("-", "_"), core.replace("-", "_")})

def _attach_ts_query(url: str) -> str:
    ts = _now_ts()
    return f"{url}{'&' if '?' in url else '?'}v=2&timestamp={ts}"

def _ws_login_signature(secret: str, method: str, request_path: str, timestamp: int) -> str:
    # Per Omni docs: key = base64(secret), msg = timestamp + method + request_path
    key_bytes = base64.b64encode(secret.encode("utf-8"))
    msg = f"{timestamp}{method}{request_path}"
    digest = hmac.new(key_bytes, msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")

# ---------- price cache ----------
class TickerBook:
    def __init__(self) -> None:
        self._mark: Dict[str, Decimal] = {}
        self._last: Dict[str, Decimal] = {}
        self._first_seen: Set[str] = set()

    def _D(self, x) -> Optional[Decimal]:
        try: return Decimal(str(x))
        except Exception: return None

    def update_from_public(self, payload: Dict[str, Any]) -> None:
        """
        Handles shapes like:
          {topic, contents:[{s:'APEXUSDT', mp:'1.23', p:'1.24', ...}]}
          {topic, data:[{s:'APEXUSDT', mp:'1.23', p:'1.24', ...}]}
          {topic, data:{data:[...]}}   # some gateways nest twice
        """
        def update_item(item: Dict[str, Any]):
            sym = (item.get("s") or item.get("symbol") or item.get("contract") or item.get("instId"))
            if not sym: return
            sym = str(sym).upper().replace("/", "-")
            mark = self._D(item.get("mp") or item.get("markPrice") or item.get("indexPrice"))
            last = self._D(item.get("p")  or item.get("last") or item.get("lastPrice") or item.get("close"))
            if mark is not None: self._mark[sym] = mark
            if last is not None: self._last[sym] = last
            if sym not in self._first_seen and (mark is not None or last is not None):
                self._first_seen.add(sym)
                logging.info("Public price live for %s (mark=%s last=%s)", sym, mark, last)
            if DEBUG:
                logging.debug("public %s mark=%s last=%s", sym, mark, last)

        # Try multiple locations
        for key in ("contents","data"):
            obj = payload.get(key)
            if isinstance(obj, list):
                for it in obj: 
                    if isinstance(it, dict): update_item(it)
            elif isinstance(obj, dict):
                # nested "data" inside "data"
                inner = obj.get("data")
                if isinstance(inner, list):
                    for it in inner:
                        if isinstance(it, dict): update_item(it)
                else:
                    update_item(obj)

    def mark_or_last(self, sym: str) -> Optional[Decimal]:
        for v in _symbol_variants(sym):
            if v in self._mark: return self._mark[v]
        for v in _symbol_variants(sym):
            if v in self._last: return self._last[v]
        return None

# ---------- positions ----------
class PositionBook:
    def __init__(self) -> None:
        self._pos: Dict[str, Dict[str, Any]] = {}

    def update_from_accounts_payload(self, payload: Dict[str, Any]) -> int:
        contents = payload.get("contents") or {}
        found = 0

        def looks_like_position(d: Dict[str, Any]) -> bool:
            keys = {k.lower() for k in d.keys()}
            has_sym   = any(k in keys for k in ("symbol","contract","instid","s"))
            has_side  = any(k in keys for k in ("side","direction"))
            has_size  = any(k in keys for k in ("size","qty","positionqty"))
            has_entry = any(k in keys for k in ("entryprice","avgentryprice","avgprice","price"))
            return has_sym and has_side and has_size and has_entry

        def D(x) -> Optional[Decimal]:
            try: return Decimal(str(x))
            except Exception: return None

        def normalize(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            sym = d.get("symbol") or d.get("contract") or d.get("instId") or d.get("s")
            side = (d.get("side") or d.get("direction") or "").upper()
            size = d.get("size") or d.get("qty") or d.get("positionQty")
            entry= d.get("entryPrice") or d.get("avgEntryPrice") or d.get("avgPrice") or d.get("price")
            if not sym or side not in ("LONG","SHORT"): return None
            sizeD, entryD = D(size), D(entry)
            if sizeD is None or entryD is None or sizeD == 0: return None
            sym = str(sym).upper().replace("/", "-")
            return {"symbol": sym, "side": side, "size": sizeD, "entry": entryD}

        def walk(node: Any):
            nonlocal found
            if isinstance(node, dict):
                if looks_like_position(node):
                    n = normalize(node)
                    if n:
                        self._pos[n["symbol"]] = n
                        found += 1
                for v in node.values(): walk(v)
            elif isinstance(node, list):
                for it in node: walk(it)

        walk(contents)
        return found

    def list_positions(self) -> List[Dict[str, Any]]:
        return [dict(p) for _, p in sorted(self._pos.items())]

    def compute_pnl(self, prices: TickerBook) -> None:
        for p in self._pos.values():
            sym, entry, size, side = p["symbol"], p["entry"], p["size"], p["side"]
            mark = prices.mark_or_last(sym)
            if mark is None:
                p["mark"]=p["pnl"]=p["pnlPct"]=None
                continue
            pnl = (mark - entry) * size if side == "LONG" else (entry - mark) * size
            notional = abs(entry * size)
            pnlpct = (pnl / notional * Decimal("100")) if notional != 0 else None
            p["mark"], p["pnl"], p["pnlPct"] = mark, pnl, pnlpct

# ---------- Discord ----------
async def post_discord(session: aiohttp.ClientSession, rows: List[Dict[str, Any]]) -> None:
    hook = DISCORD_WEBHOOK
    if not hook:
        logging.error("Missing DISCORD_WEBHOOK"); return

    header = "Active ApeX Positions"
    lines = [
        "SYMBOL      SIDE  SIZE      ENTRY       MARK        PNL         PNL%",
        "------      ----  ----      -----       ----        ---         ----",
    ]
    for p in rows:
        sym   = p["symbol"].ljust(10)
        side  = p["side"].ljust(4)
        size  = _fmt_num(p["size"], SIZE_DECIMALS).rjust(8)
        entry = _fmt_num(p["entry"], PRICE_DECIMALS).rjust(11)
        mark  = _fmt_num(p.get("mark"), PRICE_DECIMALS).rjust(11)
        pnl   = _fmt_num(p.get("pnl"),  PNL_DECIMALS).rjust(11)
        pct   = _fmt_num(p.get("pnlPct"), PNL_PCT_DECIMALS).rjust(9)
        lines.append(f"{sym}  {side}  {size}  {entry}  {mark}  {pnl}  {pct}")

    if not rows: lines.append("(no open positions)")
    content = f"**{header}**\n```{chr(10).join(lines)}```"

    try:
        async with aiohttp.ClientSession() as s2:
            async with s2.post(hook, json={"content": content}, timeout=15) as r:
                if r.status >= 300: logging.warning("Discord response %s", r.status)
    except Exception:
        logging.exception("Discord post failed")

# ---------- WebSockets ----------
class PublicWS:
    # We’ll try generic topic + per-symbol variants.
    CANDIDATE_TOPICS = [
        "ws_omni_tickers_v1",
        "ws_tickers_v1",
        "ws_tickers",
        "tickers_v1",
    ]

    def __init__(self, url: str, prices: TickerBook):
        self.url = url
        self.prices = prices
        self.topic = PUBLIC_TICKER_TOPIC or None
        self._task: Optional[asyncio.Task] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._wanted_symbols: Set[str] = set()
        self._subscribed_symbols: Set[str] = set()

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._run())

    def want_symbols(self, syms: Set[str]) -> None:
        """Called by supervisor each tick to ensure per-symbol subs."""
        self._wanted_symbols |= {s.upper().replace("/", "-") for s in syms}
        # actual sends happen inside _run when ws is connected

    async def _subscribe_generic(self, ws):
        topics = [self.topic] if self.topic else self.CANDIDATE_TOPICS
        for t in topics:
            if not t: continue
            # 1) simple topic string
            await ws.send_str(json.dumps({"op": "subscribe", "args": [t]}))
            logging.info("Public subscribed: %s", t)

    async def _subscribe_symbol_variants(self, ws, sym: str):
        """Fire a few common shapes to maximize compatibility across gateways."""
        core = sym.replace("-", "")
        frames = [
            {"op":"subscribe","args":[f"{PUBLIC_TICKER_TOPIC}:{sym}"]},
            {"op":"subscribe","args":[f"{PUBLIC_TICKER_TOPIC}:{core}"]},
            {"op":"subscribe","args":[{"channel": PUBLIC_TICKER_TOPIC, "symbol": sym}]},
            {"op":"subscribe","args":[{"channel": PUBLIC_TICKER_TOPIC, "symbol": core}]},
            {"op":"subscribe","args":[{"channel": "ws_tickers_v1", "symbol": sym}]},
            {"op":"subscribe","args":[{"channel": "ws_tickers_v1", "symbol": core}]},
        ]
        for f in frames:
            try:
                await ws.send_str(json.dumps(f))
            except Exception:
                pass
        logging.info("Public per-symbol subscribed: %s", sym)

    async def _run(self):
        backoff = 1
        while True:
            try:
                full_url = _attach_ts_query(self.url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(full_url, heartbeat=20) as ws:
                        self._ws = ws
                        self._subscribed_symbols.clear()
                        logging.info("Public WS connected (%s)", self.url)

                        await self._subscribe_generic(ws)

                        # main loop
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    self.prices.update_from_public(payload)
                                except Exception:
                                    if DEBUG: logging.debug("public parse err: %s", msg.data[:200])

                                # opportunistically send per-symbol subs when we first learn wanted symbols
                                if SUBSCRIBE_PER_SYMBOL and self._wanted_symbols:
                                    # only subscribe for those not yet subscribed
                                    pending = [s for s in self._wanted_symbols if s not in self._subscribed_symbols]
                                    for s in pending:
                                        await self._subscribe_symbol_variants(ws, s)
                                        self._subscribed_symbols.add(s)

                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break

                logging.warning("Public WS closed. Reconnecting...")
            except Exception:
                logging.exception("Public WS error")
            self._ws = None
            await asyncio.sleep(backoff)
            backoff = min(15, backoff * 2)

class PrivateWS:
    CANDIDATE_ACCT_TOPICS = [
        "ws_omni_swap_accounts_v1",
        "ws_zk_accounts_v3",
        "ws_accounts_v1",
    ]
    def __init__(self, url: str, positions: PositionBook):
        self.url = url
        self.positions = positions
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._run())

    async def _login(self, ws) -> None:
        if not (APEX_KEY and APEX_SECRET and APEX_PASSPHRASE):
            logging.warning("Missing API creds; skipping WS login")
            return
        ts = _now_ts()
        meth = "GET"
        path = "/ws/accounts"
        sig  = _ws_login_signature(APEX_SECRET, meth, path, ts)
        req = {
            "type": "login",
            "topics": self.CANDIDATE_ACCT_TOPICS,
            "httpMethod": meth,
            "requestPath": path,
            "apiKey": APEX_KEY,
            "passphrase": APEX_PASSPHRASE,
            "timestamp": ts,
            "signature": sig,
        }
        frame = {"op":"login","args":[json.dumps(req)]}
        await ws.send_str(json.dumps(frame))
        if DEBUG: logging.debug("sent WS login frame")

    async def _run(self):
        backoff = 1
        while True:
            try:
                full_url = _attach_ts_query(self.url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(full_url, heartbeat=20) as ws:
                        logging.info("Private WS connected (%s)", self.url)
                        await self._login(ws)
                        for t in self.CANDIDATE_ACCT_TOPICS:
                            await ws.send_str(json.dumps({"op":"subscribe","args":[t]}))
                            logging.info("Private subscribed: %s", t)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    topic = payload.get("topic","")
                                    if "account" in topic:
                                        n = self.positions.update_from_accounts_payload(payload)
                                        if DEBUG and n: logging.debug("private positions +%d", n)
                                except Exception:
                                    if DEBUG: logging.debug("private parse err: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break

                logging.warning("Private WS closed. Reconnecting...")
            except Exception:
                logging.exception("Private WS error")
            await asyncio.sleep(backoff)
            backoff = min(15, backoff * 2)

# ---------- supervisor ----------
class Bridge:
    def __init__(self):
        self.prices     = TickerBook()
        self.positions  = PositionBook()
        self.public_ws  = PublicWS(QUOTE_WS_PUBLIC, self.prices)
        self.private_ws = PrivateWS(QUOTE_WS_PRIVATE, self.positions)
        self._last_sent_snapshot: Optional[str] = None
        self._last_empty_post = 0.0

    def _snapshot_str(self, rows: List[Dict[str, Any]]) -> str:
        serial = []
        for p in sorted(rows, key=lambda x: x["symbol"]):
            serial.append(
                f'{p["symbol"]}|{p["side"]}|{p["size"]}|{p["entry"]}|{p.get("mark")}|{p.get("pnl")}|{p.get("pnlPct")}'
            )
        return ";".join(serial)

    async def run(self):
        await self.public_ws.start()
        await self.private_ws.start()

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    # 1) compute pnl from latest prices
                    self.positions.compute_pnl(self.prices)
                    rows = self.positions.list_positions()

                    # 2) ask public WS to subscribe per-symbol (for all current symbols)
                    if SUBSCRIBE_PER_SYMBOL and rows:
                        self.public_ws.want_symbols({p["symbol"] for p in rows})

                    # 3) post when snapshot changes
                    snap = self._snapshot_str(rows)
                    if rows:
                        if snap != self._last_sent_snapshot:
                            await post_discord(session, rows)
                            self._last_sent_snapshot = snap
                    else:
                        if POST_EMPTY_EVERY_SECS > 0:
                            now = time.time()
                            if now - self._last_empty_post >= POST_EMPTY_EVERY_SECS:
                                await post_discord(session, rows)
                                self._last_empty_post = now
                                self._last_sent_snapshot = ""  # force next change
                except Exception:
                    logging.error("Tick loop error:\n%s", traceback.format_exc())

                await asyncio.sleep(INTERVAL_SECONDS)

# ---------- entrypoint ----------
if __name__ == "__main__":
    if not DISCORD_WEBHOOK:
        logging.error("Missing DISCORD_WEBHOOK – cannot start.")
        raise SystemExit(1)

    logging.info("Bridge (WS) starting… private=%s  public=%s  DEBUG=%s",
                 QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, DEBUG)
    try:
        asyncio.run(Bridge().run())
    except KeyboardInterrupt:
        pass
