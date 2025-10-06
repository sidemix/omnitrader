#!/usr/bin/env python3
# ApeX Omni → Discord bridge (WebSocket-first, live PnL)
#
# - Private WS:  ws_omni_swap_accounts_v1 (positions via accounts snapshots/deltas)
# - Public WS :  tickers/mark topic (auto-detected; override with PUBLIC_TICKER_TOPIC)
#
# PnL (perp one-way exposure):
#   LONG : (mark - entry) * size
#   SHORT: (entry - mark) * size
# -----------------------------------------------------------

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
from typing import Dict, Any, List, Optional

import aiohttp

# ---------- logging ----------
DEBUG = (os.getenv("DEBUG") or "").strip().lower() in ("1", "true", "yes", "y", "on")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# ---------- env: normalize ONCE and use everywhere ----------
def _mask(v: Optional[str]) -> str:
    if not v:
        return "<missing>"
    v = v.strip()
    if len(v) <= 8:
        return f"<set len={len(v)}>"
    return f"{v[:2]}…{v[-2:]} (len={len(v)})"

def _pick(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        logging.info("ENV %s = %s", n, _mask(v))
        if v and v.strip():
            return v.strip()
    return None

API_KEY         = _pick("APEX_KEY", "APEX_API_KEY")
API_SECRET      = _pick("APEX_SECRET", "APEX_API_SECRET")
API_PASSPHRASE  = _pick("APEX_PASSPHRASE", "APEX_API_KEY_PASSPHRASE")
SECRET_IS_B64   = (_pick("APEX_SECRET_B64", "APEX_SECRET_BASE64") or "").lower() in ("1","true","yes","y","on")

# Other config
DISCORD_WEBHOOK     = (os.getenv("DISCORD_WEBHOOK") or "").strip()
QUOTE_WS_PRIVATE    = (os.getenv("QUOTE_WS_PRIVATE") or "wss://quote.omni.apex.exchange/realtime_private").strip()
QUOTE_WS_PUBLIC     = (os.getenv("QUOTE_WS_PUBLIC")  or "wss://quote.omni.apex.exchange/realtime_public").strip()
PUBLIC_TICKER_TOPIC = (os.getenv("PUBLIC_TICKER_TOPIC") or "").strip()

INTERVAL_SECONDS        = int(os.getenv("INTERVAL_SECONDS", "30"))
POST_EMPTY_EVERY_SECS   = int(os.getenv("POST_EMPTY_EVERY_SECS", "0"))
SIZE_DECIMALS           = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS          = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS            = int(os.getenv("PNL_DECIMALS", "2"))
PNL_PCT_DECIMALS        = int(os.getenv("PNL_PCT_DECIMALS", "2"))

# Startup report
creds_ok = all([API_KEY, API_SECRET, API_PASSPHRASE])
if creds_ok:
    logging.info("API credentials loaded ✓ key=%s pass=%s secret=%s b64=%s",
                 _mask(API_KEY), _mask(API_PASSPHRASE), _mask(API_SECRET), SECRET_IS_B64)
else:
    logging.warning("Missing API credentials (need KEY + SECRET + PASSPHRASE). Private WS will not authorize.")

logging.info("WS endpoints: private=%s  public=%s  DEBUG=%s", QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, DEBUG)

# ---------- helpers ----------

def _now_ms() -> int:
    return int(time.time() * 1000)

def _round(x: Decimal, places: int) -> str:
    q = Decimal(10) ** -places
    return str(x.quantize(q, rounding=ROUND_HALF_UP))

def _fmt_num(x: Optional[Decimal], places: int, dash="—") -> str:
    if x is None:
        return dash
    return _round(x, places)

def _secret_bytes() -> bytes:
    if not API_SECRET:
        return b""
    if SECRET_IS_B64:
        try:
            return base64.b64decode(API_SECRET)
        except Exception:
            logging.warning("APEX_SECRET_B64=true but decoding failed; using raw bytes")
    return API_SECRET.encode("utf-8")

def _hmac_sha256_b64(msg: str, secret: bytes) -> str:
    sig = hmac.new(secret, msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(sig).decode("utf-8")

def _symbol_variants(sym: str) -> List[str]:
    s = sym.upper().replace("/", "-")
    core = s.replace("-", "")
    return list({s, core, s.replace("-", "_"), core.replace("-", "_")})

# ---------- state (prices) ----------

class TickerBook:
    """Caches latest mark/last per symbol from the public WS."""

    def __init__(self) -> None:
        self._mark: Dict[str, Decimal] = {}
        self._last: Dict[str, Decimal] = {}

    def update_from_public(self, payload: Dict[str, Any]) -> Optional[str]:
        # Expect shapes like: {"topic": "...tickers...", "contents": {symbol, markPrice, last, ...}}
        contents = payload.get("contents") or payload.get("data")
        if isinstance(contents, list):
            updated = None
            for item in contents:
                u = self._update_one(item)
                updated = updated or u
            return updated
        elif isinstance(contents, dict):
            return self._update_one(contents)
        return None

    def _update_one(self, item: Dict[str, Any]) -> Optional[str]:
        sym = item.get("symbol") or item.get("contract") or item.get("instId")
        if not sym:
            return None
        sym = sym.upper().replace("/", "-")

        def D(x) -> Optional[Decimal]:
            try:
                return Decimal(str(x))
            except Exception:
                return None

        mark = D(item.get("markPrice") or item.get("indexPrice"))
        last = D(item.get("last") or item.get("lastPrice") or item.get("close"))

        if mark is not None:
            self._mark[sym] = mark
        if last is not None:
            self._last[sym] = last

        if DEBUG:
            logging.debug("public price update %s  mark=%s last=%s", sym, mark, last)
        return sym

    def mark_or_last(self, sym: str) -> Optional[Decimal]:
        for v in _symbol_variants(sym):
            if v in self._mark:
                return self._mark[v]
        for v in _symbol_variants(sym):
            if v in self._last:
                return self._last[v]
        return None

# ---------- state (positions) ----------

class PositionBook:
    """Keeps current open positions from private WS and computes PnL."""

    def __init__(self) -> None:
        self._pos: Dict[str, Dict[str, Any]] = {}  # by SYMBOL

    def update_from_accounts_payload(self, payload: Dict[str, Any]) -> int:
        contents = payload.get("contents") or {}
        accounts = contents.get("accounts") or contents.get("contractAccounts") or []
        found = 0
        if isinstance(accounts, list):
            for acct in accounts:
                found += self._scan_one_account_for_positions(acct)
        return found

    def list_positions(self) -> List[Dict[str, Any]]:
        return [dict(p) for _, p in sorted(self._pos.items())]

    def compute_pnl(self, prices: TickerBook) -> None:
        for p in self._pos.values():
            sym = p["symbol"]
            mark = prices.mark_or_last(sym)
            if mark is None:
                p["mark"] = None
                p["pnl"] = None
                p["pnlPct"] = None
                continue
            entry = p["entry"]
            size = p["size"]
            side = p["side"]
            try:
                pnl = (mark - entry) * size if side == "LONG" else (entry - mark) * size
                nominal = entry * size
                pnlPct = (pnl / nominal * Decimal("100")) if nominal != 0 else None
            except Exception:
                pnl = None
                pnlPct = None
            p["mark"] = mark
            p["pnl"] = pnl
            p["pnlPct"] = pnlPct

    # ---- internals ----

    def _scan_one_account_for_positions(self, acct: Dict[str, Any]) -> int:
        found_here: List[Dict[str, Any]] = []

        def looks_like_position(d: Dict[str, Any]) -> bool:
            keys = {k.lower() for k in d.keys()}
            has_sym = any(k in keys for k in ("symbol","contract","instid"))
            has_side = any(k in keys for k in ("side","direction"))
            has_size = any(k in keys for k in ("size","qty","positionqty"))
            has_entry = any(k in keys for k in ("entryprice","avgentryprice","avgprice","price"))
            return has_sym and has_side and has_size and has_entry

        def D(x) -> Optional[Decimal]:
            try:
                return Decimal(str(x))
            except Exception:
                return None

        def normalize_one(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            sym = d.get("symbol") or d.get("contract") or d.get("instId")
            side = (d.get("side") or d.get("direction") or "").upper()
            size = d.get("size") or d.get("qty") or d.get("positionQty")
            entry = d.get("entryPrice") or d.get("avgEntryPrice") or d.get("avgPrice") or d.get("price")
            if not sym or side not in ("LONG","SHORT"):
                return None
            size = D(size); entry = D(entry)
            if size is None or entry is None or size == 0:
                return None
            sym = sym.upper().replace("/", "-")
            return {"symbol": sym, "side": side, "size": size, "entry": entry}

        def walk(node: Any):
            if isinstance(node, dict):
                if looks_like_position(node):
                    maybe = normalize_one(node)
                    if maybe:
                        found_here.append(maybe)
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for it in node:
                    walk(it)

        walk(acct)
        for pos in found_here:
            self._pos[pos["symbol"]] = pos
        return len(found_here)

# ---------- discord ----------

async def post_discord(session: aiohttp.ClientSession, rows: List[Dict[str, Any]]) -> None:
    if not DISCORD_WEBHOOK:
        logging.error("Missing DISCORD_WEBHOOK")
        return

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
        pnl   = _fmt_num(p.get("pnl"), PNL_DECIMALS).rjust(11)
        pct   = _fmt_num(p.get("pnlPct"), PNL_PCT_DECIMALS).rjust(9)
        lines.append(f"{sym}  {side}  {size}  {entry}  {mark}  {pnl}  {pct}")

    if not rows:
        lines.append("(no open positions)")

    content = f"**{header}**\n```{chr(10).join(lines)}```"
    try:
        async with session.post(DISCORD_WEBHOOK, json={"content": content}, timeout=15) as resp:
            if resp.status >= 300:
                logging.warning("Discord response %s", resp.status)
    except Exception:
        logging.exception("Discord post failed")

# ---------- websockets ----------

class PublicWS:
    CANDIDATE_TOPICS = [
        "ws_tickers_v1",
        "ws_omni_tickers_v1",
        "ws_tickers",
        "tickers_v1",
    ]
    def __init__(self, url: str, prices: TickerBook):
        self.url = url
        self.prices = prices
        self.topic = PUBLIC_TICKER_TOPIC or None
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._run())

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, heartbeat=20) as ws:
                        logging.info("Public WS connected (%s)", self.url)
                        topics = [self.topic] if self.topic else self.CANDIDATE_TOPICS
                        for t in topics:
                            if t:
                                await ws.send_str(json.dumps({"op":"subscribe","args":[t]}))
                                logging.info("Public subscribed: %s", t)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    self.prices.update_from_public(json.loads(msg.data))
                                except Exception:
                                    if DEBUG: logging.debug("public parse err: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                logging.warning("Public WS closed. Reconnecting...")
            except Exception:
                logging.exception("Public WS error")
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
        """API-key login frame (some deployments require)."""
        if not (API_KEY and API_SECRET and API_PASSPHRASE):
            logging.warning("No API credentials set; private WS may not authorize.")
            return
        ts = _now_ms()
        prehash = str(ts)           # observed prehash (timestamp as string)
        sig = _hmac_sha256_b64(prehash, _secret_bytes())
        args = {
            "apiKey": API_KEY,
            "passphrase": API_PASSPHRASE,
            "timestamp": ts,
            "signature": sig,
        }
        frame = {"op": "login", "args": [json.dumps(args)]}
        await ws.send_str(json.dumps(frame))
        if DEBUG: logging.debug("sent WS login frame")

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, heartbeat=20) as ws:
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
                                        if DEBUG and n: logging.debug("positions updated: +%d", n)
                                except Exception:
                                    if DEBUG: logging.debug("private parse err: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                logging.warning("Private WS closed. Reconnecting...")
            except Exception:
                logging.exception("Private WS error")
            await asyncio.sleep(backoff)
            backoff = min(15, backoff * 2)

# ---------- supervisor ----------

class Bridge:
    def __init__(self):
        self.prices = TickerBook()
        self.positions = PositionBook()
        self.public_ws = PublicWS(QUOTE_WS_PUBLIC, self.prices)
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
                    self.positions.compute_pnl(self.prices)
                    rows = self.positions.list_positions()
                    snap = self._snapshot_str(rows)
                    changed = (snap != self._last_sent_snapshot)

                    if rows:
                        if changed:
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
