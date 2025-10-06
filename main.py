#!/usr/bin/env python3
# ApeX Omni → Discord bridge (WebSocket-first, live PnL)
# - Private WS auth per docs (sign timestamp+method+path using base64(secret) as key)
# - Public WS parses ws_omni_tickers_v1 to fetch mark/last for PnL
#
# Env you must set on Render:
#   DISCORD_WEBHOOK
#   APEX_KEY, APEX_SECRET, APEX_PASSPHRASE
# Optional:
#   QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, PUBLIC_TICKER_TOPIC, DEBUG, INTERVAL_SECONDS
#   SIZE_DECIMALS, PRICE_DECIMALS, PNL_DECIMALS, PNL_PCT_DECIMALS
#   APEX_TS_STYLE = "ms" (default) or "s"   — used for WS login timestamp
#
# Author: you + ChatGPT

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
DEBUG = os.getenv("DEBUG", "0").strip().lower() not in ("", "0", "false", "no")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

# ---------- env helpers ----------
def _mask(v: Optional[str]) -> str:
    if not v:
        return "<missing>"
    v = v.strip()
    if len(v) <= 8:
        return f"<set len={len(v)}>"
    return f"{v[:2]}…{v[-2:]} (len={len(v)})"

def _get_any(*names: str) -> Optional[str]:
    """Return first non-empty env among the provided names (and log masked)."""
    chosen = None
    for n in names:
        raw = os.getenv(n)
        logging.info("ENV %s = %s", n, _mask(raw))
        if (raw or "").strip() and chosen is None:
            chosen = raw.strip()
    return chosen

# Load keys (accept both short & legacy names)
APEX_KEY        = _get_any("APEX_KEY", "APEX_API_KEY")
APEX_SECRET     = _get_any("APEX_SECRET", "APEX_API_SECRET")
APEX_PASSPHRASE = _get_any("APEX_PASSPHRASE", "APEX_API_PASSPHRASE", "APEX_API_KEY_PASSPHRASE")

APEX_TS_STYLE   = (_get_any("APEX_TS_STYLE") or "ms").lower()  # "ms" or "s"

DISCORD_WEBHOOK = (os.getenv("DISCORD_WEBHOOK") or "").strip()

# WS endpoints (defaults from docs)
QUOTE_WS_PRIVATE = (os.getenv("QUOTE_WS_PRIVATE") or "wss://quote.omni.apex.exchange/realtime_private").strip()
QUOTE_WS_PUBLIC  = (os.getenv("QUOTE_WS_PUBLIC")  or "wss://quote.omni.apex.exchange/realtime_public").strip()
PUBLIC_TICKER_TOPIC = (os.getenv("PUBLIC_TICKER_TOPIC") or "").strip()

# Posting cadence / formatting
INTERVAL_SECONDS       = int(os.getenv("INTERVAL_SECONDS", "30"))
POST_EMPTY_EVERY_SECS  = int(os.getenv("POST_EMPTY_EVERY_SECS", "0"))
SIZE_DECIMALS          = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS         = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS           = int(os.getenv("PNL_DECIMALS", "2"))
PNL_PCT_DECIMALS       = int(os.getenv("PNL_PCT_DECIMALS", "2"))

if APEX_KEY and APEX_SECRET and APEX_PASSPHRASE:
    logging.info(
        "API credentials loaded \u2713 key=%s pass=%s secret=%s",
        _mask(APEX_KEY), _mask(APEX_PASSPHRASE), _mask(APEX_SECRET)
    )
else:
    logging.warning("One or more API credentials missing — private WS may not authorize.")

logging.info(
    "WS endpoints: private=%s  public=%s  DEBUG=%s",
    QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, DEBUG
)

# ---------- numeric helpers ----------
def _now_ts():
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

# ---------- public price cache ----------
class TickerBook:
    """Caches latest mark/last per symbol from public WS."""

    def __init__(self) -> None:
        self._mark: Dict[str, Decimal] = {}
        self._last: Dict[str, Decimal] = {}

    def update_from_public(self, payload: Dict[str, Any]) -> None:
        """
        Expected shapes (from docs):
          topic: "ws_omni_tickers_v1" (etc)
          contents: [
            {"s": "ETHUSDT", "p":"1680.45", "mp":"1681.0", ...},
            ...
          ]
        Some feeds push a single dict in `contents` or under `data`.
        """
        topic = payload.get("topic") or payload.get("channel") or ""
        contents = payload.get("contents") or payload.get("data") or None
        if contents is None:
            return

        if isinstance(contents, list):
            for item in contents:
                self._update_one(item)
        elif isinstance(contents, dict):
            self._update_one(contents)

    def _update_one(self, item: Dict[str, Any]) -> None:
        # Symbol keys we’ve seen: s, symbol, contract, instId
        sym = (
            item.get("s")
            or item.get("symbol")
            or item.get("contract")
            or item.get("instId")
        )
        if not sym:
            return
        sym = sym.upper().replace("/", "-")

        def D(x) -> Optional[Decimal]:
            try:
                return Decimal(str(x))
            except Exception:
                return None

        # Mark / last field names seen in Omni feeds
        mark = D(item.get("mp") or item.get("markPrice") or item.get("indexPrice"))
        last = D(item.get("p")  or item.get("last") or item.get("lastPrice") or item.get("close"))

        if mark is not None:
            self._mark[sym] = mark
        if last is not None:
            self._last[sym] = last

        if DEBUG:
            logging.debug("public price %s mark=%s last=%s", sym, mark, last)

    def mark_or_last(self, sym: str) -> Optional[Decimal]:
        for v in _symbol_variants(sym):
            if v in self._mark:
                return self._mark[v]
        for v in _symbol_variants(sym):
            if v in self._last:
                return self._last[v]
        return None

# ---------- positions cache ----------
class PositionBook:
    """Keeps current open positions found in private WS account frames and computes PnL."""

    def __init__(self) -> None:
        self._pos: Dict[str, Dict[str, Any]] = {}

    def update_from_accounts_payload(self, payload: Dict[str, Any]) -> int:
        contents = payload.get("contents") or {}
        # Docs show positions sometimes at top-level contents["positions"],
        # and account containers under "contractAccounts" / "accounts".
        found = 0

        def scan_node(node: Any):
            nonlocal found
            if isinstance(node, dict):
                # direct position object?
                if self._looks_like_position(node):
                    maybe = self._normalize_pos(node)
                    if maybe:
                        self._pos[maybe["symbol"]] = maybe
                        found += 1
                # traverse deeper
                for v in node.values():
                    scan_node(v)
            elif isinstance(node, list):
                for it in node:
                    scan_node(it)

        scan_node(contents)
        return found

    def list_positions(self) -> List[Dict[str, Any]]:
        return [dict(p) for _, p in sorted(self._pos.items())]

    @staticmethod
    def _looks_like_position(d: Dict[str, Any]) -> bool:
        keys = {k.lower() for k in d.keys()}
        has_sym   = any(k in keys for k in ("symbol", "contract", "instid", "s"))
        has_side  = any(k in keys for k in ("side", "direction"))
        has_size  = any(k in keys for k in ("size", "qty", "positionqty"))
        has_entry = any(k in keys for k in ("entryprice", "avgentryprice", "avgprice", "price"))
        return has_sym and has_side and has_size and has_entry

    @staticmethod
    def _D(x) -> Optional[Decimal]:
        try:
            return Decimal(str(x))
        except Exception:
            return None

    def _normalize_pos(self, d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        sym = d.get("symbol") or d.get("contract") or d.get("instId") or d.get("s")
        side = (d.get("side") or d.get("direction") or "").upper()
        size = d.get("size") or d.get("qty") or d.get("positionQty")
        entry = d.get("entryPrice") or d.get("avgEntryPrice") or d.get("avgPrice") or d.get("price")

        if not sym or side not in ("LONG", "SHORT"):
            return None

        sizeD  = self._D(size)
        entryD = self._D(entry)
        if sizeD is None or entryD is None or sizeD == 0:
            return None

        sym = str(sym).upper().replace("/", "-")
        return {"symbol": sym, "side": side, "size": sizeD, "entry": entryD}

    def compute_pnl(self, prices: TickerBook) -> None:
        for k, p in self._pos.items():
            sym   = p["symbol"]
            entry = p["entry"]
            size  = p["size"]
            side  = p["side"]
            mark  = prices.mark_or_last(sym)

            if mark is None:
                p["mark"] = None
                p["pnl"] = None
                p["pnlPct"] = None
                continue

            try:
                if side == "LONG":
                    pnl = (mark - entry) * size
                else:
                    pnl = (entry - mark) * size
                notional = abs(entry * size)
                pnlpct = (pnl / notional * Decimal("100")) if notional != 0 else None
            except Exception:
                pnl, pnlpct = None, None

            p["mark"]   = mark
            p["pnl"]    = pnl
            p["pnlPct"] = pnlpct

# ---------- Discord ----------
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
        sym    = p["symbol"].ljust(10)
        side   = p["side"].ljust(4)
        size   = _fmt_num(p["size"], SIZE_DECIMALS).rjust(8)
        entry  = _fmt_num(p["entry"], PRICE_DECIMALS).rjust(11)
        mark   = _fmt_num(p.get("mark"), PRICE_DECIMALS).rjust(11)
        pnl    = _fmt_num(p.get("pnl"),  PNL_DECIMALS).rjust(11)
        pnlpct = (_fmt_num(p.get("pnlPct"), PNL_PCT_DECIMALS) + "").rjust(9)
        lines.append(f"{sym}  {side}  {size}  {entry}  {mark}  {pnl}  {pnlpct}")

    if not rows:
        lines.append("(no open positions)")

    content = f"**{header}**\n```{chr(10).join(lines)}```"
    try:
        async with session.post(DISCORD_WEBHOOK, json={"content": content}, timeout=15) as resp:
            if resp.status >= 300:
                logging.warning("Discord response %s", resp.status)
    except Exception:
        logging.exception("Discord post failed")

# ---------- WS clients ----------
def _attach_ts_query(url: str) -> str:
    # Docs show ?v=2&timestamp=... — not strictly required, but harmless.
    ts = _now_ts()
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}v=2&timestamp={ts}"

def _ws_login_signature(secret: str, method: str, request_path: str, timestamp: int) -> str:
    """
    Per docs: key = base64(secret), message = timestamp + method + request_path
              signature = base64(HMAC_SHA256(message, key))
    """
    key_bytes = base64.b64encode(secret.encode("utf-8"))
    msg = f"{timestamp}{method}{request_path}"
    digest = hmac.new(key_bytes, msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")

class PublicWS:
    CANDIDATE_TOPICS = [
        # Override with PUBLIC_TICKER_TOPIC if DevTools shows a different one.
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

    async def start(self):
        if self._task:
            return
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        backoff = 1
        while True:
            try:
                full_url = _attach_ts_query(self.url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(full_url, heartbeat=20) as ws:
                        logging.info("Public WS connected (%s)", self.url)
                        topics = [self.topic] if self.topic else self.CANDIDATE_TOPICS
                        for t in topics:
                            if not t:
                                continue
                            await ws.send_str(json.dumps({"op":"subscribe","args":[t]}))
                            logging.info("Public subscribed: %s", t)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    self.prices.update_from_public(payload)
                                except Exception:
                                    if DEBUG:
                                        logging.debug("public parse error: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break

                logging.warning("Public WS closed. Reconnecting...")
            except Exception:
                logging.exception("Public WS error")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)

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
        if self._task:
            return
        self._task = asyncio.create_task(self._run())

    async def _login(self, ws) -> None:
        if not (APEX_KEY and APEX_SECRET and APEX_PASSPHRASE):
            logging.warning("Missing API creds; skipping WS login")
            return

        ts   = _now_ts()
        meth = "GET"
        path = "/ws/accounts"
        sig  = _ws_login_signature(APEX_SECRET, meth, path, ts)

        req = {
            "type": "login",
            "topics": self.CANDIDATE_ACCT_TOPICS,   # can pre-subscribe via login
            "httpMethod": meth,
            "requestPath": path,
            "apiKey": APEX_KEY,
            "passphrase": APEX_PASSPHRASE,
            "timestamp": ts,
            "signature": sig,
        }
        frame = {"op":"login", "args":[json.dumps(req)]}
        await ws.send_str(json.dumps(frame))
        if DEBUG:
            logging.debug("sent WS login frame")

    async def _run(self):
        backoff = 1
        while True:
            try:
                full_url = _attach_ts_query(self.url)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(full_url, heartbeat=20) as ws:
                        logging.info("Private WS connected (%s)", self.url)

                        # login (authorizes + topics in the same frame)
                        await self._login(ws)

                        # (Optional) also send explicit subscribes (harmless if already authorized)
                        for t in self.CANDIDATE_ACCT_TOPICS:
                            await ws.send_str(json.dumps({"op":"subscribe","args":[t]}))
                            logging.info("Private subscribed: %s", t)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    topic = payload.get("topic", "")
                                    if any(x in topic for x in ("accounts", "account")):
                                        n = self.positions.update_from_accounts_payload(payload)
                                        if DEBUG and n:
                                            logging.debug("private positions +%d", n)
                                except Exception:
                                    if DEBUG:
                                        logging.debug("private parse err: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break

                logging.warning("Private WS closed. Reconnecting...")
            except Exception:
                logging.exception("Private WS error")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)

# ---------- main supervisor ----------
class Bridge:
    def __init__(self):
        self.prices    = TickerBook()
        self.positions = PositionBook()
        self.public_ws = PublicWS(QUOTE_WS_PUBLIC, self.prices)
        self.private_ws= PrivateWS(QUOTE_WS_PRIVATE, self.positions)
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
                    # compute pnl & snapshot
                    self.positions.compute_pnl(self.prices)
                    rows = self.positions.list_positions()
                    snap = self._snapshot_str(rows)
                    changed = snap != self._last_sent_snapshot

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
                                self._last_sent_snapshot = ""  # force post when first pos appears
                except Exception:
                    logging.error("Tick loop error:\n%s", traceback.format_exc())

                await asyncio.sleep(INTERVAL_SECONDS)

# ---------- entrypoint ----------
if __name__ == "__main__":
    if not DISCORD_WEBHOOK:
        logging.error("Missing DISCORD_WEBHOOK – cannot start.")
        raise SystemExit(1)

    logging.info(
        "Bridge (WS) starting… private=%s  public=%s  DEBUG=%s",
        QUOTE_WS_PRIVATE, QUOTE_WS_PUBLIC, DEBUG
    )
    try:
        asyncio.run(Bridge().run())
    except KeyboardInterrupt:
        pass
