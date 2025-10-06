#!/usr/bin/env python3
# ApeX Omni → Discord bridge (WebSocket-first, live PnL)
#
# - Private WS:  ws_omni_swap_accounts_v1 (plus fallbacks)
# - Public WS :  tries a few ticker/mark topics (or use PUBLIC_TICKER_TOPIC)
#
# PnL (perpetuals one-way exposure):
#   long : (mark - entry) * size
#   short: (entry - mark) * size
#
# Author: you + ChatGPT
# ----------------------------------------------

import os
import re
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

# ========= ENV LOADING + MASKED DIAGNOSTICS (must be near top) =========
import os, logging

def _mask(v: str) -> str:
    if not v:
        return "<missing>"
    v = v.strip()
    if len(v) <= 8:
        return f"<set len={len(v)}>"
    return f"{v[:4]}...{v[-4:]} (len={len(v)})"

def _get_any(*names: str):
    """
    Return the first non-empty env value (stripped) among the provided names.
    Logs each candidate so we can see exactly what the process receives.
    """
    chosen = None
    for n in names:
        raw = os.getenv(n)
        logging.info(f"ENV {n} = {_mask(raw)}")
        if (raw or "").strip() and chosen is None:
            chosen = (raw or "").strip()
    return chosen

# Accept both short and legacy names
APEX_KEY        = _get_any("APEX_KEY", "APEX_API_KEY")
APEX_SECRET     = _get_any("APEX_SECRET", "APEX_API_SECRET")
APEX_PASSPHRASE = _get_any("APEX_PASSPHRASE", "APEX_API_PASSPHRASE", "APEX_API_KEY_PASSPHRASE")

APEX_TS_STYLE   = (_get_any("APEX_TS_STYLE") or "ms").lower()
APEX_BASE_URL   = _get_any("APEX_BASE_URL") or "https://omni.apex.exchange/api"
APEX_SECRET_B64 = (_get_any("APEX_SECRET_BASE64") or "").lower() in ("1","true","yes","on")

if not (APEX_KEY and APEX_SECRET and APEX_PASSPHRASE):
    logging.warning("No API credentials set; private WS may not authorize.")
else:
    logging.info("API creds present ✓ (masked above)")

# Extra: show all APEX_* names present so you can catch duplicates/overrides
_present = [k for k in os.environ.keys() if k.startswith("APEX_")]
logging.info(f"APEX_* present: {', '.join(sorted(_present)) or '(none)'}")
logging.info(f"Using BASE_URL={APEX_BASE_URL}  TS_STYLE={APEX_TS_STYLE}  SECRET_B64={APEX_SECRET_B64}")
# ======================================================================


# ---------- config & logging ---------- 

DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "").strip()

QUOTE_WS_PRIVATE = os.getenv(
    "QUOTE_WS_PRIVATE", "wss://quote.omni.apex.exchange/realtime_private"
).strip()

QUOTE_WS_PUBLIC = os.getenv(
    "QUOTE_WS_PUBLIC", "wss://quote.omni.apex.exchange/realtime_public"
).strip()

PUBLIC_TICKER_TOPIC = (os.getenv("PUBLIC_TICKER_TOPIC") or "").strip()

APEX_API_KEY = os.getenv("APEX_API_KEY", "").strip()
_APEX_SECRET_B64 = os.getenv("APEX_SECRET_BASE64", "").strip()
_APEX_SECRET_RAW = os.getenv("APEX_SECRET", "").strip()

INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", "30"))
POST_EMPTY_EVERY_SECS = int(os.getenv("POST_EMPTY_EVERY_SECS", "0"))

SIZE_DECIMALS = int(os.getenv("SIZE_DECIMALS", "4"))
PRICE_DECIMALS = int(os.getenv("PRICE_DECIMALS", "4"))
PNL_DECIMALS = int(os.getenv("PNL_DECIMALS", "2"))
PNL_PCT_DECIMALS = int(os.getenv("PNL_PCT_DECIMALS", "2"))

DEBUG = os.getenv("DEBUG", "0").strip() not in ("", "0", "false", "False")

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

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
    """Resolve secret bytes from either base64 or raw env."""
    if _APEX_SECRET_B64:
        try:
            return base64.b64decode(_APEX_SECRET_B64)
        except Exception:
            logging.warning("Invalid APEX_SECRET_BASE64; falling back to APEX_SECRET")
    return _APEX_SECRET_RAW.encode("utf-8")

def _hmac_sha256_b64(msg: str, secret: bytes) -> str:
    sig = hmac.new(secret, msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(sig).decode("utf-8")

def _symbol_variants(sym: str) -> List[str]:
    """Normalize a few symbol spellings. E.g. APEX-USDT / APEXUSDT."""
    s = sym.upper().replace("/", "-")
    core = s.replace("-", "")
    return list({s, core, s.replace("-", "_"), core.replace("-", "_")})

# ---------- state ----------

class TickerBook:
    """Caches the latest mark/last per symbol from the public WS."""

    def __init__(self) -> None:
        self._mark: Dict[str, Decimal] = {}
        self._last: Dict[str, Decimal] = {}

    def update_from_public(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        Try to recognize a public ticker frame,
        and return the symbol we updated (or None).
        """
        # Many public topics push {topic: "...tickers...", contents: {symbol, last, markPrice, ...}}
        topic = payload.get("topic", "") or payload.get("channel", "")
        contents = payload.get("contents") or payload.get("data") or {}
        if not isinstance(contents, dict):
            return None

        # Common keys we’ve seen on tickers:
        sym = contents.get("symbol") or contents.get("contract") or contents.get("instId")
        if not sym:
            # Some feeds deliver an array of products; try to handle the first
            if isinstance(contents, list) and contents and isinstance(contents[0], dict):
                sym = contents[0].get("symbol")
                # If it’s a list of many, update them all:
                updated = None
                for item in contents:
                    u = self._update_one(item)
                    updated = updated or u
                return updated
            return None

        return self._update_one(contents)

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
            logging.debug(f"public price update {sym}  mark={mark} last={last}")
        return sym

    def mark_or_last(self, sym: str) -> Optional[Decimal]:
        for v in _symbol_variants(sym):
            if v in self._mark:
                return self._mark[v]
        for v in _symbol_variants(sym):
            if v in self._last:
                return self._last[v]
        return None


class PositionBook:
    """Keeps current open positions from private WS and computes PnL."""

    def __init__(self) -> None:
        self._pos: Dict[str, Dict[str, Any]] = {}  # key by canonical SYMBOL

    # ------ public API ------

    def update_from_accounts_payload(self, payload: Dict[str, Any]) -> int:
        """
        Accepts frames like:
        {
          "topic": "ws_omni_swap_accounts_v1",
          "type": "snapshot" | "delta",
          "contents": { "accounts": [ { ... } ], ... }
        }
        Within each account object, we scan recursively for arrays of positions.
        Returns number of positions found.
        """
        contents = payload.get("contents") or {}
        accounts = contents.get("accounts") or contents.get("contractAccounts") or []
        found = 0
        for acct in accounts if isinstance(accounts, list) else []:
            found += self._scan_one_account_for_positions(acct)
        return found

    def list_positions(self) -> List[Dict[str, Any]]:
        """Return a list of current open positions normalized as {symbol, side, size, entry, mark?}"""
        out = []
        for _, p in sorted(self._pos.items()):
            out.append(dict(p))
        return out

    def compute_pnl(self, prices: TickerBook) -> None:
        """Enrich stored positions with mark and PnL/PnL% using latest public price cache."""
        for k, p in self._pos.items():
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
            pnl = None

            try:
                if side == "LONG":
                    pnl = (mark - entry) * size
                else:
                    pnl = (entry - mark) * size
                nominal = (entry * size) if size != 0 else Decimal("0")
                pnlPct = (pnl / nominal * Decimal("100")) if nominal != 0 else None
            except Exception:
                pnl = None
                pnlPct = None

            p["mark"] = mark
            p["pnl"] = pnl
            p["pnlPct"] = pnlPct

    # ------ internals ------

    def _scan_one_account_for_positions(self, acct: Dict[str, Any]) -> int:
        """
        Recursively scan an account object for any array that looks like positions.
        A “position-like” dict usually has symbol/side/size and entry/avgEntryPrice keys.
        """
        found_here = []

        def looks_like_position(d: Dict[str, Any]) -> bool:
            keys = set(k.lower() for k in d.keys())
            has_sym = any(k in keys for k in ("symbol", "contract", "instid"))
            has_side = any(k in keys for k in ("side", "direction"))
            has_size = "size" in keys or "qty" in keys or "positionqty" in keys
            has_entry = any(k in keys for k in ("entryprice", "avgentryprice", "avgprice", "price"))
            return has_sym and has_side and has_size and has_entry

        def D(x) -> Optional[Decimal]:
            try:
                return Decimal(str(x))
            except Exception:
                return None

        def normalize_one(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            # extract basic fields robustly
            sym = d.get("symbol") or d.get("contract") or d.get("instId")
            side = (d.get("side") or d.get("direction") or "").upper()
            size = d.get("size") or d.get("qty") or d.get("positionQty")
            entry = d.get("entryPrice") or d.get("avgEntryPrice") or d.get("avgPrice") or d.get("price")

            if not sym or side not in ("LONG", "SHORT"):
                return None

            size = D(size)
            entry = D(entry)

            # filter “closed” or zero exposure
            if size is None or entry is None or size == 0:
                return None

            sym = sym.upper().replace("/", "-")
            return {
                "symbol": sym,
                "side": side,
                "size": size,
                "entry": entry,
                # "mark" / "pnl" / "pnlPct" will be filled later
            }

        # DFS through dict/list to find arrays of dicts that look like positions
        def walk(node: Any):
            if isinstance(node, dict):
                # direct position object?
                if looks_like_position(node):
                    maybe = normalize_one(node)
                    if maybe:
                        found_here.append(maybe)
                # or explore children
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(acct)

        # install into book
        for pos in found_here:
            self._pos[pos["symbol"]] = pos
        return len(found_here)


# ---------- discord ----------

async def post_discord(session: aiohttp.ClientSession, rows: List[Dict[str, Any]]) -> None:
    """Render a table and post to Discord."""
    if not DISCORD_WEBHOOK:
        logging.error("Missing DISCORD_WEBHOOK")
        return

    if rows:
        header = "Active ApeX Positions"
    else:
        header = "Active ApeX Positions"

    # table
    lines = []
    lines.append("SYMBOL      SIDE  SIZE      ENTRY       MARK        PNL         PNL%")
    lines.append("------      ----  ----      -----       ----        ---         ----")

    for p in rows:
        sym = p["symbol"].ljust(10)
        side = p["side"].ljust(4)
        size = _fmt_num(p["size"], SIZE_DECIMALS).rjust(8)
        entry = _fmt_num(p["entry"], PRICE_DECIMALS).rjust(11)
        mark = _fmt_num(p.get("mark"), PRICE_DECIMALS).rjust(11)
        pnl = _fmt_num(p.get("pnl"), PNL_DECIMALS).rjust(11)
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


# ---------- websocket clients ----------

class PublicWS:
    """Subscribe to public ticker/mark topic and feed a TickerBook."""

    CANDIDATE_TOPICS = [
        # If your DevTools shows another topic, set env PUBLIC_TICKER_TOPIC to it.
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
        if self._task:
            return
        self._task = asyncio.create_task(self._run())

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, heartbeat=20) as ws:
                        logging.info("Public WS connected (%s)", self.url)

                        # pick topic
                        topics = [self.topic] if self.topic else self.CANDIDATE_TOPICS
                        for t in topics:
                            if not t:
                                continue
                            sub = {"op": "subscribe", "args": [t]}
                            await ws.send_str(json.dumps(sub))
                            logging.info("Public subscribed: %s", t)

                        # main loop
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    self.prices.update_from_public(payload)
                                except Exception:
                                    if DEBUG:
                                        logging.debug("public parse error: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                logging.warning("Public WS closed. Reconnecting...")
            except Exception:
                logging.exception("Public WS error")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)


class PrivateWS:
    """Subscribe to private accounts topic and update PositionBook."""

    CANDIDATE_ACCT_TOPICS = [
        "ws_omni_swap_accounts_v1",
        "ws_zk_accounts_v3",           # legacy observed in the wild
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
        """Try API-key login frame (many Omni deployments require it)."""
        if not APEX_API_KEY or (not _APEX_SECRET_B64 and not _APEX_SECRET_RAW):
            logging.warning("No API credentials set; private WS may not authorize.")
            return

        ts = _now_ms()
        prehash = str(ts)
        sig = _hmac_sha256_b64(prehash, _secret_bytes())
        args = {"apiKey": APEX_API_KEY, "timestamp": ts, "signature": sig}
        frame = {"op": "login", "args": [json.dumps(args)]}
        await ws.send_str(json.dumps(frame))
        if DEBUG:
            logging.debug("sent WS login")

    async def _run(self):
        backoff = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, heartbeat=20) as ws:
                        logging.info("Private WS connected (%s)", self.url)

                        await self._login(ws)

                        # subscribe to account topics
                        for t in self.CANDIDATE_ACCT_TOPICS:
                            sub = {"op": "subscribe", "args": [t]}
                            await ws.send_str(json.dumps(sub))
                            logging.info("Private subscribed: %s", t)

                        # stream
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                    topic = payload.get("topic", "")
                                    if any(x in topic for x in ("accounts", "account")):
                                        n = self.positions.update_from_accounts_payload(payload)
                                        if DEBUG and n:
                                            logging.debug("private positions updated: +%d", n)
                                except Exception:
                                    if DEBUG:
                                        logging.debug("private parse error: %s", msg.data[:200])
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                logging.warning("Private WS closed. Reconnecting...")
            except Exception:
                logging.exception("Private WS error")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)


# ---------- main supervisor ----------

class Bridge:
    def __init__(self):
        self.prices = TickerBook()
        self.positions = PositionBook()
        self.public_ws = PublicWS(QUOTE_WS_PUBLIC, self.prices)
        self.private_ws = PrivateWS(QUOTE_WS_PRIVATE, self.positions)
        self._last_sent_snapshot: Optional[str] = None
        self._last_empty_post = 0.0

    def _snapshot_str(self, rows: List[Dict[str, Any]]) -> str:
        # canonical string to detect changes
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
                    # compute pnl with latest prices
                    self.positions.compute_pnl(self.prices)
                    rows = self.positions.list_positions()

                    snap = self._snapshot_str(rows)
                    changed = snap != self._last_sent_snapshot

                    if rows:
                        if changed:
                            await post_discord(session, rows)
                            self._last_sent_snapshot = snap
                        else:
                            # unchanged → post at slow cadence only if INTERVAL_SECONDS elapsed
                            # (avoids spam; keeps the channel fresh when price moves but pnl rounding
                            #  has not crossed a display tick).
                            pass
                    else:
                        # no positions
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
        "Bridge (WS) online. private=%s  public=%s  DEBUG=%s",
        QUOTE_WS_PRIVATE,
        QUOTE_WS_PUBLIC,
        DEBUG,
    )

    try:
        asyncio.run(Bridge().run())
    except KeyboardInterrupt:
        pass
