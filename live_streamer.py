"""
Live WebSocket Data Streamer
=============================
Streams real-time data from Binance via WebSocket:
  1. Diff Depth Stream (@depth@100ms) - L2 order book updates every 100ms
  2. Trade Stream (@trade) - individual trades as they happen

Order Book Maintenance (per Binance docs):
  - Get a depth snapshot via REST API
  - Buffer depth updates from WebSocket
  - Apply diff updates: if update_id > snapshot last_update_id,
    set qty at price level (qty=0 means remove level)
  - Re-snapshot periodically to correct drift

Storage: Parquet files flushed every 5 minutes.

No API key needed - all market data streams are public.
"""

import asyncio
import json
import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    raise

from config import (
    BINANCE_API_BASE,
    BINANCE_WS_COMBINED,
    DEPTH_LEVELS,
    DEPTH_UPDATE_SPEED,
    LIVE_DEPTH_DIR,
    LIVE_TRADES_DIR,
    MAX_BUFFER_SIZE,
    PARQUET_FLUSH_INTERVAL_SEC,
    SNAPSHOT_INTERVAL_SEC,
    SYMBOLS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class OrderBookManager:
    """
    Maintains a local order book from WebSocket diff depth stream.

    This is the core component for Project 1 (OFI) and Project 3 (microstructure).
    The order book state gives us bid/ask prices and sizes needed for:
      - Bid-ask spread computation
      - Order flow imbalance (changes in best bid/ask sizes)
      - Kyle's lambda estimation
      - VPIN computation
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[float, float] = {}  # price → qty
        self.asks: dict[float, float] = {}  # price → qty
        self.last_update_id: int = 0
        self.initialized: bool = False
        self._pending_updates: list[dict] = []

    def get_snapshot(self) -> bool:
        """Fetch full order book snapshot from REST API."""
        url = f"{BINANCE_API_BASE}/depth?symbol={self.symbol}&limit=1000"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            self.bids = {float(p): float(q) for p, q in data["bids"]}
            self.asks = {float(p): float(q) for p, q in data["asks"]}
            self.last_update_id = data["lastUpdateId"]
            self.initialized = True

            # Apply any buffered updates
            for update in self._pending_updates:
                if update["u"] > self.last_update_id:
                    self._apply_update(update)
            self._pending_updates.clear()

            logger.info(
                f"[{self.symbol}] Snapshot: {len(self.bids)} bids, "
                f"{len(self.asks)} asks, last_id={self.last_update_id}"
            )
            return True
        except Exception as e:
            logger.error(f"[{self.symbol}] Snapshot failed: {e}")
            return False

    def process_depth_update(self, data: dict) -> Optional[dict]:
        """
        Process a diff depth update from WebSocket.

        Returns a dict with current top-of-book state if initialized,
        None otherwise. This state is what we log for analysis.
        """
        if not self.initialized:
            self._pending_updates.append(data)
            return None

        # Skip old updates
        if data["u"] <= self.last_update_id:
            return None

        self._apply_update(data)

        # Extract top-of-book state for logging
        return self._get_top_of_book(data)

    def _apply_update(self, data: dict):
        """Apply bid/ask updates to local book."""
        for price_str, qty_str in data.get("b", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price_str, qty_str in data.get("a", []):
            price, qty = float(price_str), float(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = data["u"]

    def _get_top_of_book(self, raw_update: dict) -> dict:
        """Extract top N levels + metadata for logging."""
        sorted_bids = sorted(self.bids.items(), key=lambda x: -x[0])[:DEPTH_LEVELS]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:DEPTH_LEVELS]

        best_bid = sorted_bids[0] if sorted_bids else (0.0, 0.0)
        best_ask = sorted_asks[0] if sorted_asks else (0.0, 0.0)

        return {
            "symbol": self.symbol,
            "event_time": raw_update.get("E", 0),
            "update_id": raw_update["u"],
            # Best bid/ask (Level 1)
            "best_bid_price": best_bid[0],
            "best_bid_qty": best_bid[1],
            "best_ask_price": best_ask[0],
            "best_ask_qty": best_ask[1],
            "mid_price": (best_bid[0] + best_ask[0]) / 2 if best_bid[0] and best_ask[0] else 0,
            "spread": best_ask[0] - best_bid[0] if best_bid[0] and best_ask[0] else 0,
            # Depth summary (for OFI)
            "bid_depth_5": sum(q for _, q in sorted_bids[:5]),
            "ask_depth_5": sum(q for _, q in sorted_asks[:5]),
            "bid_depth_10": sum(q for _, q in sorted_bids[:10]),
            "ask_depth_10": sum(q for _, q in sorted_asks[:10]),
            # Full top-5 levels (for microstructure analysis)
            **{f"bid_p{i}": sorted_bids[i][0] if i < len(sorted_bids) else 0.0 for i in range(5)},
            **{f"bid_q{i}": sorted_bids[i][1] if i < len(sorted_bids) else 0.0 for i in range(5)},
            **{f"ask_p{i}": sorted_asks[i][0] if i < len(sorted_asks) else 0.0 for i in range(5)},
            **{f"ask_q{i}": sorted_asks[i][1] if i < len(sorted_asks) else 0.0 for i in range(5)},
        }


class DataBuffer:
    """Buffers incoming data and flushes to Parquet periodically."""

    def __init__(self, name: str, output_dir: Path):
        self.name = name
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rows: list[dict] = []
        self.last_flush = time.time()
        self.total_flushed = 0

    def append(self, row: dict):
        self.rows.append(row)
        if (
            len(self.rows) >= MAX_BUFFER_SIZE
            or time.time() - self.last_flush >= PARQUET_FLUSH_INTERVAL_SEC
        ):
            self.flush()

    def flush(self):
        if not self.rows:
            return

        df = pd.DataFrame(self.rows)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = self.output_dir / f"{self.name}_{ts}.parquet"
        df.to_parquet(path, engine="pyarrow", index=False)

        self.total_flushed += len(self.rows)
        logger.info(
            f"[{self.name}] Flushed {len(self.rows):,} rows → {path.name} "
            f"(total: {self.total_flushed:,})"
        )
        self.rows.clear()
        self.last_flush = time.time()


class LiveStreamer:
    """
    Main streamer: connects to Binance combined WebSocket stream,
    processes depth updates and trades, maintains local order books.
    """

    def __init__(self, symbols: list[str]):
        self.symbols = [s.upper() for s in symbols]
        self.order_books: dict[str, OrderBookManager] = {
            s: OrderBookManager(s) for s in self.symbols
        }

        # Separate buffers for each data type
        self.depth_buffers: dict[str, DataBuffer] = {
            s: DataBuffer(f"{s}_depth", LIVE_DEPTH_DIR / s) for s in self.symbols
        }
        self.trade_buffers: dict[str, DataBuffer] = {
            s: DataBuffer(f"{s}_trades", LIVE_TRADES_DIR / s) for s in self.symbols
        }

        self._running = False
        self._last_snapshot: dict[str, float] = {s: 0 for s in self.symbols}

    def _build_stream_url(self) -> str:
        """Build combined WebSocket stream URL for all symbols."""
        streams = []
        for s in self.symbols:
            s_lower = s.lower()
            streams.append(f"{s_lower}@depth@{DEPTH_UPDATE_SPEED}")
            streams.append(f"{s_lower}@trade")
        return f"{BINANCE_WS_COMBINED}?streams={'/'.join(streams)}"

    async def _initialize_order_books(self):
        """Get initial snapshots for all order books."""
        for symbol, ob in self.order_books.items():
            success = ob.get_snapshot()
            if not success:
                logger.error(f"Failed to initialize {symbol} order book")
            self._last_snapshot[symbol] = time.time()

    async def _periodic_re_snapshot(self):
        """Re-snapshot order books periodically to correct drift."""
        while self._running:
            await asyncio.sleep(60)  # check every minute
            for symbol, ob in self.order_books.items():
                if time.time() - self._last_snapshot[symbol] >= SNAPSHOT_INTERVAL_SEC:
                    logger.info(f"[{symbol}] Periodic re-snapshot...")
                    ob.get_snapshot()
                    self._last_snapshot[symbol] = time.time()

    async def _periodic_flush(self):
        """Flush all buffers periodically."""
        while self._running:
            await asyncio.sleep(PARQUET_FLUSH_INTERVAL_SEC)
            for buf in list(self.depth_buffers.values()) + list(self.trade_buffers.values()):
                buf.flush()

    def _handle_depth(self, data: dict):
        """Process a depth update event."""
        symbol = data["s"]
        ob = self.order_books.get(symbol)
        if ob is None:
            return

        state = ob.process_depth_update(data)
        if state is not None:
            self.depth_buffers[symbol].append(state)

    def _handle_trade(self, data: dict):
        """Process a trade event."""
        symbol = data["s"]
        buf = self.trade_buffers.get(symbol)
        if buf is None:
            return

        row = {
            "symbol": symbol,
            "event_time": data["E"],
            "trade_id": data["t"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "buyer_order_id": data["b"],
            "seller_order_id": data["a"],
            "trade_time": data["T"],
            "is_buyer_maker": data["m"],
            "trade_direction": -1 if data["m"] else 1,
            "notional": float(data["p"]) * float(data["q"]),
        }
        buf.append(row)

    async def run(self):
        """Main event loop."""
        self._running = True
        url = self._build_stream_url()

        await self._initialize_order_books()

        # Start background tasks
        snapshot_task = asyncio.create_task(self._periodic_re_snapshot())
        flush_task = asyncio.create_task(self._periodic_flush())

        logger.info(f"Connecting to {url[:80]}...")
        logger.info(f"Streaming: {', '.join(self.symbols)}")
        logger.info(f"Press Ctrl+C to stop.\n")

        try:
            async for websocket in websockets.connect(url, ping_interval=20):
                try:
                    async for raw_msg in websocket:
                        msg = json.loads(raw_msg)
                        stream = msg.get("stream", "")
                        data = msg.get("data", {})

                        if "@depth@" in stream:
                            self._handle_depth(data)
                        elif "@trade" in stream:
                            self._handle_trade(data)

                except websockets.ConnectionClosed:
                    logger.warning("Connection closed, reconnecting in 5s...")
                    await asyncio.sleep(5)
                    # Re-snapshot after reconnect
                    await self._initialize_order_books()

        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            snapshot_task.cancel()
            flush_task.cancel()

            # Final flush
            for buf in list(self.depth_buffers.values()) + list(self.trade_buffers.values()):
                buf.flush()
            logger.info("Streamer stopped. All buffers flushed.")

    def stop(self):
        self._running = False


# ─── CLI ──────────────────────────────────────────────────────────

async def main():
    symbols = SYMBOLS
    streamer = LiveStreamer(symbols)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, streamer.stop)

    await streamer.run()


if __name__ == "__main__":
    asyncio.run(main())
