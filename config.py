"""
Binance Data Ingestion Pipeline - Configuration
================================================
Supports:
  1. Historical trade data download from data.binance.vision (no API key needed)
  2. Live L2 order book depth + trade stream via WebSocket (no API key needed)
  3. Storage in Parquet format with proper timestamps

Target pairs for our 3 projects:
  - BTC-USDT: Primary pair for OFI, microstructure stats (most liquid)
  - ETH-USDT: Secondary pair for lead-lag detection with BTC
"""

from pathlib import Path

# ─── Storage ───────────────────────────────────────────────────────
DATA_DIR = Path("./data")
RAW_DIR = DATA_DIR / "raw"
HIST_TRADES_DIR = RAW_DIR / "historical_trades"
HIST_AGGTRADES_DIR = RAW_DIR / "historical_aggtrades"
LIVE_TRADES_DIR = RAW_DIR / "live_trades"
LIVE_DEPTH_DIR = RAW_DIR / "live_depth"
PROCESSED_DIR = DATA_DIR / "processed"

for d in [HIST_TRADES_DIR, HIST_AGGTRADES_DIR, LIVE_TRADES_DIR, LIVE_DEPTH_DIR, PROCESSED_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── Symbols ───────────────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT"]

# ─── Binance Public Data Archive ──────────────────────────────────
# No API key needed - fully public
BINANCE_DATA_BASE = "https://data.binance.vision/data"

# Available data types: aggTrades, trades, klines
# We want trades (individual fills) and aggTrades (aggregated by taker order)
# aggTrades are smaller and sufficient for OFI/lead-lag

# ─── WebSocket Streams ────────────────────────────────────────────
# No API key needed for market data streams
BINANCE_WS_BASE = "wss://stream.binance.com:9443/ws"
BINANCE_WS_COMBINED = "wss://stream.binance.com:9443/stream"

# Depth stream: 100ms updates, top 20 levels
# Trade stream: real-time individual trades
DEPTH_UPDATE_SPEED = "100ms"  # options: 1000ms, 100ms
DEPTH_LEVELS = 20  # for snapshot requests

# ─── REST API (for order book snapshots) ──────────────────────────
BINANCE_API_BASE = "https://api.binance.com/api/v3"

# ─── Live Collection Settings ─────────────────────────────────────
PARQUET_FLUSH_INTERVAL_SEC = 300  # flush to parquet every 5 minutes
SNAPSHOT_INTERVAL_SEC = 3600      # full OB snapshot every hour for validation
MAX_BUFFER_SIZE = 50_000          # max rows before force flush
