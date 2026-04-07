# Binance Data Ingestion Pipeline

Data infrastructure for HFT/quant research projects. Collects trade-level and order book data from Binance for microstructure analysis.

## What This Feeds

| Project | Data Source | Key Fields |
|---|---|---|
| **P1: Order Flow Imbalance** | Live depth stream | bid/ask prices & sizes at 5 levels, 100ms updates |
| **P2: Lead-Lag Detection** | Historical aggTrades | BTC & ETH timestamps + prices, sub-second resolution |
| **P3: Microstructure Stats** | Both | Spread, depth, trade direction, signed volume |

## Quick Start

```bash
pip install -r requirements.txt
```

### 1. Download Historical Data (no API key needed)

```bash
# Download 7 days of BTC and ETH aggTrades
python historical_downloader.py --symbol BTCUSDT --start 2025-03-01 --end 2025-03-07 --type aggTrades
python historical_downloader.py --symbol ETHUSDT --start 2025-03-01 --end 2025-03-07 --type aggTrades

# Validate the download
python validate_data.py --symbol BTCUSDT --date 2025-03-01
```

### 2. Stream Live Order Book + Trades (no API key needed)

```bash
# Streams BTC-USDT and ETH-USDT depth (100ms) + trades
python live_streamer.py

# Data saved to data/raw/live_depth/ and data/raw/live_trades/
# Flushed to parquet every 5 minutes
# Ctrl+C to stop gracefully
```

### 3. Load Data in Your Analysis

```python
from data_loader import DataLoader

loader = DataLoader()

# Historical trades for lead-lag
btc = loader.load_historical_aggtrades("BTCUSDT", "2025-03-01", "2025-03-07")
eth = loader.load_historical_aggtrades("ETHUSDT", "2025-03-01", "2025-03-07")

# Resample to 1-second bars
btc_bars = loader.resample_trades_to_bars(btc, freq="1s")

# Live order book snapshots for OFI
depth = loader.load_live_depth("BTCUSDT")
```

## Data Schema

### aggTrades (Historical)
| Column | Type | Description |
|---|---|---|
| timestamp | datetime64 | Trade time (ms or μs precision) |
| price | float64 | Execution price |
| quantity | float64 | Trade size in base currency |
| is_buyer_maker | bool | True = taker sold (price went down) |
| trade_direction | int8 | +1 (taker buy) / -1 (taker sell) |
| signed_volume | float64 | direction × quantity |
| notional | float64 | price × quantity (USD value) |

### Depth Snapshots (Live)
| Column | Type | Description |
|---|---|---|
| event_time | datetime64 | Update time (100ms intervals) |
| best_bid_price/qty | float64 | Top of book bid |
| best_ask_price/qty | float64 | Top of book ask |
| mid_price | float64 | (best_bid + best_ask) / 2 |
| spread | float64 | best_ask - best_bid |
| bid_depth_5/10 | float64 | Cumulative size top 5/10 levels |
| bid_p0..4 / bid_q0..4 | float64 | Top 5 bid price levels + sizes |
| ask_p0..4 / ask_q0..4 | float64 | Top 5 ask price levels + sizes |

## Directory Structure

```
data/
├── raw/
│   ├── historical_aggtrades/
│   │   ├── BTCUSDT/
│   │   │   ├── BTCUSDT_aggTrades_2025-03-01.parquet
│   │   │   └── ...
│   │   └── ETHUSDT/
│   ├── historical_trades/
│   ├── live_depth/
│   │   ├── BTCUSDT/
│   │   │   ├── BTCUSDT_depth_20250301_091500.parquet
│   │   │   └── ...  (one file per 5-min flush)
│   │   └── ETHUSDT/
│   └── live_trades/
└── processed/
```

## Notes

- **No API key needed** for any of this. Both historical downloads and WebSocket streams are fully public.
- Historical data from `data.binance.vision` has timestamps in **milliseconds** (pre-2025) or **microseconds** (2025+). The pipeline handles both automatically.
- The live streamer maintains a local order book by applying diff depth updates to periodic REST snapshots. It re-snapshots every hour to correct any drift.
- `is_buyer_maker = True` means the maker was the buyer → the taker was the seller → trade_direction = -1 (aggressor sold).
