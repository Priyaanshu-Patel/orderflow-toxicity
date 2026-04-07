# CLAUDE.md — Orderflow Toxicity Research Pipeline

## Project Overview
Implement and analyse microstructure signals on Binance BTC-USDT and ETH-USDT.
Primary reference: **"The Price Impact of Order Book Events" — Cont, Kukanov, Stoikov (2014)**

Three planned projects:
| # | Project | Signal | Data Required |
|---|---------|--------|---------------|
| P1 | Order Flow Imbalance (OFI) | Best bid/ask quantity changes | Live depth (L2, 100ms) |
| P2 | Lead-Lag Detection | Cross-asset trade timing | Historical aggTrades |
| P3 | Microstructure Stats | Spread, depth, Kyle's λ, VPIN | Both |

---

## Repository Layout

```
orderflow_toxicity/          ← repo root (working directory)
├── config.py                ← paths, symbols, stream settings
├── data_loader.py           ← unified load interface (historical + live)
├── historical_downloader.py ← downloads aggTrades/trades from data.binance.vision
├── live_streamer.py         ← WebSocket L2 depth + trade stream → parquet
├── validate_data.py         ← data quality checks and summary stats
├── requirements.txt
├── logs/                    ← streamer logs (git-ignored)
├── data/                    ← all data (git-ignored)
│   └── raw/
│       ├── historical_aggtrades/
│       │   ├── BTCUSDT/     ← 7 files: 2025-03-01 → 2025-03-07
│       │   └── ETHUSDT/     ← 7 files: 2025-03-01 → 2025-03-07
│       ├── live_depth/      ← populated by live_streamer.py
│       │   ├── BTCUSDT/
│       │   └── ETHUSDT/
│       └── live_trades/     ← populated by live_streamer.py
└── project1_ofi/            ← P1 analysis code
    ├── ofi.py
    ├── analysis.py
    ├── notebook.ipynb
    └── results/
```

---

## Data Available

### Historical aggTrades (downloaded, ready to use)
- **BTCUSDT**: 2025-03-01 → 2025-03-07 (7 days, ~1.5M rows/day)
- **ETHUSDT**: 2025-03-01 → 2025-03-07 (7 days)
- Source: `data.binance.vision` public archive — no API key needed
- **Use for**: trade imbalance, lead-lag, OHLCV bars

```python
from data_loader import DataLoader
loader = DataLoader()
btc = loader.load_historical_aggtrades("BTCUSDT", "2025-03-01", "2025-03-07")
```

Columns: `timestamp, price, quantity, is_buyer_maker, trade_direction, signed_volume, notional`

### Live Depth Snapshots (requires live_streamer.py to be running)
- Location: `data/raw/live_depth/BTCUSDT/`
- **Use for**: OFI computation (Cont/Kukanov/Stoikov)
- Update frequency: 100ms per depth event
- Flushed to parquet every 5 minutes

```python
depth = loader.load_live_depth("BTCUSDT")
```

Columns: `event_time, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
          mid_price, spread, bid_depth_5, ask_depth_5, bid_p0..4, bid_q0..4, ask_p0..4, ask_q0..4`

> **Note**: Binance does not publish historical L2 order book data. Live depth must be
> collected in real-time via `live_streamer.py`. There is no way to backfill it.

---

## Running the Live Streamer

Start in background (collects BTCUSDT + ETHUSDT simultaneously):

```bash
mkdir -p logs
nohup venv/bin/python live_streamer.py > logs/live_streamer.log 2>&1 &
echo "PID: $!"
```

Check it's running:

```bash
tail -f logs/live_streamer.log
ps aux | grep live_streamer
```

Stop it:

```bash
kill <PID>
# or gracefully via SIGTERM — it flushes all buffers before exit
```

Validate collected data:

```bash
venv/bin/python validate_data.py --symbol BTCUSDT --live
```

---

## Project 1: OFI Signal

### Objective
Implement the Cont/Kukanov/Stoikov (2014) OFI measure and quantify its predictive power
for short-horizon price movements on BTC-USDT.

### OFI Formula (event level, per 100ms depth update)

```
e_bid(t) = bid_q(t) - bid_q(t-1)   if bid_p(t) >= bid_p(t-1)
         = -bid_q(t-1)              if bid_p(t) <  bid_p(t-1)

e_ask(t) = -(ask_q(t) - ask_q(t-1)) if ask_p(t) <= ask_p(t-1)
         = ask_q(t-1)               if ask_p(t) >  ask_p(t-1)

OFI(t) = e_bid(t) + e_ask(t)
```

Then aggregate OFI over windows: 1s, 5s, 10s, 30s, 60s.

### Comparison Signals
- **Trade imbalance**: net signed volume from aggTrades over same windows
- **Depth imbalance**: `(bid_depth_5 - ask_depth_5) / (bid_depth_5 + ask_depth_5)`

### Step 1: `project1_ofi/ofi.py`
- `compute_event_ofi(depth_df)` → event-level OFI series
- `aggregate_ofi(ofi_series, windows)` → dict of resampled OFI DataFrames
- `compute_trade_imbalance(trades_df, windows)` → for comparison
- `compute_depth_imbalance(depth_df)` → instantaneous depth ratio

### Step 2: `project1_ofi/analysis.py`
- Forward mid-price returns at each horizon h ∈ {1s, 5s, 10s, 30s, 60s}
- OLS regression: `ΔP(t, t+h) = α + β·OFI(t) + ε`
- Report: R², β, t-statistic, p-value per horizon
- Compare OFI vs trade imbalance vs depth imbalance
- IC (rank correlation) of each signal vs forward return
- Decay curve: R² vs horizon

### Step 3: `project1_ofi/notebook.ipynb`
- Time series: OFI vs mid-price changes
- R² by horizon bar chart
- Signal decay curve
- Scatter panels: OFI vs forward returns (1s, 5s, 30s)
- Rolling R² (stationarity check)
- Summary statistics table

### Success Criteria
- OFI R² at 1s horizon: 5–15% (typical for liquid crypto)
- OFI outperforms trade imbalance at short horizons
- Clear decay in R² as horizon increases
- All results reproducible from notebook

---

## Coding Standards

### Imports
All analysis code runs from repo root. Never use `sys.path` hacks.

```python
# Correct
from data_loader import DataLoader

# Wrong — do not do this
import sys; sys.path.insert(0, '..')
from binance_ingestion.data_loader import DataLoader
```

### Data paths
All paths are relative to repo root via `config.py`. Never hardcode paths.

```python
from config import LIVE_DEPTH_DIR, HIST_AGGTRADES_DIR
```

### Dependencies
Add new packages to `requirements.txt`. Install inside venv:

```bash
venv/bin/pip install <package>
venv/bin/pip freeze | grep <package> >> requirements.txt
```

### Never commit
- `data/` — too large, re-downloadable or re-streamable
- `venv/` — reproducible via `pip install -r requirements.txt`
- `logs/` — operational output
- `.env` files — no secrets needed currently, but follow this if ever added
- `skills-lock.json` — Claude Code internal

---

## Safety & Operational Notes

1. **No API key needed** for any data collection. Both `data.binance.vision` and Binance
   WebSocket streams are fully public.

2. **Rate limits**: The historical downloader makes one HTTP request per day of data.
   No throttling needed, but don't hammer the endpoint in a tight loop.

3. **WebSocket reconnection**: `live_streamer.py` auto-reconnects on disconnect and
   re-snapshots the order book. It is safe to leave running indefinitely.

4. **Order book drift**: The streamer re-snapshots every hour via REST to correct any
   accumulated diff-update errors. Do not disable this.

5. **Data integrity**: Always call `validate_data.py` after a download or after stopping
   the streamer. Check for timestamp gaps > 5s in live depth data (indicates a disconnect).

6. **Parquet files are append-only by convention**. Never overwrite existing parquet files
   in `data/raw/`. If re-downloading, delete the file first and let the downloader recreate it.
