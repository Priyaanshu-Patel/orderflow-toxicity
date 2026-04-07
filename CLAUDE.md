# CLAUDE.md — Project 1: Order Flow Imbalance (OFI) Signal

## Objective
Implement the Cont/Kukanov/Stoikov (2014) Order Flow Imbalance measure and quantify its predictive power for short-horizon price movements on Binance BTC-USDT.

## Key Paper
"The Price Impact of Order Book Events" — Cont, Kukanov, Stoikov (2014)
- OFI captures net order flow pressure from changes in best bid/ask quantities
- Shown to have significant linear relationship with contemporaneous price changes
- R² of OFI regression is the primary success metric

## Data Available

### Live Depth (primary data source for this project)
Location: `../data/raw/live_depth/BTCUSDT/`
Load via:
```python
import sys; sys.path.insert(0, '..')
from binance_ingestion.data_loader import DataLoader
loader = DataLoader()
depth = loader.load_live_depth("BTCUSDT")
```
Columns: event_time, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, mid_price, spread, bid_depth_5, ask_depth_5, bid_p0..4, bid_q0..4, ask_p0..4, ask_q0..4
Update frequency: 100ms

### Live Trades (supplementary)
Location: `../data/raw/live_trades/BTCUSDT/`
```python
trades = loader.load_live_trades("BTCUSDT")
```
Columns: event_time, price, quantity, is_buyer_maker, trade_direction, notional

## Implementation Plan

### Step 1: `ofi.py` — Core OFI Computation
Implement at event (100ms) level:
```
At each depth update t:
  e_bid(t) = 1{bid_p(t) >= bid_p(t-1)} * (bid_q(t) - bid_q(t-1))  if bid_p rises or stays
           - bid_q(t-1)                                              if bid_p drops
  
  e_ask(t) = -1{ask_p(t) <= ask_p(t-1)} * (ask_q(t) - ask_q(t-1))  if ask_p drops or stays
           + ask_q(t-1)                                              if ask_p rises

  OFI(t) = e_bid(t) + e_ask(t)
```
Then aggregate OFI over windows: 1s, 5s, 10s, 30s, 60s

Also implement for comparison:
- **Trade imbalance**: net signed volume over same windows
- **Depth imbalance**: (bid_depth_5 - ask_depth_5) / (bid_depth_5 + ask_depth_5)

### Step 2: `analysis.py` — Predictive Power Analysis
- Compute mid-price returns over forward windows: 1s, 5s, 10s, 30s, 60s
- Regress: ΔP(t, t+h) = α + β × OFI(t) + ε
- Report: R², β coefficient, t-statistic for each horizon h
- Compare R² across OFI vs trade imbalance vs depth imbalance
- Compute information coefficient (IC) = rank correlation of signal vs forward return
- Decay analysis: how quickly does OFI's predictive power decay with horizon?

### Step 3: `notebook.ipynb` — Results & Visualization
- Time series plot of OFI vs mid-price changes
- R² by horizon bar chart
- Signal decay curve
- Scatter: OFI vs forward returns (1s, 5s, 30s panels)
- Rolling R² to check stationarity of relationship
- Summary statistics table

## Output Files
```
project1_ofi/
├── CLAUDE.md          ← this file
├── ofi.py             ← OFI computation functions
├── analysis.py        ← regressions, IC, decay analysis
├── notebook.ipynb     ← all results and plots
└── results/           ← saved figures and summary stats
```

## Success Criteria
- OFI R² at 1s horizon should be 5-15% (typical for liquid crypto)
- OFI should outperform simple trade imbalance at short horizons
- Clear decay in R² as horizon increases
- All results reproducible from notebook

## What Impresses HFT Interviewers About This Project
- You understand that order flow, not price, is the primary signal source
- You computed OFI correctly from raw order book data, not from trades
- You measured predictive power properly (forward returns, not contemporaneous)
- You compared multiple signal variants and showed which works best
- Decay analysis shows you think about signal half-life and execution windows
