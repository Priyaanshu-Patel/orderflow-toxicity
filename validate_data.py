"""
Data Validation & Quick Stats
===============================
Run this after downloading data to verify quality and see what you're working with.

Usage:
    python validate_data.py --symbol BTCUSDT --date 2025-03-01
"""

import sys
from datetime import date

import pandas as pd

from data_loader import DataLoader


def validate_aggtrades(symbol: str, target_date: str):
    """Validate downloaded aggTrades data."""
    loader = DataLoader()
    d = date.fromisoformat(target_date)
    df = loader.load_historical_aggtrades(symbol, d, d)

    if df.empty:
        print(f"No data for {symbol} on {target_date}. Download it first:")
        print(f"  python historical_downloader.py --symbol {symbol} --start {target_date} --end {target_date}")
        return

    print(f"\n{'═' * 60}")
    print(f" {symbol} aggTrades — {target_date}")
    print(f"{'═' * 60}")

    print(f"\n  Rows:           {len(df):>15,}")
    print(f"  Time range:     {df['timestamp'].min()} → {df['timestamp'].max()}")
    duration_hours = (df["timestamp"].max() - df["timestamp"].min()).total_seconds() / 3600
    print(f"  Duration:       {duration_hours:.1f} hours")
    print(f"  Trades/sec avg: {len(df) / (duration_hours * 3600):.1f}")

    print(f"\n  ── Price ──")
    print(f"  Open:           {df['price'].iloc[0]:>15,.2f}")
    print(f"  Close:          {df['price'].iloc[-1]:>15,.2f}")
    print(f"  High:           {df['price'].max():>15,.2f}")
    print(f"  Low:            {df['price'].min():>15,.2f}")
    print(f"  Return:         {((df['price'].iloc[-1] / df['price'].iloc[0]) - 1) * 100:>14.2f}%")

    print(f"\n  ── Volume ──")
    print(f"  Total qty:      {df['quantity'].sum():>15,.4f}")
    print(f"  Total notional: ${df['notional'].sum():>14,.0f}")
    print(f"  Avg trade size: {df['quantity'].mean():>15,.6f}")
    print(f"  Median trade:   {df['quantity'].median():>15,.6f}")

    print(f"\n  ── Order Flow ──")
    buys = df[df["trade_direction"] == 1]
    sells = df[df["trade_direction"] == -1]
    print(f"  Buy trades:     {len(buys):>15,} ({100 * len(buys) / len(df):.1f}%)")
    print(f"  Sell trades:    {len(sells):>15,} ({100 * len(sells) / len(df):.1f}%)")
    print(f"  Buy volume:     {buys['quantity'].sum():>15,.4f}")
    print(f"  Sell volume:    {sells['quantity'].sum():>15,.4f}")
    print(f"  Net flow:       {df['signed_volume'].sum():>15,.4f}")

    # Check for gaps (>5 seconds between consecutive trades)
    time_diffs = df["timestamp"].diff().dt.total_seconds()
    big_gaps = time_diffs[time_diffs > 5]
    print(f"\n  ── Data Quality ──")
    print(f"  Null values:    {df.isnull().sum().sum()}")
    print(f"  Gaps > 5s:      {len(big_gaps)}")
    if len(big_gaps) > 0:
        print(f"  Max gap:        {time_diffs.max():.1f}s")
    print(f"  Duplicate IDs:  {df['agg_trade_id'].duplicated().sum()}")
    print(f"  Sorted:         {df['timestamp'].is_monotonic_increasing}")

    # Resample to 1-second bars for a quick preview
    loader_inst = DataLoader()
    bars = loader_inst.resample_trades_to_bars(df, freq="1s")
    print(f"\n  ── 1-Second Bars Preview ──")
    print(f"  Total bars:     {len(bars):>15,}")
    print(f"  Avg volume/bar: {bars['volume'].mean():>15,.6f}")
    print(f"  Avg trades/bar: {bars['trade_count'].mean():>15,.1f}")

    print(f"\n  ── Sample Data (first 5 rows) ──")
    print(df[["timestamp", "price", "quantity", "trade_direction", "signed_volume"]].head().to_string(index=False))
    print()


def validate_live_depth(symbol: str):
    """Validate live depth data."""
    loader = DataLoader()
    df = loader.load_live_depth(symbol)

    if df.empty:
        print(f"No live depth data for {symbol}. Run live_streamer.py first.")
        return

    print(f"\n{'═' * 60}")
    print(f" {symbol} Live Depth Data")
    print(f"{'═' * 60}")

    print(f"\n  Rows:           {len(df):,}")
    print(f"  Time range:     {df['event_time'].min()} → {df['event_time'].max()}")
    print(f"  Avg spread:     {df['spread'].mean():.4f}")
    print(f"  Avg mid price:  {df['mid_price'].mean():.2f}")
    print(f"  Avg bid depth5: {df['bid_depth_5'].mean():.4f}")
    print(f"  Avg ask depth5: {df['ask_depth_5'].mean():.4f}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--date", default=None, help="Date for historical validation (YYYY-MM-DD)")
    parser.add_argument("--live", action="store_true", help="Validate live depth data")
    args = parser.parse_args()

    if args.live:
        validate_live_depth(args.symbol)
    elif args.date:
        validate_aggtrades(args.symbol, args.date)
    else:
        print("Specify --date YYYY-MM-DD for historical or --live for live data")
