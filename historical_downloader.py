"""
Historical Data Downloader
==========================
Downloads trade data from Binance's public data archive (data.binance.vision).
No API key required. Data available from 2017 onwards.

Data types downloaded:
  - aggTrades: Aggregated trades (grouped by taker order). Columns:
      agg_trade_id, price, quantity, first_trade_id, last_trade_id,
      timestamp, is_buyer_maker, is_best_match
  - trades: Individual trade records. Columns:
      trade_id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch

For our projects:
  - OFI needs trade direction (is_buyer_maker) + price + qty → aggTrades suffice
  - Lead-lag needs timestamps + prices → aggTrades suffice
  - Microstructure needs trade-level detail → trades for finer analysis
"""

import io
import zipfile
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from config import (
    BINANCE_DATA_BASE,
    HIST_AGGTRADES_DIR,
    HIST_TRADES_DIR,
    SYMBOLS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── Column schemas ───────────────────────────────────────────────

AGGTRADES_COLS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "timestamp",
    "is_buyer_maker",
    "is_best_match",
]

TRADES_COLS = [
    "trade_id",
    "price",
    "quantity",
    "quote_quantity",
    "timestamp",
    "is_buyer_maker",
    "is_best_match",
]


def _download_and_extract_zip(url: str) -> Optional[bytes]:
    """Download a zip file from Binance and extract the CSV content."""
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            logger.warning(f"Not found: {url}")
            return None
        resp.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            # Each zip contains exactly one CSV
            csv_name = zf.namelist()[0]
            return zf.read(csv_name)
    except zipfile.BadZipFile:
        logger.error(f"Bad zip file: {url}")
        return None
    except requests.RequestException as e:
        logger.error(f"Download failed for {url}: {e}")
        return None


def download_aggtrades_daily(
    symbol: str,
    target_date: date,
    output_dir: Optional[Path] = None,
) -> Optional[Path]:
    """
    Download one day of aggregated trades for a symbol.

    Returns path to the saved parquet file, or None on failure.
    """
    output_dir = output_dir or HIST_AGGTRADES_DIR / symbol
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = target_date.strftime("%Y-%m-%d")
    parquet_path = output_dir / f"{symbol}_aggTrades_{date_str}.parquet"

    if parquet_path.exists():
        logger.info(f"Already exists: {parquet_path.name}")
        return parquet_path

    url = (
        f"{BINANCE_DATA_BASE}/spot/daily/aggTrades/"
        f"{symbol}/{symbol}-aggTrades-{date_str}.zip"
    )

    csv_bytes = _download_and_extract_zip(url)
    if csv_bytes is None:
        return None

    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        header=None,
        names=AGGTRADES_COLS,
        dtype={
            "agg_trade_id": "int64",
            "price": "float64",
            "quantity": "float64",
            "first_trade_id": "int64",
            "last_trade_id": "int64",
            "timestamp": "int64",
            "is_buyer_maker": "bool",
            "is_best_match": "bool",
        },
    )

    # Convert timestamp: Binance uses milliseconds (or microseconds from 2025+)
    if df["timestamp"].iloc[0] > 1e15:
        # Microsecond timestamps (2025+)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
    else:
        # Millisecond timestamps (pre-2025)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    # Derived columns useful for our projects
    df["trade_direction"] = df["is_buyer_maker"].map({True: -1, False: 1})
    # is_buyer_maker=True means the MAKER was the buyer,
    # so the TAKER was the seller → price moved down → direction = -1
    df["signed_volume"] = df["trade_direction"] * df["quantity"]
    df["notional"] = df["price"] * df["quantity"]

    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    logger.info(
        f"Saved {len(df):,} aggTrades for {symbol} on {date_str} → {parquet_path.name}"
    )
    return parquet_path


def download_trades_daily(
    symbol: str,
    target_date: date,
    output_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Download one day of individual trades."""
    output_dir = output_dir or HIST_TRADES_DIR / symbol
    output_dir.mkdir(parents=True, exist_ok=True)

    date_str = target_date.strftime("%Y-%m-%d")
    parquet_path = output_dir / f"{symbol}_trades_{date_str}.parquet"

    if parquet_path.exists():
        logger.info(f"Already exists: {parquet_path.name}")
        return parquet_path

    url = (
        f"{BINANCE_DATA_BASE}/spot/daily/trades/"
        f"{symbol}/{symbol}-trades-{date_str}.zip"
    )

    csv_bytes = _download_and_extract_zip(url)
    if csv_bytes is None:
        return None

    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        header=None,
        names=TRADES_COLS,
        dtype={
            "trade_id": "int64",
            "price": "float64",
            "quantity": "float64",
            "quote_quantity": "float64",
            "timestamp": "int64",
            "is_buyer_maker": "bool",
            "is_best_match": "bool",
        },
    )

    if df["timestamp"].iloc[0] > 1e15:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    df["trade_direction"] = df["is_buyer_maker"].map({True: -1, False: 1})
    df["signed_volume"] = df["trade_direction"] * df["quantity"]

    df.to_parquet(parquet_path, engine="pyarrow", index=False)
    logger.info(
        f"Saved {len(df):,} trades for {symbol} on {date_str} → {parquet_path.name}"
    )
    return parquet_path


def download_date_range(
    symbol: str,
    start_date: date,
    end_date: date,
    data_type: str = "aggTrades",
) -> list[Path]:
    """
    Download a range of dates for a symbol.

    Args:
        symbol: e.g. "BTCUSDT"
        start_date: inclusive start
        end_date: inclusive end
        data_type: "aggTrades" or "trades"

    Returns:
        List of paths to successfully downloaded parquet files.
    """
    downloader = download_aggtrades_daily if data_type == "aggTrades" else download_trades_daily
    paths = []
    current = start_date

    while current <= end_date:
        path = downloader(symbol, current)
        if path is not None:
            paths.append(path)
        current += timedelta(days=1)

    logger.info(
        f"Downloaded {len(paths)} days of {data_type} for {symbol} "
        f"({start_date} to {end_date})"
    )
    return paths


def load_aggtrades(
    symbol: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Load previously downloaded aggTrades into a single DataFrame.
    Downloads missing dates automatically.
    """
    paths = download_date_range(symbol, start_date, end_date, "aggTrades")
    if not paths:
        return pd.DataFrame()

    dfs = [pd.read_parquet(p) for p in paths]
    df = pd.concat(dfs, ignore_index=True)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(
        f"Loaded {len(df):,} aggTrades for {symbol} "
        f"({start_date} to {end_date})"
    )
    return df


# ─── CLI usage ────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download Binance historical trade data")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--type",
        default="aggTrades",
        choices=["aggTrades", "trades"],
        help="Data type to download",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    paths = download_date_range(args.symbol, start, end, args.type)
    print(f"\nDownloaded {len(paths)} files.")
