"""
Data Loader
============
Unified interface to load data from both historical downloads and live collections.
Returns clean, project-ready DataFrames.

Usage:
    from data_loader import DataLoader

    loader = DataLoader()

    # Historical aggTrades for lead-lag analysis
    btc = loader.load_historical_aggtrades("BTCUSDT", "2025-03-01", "2025-03-07")
    eth = loader.load_historical_aggtrades("ETHUSDT", "2025-03-01", "2025-03-07")

    # Live depth snapshots for OFI
    depth = loader.load_live_depth("BTCUSDT")

    # Combined trades (historical + live)
    trades = loader.load_all_trades("BTCUSDT", "2025-03-01", "2025-03-07")
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from config import (
    HIST_AGGTRADES_DIR,
    HIST_TRADES_DIR,
    LIVE_DEPTH_DIR,
    LIVE_TRADES_DIR,
    PROCESSED_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


class DataLoader:
    """Unified data loading for all projects."""

    # ─── Historical Data ──────────────────────────────────────────

    def load_historical_aggtrades(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
    ) -> pd.DataFrame:
        """
        Load historical aggregated trades from downloaded parquet files.

        Returns DataFrame with columns:
            timestamp, price, quantity, is_buyer_maker,
            trade_direction, signed_volume, notional
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        data_dir = HIST_AGGTRADES_DIR / symbol
        if not data_dir.exists():
            logger.warning(f"No historical data for {symbol}. Run historical_downloader.py first.")
            return pd.DataFrame()

        files = sorted(data_dir.glob(f"{symbol}_aggTrades_*.parquet"))
        dfs = []
        for f in files:
            # Extract date from filename
            file_date_str = f.stem.split("_")[-1]
            file_date = date.fromisoformat(file_date_str)
            if start_date <= file_date <= end_date:
                dfs.append(pd.read_parquet(f))

        if not dfs:
            logger.warning(f"No data found for {symbol} between {start_date} and {end_date}")
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)

        logger.info(
            f"Loaded {len(df):,} aggTrades for {symbol} "
            f"({start_date} → {end_date})"
        )
        return df

    def load_historical_trades(
        self,
        symbol: str,
        start_date: Union[str, date],
        end_date: Union[str, date],
    ) -> pd.DataFrame:
        """Load historical individual trades."""
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        data_dir = HIST_TRADES_DIR / symbol
        if not data_dir.exists():
            return pd.DataFrame()

        files = sorted(data_dir.glob(f"{symbol}_trades_*.parquet"))
        dfs = []
        for f in files:
            file_date_str = f.stem.split("_")[-1]
            file_date = date.fromisoformat(file_date_str)
            if start_date <= file_date <= end_date:
                dfs.append(pd.read_parquet(f))

        if not dfs:
            return pd.DataFrame()

        df = pd.concat(dfs, ignore_index=True)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    # ─── Live Data ────────────────────────────────────────────────

    def load_live_depth(
        self,
        symbol: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load live order book depth snapshots.

        Returns DataFrame with columns:
            event_time, best_bid_price, best_bid_qty,
            best_ask_price, best_ask_qty, mid_price, spread,
            bid_depth_5, ask_depth_5, bid_p0..4, bid_q0..4, ask_p0..4, ask_q0..4
        """
        data_dir = LIVE_DEPTH_DIR / symbol
        if not data_dir.exists():
            logger.warning(f"No live depth data for {symbol}. Run live_streamer.py first.")
            return pd.DataFrame()

        files = sorted(data_dir.glob(f"{symbol}_depth_*.parquet"))
        if not files:
            return pd.DataFrame()

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

        # Convert event_time from millis to datetime
        if "event_time" in df.columns and df["event_time"].dtype in ("int64", "float64"):
            df["event_time"] = pd.to_datetime(df["event_time"], unit="ms")

        df.sort_values("event_time", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Time filter
        if start_time:
            df = df[df["event_time"] >= pd.Timestamp(start_time)]
        if end_time:
            df = df[df["event_time"] <= pd.Timestamp(end_time)]

        logger.info(f"Loaded {len(df):,} depth snapshots for {symbol}")
        return df

    def load_live_trades(
        self,
        symbol: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> pd.DataFrame:
        """Load live trade data."""
        data_dir = LIVE_TRADES_DIR / symbol
        if not data_dir.exists():
            return pd.DataFrame()

        files = sorted(data_dir.glob(f"{symbol}_trades_*.parquet"))
        if not files:
            return pd.DataFrame()

        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

        if "event_time" in df.columns and df["event_time"].dtype in ("int64", "float64"):
            df["event_time"] = pd.to_datetime(df["event_time"], unit="ms")
        if "trade_time" in df.columns and df["trade_time"].dtype in ("int64", "float64"):
            df["trade_time"] = pd.to_datetime(df["trade_time"], unit="ms")

        df.sort_values("event_time", inplace=True)
        df.reset_index(drop=True, inplace=True)

        if start_time:
            df = df[df["event_time"] >= pd.Timestamp(start_time)]
        if end_time:
            df = df[df["event_time"] <= pd.Timestamp(end_time)]

        return df

    # ─── Utilities ────────────────────────────────────────────────

    def resample_trades_to_bars(
        self,
        trades: pd.DataFrame,
        freq: str = "1s",
        timestamp_col: str = "timestamp",
    ) -> pd.DataFrame:
        """
        Resample trade data into OHLCV bars at given frequency.

        Useful for lead-lag analysis at uniform time intervals.
        """
        if trades.empty:
            return pd.DataFrame()

        df = trades.set_index(timestamp_col)

        bars = df["price"].resample(freq).ohlc()
        bars["volume"] = df["quantity"].resample(freq).sum()
        bars["notional"] = df["notional"].resample(freq).sum()
        bars["trade_count"] = df["price"].resample(freq).count()
        bars["net_volume"] = df["signed_volume"].resample(freq).sum()
        bars["buy_volume"] = (
            df.loc[df["trade_direction"] == 1, "quantity"].resample(freq).sum()
        )
        bars["sell_volume"] = (
            df.loc[df["trade_direction"] == -1, "quantity"].resample(freq).sum()
        )

        # Volume imbalance (useful for OFI)
        bars["volume_imbalance"] = bars["buy_volume"] - bars["sell_volume"]

        bars.dropna(subset=["open"], inplace=True)
        return bars

    def get_data_summary(self) -> dict:
        """Print summary of all available data."""
        summary = {"historical_aggtrades": {}, "historical_trades": {}, "live_depth": {}, "live_trades": {}}

        for symbol_dir in HIST_AGGTRADES_DIR.iterdir():
            if symbol_dir.is_dir():
                files = list(symbol_dir.glob("*.parquet"))
                if files:
                    summary["historical_aggtrades"][symbol_dir.name] = {
                        "files": len(files),
                        "date_range": f"{files[0].stem.split('_')[-1]} → {files[-1].stem.split('_')[-1]}",
                    }

        for symbol_dir in LIVE_DEPTH_DIR.iterdir():
            if symbol_dir.is_dir():
                files = list(symbol_dir.glob("*.parquet"))
                if files:
                    summary["live_depth"][symbol_dir.name] = {"files": len(files)}

        for symbol_dir in LIVE_TRADES_DIR.iterdir():
            if symbol_dir.is_dir():
                files = list(symbol_dir.glob("*.parquet"))
                if files:
                    summary["live_trades"][symbol_dir.name] = {"files": len(files)}

        return summary


if __name__ == "__main__":
    loader = DataLoader()
    summary = loader.get_data_summary()

    print("\n═══ Data Summary ═══")
    for category, data in summary.items():
        if data:
            print(f"\n{category}:")
            for symbol, info in data.items():
                print(f"  {symbol}: {info}")
        else:
            print(f"\n{category}: (empty)")
