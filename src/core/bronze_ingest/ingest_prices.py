# src/core/bronze_ingest/ingest_prices.py
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from ..config_loader import Config
from ..fmp_client import FMPClient

import logging

logger = logging.getLogger(__name__)


def _load_latest_universe_parquet(cfg: Config) -> Path:
    """
    Find the latest bronze/universe parquet file under cfg.data_root.
    """
    bronze_universe_dir = Path(cfg.data_root) / "bronze" / "universe"
    paths = sorted(bronze_universe_dir.glob("ingestion_date=*.parquet"))
    if not paths:
        raise RuntimeError(f"No bronze universe parquet files found in {bronze_universe_dir}")
    return paths[-1]


def _normalize_hist_response(symbol: str, resp):
    """
    FMP endpoints are inconsistent:
      - /api/v3/historical-price-full returns {"symbol": ..., "historical": [...]}
      - /stable/historical-price-eod/full returns [...]
    Normalize both into a list of price dicts.
    """
    if isinstance(resp, dict):
        hist = resp.get("historical", [])
    elif isinstance(resp, list):
        hist = resp
    else:
        logger.error("Unexpected response type for %s: %s", symbol, type(resp))
        return None

    if not hist:
        logger.warning("No historical data for %s", symbol)
        return None

    return hist


def _fetch_prices_for_symbol(symbol: str, client: FMPClient) -> Optional[pd.DataFrame]:
    try:
        raw = client.get_historical_prices_eod(symbol)
    except Exception as e:
        logger.error("Fetching prices for %s: %s", symbol, e)
        return None

    hist = _normalize_hist_response(symbol, raw)
    if hist is None:
        return None

    df = pd.DataFrame(hist)

    # Normalize column names
    if "adjClose" in df.columns and "adj_close" not in df.columns:
        df = df.rename(columns={"adjClose": "adj_close"})

    if "adj_close" not in df.columns:
        # Fall back to close if adjusted not available
        if "close" not in df.columns:
            logger.error("Missing close/adj_close for %s", symbol)
            return None
        df["adj_close"] = df["close"]

    if "date" not in df.columns:
        logger.error("Missing date column for %s", symbol)
        return None

    df["symbol"] = symbol
    df["date"] = pd.to_datetime(df["date"]).dt.date

    required_cols = ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error("Missing columns for %s: %s", symbol, missing)
        return None

    return df[required_cols]


def ingest_prices(cfg: Config, client: Optional[FMPClient] = None) -> Path:
    """
    Ingest full historical prices for all symbols in the latest bronze_universe parquet.
    Writes a single parquet file under bronze/prices with an ingestion_date partition.
    """
    if client is None:
        client = FMPClient()

    uni_path = _load_latest_universe_parquet(cfg)
    uni_df = pd.read_parquet(uni_path)
    symbols = sorted(uni_df["symbol"].unique().tolist())

    max_workers = cfg.max_workers or 4
    logger.info("Ingesting prices for %d symbols with max_workers=%d", len(symbols), max_workers)

    frames = []
    total = len(symbols)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_prices_for_symbol, sym, client): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                df = future.result()
            except Exception as e:
                logger.error("Unhandled exception fetching %s: %s", sym, e)
                df = None

            completed += 1
            if df is not None and not df.empty:
                frames.append(df)

            if completed % 25 == 0 or completed == total:
                logger.info("Completed %d/%d symbols", completed, total)

    if not frames:
        raise RuntimeError("No historical price data ingested for any symbol.")

    all_df = pd.concat(frames, ignore_index=True)
    ingestion_date = date.today().isoformat()
    all_df["ingestion_date"] = ingestion_date

    bronze_prices_dir = Path(cfg.data_root) / "bronze" / "prices"
    bronze_prices_dir.mkdir(parents=True, exist_ok=True)
    out_path = bronze_prices_dir / f"ingestion_date={ingestion_date}.parquet"
    all_df.to_parquet(out_path, index=False)
    logger.info("Wrote bronze prices to %s", out_path)

    return out_path
