# tests/test_silver_and_signals.py
from datetime import date, timedelta

import duckdb
import pandas as pd

from core.silver_transform.build_price_daily import build_silver_price_daily
from core.silver_transform.build_universe import build_silver_universe
from core.signals.compute_signals import compute_signals


def _make_date_range(n_days: int, start: date) -> list[date]:
    return [start + timedelta(days=i) for i in range(n_days)]


def test_silver_price_and_basic_signals(tmp_project_root, test_config):
    """
    End-to-end-ish:
      - Create bronze_universe and bronze_prices for one symbol (AAA)
      - Build silver_universe and silver_price_daily
      - Compute signals for the last date
      - Verify basic fields (ret_1d, z_ret_1d, rvol_60, 52w high flag, etc.)
    """
    data_root = tmp_project_root / "data"

    # 1) Create bronze_universe
    bronze_universe_dir = data_root / "bronze" / "universe"
    bronze_universe_dir.mkdir(parents=True, exist_ok=True)
    uni_df = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "name": "AAA Corp",
                "sector": "Tech",
                "subSector": "Software",
                "ingestion_date": "2025-01-01",
            }
        ]
    )
    uni_df.to_parquet(bronze_universe_dir / "ingestion_date=2025-01-01.parquet", index=False)

    # 2) Create bronze_prices for AAA: a simple, steadily rising series
    bronze_prices_dir = data_root / "bronze" / "prices"
    bronze_prices_dir.mkdir(parents=True, exist_ok=True)

    start_d = date(2025, 1, 1)
    dates = _make_date_range(70, start_d)  # 70 days of history
    # Simple pattern: price starts at 100 and increases by 1 each day
    closes = [100 + i for i in range(len(dates))]
    opens = closes  # for simplicity
    highs = [c + 1 for c in closes]
    lows = [c - 1 for c in closes]
    vols = [1_000 + (i % 5) * 100 for i in range(len(dates))]

    bronze_prices_df = pd.DataFrame(
        {
            "symbol": ["AAA"] * len(dates),
            "date": dates,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "adj_close": closes,
            "volume": vols,
            "ingestion_date": ["2025-01-10"] * len(dates),
        }
    )
    bronze_prices_df.to_parquet(
        bronze_prices_dir / "ingestion_date=2025-01-10.parquet", index=False
    )

    # 3) Build silver tables
    build_silver_universe(test_config)
    build_silver_price_daily(test_config)

    # Quick sanity check on silver_price_daily
    con = duckdb.connect(test_config.duckdb_path)
    silver_df = con.execute("SELECT * FROM silver_price_daily").df()
    con.close()

    assert len(silver_df) == len(dates)
    assert silver_df["symbol"].nunique() == 1

    # 4) Compute signals for the last date
    last_date = str(dates[-1])  # last trading day
    sig_df = compute_signals(test_config, run_date=last_date)

    # Should have exactly one row
    assert len(sig_df) == 1
    row = sig_df.iloc[0]

    # Basic sanity checks
    assert row["symbol"] == "AAA"
    assert str(row["run_date"]) == last_date

    # Since price is steadily increasing, ret_1d should be positive
    assert row["ret_1d"] > 0

    # rvol_60 should be around 1-ish, since we used a small oscillation pattern
    assert 0.5 < row["rvol_60"] < 2.5

    # Because our prices are strictly increasing, the last day should be a 52-week high
    # Note: row["is_52w_high"] is a numpy.bool_, so we check truthiness, not identity
    assert bool(row["is_52w_high"])  # instead of `is True`

    # We have a positive trend; z_ret_1d should be non-negative (it might not be huge)
    assert row["z_ret_1d"] >= 0

    # Interestingness score should be > 0
    assert row["interestingness_score"] > 0
