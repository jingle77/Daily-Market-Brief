# tests/test_bronze_ingest.py
from pathlib import Path

import pandas as pd

from core.bronze_ingest.ingest_universe import ingest_universe
from core.bronze_ingest.ingest_prices import ingest_prices


class FakeFMPClient:
    """
    Minimal fake client for bronze ingestion tests.
    """

    def __init__(self, universe_rows=None, price_histories=None):
        self._universe_rows = universe_rows or []
        self._price_histories = price_histories or {}

    def get_sp500_constituents(self):
        return self._universe_rows

    def get_historical_prices_eod(self, symbol: str):
        # Return same structure FMP does: {"symbol":..., "historical": [...]}
        return {"symbol": symbol, "historical": self._price_histories.get(symbol, [])}


def test_ingest_universe_writes_parquet(tmp_project_root, test_config):
    """
    ingest_universe should write a parquet file under bronze/universe with
    an ingestion_date column and the expected symbols.
    """
    fake_universe = [
        {
            "symbol": "AAA",
            "name": "AAA Corp",
            "sector": "Tech",
            "subSector": "Software",
        },
        {
            "symbol": "BBB",
            "name": "BBB Inc",
            "sector": "Finance",
            "subSector": "Banks",
        },
    ]

    client = FakeFMPClient(universe_rows=fake_universe)

    out_path = ingest_universe(test_config, client=client)
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert set(df["symbol"]) == {"AAA", "BBB"}
    assert "ingestion_date" in df.columns
    assert df["name"].iloc[0] == "AAA Corp"


def test_ingest_prices_parallel_writes_parquet(tmp_project_root, test_config):
    """
    ingest_prices should fetch histories for each symbol in the latest
    bronze_universe parquet and write a combined bronze_prices parquet.
    """
    # 1) Create a bronze_universe file manually (ingest_universe test already verified that)
    bronze_universe_dir = tmp_project_root / "data" / "bronze" / "universe"
    bronze_universe_dir.mkdir(parents=True, exist_ok=True)
    uni_path = bronze_universe_dir / "ingestion_date=2025-01-01.parquet"
    uni_df = pd.DataFrame(
        [
            {"symbol": "AAA", "name": "AAA Corp", "sector": "Tech", "subSector": "Software", "ingestion_date": "2025-01-01"},
            {"symbol": "BBB", "name": "BBB Inc", "sector": "Finance", "subSector": "Banks", "ingestion_date": "2025-01-01"},
        ]
    )
    uni_df.to_parquet(uni_path, index=False)

    # 2) Fake price histories
    price_histories = {
        "AAA": [
            {"date": "2025-01-01", "open": 10, "high": 11, "low": 9, "close": 10.5, "adjClose": 10.5, "volume": 1000},
            {"date": "2025-01-02", "open": 10.5, "high": 11.5, "low": 10, "close": 11, "adjClose": 11, "volume": 1200},
        ],
        "BBB": [
            {"date": "2025-01-01", "open": 20, "high": 21, "low": 19, "close": 20.5, "adjClose": 20.5, "volume": 2000},
        ],
    }
    client = FakeFMPClient(price_histories=price_histories)

    out_path = ingest_prices(test_config, client=client)
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    # We should have 3 rows: 2 for AAA, 1 for BBB
    assert len(df) == 3
    assert set(df["symbol"]) == {"AAA", "BBB"}
    # Ingestion_date should be a single value (today), but we just assert it's present
    assert "ingestion_date" in df.columns
