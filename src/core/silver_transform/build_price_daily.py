from pathlib import Path
import duckdb
from ..config_loader import Config, load_config
from ..paths import data_path

def build_silver_price_daily(cfg: Config) -> None:
    con = duckdb.connect(cfg.duckdb_path)

    bronze_glob = data_path("bronze", "prices", "ingestion_date=*.parquet").as_posix()

    # Deduplicate by (symbol, date), keep latest ingestion_date
    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver_price_daily AS
        SELECT
            symbol,
            date,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        FROM (
            SELECT
                symbol,
                date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                ingestion_date,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, date
                    ORDER BY ingestion_date DESC
                ) AS rn
            FROM read_parquet('{bronze_glob}')
        )
        WHERE rn = 1;
        """
    )

    out_dir = data_path("silver", "price_daily")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "price_daily.parquet"
    con.execute(f"COPY silver_price_daily TO '{out_path.as_posix()}' (FORMAT PARQUET);")
    con.close()

if __name__ == "__main__":
    cfg = load_config()
    build_silver_price_daily(cfg)
    print("Built silver_price_daily")
