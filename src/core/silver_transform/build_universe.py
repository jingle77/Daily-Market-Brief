from pathlib import Path
import duckdb
from ..config_loader import Config, load_config
from ..paths import data_path

def build_silver_universe(cfg: Config) -> None:
    con = duckdb.connect(cfg.duckdb_path)
    bronze_path = data_path("bronze", "universe", "ingestion_date=*.parquet").as_posix()

    con.execute(
        f"""
        CREATE OR REPLACE TABLE silver_universe AS
        SELECT
            symbol,
            name       AS company_name,
            sector,
            subSector  AS sub_sector,
            TRUE       AS is_active
        FROM read_parquet('{bronze_path}');
        """
    )

    # Optionally materialize as Parquet
    out_dir = data_path("silver", "universe")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "universe.parquet"
    con.execute(f"COPY silver_universe TO '{out_path.as_posix()}' (FORMAT PARQUET);")
    con.close()

if __name__ == "__main__":
    cfg = load_config()
    build_silver_universe(cfg)
    print("Built silver_universe")
