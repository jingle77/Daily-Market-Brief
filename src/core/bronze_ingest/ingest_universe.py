from datetime import date
from pathlib import Path
import pandas as pd

from ..config_loader import Config, load_config
from ..fmp_client import FMPClient
from ..paths import data_path

def ingest_universe(cfg: Config, client: FMPClient) -> Path:
    today = date.today().isoformat()
    raw = client.get_sp500_constituents()
    if not raw:
        raise RuntimeError("Universe ingest returned no data")

    df = pd.DataFrame(raw)
    df["ingestion_date"] = today

    out_dir = data_path("bronze", "universe")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"ingestion_date={today}.parquet"

    df.to_parquet(out_path, index=False)
    return out_path

if __name__ == "__main__":
    cfg = load_config()
    client = FMPClient()
    path = ingest_universe(cfg, client)
    print(f"Wrote bronze universe to {path}")
