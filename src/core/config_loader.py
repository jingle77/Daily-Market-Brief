from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from .paths import config_path, data_path, db_path


@dataclass
class Config:
    data_root: str
    duckdb_path: str
    run_date: Optional[str]
    price_lookback_days: int
    ret_lookback_days: int
    vol_lookback_days: int
    min_abs_ret_z: float
    min_rvol: float
    min_gap_atr: float
    min_vol_spike_z: float
    rs_top_pct: float
    rs_bottom_pct: float
    eps_surprise_threshold: float
    news_intensity_z_threshold: float
    max_workers: int
    fmp_rate_limit_per_minute: int


def load_config() -> Config:
    """
    Load YAML config and resolve paths relative to the project.

    Also ensures that the data root directory and the parent directory
    of the DuckDB file exist so any code that connects/writes can
    safely assume the directory structure is there.
    """
    cfg_file = config_path()
    with cfg_file.open("r") as f:
        raw = yaml.safe_load(f)

    # Resolve data_root and duckdb_path from our paths helper
    data_root = data_path().as_posix()
    duckdb = db_path().as_posix()

    cfg = Config(
        data_root=data_root,
        duckdb_path=duckdb,
        run_date=raw.get("run_date"),
        price_lookback_days=raw.get("price_lookback_days", 252),
        ret_lookback_days=raw.get("ret_lookback_days", 60),
        vol_lookback_days=raw.get("vol_lookback_days", 60),
        min_abs_ret_z=raw.get("min_abs_ret_z", 2.0),
        min_rvol=raw.get("min_rvol", 2.0),
        min_gap_atr=raw.get("min_gap_atr", 1.5),
        min_vol_spike_z=raw.get("min_vol_spike_z", 2.0),
        rs_top_pct=raw.get("rs_top_pct", 0.9),
        rs_bottom_pct=raw.get("rs_bottom_pct", 0.1),
        eps_surprise_threshold=raw.get("eps_surprise_threshold", 0.1),
        news_intensity_z_threshold=raw.get("news_intensity_z_threshold", 2.0),
        max_workers=raw.get("max_workers", 8),
        fmp_rate_limit_per_minute=raw.get("fmp_rate_limit_per_minute", 750),
    )

    # Ensure directories exist so DuckDB and data writes don't blow up
    Path(cfg.data_root).mkdir(parents=True, exist_ok=True)
    Path(cfg.duckdb_path).parent.mkdir(parents=True, exist_ok=True)

    return cfg
