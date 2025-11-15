# tests/conftest.py
import os
import sys
from pathlib import Path

import pytest

# --- Make src/ importable ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from core.config_loader import Config  # noqa: E402
import core.paths as core_paths        # noqa: E402


@pytest.fixture
def tmp_project_root(tmp_path, monkeypatch):
    """
    Make core.paths.PROJECT_ROOT point to a temporary directory
    so data_path(...) writes into tmp_path/data instead of the real repo.
    """
    monkeypatch.setattr(core_paths, "PROJECT_ROOT", tmp_path)
    return tmp_path


@pytest.fixture
def tmp_data_dir(tmp_project_root):
    data_dir = tmp_project_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


@pytest.fixture
def tmp_db_path(tmp_project_root):
    db_dir = tmp_project_root / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "test.duckdb"


@pytest.fixture
def test_config(tmp_data_dir, tmp_db_path):
    """
    Returns a Config object pointing to temporary data + DB locations.
    We bypass load_config() here so tests don't depend on your real config.yml.
    """
    return Config(
        data_root=str(tmp_data_dir),
        duckdb_path=str(tmp_db_path),
        run_date=None,
        price_lookback_days=252,
        ret_lookback_days=60,
        vol_lookback_days=60,
        min_abs_ret_z=2.0,
        min_rvol=2.0,
        min_gap_atr=1.5,
        min_vol_spike_z=2.0,
        rs_top_pct=0.9,
        rs_bottom_pct=0.1,
        eps_surprise_threshold=0.1,
        news_intensity_z_threshold=2.0,
        max_workers=4,
        fmp_rate_limit_per_minute=750,
    )
