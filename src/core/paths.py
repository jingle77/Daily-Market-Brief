from pathlib import Path
import yaml

# Base project root = parent of src/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

def data_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath("data", *parts)

def db_path() -> Path:
    return PROJECT_ROOT.joinpath("db", "market.duckdb")

def config_path() -> Path:
    return PROJECT_ROOT.joinpath("config", "config.yml")
