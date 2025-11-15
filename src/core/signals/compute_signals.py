from typing import Optional

import duckdb
import pandas as pd

from ..config_loader import Config, load_config


def resolve_run_date(cfg: Config, con: duckdb.DuckDBPyConnection) -> str:
    """
    Resolve the run_date:
      - if cfg.run_date is set, use that
      - otherwise, use the latest date available in silver_price_daily
    """
    if cfg.run_date:
        return cfg.run_date

    row = con.execute("SELECT max(date) AS max_date FROM silver_price_daily").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("silver_price_daily is empty; run silver transforms first.")
    return str(row[0])


def compute_signals(cfg: Config, run_date: Optional[str] = None) -> pd.DataFrame:
    """
    Compute daily signals for a given run_date using silver_price_daily and silver_universe.

    MVP signals included:
      - ret_1d, z_ret_1d (60D window)
      - rvol_60 (relative volume vs 60D median)
      - 52-week high/low flags
      - 200D moving average cross flags
      - basic event_flag_count
      - simple interestingness_score

    Returns a pandas DataFrame sorted by interestingness_score descending,
    with at most one row per (symbol, run_date).
    """
    con = duckdb.connect(cfg.duckdb_path)

    if run_date is None:
        run_date = resolve_run_date(cfg, con)

    query = f"""
    WITH base AS (
        SELECT
            p.symbol,
            p.date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.volume,
            LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS close_d2,
            LAG(p.volume) OVER (PARTITION BY p.symbol ORDER BY p.date) AS volume_d2,
            AVG(p.close) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS sma_50,
            AVG(p.close) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS sma_200,
            AVG(p.close) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
            ) AS close_60d_mean,
            STDDEV_POP(p.close) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
            ) AS close_60d_std,
            MEDIAN(p.volume) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING
            ) AS vol_60d_median,
            MAX(p.high) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 251 PRECEDING AND 1 PRECEDING
            ) AS high_252d_max,
            MIN(p.low) OVER (
                PARTITION BY p.symbol
                ORDER BY p.date
                ROWS BETWEEN 251 PRECEDING AND 1 PRECEDING
            ) AS low_252d_min
        FROM silver_price_daily p
        JOIN silver_universe u
          ON p.symbol = u.symbol
        WHERE u.is_active
    ),
    returns AS (
        SELECT
            *,
            (close - close_d2) / NULLIF(close_d2, 0) AS ret_1d
        FROM base
    )
    SELECT
        symbol,
        date AS run_date,
        close AS close_d1,
        open AS open_d1,
        high AS high_d1,
        low  AS low_d1,
        volume AS volume_d1,
        close_d2,
        volume_d2,
        sma_50,
        sma_200,
        close_60d_mean,
        close_60d_std,
        vol_60d_median,
        high_252d_max,
        low_252d_min,
        ret_1d,
        CASE
          WHEN close_60d_std IS NULL OR close_60d_std = 0 THEN NULL
          ELSE (close - close_60d_mean) / close_60d_std
        END AS z_ret_1d,
        CASE
          WHEN vol_60d_median IS NULL OR vol_60d_median = 0 THEN NULL
          ELSE volume / vol_60d_median
        END AS rvol_60,
        high >= high_252d_max AS is_52w_high,
        low  <= low_252d_min AS is_52w_low,
        LAG(close > sma_200) OVER (PARTITION BY symbol ORDER BY date) AS above_200_prev,
        (close > sma_200) AS above_200_curr
    FROM returns
    WHERE date = DATE '{run_date}';
    """

    df = con.execute(query).df()

    # Normalize run_date to a plain date, not datetime
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date

    if df.empty:
        con.close()
        raise RuntimeError(f"No signals rows for run_date={run_date}")

    # 200D cross flags
    df["flag_200d_cross_up"] = (df["above_200_prev"] == False) & (df["above_200_curr"] == True)
    df["flag_200d_cross_down"] = (df["above_200_prev"] == True) & (df["above_200_curr"] == False)

    # Big move flag
    df["flag_large_move"] = df["z_ret_1d"].abs() >= cfg.min_abs_ret_z

    # High relative volume flag
    df["flag_high_rvol"] = df["rvol_60"] >= cfg.min_rvol

    # 52w flags (volume-conditioned)
    df["flag_52w_high"] = df["is_52w_high"] & df["flag_high_rvol"]
    df["flag_52w_low"] = df["is_52w_low"] & df["flag_high_rvol"]

    # Event flag count
    event_flags = [
        "flag_large_move",
        "flag_high_rvol",
        "flag_52w_high",
        "flag_52w_low",
        "flag_200d_cross_up",
        "flag_200d_cross_down",
    ]
    df["event_flag_count"] = df[event_flags].sum(axis=1)

    # Simple interestingness score
    df["interestingness_score"] = (
        0.5 * df["z_ret_1d"].abs().clip(upper=4).fillna(0)
        + 0.3 * df["rvol_60"].clip(upper=5).fillna(0)
        + 0.2 * df["event_flag_count"].fillna(0)
    )

    # Sort by interestingness, then dedupe per (symbol, run_date)
    df = df.sort_values("interestingness_score", ascending=False)
    df = df.drop_duplicates(subset=["symbol", "run_date"], keep="first").reset_index(drop=True)

    con.close()
    return df


if __name__ == "__main__":
    cfg = load_config()
    signals = compute_signals(cfg)
    print(signals.head(10))
