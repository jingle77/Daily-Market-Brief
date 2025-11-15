# Daily-Market-Brief  
_S&P 500 Daily Anomaly Radar for Content Creation_

This project powers a recurring “daily market brief” focused on **S&P 500 stocks that actually did something interesting yesterday** — not just “what SPY did.”

It:

- Ingests S&P 500 prices and universe data from the **Financial Modeling Prep (FMP)** API.
- Stores raw data in a **bronze** layer (parquet files).
- Builds a **silver** analytical layer in **DuckDB**.
- Computes a set of **anomaly/interestingness signals** per symbol for a given trading day.
- Exposes a **Streamlit app** that lets you:
  - Run the entire ingest + transform pipeline.
  - Select a trading date and anomaly threshold.
  - Browse the most interesting names (big moves, volume anomalies, technical events).
  - Drill into a symbol: 1-year chart + recent news + heuristic summary.
  - Download a ready-made **AI prompt + news bundle** as a `.txt` file to feed into an LLM for writing your article.

---

## Features (Current MVP)

- **FMP integration with rate limiting**
  - S&P 500 universe and daily price history.
  - Per-symbol news via FMP’s stock-specific news endpoint.
  - ThreadPool-based parallel fetching with a `RateLimiter` enforcing your **750 calls/min** subscription limit.

- **Data lake-ish structure**
  - **Bronze layer** (parquet): raw universe & price data, partitioned by `ingestion_date`.
  - **Silver layer** (DuckDB): cleaned, canonical `silver_universe` and `silver_price_daily` tables.

- **Signal engine (per symbol, per run_date)**
  - `ret_1d`: 1-day return.
  - `z_ret_1d`: z-score of 1-day return vs trailing N days.
  - `rvol_60`: relative volume vs trailing 60-day median.
  - `is_52w_high` / `is_52w_low`: new 52-week extremes.
  - `flag_200d_cross_up` / `flag_200d_cross_down`: 200-day moving average cross flags.
  - `event_flag_count`: how many technical/event flags fired.
  - `interestingness_score`: a composite score combining:
    - absolute move (`|z_ret_1d|`),
    - volume anomaly,
    - event flags.

- **Streamlit app**
  - Sidebar button to run **full bronze + silver refresh**.
  - Select **run_date** (trading day) and **min interestingness score**.
  - Overview table of top anomaly names (ranked by score).
  - Symbol detail view:
    - 1-year price chart up to run_date.
    - Recent per-symbol FMP news (last few days).
    - Heuristic summary (price/volume + flags + news skew).
    - **Downloadable AI prompt**: rich `.txt` with:
      - Instructions for the AI about the purpose of the tool.
      - Price/signal context.
      - All relevant news headlines + snippets.

---

## Project Structure

```text
Daily-Market-Brief/
├── db/
│   └── market.duckdb           # DuckDB database (created on first use)
├── data/
│   └── bronze/
│       ├── universe/           # bronze universe parquet: ingestion_date=YYYY-MM-DD.parquet
│       └── prices/             # bronze prices parquet: ingestion_date=YYYY-MM-DD.parquet
├── src/
│   ├── app/
│   │   ├── __init__.py
│   │   └── streamlit_app.py    # Streamlit UI
│   └── core/
│       ├── __init__.py
│       ├── config_loader.py    # Config dataclass + loader
│       ├── fmp_client.py       # FMPClient + RateLimiter
│       ├── bronze_ingest/
│       │   ├── __init__.py
│       │   ├── ingest_universe.py  # S&P 500 constituents -> bronze_universe
│       │   └── ingest_prices.py    # Prices -> bronze_prices
│       ├── silver_transform/
│       │   ├── __init__.py
│       │   ├── build_universe.py   # bronze_universe -> silver_universe (DuckDB)
│       │   └── build_price_daily.py# bronze_prices -> silver_price_daily (DuckDB)
│       └── signals/
│           ├── __init__.py
│           └── compute_signals.py  # anomaly signals per symbol/run_date
├── tests/
│   ├── test_bronze_ingest.py
│   ├── test_rate_limiter_and_fmp_client.py
│   └── test_silver_and_signals.py
├── requirements.txt
└── README.md
