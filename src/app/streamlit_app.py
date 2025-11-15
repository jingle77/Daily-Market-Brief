import sys
import re
from pathlib import Path
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import streamlit as st

# Make sure src/ is on sys.path
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from core.config_loader import load_config
from core.signals.compute_signals import compute_signals
from core.fmp_client import FMPClient
from core.bronze_ingest.ingest_universe import ingest_universe
from core.bronze_ingest.ingest_prices import ingest_prices
from core.silver_transform.build_universe import build_silver_universe
from core.silver_transform.build_price_daily import build_silver_price_daily


def get_available_dates(cfg) -> list[str]:
    con = duckdb.connect(cfg.duckdb_path)
    try:
        dates = con.execute(
            "SELECT DISTINCT date FROM silver_price_daily ORDER BY date DESC"
        ).fetchall()
    except duckdb.Error:
        dates = []
    finally:
        con.close()
    return [str(d[0]) for d in dates]


def run_full_data_refresh(cfg):
    """Runs bronze universe + bronze prices + silver universe + silver price_daily
    using the same logic as the CLI, but callable from the UI.
    """
    client = FMPClient()

    # Universe
    ingest_universe(cfg, client=client)

    # Prices (parallel, rate-limited inside FMPClient)
    ingest_prices(cfg, client=client)

    # Silver tables
    build_silver_universe(cfg)
    build_silver_price_daily(cfg)


@st.cache_data(show_spinner=False, ttl=600)
def fetch_news_for_symbol(symbol: str, limit: int = 50):
    """Fetch and cache recent news for a symbol."""
    client = FMPClient()
    raw = client.get_stock_news(symbol, limit=limit)

    if isinstance(raw, dict):
        articles = raw.get("items", [])
    else:
        articles = raw or []

    return articles


def _classify_article_sentiment(title: str, text: str) -> int:
    """Super simple heuristic sentiment:
      >0 = positive, <0 = negative, 0 = neutral.
    """
    content = f"{title} {text}".lower()

    positive_keywords = [
        "beat estimates",
        "beats estimates",
        "beat earnings",
        "beats earnings",
        "raises guidance",
        "raise guidance",
        "upgraded",
        "upgrade",
        "record",
        "strong",
        "surge",
        "rally",
        "buy rating",
        "overweight",
        "positive",
    ]
    negative_keywords = [
        "misses estimates",
        "missed estimates",
        "miss earnings",
        "missed earnings",
        "cuts guidance",
        "cut guidance",
        "downgraded",
        "downgrade",
        "lawsuit",
        "probe",
        "investigation",
        "weak",
        "slump",
        "plunge",
        "sell rating",
        "underweight",
        "negative",
    ]

    score = 0
    for kw in positive_keywords:
        if kw in content:
            score += 1
    for kw in negative_keywords:
        if kw in content:
            score -= 1

    return score


def _safe_flag(val) -> bool:
    """Safely convert pandas/NumPy scalars (including pd.NA/NaN) to bool.

    Missing values are treated as False.
    """
    try:
        # pd.isna handles most scalar types including pd.NA and NaN
        if pd.isna(val):
            return False
    except Exception:
        pass

    try:
        return bool(val)
    except Exception:
        return False


def summarize_symbol_and_news(sym_row: pd.Series, articles: list[dict], run_date: str) -> str:
    """Heuristic summary combining:
      - price/volume move
      - key flags (52w high/low, 200d cross)
      - rough sentiment & headline themes

    Returns markdown string.
    """
    sym = sym_row.get("symbol", "")
    ret_1d = sym_row.get("ret_1d")
    z_ret_1d = sym_row.get("z_ret_1d")
    rvol_60 = sym_row.get("rvol_60")

    def _fmt_pct(x):
        try:
            return f"{float(x):+.2%}"
        except Exception:
            return "n/a"

    def _fmt_float(x, nd=2):
        try:
            return f"{float(x):.{nd}f}"
        except Exception:
            return "n/a"

    # Magnitude bucket based on |z|
    try:
        z_abs = abs(float(z_ret_1d))
        if z_abs >= 4:
            mag_label = "an extreme move"
        elif z_abs >= 2.5:
            mag_label = "a large move"
        elif z_abs >= 1.5:
            mag_label = "a moderate move"
        elif z_abs >= 0.5:
            mag_label = "a small move"
        else:
            mag_label = "a very small move"
    except Exception:
        mag_label = "a move"

    # Flag context
    flags = []
    if _safe_flag(sym_row.get("is_52w_high", False)):
        flags.append("reached a new 52-week high")
    if _safe_flag(sym_row.get("is_52w_low", False)):
        flags.append("hit a new 52-week low")
    if _safe_flag(sym_row.get("flag_200d_cross_up", False)):
        flags.append("crossed up through its 200-day moving average")
    if _safe_flag(sym_row.get("flag_200d_cross_down", False)):
        flags.append("crossed down through its 200-day moving average")

    flags_clause = ""
    if flags:
        if len(flags) == 1:
            flags_clause = f" and {flags[0]}"
        else:
            flags_clause = " and " + "; ".join(flags)

    # News sentiment & notable headlines
    pos_count = 0
    neg_count = 0
    neu_count = 0
    scored_articles = []

    for art in articles:
        title = art.get("title", "") or ""
        text = art.get("text", "") or ""
        sent_score = _classify_article_sentiment(title, text)

        if sent_score > 0:
            pos_count += 1
        elif sent_score < 0:
            neg_count += 1
        else:
            neu_count += 1

        published_raw = art.get("publishedDate") or art.get("published_at") or ""
        try:
            published_dt = pd.to_datetime(published_raw)
        except Exception:
            published_dt = pd.NaT

        scored_articles.append(
            {
                "article": art,
                "sent_score": sent_score,
                "published_dt": published_dt,
                "title": title,
            }
        )

    scored_articles.sort(
        key=lambda x: (
            x["published_dt"] if pd.notna(x["published_dt"]) else pd.Timestamp.min,
            abs(x["sent_score"]),
        ),
        reverse=True,
    )

    notable_titles = [s["title"] for s in scored_articles[:3] if s["title"]]

    total_news = pos_count + neg_count + neu_count
    if total_news == 0:
        news_clause = "No recent headlines were available in the last few days."
    else:
        if pos_count > neg_count and pos_count >= 1:
            skew = "positive"
        elif neg_count > pos_count and neg_count >= 1:
            skew = "negative"
        else:
            skew = "mixed"

        news_clause = (
            f"News flow over the last few days was **{skew}** "
            f"({pos_count} positive, {neg_count} negative, {neu_count} neutral headline"
            f"{'' if total_news == 1 else 's'})."
        )

        if notable_titles:
            clean_titles = [re.sub(r"[\s\.\-–—]+$", "", t) for t in notable_titles]
            joined = "; ".join(clean_titles)
            news_clause += f" Notable themes include: {joined}."

    # Build the intro price/volume sentence
    run_date_str = run_date
    try:
        rd = datetime.fromisoformat(run_date)
        run_date_str = rd.strftime("%Y-%m-%d")
    except Exception:
        pass

    price_sentence = (
        f"On **{run_date_str}**, **{sym}** moved **{_fmt_pct(ret_1d)} "
        f"({_fmt_float(z_ret_1d)}σ)** on **{_fmt_float(rvol_60, nd=1)}×** "
        "its 60-day median volume"
    )

    if flags_clause:
        price_sentence += flags_clause
    price_sentence += "."

    bullets = ""
    if flags:
        bullets = "\n\n**Key technical context:**\n" + "\n".join(
            f"- {f.capitalize()}" for f in flags
        )

    summary = price_sentence + "\n\n" + news_clause + bullets
    return summary


def build_ai_export_text(sym_row: pd.Series, articles: list[dict], run_date: str) -> str:
    """Build a plain-text block containing:
      - Instructions for an AI tool
      - Price/signal context
      - News articles (title, meta, snippet)

    This is what the user can download and paste into ChatGPT/other LLMs.
    """
    sym = sym_row.get("symbol", "")

    def _safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    ret_1d = _safe_float(sym_row.get("ret_1d"))
    z_ret_1d = _safe_float(sym_row.get("z_ret_1d"))
    rvol_60 = _safe_float(sym_row.get("rvol_60"))
    is_52w_high = _safe_flag(sym_row.get("is_52w_high", False))
    is_52w_low = _safe_flag(sym_row.get("is_52w_low", False))
    flag_200d_up = _safe_flag(sym_row.get("flag_200d_cross_up", False))
    flag_200d_down = _safe_flag(sym_row.get("flag_200d_cross_down", False))

    lines: list[str] = []

    # Instructions / context
    lines.append("INSTRUCTIONS FOR AI TOOL")
    lines.append("")
    lines.append(
        "You are helping me write a daily stock market recap article for Medium.com."
    )
    lines.append(
        "The article is part of a recurring series built on a tool called the "
        '"S&P 500 Daily Anomaly Radar."'
    )
    lines.append(
        "This tool scans the S&P 500 for unusual price and volume behavior "
        "and aggregates recent news headlines for each stock."
    )
    lines.append("")
    lines.append(
        "Your job: Based on the price/volume context and the news articles "
        "below, write a concise, neutral explanation of what may have driven "
        "the move in this stock."
    )
    lines.append("")
    lines.append("Constraints:")
    lines.append("- Do NOT give investment advice, recommendations, or price targets.")
    lines.append('- Do NOT claim certainty about causality; say things "may have contributed".')
    lines.append("- Assume the reader is a financially literate investor.")
    lines.append("- Focus on linking news themes to the price/volume move.")
    lines.append("")
    lines.append("Desired output:")
    lines.append(
        "- 2–4 short paragraphs I can lightly edit into my daily Medium article "
        "for this stock."
    )
    lines.append(
        "- Mention the price move, volume, and any key technical context "
        "(52-week highs/lows, 200-day moving average crosses)."
    )
    lines.append("- Then describe the main themes from the recent news.")
    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    lines.append("PRICE & SIGNAL CONTEXT")
    lines.append("----------------------")
    lines.append(f"Symbol: {sym}")
    lines.append(f"Run date (trading day): {run_date}")

    if ret_1d is not None:
        lines.append(f"1-day return: {ret_1d:+.4f} ({ret_1d:+.2%})")
    else:
        lines.append("1-day return: n/a")

    if z_ret_1d is not None:
        lines.append(f"1-day z-score (vs 60D closes): {z_ret_1d:.2f}")
    else:
        lines.append("1-day z-score: n/a")

    if rvol_60 is not None:
        lines.append(f"Relative volume (vs 60D median): {rvol_60:.2f}x")
    else:
        lines.append("Relative volume: n/a")

    tech_flags = []
    if is_52w_high:
        tech_flags.append("Reached a new 52-week high")
    if is_52w_low:
        tech_flags.append("Hit a new 52-week low")
    if flag_200d_up:
        tech_flags.append("Crossed UP through its 200-day moving average")
    if flag_200d_down:
        tech_flags.append("Crossed DOWN through its 200-day moving average")

    if tech_flags:
        lines.append("Technical flags:")
        for f in tech_flags:
            lines.append(f"  - {f}")
    else:
        lines.append("Technical flags: none of the tracked events triggered.")

    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    lines.append("NEWS ARTICLES (LAST FEW DAYS)")
    lines.append("-----------------------------")

    if not articles:
        lines.append(
            "No recent news articles were retrieved for this symbol in the last few days. "
            "Please explicitly mention that there was no obvious news catalyst, and that "
            "the move may have been driven by broader market factors or positioning."
        )
    else:
        for i, art in enumerate(articles, start=1):
            title = art.get("title", "Untitled")
            url = art.get("url", "")
            published = art.get("publishedDate", art.get("published_at", ""))
            site = art.get("site", art.get("source", "")) or ""
            text = art.get("text", "") or ""

            lines.append(f"Article {i}:")
            lines.append(f"  Title    : {title}")
            if site:
                lines.append(f"  Source   : {site}")
            if published:
                lines.append(f"  Published: {published}")
            if url:
                lines.append(f"  URL      : {url}")
            if text:
                snippet = text if len(text) <= 1000 else text[:1000] + "…"
                lines.append("  Snippet  :")
                for line in snippet.splitlines():
                    lines.append(f"    {line}")
            lines.append("")

        lines.append(
            "Use these articles to infer the main themes (e.g., earnings results, "
            "guidance changes, product launches, regulatory issues, "
            "analyst upgrades/downgrades, macro commentary, etc.)."
        )

    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    lines.append(
        "Now, please write the summary as described in the constraints above."
    )

    return "\n".join(lines)


def main():
    st.set_page_config(page_title="Market Brief Assistant", layout="wide")
    st.title("S&P 500 Daily Anomaly Radar")

    cfg = load_config()

    # ==============
    # DATA REFRESH UI
    # ==============
    st.sidebar.subheader("Data management")

    if st.sidebar.button("Run full data refresh (bronze + silver)"):
        ingest_progress = st.progress(0)
        ingest_status = st.empty()
        ingest_log_box = st.empty()
        logs: list[str] = []

        def log_step(msg: str, pct: int):
            logs.append(msg)
            ingest_status.write(f"**Ingestion status:** {msg}")
            ingest_log_box.write("\n".join(f"- {line}" for line in logs))
            ingest_progress.progress(pct)

        try:
            log_step("Starting full data refresh...", 5)
            client = FMPClient()

            log_step("Ingesting S&P 500 universe (bronze_universe)...", 20)
            ingest_universe(cfg, client=client)

            log_step("Ingesting historical prices for all symbols (bronze_prices)...", 50)
            ingest_prices(cfg, client=client)

            log_step("Building silver_universe table...", 70)
            build_silver_universe(cfg)

            log_step("Building silver_price_daily table...", 90)
            build_silver_price_daily(cfg)

            log_step("Data refresh complete.", 100)
            st.success("Data refresh completed successfully. You can now recompute signals.")
        except Exception as e:
            log_step(f"Error during data refresh: {e}", 100)
            st.error(f"Data refresh failed: {e}")

    st.markdown("---")

    # ==============
    # SIGNALS / ANALYSIS UI
    # ==============
    dates = get_available_dates(cfg)
    if not dates:
        st.error(
            "No silver_price_daily data found. "
            "Run the full data refresh from the sidebar first."
        )
        return

    st.sidebar.subheader("Analysis settings")
    run_date = st.sidebar.selectbox("Run date (trading day)", dates, index=0)
    st.sidebar.write(f"Selected date: {run_date}")

    min_score = st.sidebar.slider("Min interestingness score", 0.0, 10.0, 2.0, 0.1)

    # --- PROGRESS + STATUS AREA FOR SIGNALS ---
    sig_progress_bar = st.progress(0)
    sig_status_placeholder = st.empty()
    sig_log_placeholder = st.empty()

    sig_logs: list[str] = []

    def sig_log(msg: str, pct: int):
        sig_logs.append(msg)
        sig_status_placeholder.write(f"**Signal status:** {msg}")
        sig_log_placeholder.write("\n".join(f"- {line}" for line in sig_logs))
        sig_progress_bar.progress(pct)

    # Step 1: set run_date in config
    sig_log("Initializing configuration and resolving run_date...", 10)
    cfg.run_date = run_date

    # Step 2: compute signals (heavy part)
    sig_log("Computing signals from DuckDB (this may take a few seconds)...", 40)
    sig_df = compute_signals(cfg, run_date=run_date)

    # Step 3: apply filters and prepare view
    sig_log("Filtering symbols by interestingness and preparing overview...", 80)
    filtered = sig_df[sig_df["interestingness_score"] >= min_score].copy()
    filtered = filtered.reset_index(drop=True)

    sig_log("Done. Displaying results.", 100)

    # --- OVERVIEW ---
    st.subheader("Overview")
    st.write(
        f"{len(filtered)} symbols passed the interestingness threshold "
        f"(min_score={min_score}). Showing top 50 by score."
    )

    top_n = filtered.head(50)

    col1, col2, col3 = st.columns(3)
    col1.metric("Symbols (filtered)", len(filtered))
    col2.metric(
        "Max score",
        f"{top_n['interestingness_score'].max():.2f}" if not top_n.empty else "—",
    )
    col3.metric(
        "Avg |1D z-move|",
        f"{top_n['z_ret_1d'].abs().mean():.2f}σ" if not top_n.empty else "—",
    )

    st.subheader("Signals table")

    display_cols = [
        "symbol",
        "run_date",
        "ret_1d",
        "z_ret_1d",
        "rvol_60",
        "is_52w_high",
        "is_52w_low",
        "flag_200d_cross_up",
        "flag_200d_cross_down",
        "event_flag_count",
        "interestingness_score",
    ]

    if not top_n.empty and set(display_cols).issubset(top_n.columns):
        st.dataframe(
            top_n[display_cols],
            hide_index=True,
        )
    else:
        st.dataframe(top_n)

    # --- SYMBOL DETAIL ---
    st.subheader("Symbol detail")
    if not top_n.empty:
        sym = st.selectbox("Select a symbol", top_n["symbol"].tolist())
        sym_row = top_n[top_n["symbol"] == sym].iloc[0]

        st.write(
            f"**{sym}** – score: {sym_row['interestingness_score']:.2f}, "
            f"1D move: {sym_row['ret_1d']:.2%}, z={sym_row['z_ret_1d']:.2f}"
        )

        # Last 1 year of price history relative to run_date
        try:
            run_dt = datetime.fromisoformat(run_date)
        except ValueError:
            run_dt = pd.to_datetime(run_date).to_pydatetime()

        start_dt = (run_dt - timedelta(days=365)).date()

        con = duckdb.connect(cfg.duckdb_path)
        price_df = con.execute(
            "SELECT date, close, volume "
            "FROM silver_price_daily "
            "WHERE symbol = ? "
            "AND date >= ? "
            "ORDER BY date",
            [sym, start_dt],
        ).df()
        con.close()

        if not price_df.empty:
            st.line_chart(
                price_df.set_index("date")[["close"]],
                height=300,
            )
            st.caption(f"Close price history (last 1 year up to {run_date})")
        else:
            st.info("No price data found for last year for this symbol.")

        # Recent news for this symbol (last ~few days via FMP stock news endpoint)
        st.markdown("### Recent news")
        articles = fetch_news_for_symbol(sym, limit=50)

        # Downloadable AI prompt + articles
        export_text = build_ai_export_text(sym_row, articles, run_date)
        st.download_button(
            label="Download AI prompt + articles (.txt)",
            data=export_text,
            file_name=f"{sym}_{run_date}_news_prompt.txt",
            mime="text/plain",
            help="Download a ready-made prompt + articles you can paste into ChatGPT or another LLM.",
        )

        # Aggregated heuristic summary at top of news section
        summary_md = summarize_symbol_and_news(sym_row, articles, run_date)
        st.info(summary_md)

        # Individual articles below summary
        if not articles:
            st.info(
                "No recent news articles found for this symbol "
                "(news window is only a few days)."
            )
        else:
            for art in articles:
                title = art.get("title", "Untitled")
                url = art.get("url", "")
                published = art.get("publishedDate", art.get("published_at", ""))
                site = art.get("site", "")

                header = f"**{title}**"
                if url:
                    header = f"**[{title}]({url})**"

                st.markdown(header)
                meta = " · ".join([p for p in [published, site] if p])
                if meta:
                    st.caption(meta)

                text = art.get("text", "")
                if text:
                    snippet = text if len(text) <= 400 else text[:400] + "…"
                    st.write(snippet)

                st.markdown("---")

        st.markdown("#### Raw feature row")
        st.json(sym_row.to_dict())
    else:
        st.info("No symbols passed the filters.")


if __name__ == "__main__":
    main()
