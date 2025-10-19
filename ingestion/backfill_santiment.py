from __future__ import annotations
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict
import httpx
import pandas as pd
from typer import Typer, Option

from ..utils.logger import get_logger
from ..utils.config import load_config
from ..utils.db_connection import make_engine, ensure_tables
from ..utils.schemas import BFMarketSentiment, BFOnchainMetrics

app = Typer(help="Backfill historical sentiment data from Santiment")
logger = get_logger(__name__)


GQL_ENDPOINT = "https://api.santiment.net/graphql"


def _date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _mk_query(metric: str) -> str:
    return f"""
    query($slug: String!, $from: DateTime!, $to: DateTime!, $interval: String!) {{
      getMetric(metric: "{metric}") {{
        timeseriesData(
          slug: $slug
          from: $from
          to: $to
          interval: $interval
        ) {{
          datetime
          value
        }}
      }}
    }}
    """


async def fetch_metric(client: httpx.AsyncClient, key: str, slug: str, metric: str, start: str, end: str, interval: str = "1d") -> List[Dict]:
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    query = _mk_query(metric)
    variables = {"slug": slug, "from": f"{start}T00:00:00Z", "to": f"{end}T00:00:00Z", "interval": interval}
    r = await client.post(GQL_ENDPOINT, json={"query": query, "variables": variables}, headers=headers, timeout=30)
    if r.status_code != 200:
        logger.error({"event": "santiment_http_error", "status": r.status_code, "body": r.text[:200]})
        return []
    data = r.json()
    series = (((data or {}).get("data") or {}).get("getMetric") or {}).get("timeseriesData") or []
    return series


@app.command()
def run(
    start: str = Option(..., help="Start date YYYY-MM-DD"),
    end: str = Option(..., help="End date YYYY-MM-DD"),
    tokens: str = Option(..., help="Comma separated slugs or symbols e.g. bitcoin,ethereum or BTC,ETH"),
    run_id: str = Option(...),
):
    cfg = load_config()
    assert cfg.santiment_key, "SANTIMENT_KEY not set in config/api_keys.env"
    engine = make_engine(cfg.db_host, cfg.db_name, cfg.db_user, cfg.db_password, cfg.db_port)
    ensure_tables(engine)

    token_list = [t.strip() for t in tokens.split(",") if t.strip()]
    exports_dir = Path(__file__).resolve().parent.parent / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    # Map common symbols to slugs if needed
    symbol_to_slug = {"BTC": "bitcoin", "ETH": "ethereum"}

    async def main_async():
        sentiment_rows: List[BFMarketSentiment] = []
        onchain_rows: List[BFOnchainMetrics] = []
        async with httpx.AsyncClient() as client:
            for tok in token_list:
                slug = symbol_to_slug.get(tok.upper(), tok.lower())
                # Sentiment-like metrics
                metrics_sent = [
                    "sentiment_volume_consumed_1d",  # proxy for mentions
                    "social_volume_total",
                    "sentiment_balance_total",  # positive - negative
                ]
                # On-chain metrics
                metrics_onchain = [
                    "active_addresses_24h",
                    "dev_activity",
                    "transaction_volume",
                ]
                # Fetch sentiment metrics
                sent_data: Dict[str, List[Dict]] = {}
                for m in metrics_sent:
                    sent_data[m] = await fetch_metric(client, cfg.santiment_key or "", slug, m, start, end)

                # Consolidate by day
                by_day: Dict[str, Dict[str, float]] = {}
                for m, series in sent_data.items():
                    for point in series:
                        dt = (point.get("datetime") or "").split("T")[0]
                        by_day.setdefault(dt, {})[m] = float(point.get("value") or 0.0)

                for dt, vals in by_day.items():
                    sentiment_score = float(vals.get("sentiment_balance_total", 0.0))
                    positive = max(int(round((sentiment_score + abs(sentiment_score)) / 2)), 0)
                    negative = max(int(round((abs(sentiment_score) - sentiment_score) / 2)), 0)
                    neutral = 0
                    sentiment_rows.append(
                        BFMarketSentiment(
                            token_symbol=tok.upper(),
                            date=datetime.strptime(dt, "%Y-%m-%d").date(),
                            sentiment_score=sentiment_score,
                            positive_mentions=positive,
                            negative_mentions=negative,
                            neutral_mentions=neutral,
                            source_api="santiment",
                            timestamp_fetched=datetime.utcnow(),
                            backfill_run_id="",
                        )
                    )

                # Fetch on-chain metrics
                for m in metrics_onchain:
                    series = await fetch_metric(client, cfg.santiment_key or "", slug, m, start, end)
                    for point in series:
                        dt = (point.get("datetime") or "").split("T")[0]
                        onchain_rows.append(
                            BFOnchainMetrics(
                                token_symbol=tok.upper(),
                                date=datetime.strptime(dt, "%Y-%m-%d").date(),
                                metric_name=m,
                                metric_value=float(point.get("value") or 0.0),
                                source_api="santiment",
                                timestamp_fetched=datetime.utcnow(),
                                backfill_run_id="",
                            )
                        )

        # Export and insert
        if sentiment_rows:
            df_s = pd.DataFrame([r.__dict__ for r in sentiment_rows])
            df_s["backfill_run_id"] = run_id
            df_s.to_csv(exports_dir / f"santiment_sentiment_{run_id}.csv", index=False)
            df_s.to_parquet(exports_dir / f"santiment_sentiment_{run_id}.parquet", index=False)
            from sqlalchemy import text
            with engine.begin() as conn:
                for _, row in df_s.iterrows():
                    conn.execute(
                        text(
                            """
                            INSERT INTO bf_market_sentiment (
                              token_symbol, date, sentiment_score, positive_mentions, negative_mentions, neutral_mentions, source_api, timestamp_fetched, backfill_run_id
                            ) VALUES (
                              %(token_symbol)s, %(date)s, %(sentiment_score)s, %(positive_mentions)s, %(negative_mentions)s, %(neutral_mentions)s, %(source_api)s, %(timestamp_fetched)s, %(backfill_run_id)s
                            )
                            """
                        ),
                        row.to_dict(),
                    )
        if onchain_rows:
            df_o = pd.DataFrame([r.__dict__ for r in onchain_rows])
            df_o["backfill_run_id"] = run_id
            df_o.to_csv(exports_dir / f"santiment_onchain_{run_id}.csv", index=False)
            df_o.to_parquet(exports_dir / f"santiment_onchain_{run_id}.parquet", index=False)
            from sqlalchemy import text
            with engine.begin() as conn:
                for _, row in df_o.iterrows():
                    conn.execute(
                        text(
                            """
                            INSERT INTO bf_onchain_metrics (
                              token_symbol, date, metric_name, metric_value, source_api, timestamp_fetched, backfill_run_id
                            ) VALUES (
                              %(token_symbol)s, %(date)s, %(metric_name)s, %(metric_value)s, %(source_api)s, %(timestamp_fetched)s, %(backfill_run_id)s
                            )
                            """
                        ),
                        row.to_dict(),
                    )

        logger.info({"event": "santiment_backfill_complete", "sentiment_count": len(sentiment_rows), "onchain_count": len(onchain_rows), "run_id": run_id})

    import asyncio as _asyncio
    _asyncio.run(main_async())


if __name__ == "__main__":
    app()
