from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import List
import httpx
import pandas as pd
from typer import Typer, Option

from ..utils.logger import get_logger
from ..utils.config import load_config
from ..utils.db_connection import make_engine, ensure_tables
from ..utils.schemas import BFMarketSentiment

app = Typer(help="Backfill sentiment/engagement from LunarCrush")
logger = get_logger(__name__)


API_BASE = "https://lunarcrush.com/api3"


async def fetch_daily(client: httpx.AsyncClient, api_key: str, symbol: str, start: str, end: str) -> List[BFMarketSentiment]:
    # LunarCrush v3 example endpoint for assets timeseries
    params = {
        "symbol": symbol.upper(),
        "interval": "day",
        "data_points": 1000,
        "start_time": start,
        "end_time": end,
        "key": api_key,
    }
    url = f"{API_BASE}/assets"
    r = await client.get(url, params=params, timeout=30)
    if r.status_code != 200:
        logger.error({"event": "lunarcrush_http_error", "status": r.status_code, "body": r.text[:200]})
        return []
    payload = r.json()
    data = (payload.get("data") or [])
    rows: List[BFMarketSentiment] = []
    for series in data:
        ts = series.get("timeSeries", []) or series.get("time_series", [])
        for p in ts:
            # fields vary by plan, try common names
            dt = datetime.utcfromtimestamp(p.get("time") or p.get("timestamp") or 0).date()
            score = float(p.get("galaxy_score") or p.get("sentiment") or 0.0)
            pos = int(p.get("social_bullish") or p.get("social_positive") or 0)
            neg = int(p.get("social_bearish") or p.get("social_negative") or 0)
            neu = max(int((p.get("social_volume") or 0) - pos - neg), 0)
            rows.append(
                BFMarketSentiment(
                    token_symbol=symbol.upper(),
                    date=dt,
                    sentiment_score=score,
                    positive_mentions=pos,
                    negative_mentions=neg,
                    neutral_mentions=neu,
                    source_api="lunarcrush",
                    timestamp_fetched=datetime.utcnow(),
                    backfill_run_id="",
                )
            )
    return rows


@app.command()
def run(
    start: str = Option(..., help="Start date YYYY-MM-DD"),
    end: str = Option(..., help="End date YYYY-MM-DD"),
    tokens: str = Option(..., help="Comma separated symbols e.g. BTC,ETH"),
    run_id: str = Option(...),
):
    cfg = load_config()
    assert cfg.lunarcrush_key, "LUNARCRUSH_KEY not set in config/api_keys.env"
    engine = make_engine(cfg.db_host, cfg.db_name, cfg.db_user, cfg.db_password, cfg.db_port)
    ensure_tables(engine)

    token_list = [t.strip().upper() for t in tokens.split(",") if t.strip()]
    exports_dir = Path(__file__).resolve().parent.parent / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    async def main_async():
        all_rows: List[BFMarketSentiment] = []
        async with httpx.AsyncClient() as client:
            for sym in token_list:
                rows = await fetch_daily(client, cfg.lunarcrush_key or "", sym, f"{start}T00:00:00Z", f"{end}T00:00:00Z")
                all_rows.extend(rows)

        df = pd.DataFrame([r.__dict__ for r in all_rows]) if all_rows else pd.DataFrame()
        if not df.empty:
            df["backfill_run_id"] = run_id
            df.to_csv(exports_dir / f"lunarcrush_sentiment_{run_id}.csv", index=False)
            df.to_parquet(exports_dir / f"lunarcrush_sentiment_{run_id}.parquet", index=False)
            from sqlalchemy import text
            with engine.begin() as conn:
                for _, row in df.iterrows():
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
        logger.info({"event": "lunarcrush_backfill_complete", "count": len(all_rows), "run_id": run_id})

    import asyncio as _asyncio
    _asyncio.run(main_async())


if __name__ == "__main__":
    app()
