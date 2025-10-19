from __future__ import annotations
import asyncio
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List

import httpx
import pandas as pd
from typer import Typer, Option
from sqlalchemy import text

from ..utils.logger import get_logger
from ..utils.config import load_config
from ..utils.db_connection import make_engine, ensure_tables
from ..utils.schemas import BFHistoricalMarketData

app = Typer(help="Backfill daily OHLCV from CoinAPI")
logger = get_logger(__name__)


def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _symbol_id(symbol: str, quote: str = "USD", exchange: str = "BITSTAMP") -> str:
    return f"{exchange}_SPOT_{symbol.upper()}_{quote.upper()}"


async def fetch_day(client: httpx.AsyncClient, api_key: str, symbol: str, day: date) -> List[BFHistoricalMarketData]:
    # CoinAPI daily OHLCV official endpoint shape
    params = {
        "period_id": "1DAY",
        "time_start": day.strftime("%Y-%m-%dT00:00:00"),
        "time_end": (day + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00"),
        "limit": 1,
    }
    headers = {"X-CoinAPI-Key": api_key}
    url = f"https://rest.coinapi.io/v1/ohlcv/{_symbol_id(symbol)}/history"
    try:
        r = await client.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            # rate limited, brief backoff and single retry
            await asyncio.sleep(1.0)
            r = await client.get(url, headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            logger.error({"event": "coinapi_http_error", "symbol": symbol, "day": str(day), "status": r.status_code, "body": r.text[:200]})
            return []
        data = r.json()
        if not data:
            return []
        row = data[0]
        item = BFHistoricalMarketData(
            token_symbol=symbol,
            date=day,
            open_price=float(row.get("price_open", 0.0)),
            high_price=float(row.get("price_high", 0.0)),
            low_price=float(row.get("price_low", 0.0)),
            close_price=float(row.get("price_close", 0.0)),
            volume=float(row.get("volume_traded", 0.0)),
            timestamp_fetched=datetime.utcnow(),
            source_api="coinapi",
            backfill_run_id="",
        )
        return [item]
    except Exception as e:
        logger.error({"event": "coinapi_exception", "symbol": symbol, "day": str(day), "error": str(e)})
        return []


def upsert_and_export(engine, rows: List[BFHistoricalMarketData], run_id: str, exports_dir: Path):
    if not rows:
        return
    # Enrich with run_id and dataframe export
    df = pd.DataFrame([r.__dict__ for r in rows])
    df["backfill_run_id"] = run_id
    exports_dir.mkdir(parents=True, exist_ok=True)
    csv_path = exports_dir / f"coinapi_ohlcv_{run_id}.csv"
    parquet_path = exports_dir / f"coinapi_ohlcv_{run_id}.parquet"
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    # Insert into DB
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text(
                    """
                    INSERT INTO bf_historical_market_data (
                        token_symbol, date, open_price, high_price, low_price, close_price, volume, timestamp_fetched, source_api, backfill_run_id
                    ) VALUES (
                        %(token_symbol)s, %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s, %(timestamp_fetched)s, %(source_api)s, %(backfill_run_id)s
                    )
                    """
                ),
                row.to_dict(),
            )


@app.command()
def run(
    start: str = Option(..., help="Start date YYYY-MM-DD"),
    end: str = Option(..., help="End date YYYY-MM-DD"),
    tokens: str = Option(..., help="Comma separated symbols e.g. BTC,ETH"),
    run_id: str = Option(..., help="Backfill run id"),
):
    cfg = load_config()
    assert cfg.coinapi_key, "COINAPI_KEY not set in config/api_keys.env"

    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date()
    token_list = [t.strip().upper() for t in tokens.split(",") if t.strip()]

    engine = make_engine(cfg.db_host, cfg.db_name, cfg.db_user, cfg.db_password, cfg.db_port)
    ensure_tables(engine)

    exports_dir = Path(__file__).resolve().parent.parent / "exports"

    async def main_async():
        async with httpx.AsyncClient() as client:
            all_rows: List[BFHistoricalMarketData] = []
            for sym in token_list:
                for d in daterange(start_d, end_d):
                    rows = await fetch_day(client, cfg.coinapi_key or "", sym, d)
                    if rows:
                        all_rows.extend(rows)
            upsert_and_export(engine, all_rows, run_id, exports_dir)
            logger.info({"event": "coinapi_backfill_complete", "count": len(all_rows), "run_id": run_id})

    asyncio.run(main_async())


if __name__ == "__main__":
    app()
