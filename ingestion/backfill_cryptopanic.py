from __future__ import annotations
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional
import httpx
import pandas as pd
from typer import Typer, Option

from ..utils.logger import get_logger
from ..utils.config import load_config
from ..utils.db_connection import make_engine, ensure_tables
from ..utils.schemas import BFMarketNewsEvents

app = Typer(help="Backfill news and market events from CryptoPanic")
logger = get_logger(__name__)


API_BASE = "https://cryptopanic.com/api/v1"


def _parse_date(dt_str: str) -> date:
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date()
    except Exception:
        return datetime.utcnow().date()


def _sentiment_from_item(item: dict) -> float:
    # CryptoPanic provides 'panic_score' in some plans; fallback to votes sentiment
    score = item.get("panic_score")
    if isinstance(score, (int, float)):
        try:
            return float(score)
        except Exception:
            pass
    votes = item.get("votes") or {}
    bullish = (votes.get("bullish") or 0) + (votes.get("liked") or 0)
    bearish = (votes.get("bearish") or 0) + (votes.get("disliked") or 0)
    total = bullish + bearish
    return (bullish - bearish) / total if total > 0 else 0.0


async def fetch_posts(client: httpx.AsyncClient, token: str, auth_token: str, start: str, end: str) -> List[BFMarketNewsEvents]:
    # Map token to CryptoPanic 'currencies' filter
    params = {
        "auth_token": auth_token,
        "currencies": token.upper(),
        "filter": "rising|hot|important|bullish|bearish",  # wide net
        "kind": "news|media",
        "public": "true",
        "with_content": "true",
        "size": 50,
    }
    url = f"{API_BASE}/posts/"
    items: List[BFMarketNewsEvents] = []
    next_url: Optional[str] = url
    while next_url:
        r = await client.get(next_url, params=params if next_url == url else None, timeout=30)
        if r.status_code != 200:
            logger.error({"event": "cryptopanic_http_error", "status": r.status_code, "body": r.text[:200]})
            break
        payload = r.json()
        for it in payload.get("results", []):
            dt = _parse_date(it.get("published_at") or it.get("created_at") or "")
            title = it.get("title") or ""
            desc = it.get("description") or (it.get("body") or "")
            source = (it.get("source") or {}).get("title") or "cryptopanic"
            url_item = it.get("url") or (it.get("source") or {}).get("url") or ""
            sent = _sentiment_from_item(it)
            items.append(
                BFMarketNewsEvents(
                    token_symbol=token.upper(),
                    date=dt,
                    title=title,
                    description=desc,
                    source=source,
                    sentiment_score=float(sent),
                    url=url_item,
                    timestamp_fetched=datetime.utcnow(),
                    backfill_run_id="",
                )
            )
        next_url = payload.get("next")
    return items


@app.command()
def run(
    start: str = Option(..., help="Start date YYYY-MM-DD (server-side filter limited by plan)"),
    end: str = Option(..., help="End date YYYY-MM-DD"),
    tokens: str = Option(..., help="Comma separated symbols e.g. BTC,ETH"),
    run_id: str = Option(...),
):
    cfg = load_config()
    assert cfg.cryptopanic_key, "CRYPTOPANIC_KEY not set in config/api_keys.env"
    engine = make_engine(cfg.db_host, cfg.db_name, cfg.db_user, cfg.db_password, cfg.db_port)
    ensure_tables(engine)

    token_list = [t.strip().upper() for t in tokens.split(",") if t.strip()]
    exports_dir = Path(__file__).resolve().parent.parent / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    async def main_async():
        async with httpx.AsyncClient() as client:
            all_rows: List[BFMarketNewsEvents] = []
            for sym in token_list:
                items = await fetch_posts(client, sym, cfg.cryptopanic_key or "", start, end)
                # filter by date range client-side
                start_d = datetime.strptime(start, "%Y-%m-%d").date()
                end_d = datetime.strptime(end, "%Y-%m-%d").date()
                items = [x for x in items if start_d <= x.date <= end_d]
                all_rows.extend(items)

            df = pd.DataFrame([r.__dict__ for r in all_rows]) if all_rows else pd.DataFrame()
            if not df.empty:
                df["backfill_run_id"] = run_id
                df.to_csv(exports_dir / f"cryptopanic_news_{run_id}.csv", index=False)
                df.to_parquet(exports_dir / f"cryptopanic_news_{run_id}.parquet", index=False)
                from sqlalchemy import text
                with engine.begin() as conn:
                    for _, row in df.iterrows():
                        conn.execute(
                            text(
                                """
                                INSERT INTO bf_market_news_events (
                                    token_symbol, date, title, description, source, sentiment_score, url, timestamp_fetched, backfill_run_id
                                ) VALUES (
                                    %(token_symbol)s, %(date)s, %(title)s, %(description)s, %(source)s, %(sentiment_score)s, %(url)s, %(timestamp_fetched)s, %(backfill_run_id)s
                                )
                                """
                            ),
                            row.to_dict(),
                        )
            logger.info({"event": "cryptopanic_backfill_complete", "count": len(all_rows), "run_id": run_id})

    import asyncio as _asyncio
    _asyncio.run(main_async())


if __name__ == "__main__":
    app()
