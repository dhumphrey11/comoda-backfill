from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import List
import pandas as pd
from typer import Typer, Option

from ..utils.logger import get_logger
from ..utils.config import load_config
from ..utils.db_connection import make_engine, ensure_tables
from ..utils.schemas import BFMacroData
import yfinance as yf

app = Typer(help="Backfill macro and market indices from Yahoo Finance")
logger = get_logger(__name__)


@app.command()
def run(
    start: str = Option(..., help="Start date YYYY-MM-DD"),
    end: str = Option(..., help="End date YYYY-MM-DD"),
    symbols: str = Option(..., help="Comma separated macro symbols e.g. ^GSPC,DX-Y.NYB"),
    run_id: str = Option(...),
):
    cfg = load_config()
    engine = make_engine(cfg.db_host, cfg.db_name, cfg.db_user, cfg.db_password, cfg.db_port)
    ensure_tables(engine)

    symbols_list = [s.strip() for s in symbols.split(",") if s.strip()]
    all_rows: List[BFMacroData] = []
    for sym in symbols_list:
        try:
            df_hist = yf.download(sym, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
        except Exception as e:
            logger.error({"event": "yfinance_error", "symbol": sym, "error": str(e)})
            continue
        if df_hist is None or df_hist.empty:
            continue
        df_hist = df_hist.reset_index()
        # Columns: Date, Open, High, Low, Close, Adj Close, Volume
        for _, r in df_hist.iterrows():
            dt = r["Date"].date() if hasattr(r["Date"], "date") else r["Date"]
            def _f(v):
                return float(v) if pd.notna(v) else None
            all_rows.append(
                BFMacroData(
                    symbol=sym,
                    date=dt,
                    open_price=_f(r.get("Open")),
                    high_price=_f(r.get("High")),
                    low_price=_f(r.get("Low")),
                    close_price=_f(r.get("Close")),
                    volume=int(r.get("Volume", 0) or 0),
                    value=None,
                    timestamp_fetched=datetime.utcnow(),
                    backfill_run_id="",
                )
            )

    df = pd.DataFrame([r.__dict__ for r in all_rows]) if all_rows else pd.DataFrame()
    exports_dir = Path(__file__).resolve().parent.parent / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df["backfill_run_id"] = run_id
        df.to_csv(exports_dir / f"yahoo_macro_{run_id}.csv", index=False)
        df.to_parquet(exports_dir / f"yahoo_macro_{run_id}.parquet", index=False)
        from sqlalchemy import text
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    text(
                        """
                        INSERT INTO bf_macro_data (
                          symbol, date, open_price, high_price, low_price, close_price, volume, value, timestamp_fetched, backfill_run_id
                        ) VALUES (
                          %(symbol)s, %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s, %(value)s, %(timestamp_fetched)s, %(backfill_run_id)s
                        )
                        """
                    ),
                    row.to_dict(),
                )
    logger.info({"event": "yahoo_backfill_complete", "count": len(all_rows), "run_id": run_id})


if __name__ == "__main__":
    app()
