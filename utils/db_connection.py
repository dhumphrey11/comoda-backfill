from __future__ import annotations
from typing import Generator
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def make_engine(host: str, db: str, user: str, password: str, port: int = 5432) -> Engine:
    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


@contextmanager
def get_conn(engine: Engine):
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


DDL = {
    "bf_historical_market_data": """
    CREATE TABLE IF NOT EXISTS bf_historical_market_data (
      token_symbol VARCHAR(20),
      date DATE,
      open_price DECIMAL(20,8),
      high_price DECIMAL(20,8),
      low_price DECIMAL(20,8),
      close_price DECIMAL(20,8),
      volume DECIMAL(20,8),
      timestamp_fetched TIMESTAMP,
      source_api VARCHAR(50),
      backfill_run_id VARCHAR
    );
    """,
    "bf_market_news_events": """
    CREATE TABLE IF NOT EXISTS bf_market_news_events (
      token_symbol VARCHAR(20),
      date DATE,
      title TEXT,
      description TEXT,
      source VARCHAR(50),
      sentiment_score DECIMAL(5,2),
      url TEXT,
      timestamp_fetched TIMESTAMP,
      backfill_run_id VARCHAR
    );
    """,
    "bf_market_sentiment": """
    CREATE TABLE IF NOT EXISTS bf_market_sentiment (
      token_symbol VARCHAR(20),
      date DATE,
      sentiment_score DECIMAL(5,2),
      positive_mentions INT,
      negative_mentions INT,
      neutral_mentions INT,
      source_api VARCHAR(50),
      timestamp_fetched TIMESTAMP,
      backfill_run_id VARCHAR
    );
    """,
    "bf_onchain_metrics": """
    CREATE TABLE IF NOT EXISTS bf_onchain_metrics (
      token_symbol VARCHAR(20),
      date DATE,
      metric_name VARCHAR(50),
      metric_value DECIMAL(20,8),
      source_api VARCHAR(50),
      timestamp_fetched TIMESTAMP,
      backfill_run_id VARCHAR
    );
    """,
    "bf_macro_data": """
    CREATE TABLE IF NOT EXISTS bf_macro_data (
      symbol VARCHAR(20),
      date DATE,
      open_price DECIMAL(20,8),
      high_price DECIMAL(20,8),
      low_price DECIMAL(20,8),
      close_price DECIMAL(20,8),
      volume BIGINT,
      value DECIMAL(20,8),
      timestamp_fetched TIMESTAMP,
      backfill_run_id VARCHAR
    );
    """,
}


def ensure_tables(engine: Engine) -> None:
    with get_conn(engine) as conn:
        for ddl in DDL.values():
            conn.execute(text(ddl))
        conn.commit()
