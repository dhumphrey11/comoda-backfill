from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class BFHistoricalMarketData:
    token_symbol: str
    date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    timestamp_fetched: datetime
    source_api: str
    backfill_run_id: str


@dataclass
class BFMarketNewsEvents:
    token_symbol: str
    date: date
    title: str
    description: str
    source: str
    sentiment_score: float
    url: str
    timestamp_fetched: datetime
    backfill_run_id: str


@dataclass
class BFMarketSentiment:
    token_symbol: str
    date: date
    sentiment_score: float
    positive_mentions: int
    negative_mentions: int
    neutral_mentions: int
    source_api: str
    timestamp_fetched: datetime
    backfill_run_id: str


@dataclass
class BFOnchainMetrics:
    token_symbol: str
    date: date
    metric_name: str
    metric_value: float
    source_api: str
    timestamp_fetched: datetime
    backfill_run_id: str


@dataclass
class BFMacroData:
    symbol: str
    date: date
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None
    close_price: Optional[float] = None
    volume: Optional[int] = None
    value: Optional[float] = None
    timestamp_fetched: datetime = datetime.utcnow()
    backfill_run_id: str = ""
