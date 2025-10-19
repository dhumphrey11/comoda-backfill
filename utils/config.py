from __future__ import annotations
import json
import os
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv


@dataclass
class BackfillConfig:
    coinapi_key: Optional[str]
    santiment_key: Optional[str]
    yahoo_key: Optional[str]
    lunarcrush_key: Optional[str]
    cryptopanic_key: Optional[str]
    db_host: str
    db_name: str
    db_user: str
    db_password: str
    db_port: int
    gcp_project_id: Optional[str]


def load_config(env_path: str = "config/api_keys.env") -> BackfillConfig:
    load_dotenv(env_path)
    return BackfillConfig(
        coinapi_key=os.getenv("COINAPI_KEY"),
        santiment_key=os.getenv("SANTIMENT_KEY"),
        yahoo_key=os.getenv("YAHOO_KEY"),
        lunarcrush_key=os.getenv("LUNARCRUSH_KEY"),
        cryptopanic_key=os.getenv("CRYPTOPANIC_KEY"),
        db_host=os.getenv("DB_HOST", "127.0.0.1"),
        db_name=os.getenv("DB_NAME", "portfolio"),
        db_user=os.getenv("DB_USER", "app"),
        db_password=os.getenv("DB_PASSWORD", "change_me"),
        db_port=int(os.getenv("DB_PORT", "5432")),
        gcp_project_id=os.getenv("GCP_PROJECT_ID"),
    )


def load_token_universe(path: str = "config/token_universe.json") -> List[str]:
    with open(path, "r") as f:
        data = json.load(f)
    return [str(t).upper() for t in data]


def load_date_ranges(path: str = "config/date_ranges.json") -> dict:
    with open(path, "r") as f:
        return json.load(f)
