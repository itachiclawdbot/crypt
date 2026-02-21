from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RiskConfig:
    daily_drawdown_limit: float = 0.05
    hourly_drawdown_limit: float = 0.02
    max_consecutive_losses: int = 5
    max_notional_fraction: float = 0.10
    risk_per_trade_fraction: float = 0.005
    quarter_kelly_fraction: float = 0.25
    default_fallback_stop_fraction: float = 0.01
    db_path: str = "project_crypt/cryptobot.sqlite3"
    kill_switch_file: str = "/var/run/cryptobot/STOP"


@dataclass(frozen=True)
class EnvConfig:
    cryptocom_api_key: str | None
    cryptocom_api_secret: str | None
    telegram_bot_token: str | None
    telegram_chat_id: str | None
    kill_switch_file: str



def load_env_config() -> EnvConfig:
    return EnvConfig(
        cryptocom_api_key=os.getenv("CRYPTOCOM_API_KEY"),
        cryptocom_api_secret=os.getenv("CRYPTOCOM_API_SECRET"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
        telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
        kill_switch_file=os.getenv("KILL_SWITCH_FILE", "/var/run/cryptobot/STOP"),
    )


def ensure_parent_dir(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
