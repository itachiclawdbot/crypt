from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class OrderProposal:
    symbol: str
    side: str
    entry_price: float
    stop_price: float | None = None
    atr: float | None = None
    confidence: float = 0.5
    expected_win_rate: float = 0.5
    expected_r_multiple: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AccountState:
    equity: float
    daily_start_equity: float
    hourly_start_equity: float
    open_positions: int
    consecutive_losses: int
    timestamp: datetime = field(default_factory=utc_now)


@dataclass
class GateDecision:
    approved: bool
    reason: str
    order_payload: dict[str, Any] | None = None
