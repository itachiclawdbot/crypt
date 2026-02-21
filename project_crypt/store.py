from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from .config import ensure_parent_dir


class RiskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        ensure_parent_dir(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS risk_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    approved INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    payload_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    pnl REAL,
                    meta_json TEXT
                )
                """
            )

    def log_risk_event(
        self,
        event_type: str,
        approved: bool,
        reason: str,
        payload: dict | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO risk_log (ts, event_type, approved, reason, payload_json) VALUES (?, ?, ?, ?, ?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    event_type,
                    int(approved),
                    reason,
                    json.dumps(payload or {}, default=str),
                ),
            )

    def log_trade(self, symbol: str, side: str, pnl: float | None, meta: dict | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO trade_log (ts, symbol, side, pnl, meta_json) VALUES (?, ?, ?, ?, ?)",
                (
                    datetime.now(timezone.utc).isoformat(),
                    symbol,
                    side,
                    pnl,
                    json.dumps(meta or {}),
                ),
            )
