from __future__ import annotations
import sqlite3
import time
from datetime import datetime
from project_crypt.telegram_notify import TelegramNotifier

DB = "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3"
LOG = "/home/itachi/.openclaw/workspace/project_crypt/phase1_activity_monitor.log"

def max_id(conn: sqlite3.Connection, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"select ifnull(max(id),0) from {table}")
    return int(cur.fetchone()[0])

def log(msg: str) -> None:
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} {msg}\n")


def main() -> None:
    tg = TelegramNotifier()
    conn = sqlite3.connect(DB)
    last_risk = max_id(conn, "risk_log")
    last_trade = max_id(conn, "trade_log")
    log(f"monitor started risk={last_risk} trade={last_trade}")
    while True:
        time.sleep(300)
        cur_risk = max_id(conn, "risk_log")
        cur_trade = max_id(conn, "trade_log")
        dr = cur_risk - last_risk
        dt = cur_trade - last_trade
        if dr > 0 or dt > 0:
            msg = f"[Project Crypt][Phase-1 Monitor] activity detected in last 5m: risk_events=+{dr} trade_events=+{dt}"
            tg.send(msg)
            log(msg)
            last_risk, last_trade = cur_risk, cur_trade

if __name__ == "__main__":
    main()
