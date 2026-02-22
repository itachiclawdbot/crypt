from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone, timedelta

from project_crypt.telegram_notify import TelegramNotifier

DB_PATH = "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3"


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def hourly_summary() -> str:
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=1)).isoformat()
    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) c from intel_cache where ts >= ?", (since,))
        fetched = int(cur.fetchone()[0])

        cur.execute("select source, count(*) c from intel_cache where ts >= ? group by source order by c desc", (since,))
        by_source = cur.fetchall()

        cur.execute("select signal_type, count(*) c from intel_cache where ts >= ? group by signal_type order by c desc limit 6", (since,))
        top_signals = cur.fetchall()

        cur.execute(
            """
            select symbol, count(*) c
            from intel_cache
            where ts >= ? and symbol is not null
            group by symbol
            order by c desc
            limit 8
            """,
            (since,),
        )
        top_symbols = cur.fetchall()

        cur.execute(
            """
            select symbol, conviction, ts
            from intel_cache
            where ts >= ? and signal_type='triangulated_alpha'
            order by id desc
            limit 8
            """,
            (since,),
        )
        potentials = cur.fetchall()

        cur.execute(
            """
            select source_name, status, count(*) c
            from source_health_log
            where ts >= ?
            group by source_name, status
            order by source_name, c desc
            """,
            (since,),
        )
        health = cur.fetchall()

    src_txt = ", ".join(f"{r['source']}:{r['c']}" for r in by_source) or "none"
    sig_txt = ", ".join(f"{r['signal_type']}:{r['c']}" for r in top_signals) or "none"
    sym_txt = ", ".join(f"{r['symbol']}({r['c']})" for r in top_symbols) or "none"
    pot_txt = ", ".join(f"{r['symbol']}[{r['conviction']}]" for r in potentials if r['symbol']) or "none"
    health_txt = ", ".join(f"{r['source_name']}={r['status']}({r['c']})" for r in health) or "none"

    return (
        "[Project Crypt][Phase-2][Hourly]\n"
        f"Window: last 1h\n"
        f"Fetched signals: {fetched}\n"
        f"By source: {src_txt}\n"
        f"Trend signals: {sig_txt}\n"
        f"Trending symbols: {sym_txt}\n"
        f"Potential buys under watch: {pot_txt}\n"
        f"Source health: {health_txt}"
    )


def main() -> None:
    tg = TelegramNotifier()
    while True:
        try:
            tg.send(hourly_summary())
        except Exception:
            pass
        time.sleep(3600)


if __name__ == "__main__":
    main()
