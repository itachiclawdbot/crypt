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

        cur.execute(
            """
            select discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,reasons_json,sources_present_json,ts
            from candidate_funnel_log
            where ts >= ?
            order by id desc
            limit 1
            """,
            (since,),
        )
        funnel = cur.fetchone()

        cur.execute(
            """
            select symbol,status,alpha_score,eligibility_score,deny_reason
            from universe_state
            where ts >= ?
            order by alpha_score desc
            limit 15
            """,
            (since,),
        )
        top = cur.fetchall()

        cur.execute(
            """
            select reason, count(*) c
            from candidate_reject_log
            where ts >= ?
            group by reason
            order by c desc
            limit 6
            """,
            (since,),
        )
        rejects = cur.fetchall()

    watch = [r for r in top if r["status"] == "WATCH"][:10]
    eligible = [r for r in top if r["status"] in {"ELIGIBLE", "ACTIONABLE"}][:10]
    actionable = [r for r in top if r["status"] == "ACTIONABLE"][:5]

    watch_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f}:{r['deny_reason'] or 'ok'})" for r in watch) or "none"
    eligible_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f})" for r in eligible) or "none"
    actionable_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f})" for r in actionable) or "none"
    reject_txt = ", ".join(f"{r['reason']}:{r['c']}" for r in rejects) or "none"

    if funnel:
        funnel_txt = (
            f"discovered={funnel['discovered_total']} mapped={funnel['mapped_to_venue_total']} "
            f"eligible={funnel['eligible_total']} alpha={funnel['alpha_pass_total']} "
            f"risk={funnel['risk_pass_total']} cost={funnel['cost_pass_total']} proposed={funnel['proposed_total']}"
        )
        src_txt = (funnel["sources_present_json"] or "{}")[:220]
    else:
        funnel_txt = "none"
        src_txt = "{}"

    return (
        "[Project Crypt][Phase-2][Hourly]\n"
        f"Window: last 1h\n"
        f"Fetched signals: {fetched}\n"
        f"Funnel: {funnel_txt}\n"
        f"Sources present: {src_txt}\n"
        f"Top Watchlist: {watch_txt}\n"
        f"Top Eligible: {eligible_txt}\n"
        f"Top Actionable: {actionable_txt}\n"
        f"Top reject reasons: {reject_txt}"
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
