from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timezone, timedelta

DB_PATH = "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3"
STATUS_PATH = "/home/itachi/.openclaw/workspace/project_crypt/phase2_status.json"


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def pgrep(pattern: str) -> str:
    p = subprocess.run(f"ps -eo pid,etimes,cmd | grep -F \"{pattern}\" | grep -v grep", shell=True, text=True, capture_output=True)
    return p.stdout.strip().splitlines()[0] if p.stdout.strip() else "down"


def read_status() -> dict:
    if not os.path.exists(STATUS_PATH):
        return {"state": "unknown", "job": "no status file"}
    with open(STATUS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def render() -> str:
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    status = read_status()
    agg = pgrep("python3 -m project_crypt.phase2_alpha_aggregator")
    mon = pgrep("python3 -m project_crypt.phase2_monitor_notifier")
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) from intel_cache where ts >= ?", (since,))
        fetched = cur.fetchone()[0]

        cur.execute(
            """
            select discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,reasons_json,sources_present_json,ts
            from candidate_funnel_log
            order by id desc limit 1
            """
        )
        funnel = cur.fetchone()

        cur.execute(
            """
            select symbol,status,alpha_score,eligibility_score,deny_reason,ts
            from universe_state
            order by id desc
            limit 40
            """
        )
        universe = cur.fetchall()

        cur.execute(
            """
            select reason,count(*) c
            from candidate_reject_log
            where ts >= ?
            group by reason
            order by c desc
            limit 10
            """,
            (since,),
        )
        rejects = cur.fetchall()

    watch = [r for r in universe if r["status"] == "WATCH"][:10]
    eligible = [r for r in universe if r["status"] in {"ELIGIBLE", "ACTIONABLE"}][:10]
    actionable = [r for r in universe if r["status"] == "ACTIONABLE"][:5]

    lines = [
        "PROJECT CRYPT PHASE-2 OPS DASHBOARD",
        "=" * 80,
        f"Now: {now}",
        f"Aggregator: {agg}",
        f"Notifier:   {mon}",
        "-" * 80,
        f"Current job: {status.get('job')}",
        f"State:       {status.get('state')}",
        f"Last update: {status.get('ts')}",
        f"Fetched last 24h: {fetched}",
        "-" * 80,
        "FUNNEL",
    ]

    if funnel:
        lines.append(
            f" discovered={funnel['discovered_total']} -> mapped={funnel['mapped_to_venue_total']} -> eligible={funnel['eligible_total']}"
        )
        lines.append(
            f" alpha_pass={funnel['alpha_pass_total']} -> risk_pass={funnel['risk_pass_total']} -> cost_pass={funnel['cost_pass_total']} -> proposed={funnel['proposed_total']}"
        )
        lines.append(f" sources_present={funnel['sources_present_json']}")
        lines.append(f" top_reasons={funnel['reasons_json']}")
    else:
        lines.append(" no funnel data yet")

    lines += ["-" * 80, "TOP WATCHLIST"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f} deny={r['deny_reason'] or '-'}" for r in watch] or [" - none"]

    lines += ["-" * 80, "TOP ELIGIBLE"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f} status={r['status']}" for r in eligible] or [" - none"]

    lines += ["-" * 80, "TOP ACTIONABLE"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f}" for r in actionable] or [" - none"]

    lines += ["-" * 80, "TOP REJECT REASONS (24h)"]
    lines += [f" - {r['reason']}: {r['c']}" for r in rejects] or [" - none"]

    if funnel and int(funnel["proposed_total"]) == 0:
        lines += ["-" * 80, "ALERT: NO ACTIONABLE CANDIDATES", "Check funnel stage drops + source availability map."]

    lines.append("\nRefreshes every 3s. Ctrl+C to exit.")
    return "\n".join(lines)


def main() -> None:
    try:
        while True:
            print("\033[2J\033[H", end="")
            print(render())
            time.sleep(3)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
