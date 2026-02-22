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
    since = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) from intel_cache where ts >= ?", (since,))
        fetched = cur.fetchone()[0]
        cur.execute("select symbol, conviction, ts from intel_cache where signal_type='triangulated_alpha' order by id desc limit 8")
        last_alpha = cur.fetchall()
        cur.execute("select source_name,status,ts,last_error from source_health_log order by id desc limit 8")
        health = cur.fetchall()

    lines = [
        "PROJECT CRYPT PHASE-2 OPS DASHBOARD",
        "=" * 60,
        f"Now: {now}",
        f"Aggregator: {agg}",
        f"Notifier:   {mon}",
        "-" * 60,
        f"Current job: {status.get('job')}",
        f"State:       {status.get('state')}",
        f"Last update: {status.get('ts')}",
        f"Last cycle:  {status.get('last_cycle_summary')}",
        "-" * 60,
        f"Fetched last 1h: {fetched}",
        "Latest potential buys under check:",
    ]
    if last_alpha:
        lines += [f"  - {r['symbol']} [{r['conviction']}] at {r['ts']}" for r in last_alpha]
    else:
        lines.append("  - none yet")

    lines += ["-" * 60, "Latest source health:"]
    for r in health:
        err = f" err={r['last_error'][:60]}" if r['last_error'] else ""
        lines.append(f"  - {r['source_name']} {r['status']} @ {r['ts']}{err}")
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
