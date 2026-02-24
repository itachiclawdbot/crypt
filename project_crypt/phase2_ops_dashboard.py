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


def fmt_proc(line: str) -> str:
    if line == "down":
        return "DOWN"
    try:
        pid, etimes, *cmd = line.split()
        sec = int(float(etimes))
        h, rem = divmod(sec, 3600)
        m, s = divmod(rem, 60)
        return f"UP pid={pid} uptime={h:02d}:{m:02d}:{s:02d} {' '.join(cmd[-2:])}"
    except Exception:
        return f"UP {line}"


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
    mic = pgrep("python3 -m project_crypt.microstructure_service")
    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    since_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) from intel_cache where ts >= ?", (since_24h,))
        fetched_24h = cur.fetchone()[0]

        cur.execute("select count(*) from intel_cache where ts >= ?", (since_1h,))
        fetched_1h = cur.fetchone()[0]

        cur.execute(
            """
            select source, count(*) c
            from intel_cache
            where ts >= ?
            group by source
            order by c desc
            limit 8
            """,
            (since_1h,),
        )
        by_source = cur.fetchall()

        cur.execute(
            """
            select signal_type, count(*) c
            from intel_cache
            where ts >= ?
            group by signal_type
            order by c desc
            limit 8
            """,
            (since_1h,),
        )
        trend_signals = cur.fetchall()

        cur.execute(
            """
            select symbol, count(*) c
            from intel_cache
            where ts >= ? and symbol is not null
            group by symbol
            order by c desc
            limit 10
            """,
            (since_1h,),
        )
        trending_symbols = cur.fetchall()

        cur.execute(
            """
            select symbol, conviction, ts
            from intel_cache
            where ts >= ? and signal_type in ('triangulated_alpha','actionable_candidate')
            order by id desc
            limit 10
            """,
            (since_1h,),
        )
        potential_buys = cur.fetchall()

        cur.execute(
            """
            select source_name, status, count(*) c
            from source_health_log
            where ts >= ?
            group by source_name, status
            order by source_name, c desc
            """,
            (since_1h,),
        )
        source_health = cur.fetchall()

        cur.execute(
            """
            select discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,
                   watch_total,actionable_total,alpha_scored_total,cost_evaluated_total,risk_evaluated_total,
                   reasons_json,sources_present_json,sanity_json,regime,pressure_count,ts
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
            limit 50
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
            (since_24h,),
        )
        rejects = cur.fetchall()

        cur.execute(
            """
            select base_symbol,obi_20bps,pressure_confidence,liquidity_score
            from micro_features_latest
            where pressure_flag=1
            order by pressure_confidence desc, abs(obi_20bps) desc
            limit 8
            """
        )
        pressure = cur.fetchall()

        cur.execute(
            """
            select base_symbol,liquidity_score,spread_bps_p95_300
            from micro_features_latest
            order by liquidity_score desc
            limit 8
            """
        )
        liq = cur.fetchall()

    watch = [r for r in universe if r["status"] == "WATCH"][:10]
    eligible = [r for r in universe if r["status"] in {"ELIGIBLE", "ACTIONABLE"}][:10]
    actionable = [r for r in universe if r["status"] == "ACTIONABLE"][:5]

    lines = [
        "PROJECT CRYPT PHASE-2 OPS DASHBOARD",
        "=" * 120,
        f"Now: {now}",
        f"Aggregator: {fmt_proc(agg)}",
        f"Notifier:   {fmt_proc(mon)}",
        f"MicroSvc:   {fmt_proc(mic)}",
        "-" * 120,
        f"Current job: {status.get('job')}",
        f"State:       {status.get('state')}",
        f"Last update: {status.get('ts')}",
        f"Kill switch active: {status.get('kill_switch_active')}",
        f"Tick config: market={status.get('next_market_tick_s')}s discovery={status.get('next_discovery_tick_s')}s",
        f"Fetched signals: last1h={fetched_1h} last24h={fetched_24h}",
        "-" * 100,
        "SOURCE/SIGNAL ACTIVITY (last 1h)",
        " by_source: " + (", ".join(f"{r['source']}:{r['c']}" for r in by_source) or "none"),
        " trend_signals: " + (", ".join(f"{r['signal_type']}:{r['c']}" for r in trend_signals) or "none"),
        " trending_symbols: " + (", ".join(f"{r['symbol']}({r['c']})" for r in trending_symbols if r['symbol']) or "none"),
        " potential_buys: " + (", ".join(f"{r['symbol']}[{r['conviction']}]" for r in potential_buys if r['symbol']) or "none"),
        " source_health: " + (", ".join(f"{r['source_name']}={r['status']}({r['c']})" for r in source_health) or "none"),
        "-" * 100,
        "FUNNEL",
    ]

    if funnel:
        lines.append(
            f" discovered={funnel['discovered_total']} -> mapped={funnel['mapped_to_venue_total']} -> eligible={funnel['eligible_total']} -> watch={funnel['watch_total'] or 0} -> actionable={funnel['actionable_total'] or 0}"
        )
        lines.append(
            f" alpha_scored={funnel['alpha_scored_total'] or 0} -> cost_eval={funnel['cost_evaluated_total'] or 0} -> cost_pass={funnel['cost_pass_total']} -> risk_eval={funnel['risk_evaluated_total'] or 0} -> risk_pass={funnel['risk_pass_total']} -> proposed={funnel['proposed_total']}"
        )
        lines.append(f" regime={funnel['regime'] or '-'} pressure_count={funnel['pressure_count'] or 0}")
        lines.append(f" sources_present={funnel['sources_present_json']}")
        lines.append(f" sanity={funnel['sanity_json']}")
        lines.append(f" top_reasons={funnel['reasons_json']}")
    else:
        lines.append(" no funnel data yet")

    lines += ["-" * 100, "TOP WATCHLIST"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f} deny={r['deny_reason'] or '-'}" for r in watch] or [" - none"]

    lines += ["-" * 100, "TOP ELIGIBLE"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f} status={r['status']}" for r in eligible] or [" - none"]

    lines += ["-" * 100, "TOP ACTIONABLE"]
    lines += [f" - {r['symbol']} a={r['alpha_score']:.1f} e={r['eligibility_score']:.1f}" for r in actionable] or [" - none"]

    lines += ["-" * 100, "TOP REJECT REASONS (24h)"]
    lines += [f" - {r['reason']}: {r['c']}" for r in rejects] or [" - none"]

    lines += ["-" * 100, "MICRO PRESSURE (live)"]
    lines += [f" - {r['base_symbol']} obi={float(r['obi_20bps'] or 0):.2f} conf={float(r['pressure_confidence'] or 0):.2f} liq={float(r['liquidity_score'] or 0):.1f}x" for r in pressure] or [" - none"]

    lines += ["-" * 100, "MICRO BEST LIQUIDITY"]
    lines += [f" - {r['base_symbol']} liq={float(r['liquidity_score'] or 0):.1f}x spread_p95={float(r['spread_bps_p95_300'] or 0):.1f}bps" for r in liq] or [" - none"]

    if funnel and int(funnel["proposed_total"]) == 0:
        lines += ["-" * 100, "ALERT: NO ACTIONABLE CANDIDATES", "Check funnel stage drops + source availability map."]

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
