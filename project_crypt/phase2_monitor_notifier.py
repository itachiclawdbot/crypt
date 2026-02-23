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


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return num / den


def _periodic_analysis(cur: sqlite3.Cursor, since_6h: str) -> tuple[str, str]:
    cur.execute(
        """
        select discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total
        from candidate_funnel_log
        where ts >= ?
        order by id desc
        limit 72
        """,
        (since_6h,),
    )
    rows = cur.fetchall()
    if not rows:
        return "none", "no-funnel-data"

    discovered = sum(int(r["discovered_total"]) for r in rows)
    mapped = sum(int(r["mapped_to_venue_total"]) for r in rows)
    eligible = sum(int(r["eligible_total"]) for r in rows)
    alpha = sum(int(r["alpha_pass_total"]) for r in rows)
    risk = sum(int(r["risk_pass_total"]) for r in rows)
    cost = sum(int(r["cost_pass_total"]) for r in rows)
    proposed = sum(int(r["proposed_total"]) for r in rows)

    mapping_rate = _safe_ratio(mapped, discovered)
    eligible_rate = _safe_ratio(eligible, mapped)
    alpha_rate = _safe_ratio(alpha, eligible)
    risk_rate = _safe_ratio(risk, alpha)
    cost_rate = _safe_ratio(cost, risk)
    proposed_rate = _safe_ratio(proposed, cost)

    stage_rates = {
        "mapping": mapping_rate,
        "eligibility": eligible_rate,
        "alpha": alpha_rate,
        "risk": risk_rate,
        "cost": cost_rate,
        "proposal": proposed_rate,
    }
    bottleneck_stage = min(stage_rates, key=stage_rates.get)

    recommendation = {
        "mapping": "Expand symbol mapping aliases and venue-availability map first.",
        "eligibility": "Tune liquidity gates using percentile thresholds tied to target notional.",
        "alpha": "Rebalance alpha weights / lower hard alpha cutoff slightly when social is missing.",
        "risk": "Inspect confidence calibration and cooldown logic for excessive denials.",
        "cost": "Adjust min net-edge gate or improve spread/slippage estimation.",
        "proposal": "Inspect final proposal constraints (drift/notional/cooldown) for over-filtering.",
    }[bottleneck_stage]

    analysis_txt = (
        f"6h_rates mapping={mapping_rate:.2f} elig={eligible_rate:.2f} alpha={alpha_rate:.2f} "
        f"risk={risk_rate:.2f} cost={cost_rate:.2f} proposal={proposed_rate:.2f}; bottleneck={bottleneck_stage}"
    )
    return analysis_txt, recommendation


def hourly_summary() -> str:
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=1)).isoformat()
    since_6h = (now - timedelta(hours=6)).isoformat()
    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) c from intel_cache where ts >= ?", (since,))
        fetched = int(cur.fetchone()[0])

        # Legacy visibility metrics retained (requested)
        cur.execute("select source, count(*) c from intel_cache where ts >= ? group by source order by c desc", (since,))
        by_source = cur.fetchall()

        cur.execute("select signal_type, count(*) c from intel_cache where ts >= ? group by signal_type order by c desc limit 8", (since,))
        top_signals = cur.fetchall()

        cur.execute(
            """
            select symbol, count(*) c
            from intel_cache
            where ts >= ? and symbol is not null
            group by symbol
            order by c desc
            limit 10
            """,
            (since,),
        )
        top_symbols = cur.fetchall()

        cur.execute(
            """
            select symbol, conviction, ts
            from intel_cache
            where ts >= ? and signal_type in ('triangulated_alpha','actionable_candidate')
            order by id desc
            limit 10
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

        # New funnel/ranked outputs retained
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
            limit 20
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
            limit 8
            """,
            (since,),
        )
        rejects = cur.fetchall()

        periodic_analysis_txt, periodic_reco = _periodic_analysis(cur, since_6h)

    watch = [r for r in top if r["status"] == "WATCH"][:10]
    eligible = [r for r in top if r["status"] in {"ELIGIBLE", "ACTIONABLE"}][:10]
    actionable = [r for r in top if r["status"] == "ACTIONABLE"][:5]

    src_txt = ", ".join(f"{r['source']}:{r['c']}" for r in by_source) or "none"
    sig_txt = ", ".join(f"{r['signal_type']}:{r['c']}" for r in top_signals) or "none"
    sym_txt = ", ".join(f"{r['symbol']}({r['c']})" for r in top_symbols if r['symbol']) or "none"
    pot_txt = ", ".join(f"{r['symbol']}[{r['conviction']}]" for r in potentials if r['symbol']) or "none"
    health_txt = ", ".join(f"{r['source_name']}={r['status']}({r['c']})" for r in health) or "none"

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
        sources_present_txt = (funnel["sources_present_json"] or "{}")[:260]
    else:
        funnel_txt = "none"
        sources_present_txt = "{}"

    return (
        "[Project Crypt][Phase-2][Hourly]\n"
        f"Window: last 1h\n"
        f"Fetched signals: {fetched}\n"
        f"By source: {src_txt}\n"
        f"Trend signals: {sig_txt}\n"
        f"Trending symbols: {sym_txt}\n"
        f"Potential buys under watch: {pot_txt}\n"
        f"Source health: {health_txt}\n"
        f"Funnel: {funnel_txt}\n"
        f"Sources present: {sources_present_txt}\n"
        f"Top Watchlist: {watch_txt}\n"
        f"Top Eligible: {eligible_txt}\n"
        f"Top Actionable: {actionable_txt}\n"
        f"Top reject reasons: {reject_txt}\n"
        f"Periodic analysis: {periodic_analysis_txt}\n"
        f"Recommended focus: {periodic_reco}"
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
