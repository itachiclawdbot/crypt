from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

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
            select discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,
                   reasons_json,sources_present_json,ts,
                   watch_total,actionable_total,alpha_scored_total,cost_evaluated_total,risk_evaluated_total,
                   rejects_tradable_json,rejects_external_json,sanity_json,regime,pressure_count
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
            select coalesce(instrument_symbol,symbol) as instrument_symbol, symbol,status,alpha_score,eligibility_score,deny_reason,
                   row_number() over (partition by coalesce(instrument_symbol,symbol) order by id desc) rn
            from universe_state
            where ts >= ? and coalesce(base_ccy,symbol) not in ('USDT','USDC','USD','EUR')
            order by alpha_score desc
            limit 200
            """,
            (since,),
        )
        top = [r for r in cur.fetchall() if r['rn'] == 1][:30]

        # Decision-level reject stats (deduped one decision per instrument per cycle window)
        cur.execute(
            """
            with latest as (
              select coalesce(instrument_symbol,symbol) as ikey, deny_reason,
                     row_number() over (partition by coalesce(instrument_symbol,symbol) order by id desc) rn
              from universe_state
              where ts >= ? and deny_reason is not null
            )
            select deny_reason as reason, count(*) c
            from latest
            where rn=1
            group by deny_reason
            order by c desc
            limit 8
            """,
            (since,),
        )
        rejects = cur.fetchall()

        # Event-level reject stats (separate section)
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
        event_rejects = cur.fetchall()

        cur.execute(
            """
            select base_symbol, obi_20bps, liquidity_score
            from micro_features_latest
            where pressure_flag=1
            order by abs(obi_20bps) desc
            limit 6
            """
        )
        pressure = cur.fetchall()

        cur.execute(
            """
            select base_symbol, spread_bps_p95_300
            from micro_features_latest
            order by spread_bps_p95_300 desc
            limit 6
            """
        )
        widest = cur.fetchall()

        cur.execute(
            """
            select base_symbol, liquidity_score
            from micro_features_latest
            order by liquidity_score desc
            limit 6
            """
        )
        best_liq = cur.fetchall()

        periodic_analysis_txt, periodic_reco = _periodic_analysis(cur, since_6h)

    eligible = [r for r in top if r["status"] == "ELIGIBLE"]
    watch = [r for r in top if r["status"] == "WATCH"]
    actionable = [r for r in top if r["status"] == "ACTIONABLE"][:5]

    # Issue-1 fix: watchlist is inclusive union (WATCH ∪ ELIGIBLE), ranked by alpha.
    watch_union = sorted((watch + eligible), key=lambda r: float(r["alpha_score"] or 0), reverse=True)
    watch_top = watch_union[:10]
    eligible = eligible[:10]
    watch = watch[:10]

    src_txt = ", ".join(f"{r['source']}:{r['c']}" for r in by_source) or "none"
    sig_txt = ", ".join(f"{r['signal_type']}:{r['c']}" for r in top_signals) or "none"
    sym_txt = ", ".join(f"{r['symbol']}({r['c']})" for r in top_symbols if r['symbol']) or "none"
    pot_txt = ", ".join(f"{r['symbol']}[{r['conviction']}]" for r in potentials if r['symbol']) or "none"
    health_txt = ", ".join(f"{r['source_name']}={r['status']}({r['c']})" for r in health) or "none"

    watch_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f}:{r['deny_reason'] or 'ok'})" for r in watch_top) or "none"
    eligible_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f})" for r in eligible) or "none"
    actionable_txt = ", ".join(f"{r['symbol']}({r['alpha_score']:.1f})" for r in actionable) or "none"
    reject_txt = ", ".join(f"{r['reason']}:{r['c']}" for r in rejects) or "none"
    pressure_txt = ", ".join(f"{r['base_symbol']}[obi={r['obi_20bps']:.2f},liq={r['liquidity_score']:.1f}]" for r in pressure) or "none"
    event_reject_txt = ", ".join(f"{r['reason']}:{r['c']}" for r in event_rejects) or "none"
    widest_txt = ", ".join(f"{r['base_symbol']}({r['spread_bps_p95_300']:.1f}bps)" for r in widest) or "none"
    best_liq_txt = ", ".join(f"{r['base_symbol']}({r['liquidity_score']:.1f}x)" for r in best_liq if r['base_symbol'] not in {'USDT','USDC','USD','EUR'}) or "none"

    counts_line = "none"
    eval_line = "none"
    stages_line = "none"
    rejects_line = "none"
    external_line = "none"
    sanity_line = "none"
    regime_line = "none"
    sources_present_txt = "{}"

    if funnel:
        counts_line = f"WATCH={funnel['watch_total'] or 0} ELIGIBLE={funnel['eligible_total'] or 0} ACTIONABLE={funnel['actionable_total'] or 0}"
        eval_line = f"alpha_scored={funnel['alpha_scored_total'] or 0} cost_eval={funnel['cost_evaluated_total'] or 0} risk_eval={funnel['risk_evaluated_total'] or 0}"
        stages_line = f"scored={funnel['alpha_scored_total'] or 0} costed={funnel['cost_evaluated_total'] or 0} cost_pass={funnel['cost_pass_total'] or 0} risked={funnel['risk_evaluated_total'] or 0} risk_pass={funnel['risk_pass_total'] or 0} sized={funnel['risk_pass_total'] or 0} actionable={funnel['actionable_total'] or 0}"
        rejects_line = str(funnel['rejects_tradable_json'] or '{}')
        external_line = str(funnel['rejects_external_json'] or '{}')
        sanity_line = str(funnel['sanity_json'] or '{}')
        regime_line = f"regime={funnel['regime'] or '-'} pressure_count={funnel['pressure_count'] or 0}"
        sources_present_txt = (funnel["sources_present_json"] or "{}")[:260]

    cliff_hint = "none"
    if funnel and int(funnel['risk_evaluated_total'] or 0) == 0 and int(funnel['cost_pass_total'] or 0) == 0:
        cliff_hint = "No candidates reached risk stage because cost_pass=0 (cost gate is current cliff)."
    elif funnel and int(funnel['risk_evaluated_total'] or 0) == 0:
        cliff_hint = "No candidates reached risk stage this window."

    examples = actionable[:3] if actionable else watch[:3]
    ex_txt = "; ".join(
        f"{r['symbol']} a={float(r['alpha_score'] or 0):.1f} status={r['status']} reason={r['deny_reason'] or '-'}"
        for r in examples
    ) or "none"

    return (
        "[Project Crypt][Phase-2][Hourly]\n"
        f"Window: last 1h\n"
        f"Fetched signals: {fetched}\n"
        f"By source: {src_txt}\n"
        f"Trend signals: {sig_txt}\n"
        f"Trending symbols: {sym_txt}\n"
        f"Potential buys under watch: {pot_txt}\n"
        f"Source health: {health_txt}\n"
        f"Counts: {counts_line}\n"
        f"Evaluated: {eval_line}\n"
        f"Stages: {stages_line}\n"
        f"CliffHint: {cliff_hint}\n"
        f"Rejects(tradable): {rejects_line}\n"
        f"Rejects(external): {external_line}\n"
        f"Sanity: {sanity_line}\n"
        f"MicroStats: {regime_line}\n"
        f"Sources present: {sources_present_txt}\n"
        f"Top Watchlist: {watch_txt}\n"
        f"Top Eligible: {eligible_txt}\n"
        f"Top Actionable: {actionable_txt}\n"
        f"Top reject reasons(decision-level): {reject_txt}\n"
        f"Event-level rejects: {event_reject_txt}\n"
        f"Micro pressure flags: {pressure_txt}\n"
        f"Micro widest spreads: {widest_txt}\n"
        f"Micro best liquidity: {best_liq_txt}\n"
        f"Examples: {ex_txt}\n"
        f"Periodic analysis: {periodic_analysis_txt}\n"
        f"Recommended focus: {periodic_reco}"
    )


def seconds_until_next_hour_sgt() -> int:
    sgt = ZoneInfo("Asia/Singapore")
    now = datetime.now(sgt)
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return max(1, int((next_hour - now).total_seconds()))


def main() -> None:
    tg = TelegramNotifier()
    # Align notifications to fixed wall-clock hour boundaries in Singapore time
    # (e.g., 16:00, 17:00, 18:00) regardless of process restarts.
    while True:
        sleep_s = seconds_until_next_hour_sgt()
        time.sleep(sleep_s)
        try:
            tg.send(hourly_summary())
        except Exception:
            pass


if __name__ == "__main__":
    main()
