from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from project_crypt.telegram_notify import TelegramNotifier

DB_PATH = "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3"
NOTIFIER_LOG = Path("/home/itachi/.openclaw/workspace/project_crypt/phase2_monitor_notifier.log")
NOTIFIER_HEARTBEAT = Path("/home/itachi/.openclaw/workspace/project_crypt/phase2_notifier_heartbeat.json")
DISPLAY_EXCLUDE_BASES = {x.strip().upper() for x in ("USDT,USDC,USD,EUR,PYUSD,TUSD,USDP,BUSD,DAI,FDUSD,USDE,USAT,USD1").split(",") if x.strip()}


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def _safe_ratio(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return num / den


def _safe_ratio_nullable(num: float, den: float) -> float | None:
    if den <= 0:
        return None
    return num / den


def _periodic_analysis(cur: sqlite3.Cursor, since_ts: str, limit: int, label: str) -> tuple[str, str, str]:
    cur.execute(
        """
        select discovered_total,mapped_to_venue_total,eligible_total,actionable_total,alpha_scored_total,cost_evaluated_total,cost_pass_total,risk_evaluated_total,risk_pass_total
        from candidate_funnel_log
        where ts >= ?
        order by id desc
        limit ?
        """,
        (since_ts, limit),
    )
    rows = cur.fetchall()
    if not rows:
        return f"{label}:none", "none", "no-funnel-data"

    discovered = sum(int(r["discovered_total"] or 0) for r in rows)
    mapped = sum(int(r["mapped_to_venue_total"] or 0) for r in rows)
    eligible = sum(int(r["eligible_total"] or 0) for r in rows)
    actionable = sum(int(r["actionable_total"] or 0) for r in rows)
    scored = sum(int(r["alpha_scored_total"] or 0) for r in rows)
    costed = sum(int(r["cost_evaluated_total"] or 0) for r in rows)
    cost_pass = sum(int(r["cost_pass_total"] or 0) for r in rows)
    risk_pass = sum(int(r["risk_pass_total"] or 0) for r in rows)

    # Step-down ratios (derivative bottleneck detection)
    r_scored_to_costed = _safe_ratio_nullable(costed, scored)
    r_costed_to_costpass = _safe_ratio_nullable(cost_pass, costed)
    r_costpass_to_riskpass = _safe_ratio_nullable(risk_pass, cost_pass)
    r_riskpass_to_actionable = _safe_ratio_nullable(actionable, risk_pass)

    stage_rates = {
        "scored_to_costed": r_scored_to_costed,
        "costed_to_costpass": r_costed_to_costpass,
        "costpass_to_riskpass": r_costpass_to_riskpass,
        "riskpass_to_actionable": r_riskpass_to_actionable,
    }
    valid = {k: v for k, v in stage_rates.items() if v is not None}
    bottleneck_stage = min(valid, key=valid.get) if valid else "none"

    recommendation = {
        "scored_to_costed": "Improve micro-feature coverage + mapping-quality so scored candidates reach cost eval.",
        "costed_to_costpass": "Cost gate is primary bottleneck: tune spread-toxic handling, slippage model, and small-cap lane sizing.",
        "costpass_to_riskpass": "Risk gate is bottleneck: inspect confidence thresholds and risk flags calibration.",
        "riskpass_to_actionable": "Final proposal stage is bottleneck: inspect final proposal constraints.",
        "none": "No valid bottleneck ratio due to low sample/denominator.",
    }[bottleneck_stage]

    f2 = lambda x: ("null" if x is None else f"{x:.2f}")
    analysis_txt = (
        f"{label}_step_rates scored->costed={f2(r_scored_to_costed)} costed->cost_pass={f2(r_costed_to_costpass)} "
        f"cost_pass->risk_pass={f2(r_costpass_to_riskpass)} risk_pass->actionable={f2(r_riskpass_to_actionable)}; "
        f"n(scored={scored},costed={costed},cost_pass={cost_pass},risk_pass={risk_pass},actionable={actionable}); "
        f"bottleneck={bottleneck_stage}"
    )
    return analysis_txt, bottleneck_stage, recommendation


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
            limit 40
            """
        )
        best_liq_raw = cur.fetchall()

        cur.execute(
            """
            select metrics_json
            from candidate_reject_log
            where ts >= ? and reason in ('net-edge-too-low','watch-spread-toxic')
            order by id desc
            limit 300
            """,
            (since,),
        )
        cost_metrics = cur.fetchall()

        cur.execute(
            """
            select reject_reason, count(*) c, avg(net_pnl_bps) a
            from ghost_sim_runs
            where ts >= ? and instrument_symbol like '%_%'
            group by reject_reason
            order by c desc
            limit 5
            """,
            (since_6h,),
        )
        ghost_rows = cur.fetchall()

        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1", (since,))
        attempt_60m = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=datetime('now','-5 minutes') and event_class='ATTEMPT' and counted=1")
        attempt_5m = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='EXECUTION' and counted=1", (since,))
        exec_60m = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=datetime('now','-5 minutes') and event_class='EXECUTION' and counted=1")
        exec_5m = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select event_reason,count(*) c from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1 group by event_reason order by c desc limit 3", (since,))
        top_attempt_reasons = cur.fetchall()
        cur.execute("select ts,event_class,symbol,event_reason,coalesce(lane,'-') lane from churn_event_log order by id desc limit 10")
        last_churn_events = cur.fetchall()
        cur.execute("select symbol,until_ts from symbol_penalty_box where until_ts > ? order by until_ts asc limit 8", (datetime.now(timezone.utc).isoformat(),))
        penalty_rows = cur.fetchall()

        periodic_analysis_txt_6h, bottleneck_6h, periodic_reco = _periodic_analysis(cur, since_6h, 72, "6h")
    periodic_analysis_txt_1h, bottleneck_1h, _ = _periodic_analysis(cur, since, 24, "1h")

    eligible = [r for r in top if r["status"] == "ELIGIBLE"]
    watch = [r for r in top if r["status"] == "WATCH"]
    actionable = [r for r in top if r["status"] == "ACTIONABLE"][:5]

    # Issue-1 fix: watchlist is inclusive union (WATCH ∪ ELIGIBLE), ranked by alpha.
    watch_union = sorted((watch + eligible), key=lambda r: float(r["alpha_score"] or 0), reverse=True)
    watch_top = watch_union[:10]
    eligible = eligible[:10]
    watch = watch[:10]

    active_bases = {str(r['symbol']).upper() for r in top if r['symbol']}
    best_liq = [r for r in best_liq_raw if str(r['base_symbol']).upper() in active_bases and str(r['base_symbol']).upper() not in DISPLAY_EXCLUDE_BASES][:6]

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

    cost_breakdown = {"spread_cost_dominant": 0, "slippage_dominant": 0, "vol_penalty_dominant": 0, "safety_margin_dominant": 0}
    safety_components = {"base": 0.0, "regime_component": 0.0, "uncertainty_component": 0.0, "n": 0}
    for row in cost_metrics:
        try:
            m = json.loads(row['metrics_json'] or '{}')
            dominant = ((m.get('cost_components') or {}).get('dominant'))
            if not dominant and m.get('bypass_cost_eval'):
                dominant = 'spread_cost_dominant'
            if dominant in cost_breakdown:
                cost_breakdown[dominant] += 1
            sb = m.get('safety_margin_breakdown') or {}
            if sb:
                safety_components["base"] += float(sb.get("base", 0.0) or 0.0)
                safety_components["regime_component"] += float(sb.get("regime_component", 0.0) or 0.0)
                safety_components["uncertainty_component"] += float(sb.get("uncertainty_component", 0.0) or 0.0)
                safety_components["n"] += 1
        except Exception:
            continue
    cost_breakdown_txt = ", ".join(f"{k}:{v}" for k, v in cost_breakdown.items())
    ghost_txt = ", ".join(f"{r['reject_reason']}({r['c']},avg={float(r['a'] or 0):.1f})" for r in ghost_rows) or "none"
    churn_top_reasons_line = ", ".join(f"{r['event_reason']}:{r['c']}" for r in top_attempt_reasons) or "none"
    churn_last_events_line = " | ".join(f"{r['ts']}:{r['event_class']}:{r['symbol']}:{r['event_reason']}:{r['lane']}" for r in last_churn_events) or "none"
    penalty_symbols_line = ", ".join(f"{r['symbol']}@{r['until_ts']}" for r in penalty_rows) or "none"
    best_liq_txt = ", ".join(f"{r['base_symbol']}({r['liquidity_score']:.1f}x)" for r in best_liq if r['base_symbol'] not in {'USDT','USDC','USD','EUR'}) or "none"
    if safety_components['n'] > 0:
        safety_line = (
            f"base={safety_components['base']/safety_components['n']:.1f},"
            f"regime={safety_components['regime_component']/safety_components['n']:.1f},"
            f"uncertainty={safety_components['uncertainty_component']/safety_components['n']:.1f}"
        )

    counts_line = "none"
    eval_line = "none"
    stages_line = "none"
    rejects_line = "none"
    external_line = "none"
    sanity_line = "none"
    regime_line = "none"
    micro_cov_line = "none"
    pre_cost_line = "none"
    util_line = "none"
    risk_codes_line = "{}"
    safety_line = "base=0.0,regime=0.0,uncertainty=0.0"
    risk_state_line = "halted=False reason=- churn=0 consec=0 cooldown=0s window=60m"
    churn_basis_line = "churn_basis=ACTIONABLE_ATTEMPTS"
    macro_line = "macro_shock=False"
    bloodbath_line = "LaneActive=False reason=- attempts_hour=0"
    bloodbath_candidates_line = "none"
    bloodbath_ghost_line = "1h:trades=0 net=0.0 win=0.0 | 24h:trades=0 net=0.0 win=0.0"
    bloodbath_reject_line = "none"
    dd_line = "dd_basis=shadow_ledger dd_1h=0.0 dd_24h=0.0"
    governor_line = "mode=recommend_only suspended=True reasons=[] n=0"
    churn_diag_line = "attempt5m=0 attempt60m=0 exec5m=0 exec60m=0 penalty_symbols=0 hot_loop=False suspect=False"
    churn_top_reasons_line = "none"
    churn_last_events_line = "none"
    penalty_symbols_line = "none"
    sources_present_txt = "{}"

    if funnel:
        counts_line = f"WATCH={funnel['watch_total'] or 0} ELIGIBLE={funnel['eligible_total'] or 0} ACTIONABLE={funnel['actionable_total'] or 0}"
        eval_line = f"alpha_scored={funnel['alpha_scored_total'] or 0} cost_eval={funnel['cost_evaluated_total'] or 0} risk_eval={funnel['risk_evaluated_total'] or 0}"
        stages_line = f"scored={funnel['alpha_scored_total'] or 0} costed={funnel['cost_evaluated_total'] or 0} cost_pass={funnel['cost_pass_total'] or 0} risked={funnel['risk_evaluated_total'] or 0} risk_pass={funnel['risk_pass_total'] or 0} sized={funnel['risk_pass_total'] or 0} actionable={funnel['actionable_total'] or 0}"
        rejects_line = str(funnel['rejects_tradable_json'] or '{}')
        external_line = str(funnel['rejects_external_json'] or '{}')
        sanity_line = str(funnel['sanity_json'] or '{}')
        regime_line = f"regime={funnel['regime'] or '-'} pressure_count={funnel['pressure_count'] or 0}"
        try:
            sj = json.loads(funnel['sanity_json'] or '{}')
        except Exception:
            sj = {}
        micro_cov_line = (
            f"micro_target_K={sj.get('micro_target_k', 0)} "
            f"micro_tracked_total={sj.get('micro_tracked_total', sj.get('micro_present', 0))} "
            f"micro_pinned_total={sj.get('micro_pinned_total', 0)} "
            f"micro_interest_total={sj.get('micro_interest_total', 0)} "
            f"eligible_unique={sj.get('eligible_unique', 0)} "
            f"intersection_with_eligible={max(0, sj.get('eligible_unique', 0) - sj.get('micro_join_fail', 0))} "
            f"join_rate={((sj.get('join_rate_to_eligible', {}) or {}).get('micro', 0))}"
        )
        pre_cost_line = str(sj.get('pre_cost_skip_breakdown', {}))
        util_line = str(sj.get('avg_notional_utilization', 0))
        risk_codes_line = str(sj.get('risk_reason_codes', {}))
        rs = sj.get('risk_state', {}) or {}
        risk_state_line = (
            f"halted={bool(rs.get('halted', False))} reason={rs.get('halt_reason') or '-'} prev={rs.get('previous_halt_reason') or '-'} "
            f"attempt_churn={int(rs.get('attempt_churn_count', 0) or 0)} exec_churn={int(rs.get('execution_churn_count', 0) or 0)} "
            f"loss24h={float(rs.get('loss_pressure_24h', 0.0) or 0.0):.1f} consec={int(rs.get('consecutive_losses', 0) or 0)} "
            f"cooldown={int(rs.get('cooldown_seconds_remaining', 0) or 0)}s window={int(rs.get('window_minutes', 60) or 60)}m"
        )
        churn_basis_line = f"churn_basis={rs.get('churn_basis', 'ACTIONABLE_ATTEMPTS')}"
        m = sj.get('macro_shock', {}) or {}
        macro_line = f"macro_shock={bool(m.get('macro_shock', False))} btc_1h_abs={m.get('btc_ret_1h_abs', 0)} news_velocity={m.get('news_velocity', 0)}"
        bb = sj.get('bloodbath_lane', {}) or {}
        bloodbath_line = f"LaneActive={bool(bb.get('active', False))} reason={bb.get('reason', '-')} reasons={bb.get('activation_reasons', [])} attempts_hour={int(bb.get('attempts_hour', 0) or 0)} majors_eval={int(bb.get('majors_evaluated', 0) or 0)}"
        cands = bb.get('candidates_top', []) or []
        bloodbath_candidates_line = ", ".join(
            f"{x.get('symbol')}[pc={x.get('pressure_confidence')},ss={x.get('spread_stability')},util={x.get('utilization')}]" for x in cands[:3]
        ) or "none"
        g1 = bb.get('ghost_1h', {}) or {}
        g24 = bb.get('ghost_24h', {}) or {}
        bloodbath_reject_line = str(bb.get('reject_breakdown', {}))
        bloodbath_ghost_line = (
            f"1h:trades={int(g1.get('trades', 0) or 0)} net={float(g1.get('net_pnl_bps', 0.0) or 0.0):.1f} win={float(g1.get('winrate', 0.0) or 0.0):.2f}"
            f" fill={float(g1.get('fill_rate', 0.0) or 0.0):.2f} expired={float(g1.get('expired_rate', 0.0) or 0.0):.2f} cond_fill_pnl={float(g1.get('pnl_conditional_on_fill', 0.0) or 0.0):.1f}"
            f" | 24h:trades={int(g24.get('trades', 0) or 0)} net={float(g24.get('net_pnl_bps', 0.0) or 0.0):.1f} win={float(g24.get('winrate', 0.0) or 0.0):.2f}"
        )
        dd_line = f"dd_basis={sj.get('dd_basis','shadow_ledger')} dd_1h={float(sj.get('dd_hourly_bps',0.0) or 0.0):.1f} dd_24h={float(sj.get('dd_daily_bps',0.0) or 0.0):.1f}"
        gov = sj.get('parameter_governor', {}) or {}
        gm = gov.get('metrics', {}) or {}
        governor_line = (
            f"mode={gov.get('mode','recommend_only')} suspended={bool(gov.get('suspended', True))} "
            f"reasons={gov.get('suspend_reasons', [])} n={int(gm.get('n', 0) or 0)} "
            f"mean={float(gm.get('mean', 0.0) or 0.0):.1f} p5={float(gm.get('p5', 0.0) or 0.0):.1f}"
        )
        rd = rs.get('details', {}) or {}
        risk_eval_n = int(funnel['risk_evaluated_total'] or 0)
        suspect = bool(exec_60m > max(5, risk_eval_n * 3) or (attempt_60m == 0 and risk_eval_n > 0 and exec_60m > 0))
        churn_diag_line = (
            f"attempt5m={attempt_5m} attempt60m={attempt_60m} "
            f"exec5m={exec_5m} exec60m={exec_60m} "
            f"penalty_symbols={int(rd.get('unique_penalty_symbols', 0) or 0)} hot_loop={bool(rd.get('hot_loop', False))} "
            f"suspect={suspect}"
        )
        sources_present_txt = (funnel["sources_present_json"] or "{}")[:260]

        # hard-stop bottleneck override
        if bool(rs.get('halted', False)) or (int(funnel['cost_pass_total'] or 0) > 0 and int(funnel['risk_pass_total'] or 0) == 0):
            bottleneck_1h = "RISK_HALT"
            bottleneck_6h = "RISK_HALT"
            periodic_reco = "Global risk halt dominates; fix RiskState/churn/loss machine before tuning alpha/cost thresholds."

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
        f"MicroCoverage: {micro_cov_line}\n"
        f"PreCostSkipBreakdown: {pre_cost_line}\n"
        f"AvgNotionalUtilization(actionable): {util_line}\n"
        f"MicroStats: {regime_line}\n"
        f"RiskState: {risk_state_line}\n"
        f"RiskChurnBasis: {churn_basis_line}\n"
        f"MacroShock: {macro_line}\n"
        f"BloodbathLane: {bloodbath_line}\n"
        f"BloodbathCandidatesTop: {bloodbath_candidates_line}\n"
        f"BloodbathRejectBreakdown: {bloodbath_reject_line}\n"
        f"BloodbathGhost: {bloodbath_ghost_line}\n"
        f"DDState: {dd_line}\n"
        f"ParameterGovernor: {governor_line}\n"
        f"ChurnDiagnostics: {churn_diag_line}\n"
        f"ChurnTopReasons: {churn_top_reasons_line}\n"
        f"ChurnLast10: {churn_last_events_line}\n"
        f"PenaltyBoxSymbols: {penalty_symbols_line}\n"
        f"Sources present: {sources_present_txt}\n"
        f"Top Watchlist: {watch_txt}\n"
        f"Top Eligible: {eligible_txt}\n"
        f"Top Actionable: {actionable_txt}\n"
        f"Top reject reasons(decision-level): {reject_txt}\n"
        f"Event-level rejects: {event_reject_txt}\n"
        f"Cost fail breakdown: {cost_breakdown_txt}\n"
        f"SafetyBreakdown(avg): {safety_line}\n"
        f"Risk reason codes: {risk_codes_line}\n"
        f"AutoTuner(tradable-only ghost 6h): {ghost_txt}\n"
        f"Micro pressure flags: {pressure_txt}\n"
        f"Micro widest spreads: {widest_txt}\n"
        f"Micro best liquidity: {best_liq_txt}\n"
        f"Examples: {ex_txt}\n"
        f"Bottleneck(1h): {bottleneck_1h}\n"
        f"Bottleneck(6h): {bottleneck_6h}\n"
        f"Periodic analysis (1h): {periodic_analysis_txt_1h}\n"
        f"Periodic analysis (6h): {periodic_analysis_txt_6h}\n"
        f"Recommended focus: {periodic_reco}"
    )


def _append_log(msg: str) -> None:
    try:
        with NOTIFIER_LOG.open("a", encoding="utf-8") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
    except Exception:
        pass


def _write_heartbeat(ok: bool, error: str | None = None) -> None:
    try:
        NOTIFIER_HEARTBEAT.write_text(json.dumps({
            "ts": datetime.now(timezone.utc).isoformat(),
            "ok": bool(ok),
            "error": error,
        }))
    except Exception:
        pass


def seconds_until_next_hour_sgt() -> int:
    sgt = ZoneInfo("Asia/Singapore")
    now = datetime.now(sgt)
    next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return max(1, int((next_hour - now).total_seconds()))


def _send_hourly_with_fallback(tg: TelegramNotifier, text: str) -> bool:
    ok = bool(tg.send(text))
    if ok:
        return True
    compact = text
    if len(compact) > 3400:
        compact = compact[:3400] + "\n...[truncated for telegram safety]"
    return bool(tg.send(compact))


def main() -> None:
    tg = TelegramNotifier()
    _append_log("notifier_start")
    # Align notifications to fixed wall-clock hour boundaries in Singapore time
    # (e.g., 16:00, 17:00, 18:00) regardless of process restarts.
    while True:
        sleep_s = seconds_until_next_hour_sgt()
        _append_log(f"sleep_until_next_hour_s={sleep_s}")
        time.sleep(sleep_s)
        try:
            ok = _send_hourly_with_fallback(tg, hourly_summary())
            _append_log(f"hourly_send_ok={ok}")
            _write_heartbeat(ok=ok, error=None if ok else "send_returned_false")
        except Exception as e:
            _append_log(f"hourly_send_error={e}")
            _write_heartbeat(ok=False, error=str(e))


if __name__ == "__main__":
    main()
