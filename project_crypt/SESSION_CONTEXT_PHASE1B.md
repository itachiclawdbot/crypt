# Project Crypt — Session Context Memory (Phase-1 → Phase-1B/Phase-2)

Last updated: 2026-02-23 23:55 SGT
Owner: Hari
Assistant: Itachi

## Why this file exists
This file is the compact continuity memory for future sessions.
In the next session, load these for full context:
1. `project_crypt/architecture/PHASE1B_ARCHITECTURE.md`
2. `project_crypt/PHASE1B_BLUEPRINT.md`
3. `project_crypt/SESSION_CONTEXT_PHASE1B.md` (this file)
4. Core code under `project_crypt/`

---

## Session outcomes (what was done)

### 1) Phase-1 status checks + continuity
- Verified old Phase-1 run state.
- Re-ran Phase-1 gate check successfully (`approved`).
- Added monitor for 5-min activity checks and Telegram updates.

### 2) Phase-1 visibility
- Added live terminal dashboard for Phase-1:
  - `project_crypt/live_dashboard.py`

### 3) Phase-1B planning and consolidation
- Created/updated comprehensive blueprint:
  - `project_crypt/PHASE1B_BLUEPRINT.md`
- Added required API/approval/skill readiness checklist.

### 4) GitHub integration work
- Verified GitHub auth via token.
- Pushed **clean Phase-1 baseline** to GitHub repo `itachiclawdbot/crypt` on branch:
  - `phase1-baseline`
- Added README for phase1 branch.

### 5) Phase-2 (working-source implementation in shadow mode)
- Built and tested working-source aggregator:
  - `project_crypt/phase2_alpha_aggregator.py`
- Added preflight:
  - `project_crypt/phase1b_preflight.py`
- Added hourly notifier to Telegram:
  - `project_crypt/phase2_monitor_notifier.py`
- Added ops dashboard for current/last jobs:
  - `project_crypt/phase2_ops_dashboard.py`
- Added launcher script to avoid cwd module errors:
  - `/home/itachi/.openclaw/workspace/run_phase2_dashboard.sh`
- Branch pushed:
  - `phase2-working`

### 6) Architecture doc added
- Full architecture-format document created:
  - `project_crypt/architecture/PHASE1B_ARCHITECTURE.md`

### 7) Funnel observability + resilient scoring rollout (shadow mode)
- Added cycle-level funnel telemetry to stop blind threshold tuning:
  - `candidate_funnel_log` (discovered→mapped→eligible→alpha→risk→cost→proposed)
  - `candidate_reject_log` (symbol, stage, reason, metrics)
  - `universe_state` (per-symbol status, scores, deny stage/reason, source map)
- Replaced hard alignment dependency with weighted alpha scoring that degrades gracefully when X/Reddit are unavailable.
- Added ranked outputs every cycle:
  - top watchlist,
  - top eligible,
  - top actionable.
- Added fast tick architecture:
  - market mini-refresh every 60s (default),
  - discovery mini-refresh every 120s (default),
  - full decision cycle every 300s (default).
- Added startup Telegram ping after first successful cycle with funnel snapshot and top candidates.
- Added kill-switch runtime visibility + critical alert behavior in aggregator/dashboard status.

### 8) Venue-native universe + adaptive eligibility + schema-drift hardening
- Fixed Crypto.com instrument parsing bug due to API schema drift:
  - old expected: `result.instruments` + `instrument_name`
  - current actual: `result.data` + `symbol/base_ccy/quote_ccy`
- Added venue-native discovery stream from Crypto.com tickers:
  - top-volume symbols,
  - top %-move symbols,
  - spread-event symbols.
- Added new-listing detection by diffing current Crypto.com base universe vs persisted prior snapshot (`phase2_listing_state.json`).
- Discovery universe now merges external + venue-native sources:
  - CoinGecko/CMC/DEX + Crypto.com movers + new listings.
- Added adaptive eligibility thresholds:
  - `min_volume_24h = max(100k, P25 venue volume)`
  - `max_spread_bps = min(50, P75 venue spread)`
  - notional-aware liquidity coverage (`volume / target_notional`) contributes to eligibility scoring.
- Extended alpha scoring with microstructure component (spread + mover membership + 24h move magnitude) and source-missing reweighting.
- Result after fix: mapping stage recovered (mapped was stuck at 0; now significant mapped counts appear each cycle).

### 9) Scored decision pipeline upgrade (non-binary cliff behavior)
- Kept strict 3-state output every cycle per symbol (no silent drop):
  - `WATCH`, `ELIGIBLE`, `ACTIONABLE`.
- Added structured decision payload fields across state output/logs:
  - `stage_reached`, `deny_reason`, `alpha_score`, `cost_edge_bps`, `uncertainty`, `risk_flags`.
- Added explicit cost model function (`estimate_cost_bps`) with:
  - half-spread,
  - slippage proxy from liquidity coverage,
  - volatility penalty.
- Cost gate now uses edge accounting:
  - `cost_edge_bps = expected_edge_bps - estimated_cost_bps - safety_margin_bps`.
- Added separate stricter treatment for new listings in risk lane:
  - higher safety margin,
  - higher confidence requirement.
- Added DEX boost top endpoint ingestion (`/token-boosts/top/v1`) to strengthen attention-shock proxy signal.

### 10) Microstructure Engine v1 (single-VM microservice style)
- Added dedicated microstructure process:
  - `project_crypt/microstructure_service.py`
- Service behavior:
  - pulls top Crypto.com USD/USDT symbols by quote volume,
  - fetches orderbook snapshots (`get-book`) per symbol,
  - computes v1 microstructure features:
    - spread_bps,
    - depth_usd_10bps / depth_usd_20bps,
    - OBI_10bps / OBI_20bps,
    - pressure_flag,
    - orderbook_slope (log depth vs log band proxy),
    - liquidity_score = depth20 / target_notional,
    - rolling spread stats (median + p95).
- Persistence layer:
  - `micro_features_log` (time series)
  - `micro_features_latest` (latest state per symbol)
- Alpha/gating integration in `phase2_alpha_aggregator.py`:
  - loads latest micro features per symbol,
  - uses micro spread p95 for eligibility/cost context,
  - uses depth/liquidity for cost model and risk flags,
  - boosts alpha on pressure/OBI conditions (capped),
  - logs micro metrics into universe state for diagnostics.
- Notifier integration:
  - hourly digest includes micro highlights:
    - pressure flags,
    - widest spreads,
    - best liquidity symbols.
- Runtime processes now expected:
  - `phase2_alpha_aggregator`
  - `phase2_monitor_notifier`
  - `microstructure_service`

### 11) DEXScreener attention-feed fix (profiles/boosts symbol resolution)
- Verified DEX endpoints are reachable and healthy (200):
  - `/token-profiles/latest/v1`
  - `/token-boosts/latest/v1`
  - `/token-boosts/top/v1`
- Root issue: profile/boost payloads often do not include `tokenSymbol`/`symbol`, causing near-zero usable symbols.
- Implemented resolver in aggregator:
  - batches token addresses into `/latest/dex/tokens/{addr1,addr2,...}`,
  - resolves canonical symbol from highest-liquidity pair `baseToken.symbol`.
- Result: DEX symbols now ingest correctly (latest test window showed non-null symbol signals for profiles/boosts/top boosts).

### 12) DeFiLlama + Dune on-chain intel integration (v1)
- Added DeFiLlama ingestion into aggregator cycle:
  - endpoints used: `/v2/chains`, `/protocols` (free/public)
  - emits `chain_tvl` and `protocol_tvl` signals to `intel_cache`
  - contributes symbols to discovery universe and source-availability map.
- Added Dune ingestion hook:
  - optional via `DUNE_API_KEY` + `DUNE_QUERY_IDS` env,
  - fetches query results and emits `query_signal`/`query_summary` signals,
  - degrades gracefully to `DISABLED` when keys/queries are missing.
- Discovery union now includes on-chain symbols:
  - `... + defillama_symbols + dune_symbols`.
- Status summary now reports:
  - `defillama_symbols_total`, `defillama_points`,
  - `dune_symbols_total`, `dune_points`.
- Current runtime validation:
  - DeFiLlama active with non-zero symbols,
  - Dune currently disabled/zero due to missing configured query outputs in env.

### 13) Phase-2 Reliability + Bridge-Alpha upgrade wave
- Identity normalization upgrades:
  - canonical joins now use `instrument_symbol` (Crypto.com CCY pair like `SUI_USDT`) whenever mapped,
  - base/quote stored separately in `universe_state` (`base_ccy`, `quote_ccy`),
  - invalid quote assets filtered from candidate universe (`USDT/USDC/USD/EUR`) plus non-alnum symbol sanitization.
- Funnel/reporting correctness upgrades:
  - added additive funnel columns via runtime-safe schema migration checks:
    - `watch_total`, `actionable_total`, `alpha_scored_total`, `cost_evaluated_total`, `risk_evaluated_total`,
    - `rejects_tradable_json`, `rejects_external_json`, `sanity_json`, `regime`, `pressure_count`.
  - hourly digest now prints counts/evaluated/reject splits/sanity lines and compact examples.
- Watchlist output fix:
  - watchlist now built from deduped ELIGIBLE universe first and never silently empty when eligible exists.
- Dynamic social-proxy reweighting:
  - when X/Reddit missing, strong venue+mircostructure conditions lower effective alpha floor from 70 to 50 per symbol.
- Regime-aware OBI thresholds:
  - regime classifier (QUIET/NORMAL/VOLATILE), OBI pressure threshold adapts by regime.
- Bridge-alpha listing lane:
  - added `listing_watch` + `listing_watch_events` tables,
  - DEX + fundamentals non-tradable tokens are tracked in listing watch,
  - new Crypto.com listing matching watch emits `listing_front_run` shadow actionable signal.
- Ghost simulator:
  - added `ghost_sim_runs` table and hourly async ghost simulation from rejected mapped signals (5m horizon heuristic).
- Migration artifact added:
  - `project_crypt/migrations/2026-02-23_phase2_reliability_bridge_alpha.sql`

### 14) Four-issue reconciliation pass (watchlist/reject math/stage visibility/micro coverage)
- Issue 1 (watchlist none bug):
  - notifier now renders watchlist from `WATCH ∪ ELIGIBLE` ranked by alpha score (with deny reason), not from restrictive sub-filters.
- Issue 2 (reject reconciliation):
  - notifier reject reasons now sourced from decision-level `universe_state` (deduped by instrument),
  - event-level rejects retained in separate line from `candidate_reject_log`.
- Issue 3 (risk_eval=0 ambiguity):
  - hourly adds explicit stage reach line:
    - `scored`, `costed`, `cost_pass`, `risked`, `risk_pass`, `sized`, `actionable`.
  - adds `CliffHint` when risk stage is unreachable because cost pass is zero.
- Issue 4 (micro join too low):
  - microstructure service upgraded from fixed top-30 to dynamic active-universe tracking (default top-80),
  - tracks union of recent candidate instruments + top-volume instruments,
  - allows micro coverage to follow active decision universe instead of only blue chips.
- Additional tactical gate improvement:
  - social-missing top-K alpha fallback (`PHASE2_ALPHA_TOPK`, default 20) among eligible mapped symbols to avoid hard alpha deadlocks.

---

## Current source strategy (active)
Priority routing currently implemented for shadow mode:
1. Crypto.com (main)
2. CoinGecko trending
3. CoinMarketCap rankings/market cap
4. DEX Screener latest/boosted
5. CryptoPanic + Free-Crypto-News headline scans

X/Reddit direct ingestion:
- API keys unavailable at this time.
- Browser relay attach was unavailable during this session.
- Cookie-based direct attempts returned unauthorized/forbidden.
- So X/Reddit are currently disabled/degraded.

---

## Runtime behavior currently expected
- `phase2_alpha_aggregator.py`: 300s full cycle (+ jitter), writes `intel_cache`, `source_health_log`, `candidate_funnel_log`, `candidate_reject_log`, `universe_state`, and `phase2_status.json`.
- Fast sub-ticks inside cycle wait window:
  - market fast tick (`PHASE2_MARKET_TICK_SECONDS`, default 60s)
  - discovery fast tick (`PHASE2_DISCOVERY_TICK_SECONDS`, default 120s)
- Startup ping behavior:
  - after first successful cycle post-boot, sends Telegram startup summary with funnel + top actionable/watchlist + tick intervals.
- Kill-switch behavior:
  - checks `KILL_SWITCH_FILE` (default `/var/run/cryptobot/STOP`), enters halted hold loop if present,
  - emits critical Telegram alert once while active,
  - status JSON includes `kill_switch_active` for dashboard visibility.
- `phase2_monitor_notifier.py`: hourly Telegram digest includes both legacy and new observability fields:
  - fetched signals,
  - by source,
  - trend signals,
  - trending symbols,
  - potential buys,
  - source health,
  - funnel metrics,
  - sources-present map,
  - top watch/eligible/actionable,
  - top reject reasons.
- Periodic analytics layer added in notifier (6h rolling):
  - computes stage conversion rates (mapping, eligibility, alpha, risk, cost, proposal),
  - detects current bottleneck stage,
  - attaches a tactical recommendation in Telegram digest for the next tuning focus.
- `phase2_ops_dashboard.py`: displays funnel stages, top reject reasons, top watch/eligible/actionable, and kill-switch flag.

---

## GitHub branch map
- `phase1-baseline` → stable Phase-1 baseline + branch README
- `phase2-working` → Phase-2 shadow intelligence implementation + README + monitor + dashboard

---

## Pending work (next steps)
1. Full Phase-1B DB migrations (`proposal_log`, `model_eval_log`, `incident_log`, `universe_log`, expanded `risk_log`).
2. Multi-layer gate engine (safety/eligibility/alpha/risk/cost/sizing/drift) end-to-end.
3. Forecast feature/model loop + calibration/drift logic.
4. Parquet/DuckDB artifact layer.
5. Optional re-enable X/Reddit when compliant path is available.

---

## User directives to persist
- Keep sending significant status updates to Telegram for long-running activity.
- Keep GitHub commits frequent and structured.
- Maintain shadow mode unless explicitly told to enable live execution.
- After each meaningful update/change, append/update this memory file.

---

## Update protocol for future changes
After each material code or architecture update:
1. Update this file (`SESSION_CONTEXT_PHASE1B.md`) summary sections.
2. If architecture changed, also update `architecture/PHASE1B_ARCHITECTURE.md`.
3. Commit docs updates together with code changes.

---

## 2026-02-23 second 4-issue pass (high-priority fixes)
+ Updated session and project description


---

## 2026-02-23 second 4-issue pass (high-priority fixes)
1) Micro join coverage uplift
- microservice moved to interest-based stable universe management with hysteresis.
- Added persistent universe state (`micro_universe_state.json`) with:
  - `MICRO_MAX_REPLACEMENTS` (default 20)
  - `MICRO_MIN_RESIDENCY_SEC` (default 1800)
- Candidate symbols now come from active decision universe + top volume + pinned majors.
- Coverage target increased beyond previous 80-book fixed approach.

2) Pressure quality guardrail
- pressure signal now requires:
  - OBI threshold pass,
  - `liquidity_score >= 10`,
  - spread <= 80 bps.
- Prevents OBI=1.0 thin-book artifacts from boosting alpha.

3) Periodic analysis correctness
- 6h periodic analysis switched to outcome-based rates from stage counters:
  - mapping, eligibility, scored, cost_pass, risk_pass, actionable.
- Bottleneck now reflects actual stage outcomes, not stale legacy alpha booleans.

4) Cost lane realism for alts
- Added spread-toxic bypass (`spread > 80bps`) into watch-like handling.
- Added small-cap lane notional scaling (0.25x effective notional) for cost estimation.

5) Reporting alignment updates
- Watchlist rendered from `WATCH ∪ ELIGIBLE` ranked by alpha.
- Decision-level rejects separated from event-level rejects.
- Stage reach line + cliff hint retained.

Runtime note:
- Services restarted after this pass:
  - `project_crypt.microstructure_service`
  - `project_crypt.phase2_alpha_aggregator`
  - `project_crypt.phase2_monitor_notifier`

---

## 2026-02-23 five-issue pass (periodic math, micro coverage, cost explainability, pressure persistence, external-lane isolation)
1) Periodic analysis correctness
- `_periodic_analysis` now computes 6h stage rates from `candidate_funnel_log` audited totals (`actionable_total`, `alpha_scored_total`, `cost_evaluated_total`, `cost_pass_total`, `risk_evaluated_total`, `risk_pass_total`) only.
- No event-level leakage into stage-rate math.

2) Micro coverage scaling + observability
- `MICRO_TOP_SYMBOLS` default increased to 150 and interest-based tracked set retained with hysteresis.
- Hourly digest now includes `MicroCoverage: micro_tracked_count, intersection_with_eligible, join_rate`.

3) Cost gate explainability
- Cost rejection dominant-cause classification added:
  - spread_cost_dominant,
  - slippage_dominant,
  - vol_penalty_dominant,
  - safety_margin_dominant.
- Stored both costs in decision/reject metrics:
  - `est_cost_bps_full_size`,
  - `est_cost_bps_smallcap_lane`.
- Hourly digest prints cost failure breakdown.

4) Pressure anti-artifact persistence
- Pressure requires persistence (`>=2 updates` or `>=30s`) in microservice.
- Added artifact guard for extreme OBI when ask depth near-zero.
- OBI>0.9 now logs bid/ask depth debug fields via payload metrics.

5) Tradable-only tuning lane separation
- Hourly now surfaces `AutoTuner(tradable-only ghost 6h)` from `ghost_sim_runs` filtered to mapped instrument symbols.
- External non-tradable discovery remains visible but isolated from tradable tuning interpretation.

---

## 2026-02-23 two-issue pass (micro capacity + identity hygiene in liquidity display)
1) Micro capacity-limit fix
- microservice target tracking increased and made explicitly capacity-aware:
  - `MICRO_TOP_SYMBOLS` default raised to 150,
  - target size now `min(eligible_unique, K)` with continuity floor,
  - stable-asset bases removed from tracked candidates.
- Added persistent state fields in `micro_universe_state.json`:
  - `micro_target_k`, `eligible_unique`.
- Result: micro coverage improved significantly in current cycles (micro_present and join_rate materially higher).

2) Micro liquidity display hygiene
- hourly “best liquidity” query now filters to:
  - tradable active universe intersection (`universe_state` recent mapped bases),
  - excludes stable/quote-like bases (`USDT/USDC/USD/EUR/PYUSD/...`) from display lane.
- Prevents PYUSD-like entries from contaminating top-liquidity UX output.

---

## 2026-02-23 final 2-issue pass (display hygiene + recommendation correctness)
1) Liquidity display cleanup
- Added configurable `DISPLAY_EXCLUDE_BASES` in notifier including `USAT`, `USD1` and stable/fiat proxies.
- “Micro best liquidity” now displays only bases that are:
  - in active decision universe (`top` decision rows), and
  - not in `DISPLAY_EXCLUDE_BASES`.
- This is display-layer filtering only (no hard removal from ingestion universe).

2) Recommendation engine correctness
- Reworked periodic analysis recommendation to derivative step-down bottleneck logic:
  - `scored->costed`
  - `costed->cost_pass`
  - `cost_pass->risk_pass`
  - `risk_pass->actionable`
- Recommendation now points to actual weakest step (e.g., cost bottleneck) instead of generic final-stage over-filtering text.

---

## 2026-02-23 three-issue modeling pass (slippage/adaptive size + execution style + safety/pre-cost audit)
1) Slippage-dominant mitigation via adaptive sizing
- Added `target_notional_adj` solved from slippage-cap constraints against depth.
- Actionable candidates can now pass with reduced size (`size_reduced=true`) instead of hard rejection.

2) Execution-style aware cost model
- Dual path modeled per candidate:
  - taker path
  - maker-first proxy path
- Planner chooses cheaper feasible style (`execution_style` logged per decision).

3) Safety margin + pre-cost visibility improvements
- Safety margin now regime/liquidity adaptive (reduced in normal/high-liquidity regimes; preserved in volatile/new-listing contexts).
- Added hourly `PreCostSkipBreakdown` + `AvgNotionalUtilization(actionable)` lines for scored-vs-costed audit.
- Cost diagnostics now retain both:
  - `est_cost_bps_full_size`
  - `est_cost_bps_smallcap_lane`
  plus maker estimate for style comparison.

---

## 2026-02-23 five-issue consolidated implementation (slippage curve + safety decomposition + risk codes + maker fill-prob + pressure confidence)
1) Curve-based slippage + adaptive k_slip
- Migrated cost slippage from single-band depth to interpolated curve impact `b*` across depth bands 5/10/20/50 bps.
- Added regime/liquidity/orderbook-slope calibration for `k_slip`.
- Resizing loop now solves adjusted notional from curve slippage budget, not single-band depth.
- Added per-decision audit fields: `k_slip_used`, `b_interpolated_full`, `b_interpolated_adj`, `impact_multiplier`.

2) Safety margin decomposition + anti-double counting
- Replaced opaque margin with components: `base + regime_component + uncertainty_component` plus floor/cap rules.
- Margin now applied as additive-on-raw-cost guard, avoiding recursive multiplication of volatility penalties.
- Added `safety_margin_breakdown` to decision/reject metrics.

3) Risk gate as explicit second bottleneck
- Added standardized risk reason codes:
  - `RISK_DD_DAILY`, `RISK_DD_HOURLY`, `RISK_MAX_POSITIONS`, `RISK_CONCENTRATION`, `RISK_CHURN`, `RISK_CONSEC_LOSSES`.
- Introduced early risk checks (max positions/concentration) before expensive late-stage logic.
- Late risk checks now gate final actionables for drawdown/churn/consecutive-loss controls.
- Added regime-aware max concurrency (`NORMAL` vs `VOLATILE`) via env-configurable limits.

4) Maker-first expected-cost realism
- Added maker fill probability proxy and expected-cost calculation:
  - `E[cost] = p_fill * cost_maker + (1-p_fill) * cost_fallback`.
- Added maker time budget and audit fields:
  - `maker_time_budget_sec`, `p_fill_maker`, `est_cost_bps_maker_expected`.

5) Micro pressure quality and confidence scoring
- `microstructure_service.py` now persists `depth_usd_5bps` and `depth_usd_50bps` in addition to 10/20.
- Added strict pressure confidence score based on persistence, liquidity, and spread stability.
- Retained artifact guard for extreme OBI with thin-side depth.
- Added hourly digest observability lines for safety breakdown and risk reason code distribution.

---

## Persistent update protocol (effective 2026-02-24)
Owner directive: After **every meaningful modification** (logic/routing/schema/ops behavior), update this session context file in the same work pass.

Mandatory post-change notes to append each time:
1) Decision change summary (what changed and why)
2) Routing/flow impact (where in pipeline it now applies)
3) Data-structure/schema impact (tables/columns/json fields)
4) Runtime/ops impact (services, schedules, watchdogs, alerts)
5) Validation evidence (compile/tests/query/telegram force check)
6) Git traceability (branch + commit hash)

Operational rule:
- Do not wait for user reminder.
- Treat context update as part of “definition of done” for every code change.

---

## 2026-02-25 reliability hardening (notifier resilience + auto-restart)
1) Decision change summary
- Root cause of missing hourly Telegram alerts: process-termination events (`SIGTERM`) killed notifier/runtime sessions.
- Added dual-layer resilience: process watchdog + notifier heartbeat guard.

2) Routing/flow impact
- Existing phase2 watchdog remains minute-level restart for all 3 services.
- New notifier guard runs every 15 minutes and performs fallback force-send if heartbeat is stale (>75m).

3) Data-structure/schema impact
- No SQLite schema change.
- Added file-based heartbeat state:
  - `project_crypt/phase2_notifier_heartbeat.json`
- Added guard logs:
  - `project_crypt/phase2_notifier_guard.log`

4) Runtime/ops impact
- `phase2_monitor_notifier.py` now logs startup/sleep/send outcomes to `phase2_monitor_notifier.log` and writes heartbeat after send attempts.
- New script: `project_crypt/phase2_notifier_guard.sh`.
- Cron entries now include:
  - `* * * * * phase2_watchdog.sh`
  - `*/15 * * * * phase2_notifier_guard.sh`

5) Validation evidence
- Verified services running with active PIDs.
- Force Telegram send succeeded (`telegram_send_ok True`).
- Guard script executed and heartbeat file updated with `ok=true` timestamp.

6) Git traceability
- Pending commit in `phase2-working` for notifier resilience changes.

---

## 2026-02-25 Risk Halt + Shock Awareness fix wave (shadow mode)
1) Decision change summary
- Fixed persistent risk-halt behavior by moving churn semantics to execution-like basis only.
- Added explicit global `RiskState` so risk halts are represented once as system state rather than symbol-specific spam.
- Added lightweight macro shock detection and freeze path for tuning-like updates during shock windows.
- Hardened notifier delivery to avoid missed hourlies caused by oversized message payloads.

2) Routing/flow impact
- Risk evaluation now checks `RiskState` first at cost-pass stage; when halted, candidates become `BLOCKED_BY_RISK` with global reason (`RISK_GLOBAL_HALT`) instead of per-symbol pseudo-specific churn/loss spam.
- Churn basis is now `ACTIONABLE_ATTEMPTS (+ ghost executions)` only.
- Ghost simulator hourly updates are skipped during macro shock windows (`ghost_sim_hourly_skipped` signal).

3) Data-structure/schema impact
- Added new table: `risk_state_current` (single-row persisted global risk machine).
- Added `risk_state`, `macro_shock`, `autotuner_frozen` into cycle sanity/summary payloads (stored in `candidate_funnel_log.sanity_json` and status summary).
- Added notifier heartbeat/guard artifacts previously:
  - `phase2_notifier_heartbeat.json`
  - `phase2_notifier_guard.log`

4) Runtime/ops impact
- `phase2_alpha_aggregator.py` now computes and persists global risk state each cycle.
- `phase2_monitor_notifier.py` now reports:
  - RiskState line (halted/reason/churn/consec/cooldown/window)
  - churn basis line
  - macro shock line
  - both 1h and 6h bottleneck lines
  - SafetyBreakdown defaults always numeric (no `none` blind spots)
- Notifier send path now retries with truncated payload fallback when message size exceeds Telegram practical limits.

5) Validation evidence
- Python compile checks passed for aggregator + notifier.
- Services confirmed running after restart via watchdog.
- Forced Telegram send succeeded after fallback (`send_ok=True`) with long hourly payload.
- Watchdog and notifier guard cron remain active.

6) Git traceability
- Pending commit in `phase2-working` for this risk/shock/notifier wave.

---

## 2026-02-25 Bloodbath Lane wave (majors-only shock lane + maker realism + ghost scoreboard)
1) Decision change summary
- Added isolated `BLOODBATH_LANE` (shadow-first) for panic-bounce capture in macro shock / volatile conditions.
- Added lane-level risk isolation and cooldown behavior, without weakening global RiskState.
- Added dedicated bloodbath ghost simulation records + hourly lane scoreboard metrics.

2) Routing/flow impact
- Lane activation uses `macro_shock || regime==VOLATILE` plus hysteresis (`BLOODBATH_HYSTERESIS_SEC`).
- Lane hard-stands-down when global RiskState halted.
- Candidate universe restricted to majors allowlist (`BLOODBATH_SYMBOL_ALLOWLIST`).
- Candidate gate requires pressure-confidence + spread-stability + spread cap + depth-utilization.

3) Data-structure/schema impact
- New table `bloodbath_ghost_runs`:
  - ts, symbol, lane, entry/exit, net_pnl_bps, execution_style, utilization, regime, macro_shock, fill/cost fields.
- New table `bloodbath_lane_state`:
  - active, reason, hysteresis/cooldown times, consecutive losses, attempts counters.
- Existing summary/sanity payload now includes `bloodbath_lane` section.

4) Runtime/ops impact
- `phase2_alpha_aggregator.py` now computes/runs `run_bloodbath_lane(...)` each cycle.
- Lane-specific guards:
  - attempts per symbol/hour and total attempts/hour,
  - lane consecutive loss limit + cooldown,
  - daily lane drawdown cap (shadow).
- Maker-first realism in lane simulation uses `p_fill_maker` and maker time budget with fallback expected cost.

5) Validation evidence
- Python compile checks passed.
- Services restarted and confirmed running.
- Forced Telegram send succeeded after this wave (`telegram_send_ok True`).

6) Git traceability
- Pending commit in `phase2-working` for Bloodbath Lane implementation.

---

## 2026-02-25 unified 14-issue autotuner + risk-state fix wave
1) Decision change summary
- Implemented full operational correctness + autotuner fidelity + safe governor wave.
- Risk halt semantics now distinguish active trigger vs cooldown (`COOLDOWN`) and keep `previous_halt_reason`.
- Added robust ghost analytics (cost-fidelity, stratified sampling, shock tagging, robust distribution stats).
- Added recommend-only `parameter_governor` with suspension/tail/stability gates and audit log.

2) Routing/flow impact
- `compute_shadow_risk_state(macro_shock)` now tracks:
  - attempt churn (soft throttle), execution churn (hard halt), decayed/sliding loss pressure,
  - explicit cooldown state machine.
- During global halt, downstream candidates remain globally blocked (`RISK_GLOBAL_HALT`) without per-asset spam semantics.
- Ghost simulator runs even in macro shock but rows are tagged; mutation triggers remain guarded.

3) Data-structure/schema impact
- Added/extended DB structures:
  - `macro_state_current` (macro shock hysteresis state)
  - `parameter_governor_log` (recommendation audit)
  - `ghost_sim_runs` new columns: `macro_shock`, `estimated_cost_bps`, `execution_style`
  - `risk_state_current` new columns: `previous_halt_reason`, `attempt_churn_count`, `execution_churn_count`, `loss_pressure_24h`

4) Runtime/ops impact
- MacroShock now uses entry/exit hysteresis (not always-on) and persists state in DB.
- Bloodbath lane in shock reduces attempt caps and defaults non-fill to `EXPIRED_UNFILLED` (no taker fallback by default).
- Hourly notifier improvements:
  - bottleneck hard-stop override to `RISK_HALT` when appropriate,
  - denominator-null-safe ratios with sample-size prints,
  - richer RiskState + Governor lines.

5) Validation evidence
- Compile checks passed for aggregator + notifier.
- Services restarted and running via watchdog.
- Forced Telegram send successful after deployment.

6) Git traceability
- Pending commit in `phase2-working` for unified 14-issue wave.

---

## 2026-02-25 unified churn safety + tuner fidelity + governor UX wave (16 issues)
1) Decision summary
- Added strict churn taxonomy with dedicated telemetry (`ATTEMPT`, `EXECUTION`, `EVALUATION` semantics).
- Added hot-loop protection and tiered churn controls (symbol penalty box, systemic attempt churn halt).
- Hardened RiskState to differentiate `COOLDOWN` from active triggers; retained `previous_halt_reason`.
- Improved autotuner fidelity (decision-time cost ghosting, stratified sampling, robust/tail stats, shock-tagged rows).
- Extended governor UX to multi-cause suspension reasons list.

2) Routing/flow impact
- Risk machine now reads churn from `churn_event_log` (attempt vs execution windows) rather than inferred universe-only counts.
- BLOODBATH lane logs explicit attempt/execution outcomes and respects penalty boxes.
- During global halt, downstream behavior remains `RISK_GLOBAL_HALT` / blocked state.

3) Data/schema impact
- Added tables: `churn_event_log`, `symbol_penalty_box`, `macro_state_current`, `parameter_governor_log`.
- Extended `risk_state_current` with `previous_halt_reason`, `attempt_churn_count`, `execution_churn_count`, `loss_pressure_24h`.
- Extended `ghost_sim_runs` with `macro_shock`, `estimated_cost_bps`, `execution_style`.

4) Runtime/ops impact
- Macro shock hysteresis avoids sticky always-on shock mode.
- BLOODBATH non-fill default is `EXPIRED_UNFILLED`; includes fill/expired diagnostics.
- Hourly digest includes ChurnDiagnostics, governor suspend reasons list, and micro tracked/pinned/interest clarity.

5) Validation evidence
- Compile checks passed.
- Services restarted and verified running.
- Forced Telegram digest sent successfully after deployment.

6) Git traceability
- Pending commit for this 16-issue unified wave.

---

## 2026-02-25 churn misclassification P0 fix (event-sourced truth)
1) Decision summary
- Enforced strict churn contract: EXECUTION churn now only from explicit fill-like events.
- ATTEMPT churn now emitted exactly once at last-mile attempt start; outcomes logged without double-count increment.
- Added execution dedupe by `execution_id`; attempt dedupe by `(attempt_id,event_class)`.

2) Routing/flow impact
- BLOODBATH lane now emits:
  - ATTEMPT_STARTED (counted), then outcome events (not counted) and optional EXECUTION(FILLED_MAKER, counted).
- Risk machine remains event-sourced from `churn_event_log` windows (5m/60m), not legacy inferred counters.
- Added loop guard: re-check global RiskState in attempt loop and abort with `RISK_FLIP_ABORT`.

3) Data/schema impact
- `churn_event_log` extended with `lane`, `execution_id`.
- Added dedupe logic in logger to prevent repeated EXECUTION increments.

4) Observability impact
- Hourly ChurnDiagnostics now derived from churn_event_log source-of-truth counters:
  - attempt 5m/60m
  - execution 5m/60m
  - top attempt reasons
  - last 10 churn events
  - penalty-box symbols
  - CHURN_TELEMETRY_SUSPECT flag

5) Validation
- Compile checks passed.
- Services restarted via watchdog and confirmed running.
- Forced Telegram digest successful after fix.

6) Git traceability
- Pending commit for churn misclassification fix wave.

### 2026-02-25 follow-up alignment patch (post-summary compare)
- Added explicit retry wrapper with max retries + exponential backoff + jitter for last-mile candle fetch path (`_get_json_retry`).
- Retry loop now re-checks global RiskState in-loop and aborts with `RISK_FLIP_ABORT`.
- Added finer attempt failure classification for exceptions: `RATE_LIMIT`, `API_TIMEOUT`, `VENUE_ERROR`.
- Kept event idempotency behavior (attempt/execution dedupe) intact.

---

## 2026-02-26 unified reliability/risk/tuner consolidation
1) Decision change summary
- Consolidated event-sourced churn correctness with retry safety and stricter execution classification.
- Aligned BLOODBATH lane activation semantics with macro/regime reasons and added lane reject diagnostics.
- Fixed DD paradox by decoupling portfolio DD basis from ghost-research PnL (shadow ledger basis).
- Improved governor trust metrics with validity/extreme classification and stats metadata.

2) Routing/flow impact
- Churn counters now derive from `churn_event_log` (ATTEMPT/EXECUTION 5m/60m).
- Last-mile fetch path now uses retry wrapper with in-loop risk re-check (`RISK_FLIP_ABORT`).
- Lane now outputs explicit `activation_reasons` and reject breakdown categories.

3) Data structure/schema impact
- Added tables: `shadow_portfolio_ledger`, `ghost_stats_meta`, `audit_state_change_log`.
- Added churn event fields: `lane`, `execution_id`.
- Added ghost validity fields: `invalid_reason`, `extreme_class`, `valid_for_governor`.

4) Runtime/ops impact
- Hourly digest now includes:
  - DD basis/value line (shadow ledger)
  - Bloodbath reject breakdown
  - Churn diagnostics from source-of-truth event log
- Watchdog + notifier guard retained.

5) Validation evidence
- Compile checks passed.
- Services restarted and verified running.
- Forced Telegram digest succeeded.

6) Git traceability
- Pending commit for unified reliability/risk/tuner consolidation.

---

## 2026-02-26 capacity routing + actionable shadow execution wave
1) Decision change summary
- Fixed batch-capacity mislabeling by separating true position-full vs overflow capacity in-candidate batch.
- Added mandatory ACTIONABLE -> shadow execution attempts for core lane (no proposal-only dead-end).
- Added compact OpenPositions/Capacity/ShadowExec lines to hourly for operator validation.

2) Routing/flow impact
- Capacity logic now uses `open_positions` from shadow ledger and `remaining_slots`.
- New reason semantics:
  - `RISK_MAX_POSITIONS` only when ledger truly full (`open_positions >= max_positions`)
  - `RISK_CAPACITY_FULL` for overflow beyond remaining slots in current batch.
- ACTIONABLE core candidates now emit ATTEMPT start and terminal outcome (`FILLED_MAKER`/`EXPIRED_UNFILLED`).

3) Data/schema impact
- No new tables in this wave; reused `shadow_portfolio_ledger` + `churn_event_log`.
- Added summary/sanity fields for capacity and shadow execution counters.

4) Runtime/ops impact
- Hourly now prints:
  - `OpenPositions basis/open/max/remaining_slots`
  - `Capacity admitted/capacity_rejected`
  - `shadow_attempts/shadow_fills/shadow_expired`
- Governor compact line remains visible with tail/validity metrics.

5) Validation evidence
- Compile checks passed.
- Services restarted and running.
- Forced Telegram send succeeded.
- Live validation snapshot after cycle:
  - OpenPositions basis=shadow_ledger open=0 max=3 remaining_slots=3
  - Capacity admitted=3 capacity_rejected=89
  - shadow_attempts=3 shadow_fills=3 shadow_expired=0
  - Risk reason codes include `RISK_CAPACITY_FULL` and no false `RISK_MAX_POSITIONS` flood.

6) Git traceability
- Pending commit for capacity+shadow-exec wave.

---

## 2026-02-27 live shadow physics + message passing execution wave
1) Decision change summary
- Implemented queue-based execution architecture (`execution_intent_queue` / `execution_results_queue`) with single-writer main loop semantics for shadow state.
- Enforced cycle-separated fill physics: attempts start in cycle T and are only evaluated from cycle T+1 onward.
- Removed DB dependency from capacity hot-path by using in-memory `shadow_state` open positions.

2) Routing/flow impact
- Main loop now:
  - drains results queue at cycle start and applies state,
  - evaluates pending attempts on fresh market snapshot,
  - enqueues new intents for current actionable candidates,
  - starts attempts this cycle (fills deferred by physics).
- Worker never mutates ledger DB; only main loop writes ledger/event persistence.

3) Data/model impact
- Added in-memory `ShadowExecutionWorker` + pending attempt map.
- Added telemetry fields:
  - `avg_fill_latency_sec`, `min_fill_delay_cycles`
  - `shadow_attempts`, `shadow_fills`, `shadow_expired`
  - `open_positions_basis=shadow_state` and capacity counters.

4) Runtime/ops impact
- Hourly lines now surface OpenPositions/Capacity/ShadowExec timing physics.
- Bloodbath lane includes activation reasons + reject breakdown + thresholded candidate view.

5) Validation
- Compile checks passed.
- Services restarted and running.
- Forced Telegram digest succeeded.

6) Git traceability
- Pending commit for message-passing/live-shadow-physics wave.
