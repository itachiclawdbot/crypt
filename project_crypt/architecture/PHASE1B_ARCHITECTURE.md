# Project Crypt — Phase-1B Architecture Plan

Last updated: 2026-02-23 (Asia/Singapore)
Status: **Planned + partially implemented (shadow mode only, no live execution)**

---

## 1) Executive Summary

Phase-1B upgrades Project Crypt from a single gate smoke-test into a **continuous multi-source alpha aggregation + forecasting system**.

It remains **paper/shadow only**:
- no live order placement,
- no autonomous trading,
- full audit logging and replayability.

Core intent:
1. discover small-cap/micro-cap opportunities,
2. validate with cross-source triangulation,
3. gate with strict risk/eligibility/cost constraints,
4. log every decision path end-to-end.

---

## 2) Target Architecture (Phase-1B)

### 2.1 Data flow
Ingestion (market + discovery + news/social)  
→ normalization  
→ feature generation  
→ forecast ensemble  
→ proposal candidate  
→ multi-layer gating  
→ proposal/risk logs + alerts + dashboards.

### 2.2 Source priority (current strategy)
1. **Crypto.com** (primary market/account context)
2. **CoinGecko** (trending/new-listing signals where allowed)
3. **CoinMarketCap** (market cap/rankings)
4. **DEX Screener** (fallback discovery + boosted signals)
5. **CryptoPanic + Free-Crypto-News** (headline/sentiment intelligence)
6. **X/Reddit** (planned; currently disabled/degraded due API/relay constraints)

### 2.3 Control-plane vs data-plane
- **SQLite** for audit/control logs (`risk_log`, `proposal_log`, health/incidents/eval).
- **Parquet/DuckDB** planned for high-volume artifacts (raw texts/orderbook/features/forecasts).
- SQLite stores references (`features_ref`, `model_ref`) to heavy artifacts.

---

## 3) Decision System (Final Gate Stack)

### Layer 0 — Safety gates (hard stop)
- kill switch file present,
- stale market feed,
- missing features window,
- model unavailable,
- circuit breaker open,
- global DD breach.

### Layer 1 — Eligibility gates (small-cap protection)
- min listing age,
- min 24h volume,
- max spread bps,
- min top-book depth,
- min trade cadence,
- anomaly checks (wick/gap/depth divergence).

### Layer 2 — Alpha/signal gates
- hype score + sentiment polarity threshold,
- triangulation confidence (cross-source alignment),
- optional boost flag logic.

### Layer 3 — Portfolio/risk gates
- daily/hourly drawdown caps,
- consecutive losses cap,
- max concurrent positions,
- concentration and churn caps,
- cost gate: expected edge net of fees/spread/slippage.

### Layer 4 — Sizing and uncertainty
- base risk budget + capped quarter-Kelly,
- confidence/uncertainty scaling,
- small-cap volatility scalar.

### Layer 5 — Drift/execution sanity (paper)
- proposal price vs current price tolerance.

---

## 4) Migration Map: Phase-1 → Phase-1B

## 4.1 Already migrated / in place
1. **Core risk gate foundation** (Phase-1 RiskManager)
2. **SQLite logging baseline** (`risk_log`, `trade_log`)
3. **Telegram notification plumbing**
4. **Background worker pattern** (continuous loops)
5. **Live dashboard capability** (terminal UI pattern)
6. **Phase-2 shadow aggregator implemented** using working sources:
   - `phase2_alpha_aggregator.py`
   - `phase2_monitor_notifier.py`
   - `phase2_ops_dashboard.py`
7. **Hourly Telegram monitoring digest** for fetch/trend updates.

## 4.2 Partially migrated
1. Multi-source ingestion + triangulation in `intel_cache` ✅
2. Source health logging ✅
3. Current job/state tracking (`phase2_status.json`) ✅
4. Potential-buy watch tagging (`triangulated_alpha`) ✅
5. Full eligibility/risk/cost layered gate routing ❌ (design complete, full code pending)
6. Forecast/model loop + calibration/drift ❌ (pending)
7. Artifact lake (Parquet/DuckDB) ❌ (pending)

## 4.3 Not yet migrated (planned)
- Full `proposal_log`, `model_eval_log`, `incident_log`, `universe_log` rollout.
- Forecast ensemble and confidence quantiles.
- Model reweighting/retrain triggers.
- X/Reddit compliant ingestion connectors.

---

## 5) Current Runtime Components

- `phase2_alpha_aggregator.py`
  - 300s poll cycle (jittered)
  - ingests sources listed above
  - writes `intel_cache` and `source_health_log`
  - computes triangulated signals
  - updates status JSON for UI

- `phase2_monitor_notifier.py`
  - hourly Telegram summary:
    - fetch volume,
    - trends,
    - potential buys under watch,
    - source health.

- `phase2_ops_dashboard.py`
  - displays:
    - current job and state,
    - last cycle summary,
    - latest candidate symbols,
    - source health snapshots,
    - worker process status.

---

## 6) Branching / Repository State

- `phase1-baseline`: stable Phase-1 code + README.
- `phase2-working`: active shadow intelligence implementation + README + ops scripts.

Phase-1B design docs:
- `project_crypt/PHASE1B_BLUEPRINT.md`
- `project_crypt/architecture/PHASE1B_ARCHITECTURE.md` (this document)

---

## 7) Next Implementation Steps (recommended)

1. DB migration script for full Phase-1B schema.
2. Integrate multi-layer gate engine (safety→eligibility→risk→cost→sizing).
3. Add proposal lifecycle logs (`candidate_rejected_pre_proposal`, `proposal_log`, `risk_log` stage detail).
4. Introduce feature store + forecast scaffold.
5. Add drift/calibration evaluation jobs.
6. Enable X/Reddit once compliant API or relay path is stable.

---

## 8) Guardrails

- Shadow mode only until explicit live-trading approval.
- No ToS-violating scraping route.
- Kill-switch remains hard-stop at highest priority.
- All important actions must be auditable in local logs.

---

## 2026-02-26 Routing/State-machine updates (Reliability + Risk/Tuner correctness)
- Churn telemetry is now **event-sourced** from `churn_event_log` only.
  - `ATTEMPT` = one per last-mile attempt start (dedup by `attempt_id,event_class`)
  - `EXECUTION` = only accepted/fill events (dedup by `execution_id`)
- Added tiered churn safety routing:
  - Symbol penalty box (`symbol_penalty_box`)
  - Systemic halt trigger by unique penalty-boxed symbols
  - Hot-loop signature detector `(symbol+reason+params_hash)`
- Macro/lane consistency routing:
  - Bloodbath lane now emits `activation_reasons` list and reason labels aligned with macro/regime state.
  - Lane reject breakdown exported for observability.
- DD risk basis decoupling:
  - Portfolio DD now reads `shadow_portfolio_ledger` (execution-like fills only).
  - Ghost research PnL is analytics-only and not circuit-breaker input.
- Governor fidelity updates:
  - ghost invalid/extreme classification fields added (`invalid_reason`, `extreme_class`, `valid_for_governor`).
  - governor metrics now expose `valid_rate`, class counts, and stats-version metadata.

### 2026-02-26 Capacity routing update
- Risk capacity gate now routes through ledger-based open-position state:
  - `RISK_MAX_POSITIONS` = ledger full
  - `RISK_CAPACITY_FULL` = batch overflow vs remaining slots
- Actionable routing now includes mandatory shadow execution attempt path (core lane), producing ATTEMPT/EXECUTION telemetry and shadow ledger updates.

### 2026-02-27 Execution orchestration update (queue-based shadow engine)
- Introduced explicit main↔worker message passing queues:
  - `execution_intent_queue` (main -> worker)
  - `execution_results_queue` (worker -> main)
- Worker owns pending-attempt evaluation only; main loop is single writer for shadow ledger + SQLite persistence.
- Fill physics now cycle-separated (`fill_cycle_id > attempt_cycle_id`) to avoid same-cycle fill artifacts.

### 2026-02-27 Telemetry/state coherence update
- Added cycle-level short-circuit semantics and evaluated-mode markers to prevent stage-logic contradictions.
- Capacity admission proof now exported as deterministic admitted-top list.
- Execution timing sanity (`min_fill_delay_cycles`) promoted to top-level hourly visibility.

### 2026-02-27 Cooldown control-plane ordering patch
- Added pre-ingest cooldown gate in cycle orchestrator to prevent unnecessary feed pulls/scoring while in cooldown.
- Telemetry now distinguishes skipped/partial/full stage evaluation mode with explicit short-circuit reason.

### 2026-02-27 Nervous-system hardening
- Boot now fail-closes on invalid config and rehydrates runtime shadow state before cycle loop.
- Status channel hardened with atomic writes + sequence/schema metadata.
- Queue drain now bounded with explicit backpressure telemetry to preserve cycle cadence under load.

### 2026-02-27 status-channel health v2
- Status parse health is now independent from DB-rendered digest content and always surfaced via `StatusSnapshot`.
- Coherency gate suppresses control-plane render when latest snapshot is not `cycle_complete/full`.

### 2026-03-03 Layer-2 ingestion contract additions
- Added per-cycle ingestion bundle contract attached to cycle summary payload.
- Request accounting split by domain (signals vs marketdata) with per-source counters.
- Added freshness/time-skew and mapping-overflow telemetry to ingestion output.
