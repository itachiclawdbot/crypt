# Project Crypt — Session Context Memory (Phase-1 → Phase-1B/Phase-2)

Last updated: 2026-02-23 08:50 SGT
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
