# Project Crypt — Session Context Memory (Phase-1 → Phase-1B/Phase-2)

Last updated: 2026-02-23 14:20 SGT
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
