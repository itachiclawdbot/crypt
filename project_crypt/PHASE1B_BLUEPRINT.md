# Project Crypt — Phase-1B Combined Blueprint

Status: **Design baseline (no live execution)**  
Mode: **Intelligence + forecasting + paper proposals + risk gating + audit**

---

## 1) System Purpose (Phase-1B)

Phase-1B upgrades Phase-1 from smoke-test gating to a **continuous multi-source intelligence and forecasting pipeline** that still does **not execute real orders**.

### Core objective shift
- From **market following** → **alpha prediction**.
- Generate **tradable-grade paper proposals** with replayable data and full audit trails.

### Strategic objectives
1. **Hype Correlation Alpha**
   - Detect micro-cap setups where social/news sentiment leads price by ~15–30 min.
2. **Asymmetric Risk Discovery**
   - Surface low-cap/new listings with volume anomalies (e.g., sharp volume spike without proportional price move).
3. **Pattern Memorization**
   - Persist pump/fade (“failed moon”) signatures and down-weight similar setups.
4. **No autonomous live trading**
   - Forecast + propose + gate + log only.

---

## 2) Ingestion + Replayability Architecture

## 2.1 Continuous ingestion (multi-frequency)
- **Market (Crypto.com):** instruments, tickers, OHLCV, orderbook snapshots, recent trades (if available).
- **News/Web:** RSS/Atom, exchange announcements, project blogs (static fetch only).
- **Social:**
  - Prefer X official API if keys exist.
  - Else Reddit API and other ToS-compliant sources (no scraping).
- **Universe discovery:**
  - New listings from instruments,
  - mention bursts from news/social,
  - optional discovery feeds (watchlist only).

## 2.2 Local storage + replay
- SQLite for control-plane/audit/ops logs.
- Parquet/DuckDB for high-volume data (raw text, orderbook, features, forecasts).
- All decisions link to artifact pointers (`features_ref`, `model_ref`) for replayability.

---

## 3) Decision Routing (Combined Gate Stack)

`Priority` means hard ordering.

### Layer 0 — System Safety (Priority 0)
Hard-stop first:
1. Kill switch file exists → `DENY: kill-switch-file-detected`
2. Global DD breach (default 5%) → `DENY: daily-dd-breach`
3. Market feed stale beyond threshold → `DENY: market-data-stale`
4. Missing feature window → `DENY: missing-features`
5. Model unavailable → `DENY: model-unavailable`
6. Circuit breaker open → `DENY: circuit-breaker-open`

### Layer 1 — Eligibility + small-cap protection
Symbol-level filters before expensive sizing:
- Listing age >= 7 days (unless dedicated new-listing strategy)
- 24h quote volume >= 250,000 USDT
- Median spread <= 30 bps over rolling window
- Top-of-book depth >= 10,000 USDT within ±20 bps
- Trade cadence >= 20 trades / 5 min (if endpoint exists)
- Anomaly controls:
  - volume spike without depth increase,
  - repeated wick spikes,
  - extreme gap risk (> configured sigma)
- If fail → `DENY: ineligible:<reason>`

### Layer 2 — Alpha/Sentiment eligibility
- `HypeScore > 75`
- `SentimentPolarity > 0.6`
- Must include non-empty source rationale (link/id) for auditability.
- If fail → `DENY: weak-alpha-signal`

### Layer 3 — Portfolio/Risk gates (extended)
1. Daily DD >= 5% → deny
2. Hourly DD >= 2% → deny
3. Consecutive losses >= 5 → deny
4. Max concurrent positions >= 2 → deny
5. Exposure concentration cap (per asset notional, default 10% equity)
6. Turnover/churn gate (trades_last_hour cap or flip-rate high)
7. Cost gate:
   - `E[return] - (fees + slippage_est + spread_cost) >= min_edge`
   - Default `min_edge`: 10–20 bps (timeframe dependent)

### Layer 4 — Sizing + uncertainty controls
- Base sizing: risk budget + capped quarter-Kelly.
- Add confidence scaling:
  - size *= confidence_factor (forecast uncertainty width, calibration, regime stability).
- Small-cap volatility scalar:
  - if `market_cap < small_cap_threshold` then `size *= 0.5`.

### Layer 5 — Execution drift sanity (paper execution check)
- Approve proposal only if current price is within drift tolerance of proposal price.
- Default: abs drift <= 0.2% else `DENY: price-drift-too-high`.

### Proposal creation rule
Only create full `OrderProposal` when all of the below hold:
- Symbol eligible,
- Confidence >= threshold (default 0.55),
- Cost gate passes,
- Liquidity floor passes:
  - `ProposalNotional < 0.5% of 10-min orderbook depth`.
Else log `candidate_rejected_pre_proposal`.

---

## 4) Data Model Evolution

## 4.1 SQLite (audit/control plane)

### Expand `risk_log`
Add:
- `symbol TEXT`
- `gate_stage TEXT` (`kill_switch|eligibility|alpha|risk|cost|sizing|drift`)
- `metrics_json TEXT`
- `model_ref TEXT`
- `features_ref TEXT`
- `is_small_cap INTEGER`
- `alpha_source TEXT`
- `expected_slippage REAL`

### New `proposal_log`
- `id, ts, symbol, side, horizon, target_notional, stop_price, take_profit, rationale_json, model_ref, features_ref`

### New `source_health_log`
- `id, ts, source_name, status, latency_ms, items_ingested, error_count, last_error, loop_lag_ms`

### New `incident_log`
- `id, ts, severity, component, code, message, context_json`

### New `model_eval_log`
- `id, ts, symbol, horizon, model_version, y_true, y_pred_json, error_json, calibration_json`

### New `universe_log`
- `id, ts, symbol, eligible, eligibility_score, reasons_json, liquidity_json, listing_age_days, volume_24h`

### New `sentiment_intel`
- `id, ts, ticker, hype_score, sentiment_polarity, source_type, discovery_ts, impact_score, source_ref`

## 4.2 High-volume data lake (Parquet/DuckDB)
- `raw_news`
- `raw_social`
- `market_ohlcv`
- `orderbook_snapshots`
- `features`
- `forecasts`

Keep only pointers in SQLite.

---

## 5) Routing + Alerting

## 5.1 Intelligence routing
Incoming web/X/Reddit data:
`Ingestion -> Normalization -> Sentiment Classifier -> Feature Store -> Forecast Ensemble -> Proposal Candidate -> RiskManager`

## 5.2 Decision routing
Always write:
1. `proposal_log` (candidate/proposal path)
2. `risk_log` (approve/deny with stage + metrics)
3. `model_eval_log` (when outcome truth arrives)

## 5.3 Telegram alert routing
### Immediate (push)
- Kill switch triggered (CRITICAL)
- Circuit breaker opened
- Data feed stale / ingestion failure
- New listing detected
- Burst detected (news/social)
- Eligible small-cap enters top-N
- Deny rate spike (>80% over last hour)
- Moonshot (high hype + high liquidity)

### Digest (hourly)
- Top 10 eligible symbols by score
- Model snapshot (MAE, calibration, directional hit-rate)
- Deny reason distribution
- Drift/reweight events
- Alert-vs-price reconciliation accuracy

Telegram payload fields:
- `event_id`, `symbol`, `reason`, key metrics (spread bps, depth, volume, confidence, DD), local log pointer (e.g., `risk_log.id`).

---

## 6) Dashboard (Observability 2.0)

Single pane sections:
1. **System Health** — uptime, loop lag, feed staleness timers
2. **Universe** — monitored count, eligible count, top-N table
3. **Signals/Forecasts** — horizon, q50/q90, confidence, uncertainty, no-trade reasons
4. **Risk Outcomes** — approvals/denials (1h/24h), deny histogram
5. **Model Quality** — rolling MAE, directional accuracy, calibration, drift status
6. **Intel Heatmap** — top 5 hype tickers and source mix
7. **Drawdown Progress** — progress bar toward safety threshold

Color semantics:
- Green: healthy/eligible/approved
- Yellow: warning / alpha detected
- Red: deny/feed down/circuit/kill switch
- Magenta: risk denial emphasis
- Cyan: new listing / informational discovery
- Blue: neutral info/retrain scheduled

Optional UX:
- ASCII bell `\a` for critical local alerts.
- Telegram mute/unmute by signal strength.

---

## 7) Ops + Resiliency

## 7.1 Scheduling defaults (with ±10% jitter)
- Market tickers/OHLCV: 10–60s (10s preferred where limits allow)
- Orderbook snapshots: 10–30s (top symbols)
- News/social incrementals: 180s
- Full feature→forecast→proposal cycle: 300s
- Retrospective eval/reconciliation: 3600s

## 7.2 Retry policy
- Max retries per call: 3
- Exponential backoff with jitter:
  - general: 0.5s, 1s, 2s (+ random 0–250ms)
  - HTTP 429 variant: `2^n` seconds, respect `Retry-After`
- On repeated failure:
  - mark source `DEGRADED`,
  - open source circuit breaker (cooldown default 5m; severe mode 15m after 3 consecutive failures),
  - send alert.

## 7.3 Kill-switch behavior (strengthened)
On STOP signal detection:
1. Immediately force all approvals to deny.
2. Emit CRITICAL Telegram alert (`Emergency Halt`).
3. Write `incident_log` entry.
4. Enter safe hold loop (sleep 60) until switch removed.
5. (Future Phase-2 execution only): optional cancel/flatten via config.

## 7.4 Operational hardening
- Persist connector cursors (id/time) to avoid duplicates.
- Dedupe keys:
  - news/social by content hash,
  - market by timestamp+symbol (+ venue).
- Batch DB writes.
- Track heartbeat + lag in `source_health_log`.

---

## 8) Out of Scope (still Phase-1B)
- No live order placement.
- No autonomous capital deployment.
- No non-compliant scraping.

---

## 9) Implementation mode switch
Add config flag:
- `PHASE_MODE=phase1|phase1b`

Rollout recommendation:
1. Schema + logging first
2. Ingestion connectors + health
3. Eligibility + alpha gates
4. Forecast + cost/confidence sizing
5. Dashboard/alerts v2
6. Keep Phase-1 fallback until burn-in passes

---

## 10) Required skills, APIs, and approvals (Phase-1B readiness checklist)

### 10.1 External APIs / credentials needed
1. **Crypto.com market data access**
   - REST/WebSocket endpoints for instruments, tickers, OHLCV, orderbook, trades.
2. **Telegram bot credentials**
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` for structured alerts/digests.
3. **X API credentials (optional but preferred)**
   - If unavailable, Phase-1B falls back to Reddit + compliant web/RSS sources.
4. **Reddit API credentials**
   - Required for social fallback ingestion.
5. **Optional discovery feed API** (CoinGecko-like watchlist metadata).

### 10.2 Infrastructure / storage approvals
- Permission to create local data lake directories for Parquet/DuckDB.
- Retention policy approval for raw social/news text and market snapshots.
- Approval for periodic network calls at configured polling frequencies.

### 10.3 Compliance and source policy approvals
- Explicit confirmation of **no scraping** policy for protected sources.
- Confirmation of acceptable source list (RSS/blog/exchange announcements/X/Reddit).
- Approval for storing source links/IDs in audit logs (`alpha_source`, `source_ref`).

### 10.4 Optional OpenClaw skills that may help implementation
(These are convenience skills, not strict blockers.)
- `playwright-scraper-skill` **not required** for current policy path (static fetch + APIs only).
- `summarize` optional for digest-quality briefing generation.
- `automation-workflows` optional for orchestration patterns.

### 10.5 Final go-live approvals needed before coding Stage-2+
- Confirm API keys provisioned (Crypto.com, Telegram, X/Reddit as applicable).
- Confirm storage/retention policy.
- Confirm alert recipients + severity routing.
- Confirm kill-switch operational semantics (`STOP_SIGNAL` path and halt behavior).
