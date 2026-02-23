# Project Crypt — Phase-2 SQL Playbook

Location: `project_crypt/sql/playbooks/`
Database: `project_crypt/cryptobot.sqlite3`

This file documents each operational SQL query: what it checks, why it matters, and what "good" vs "bad" output looks like.

---

## 0) How to run queries

```bash
sqlite3 /home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3
```

Then paste a query. Or run directly:

```bash
sqlite3 /home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3 "<SQL_HERE>"
```

---

## 1) Funnel correctness (ACTIONABLE consistency)

### Query
```sql
select
  ts,
  watch_total,
  eligible_total,
  actionable_total,
  proposed_total,
  alpha_scored_total,
  cost_evaluated_total,
  risk_evaluated_total,
  rejects_tradable_json,
  rejects_external_json,
  regime,
  pressure_count
from candidate_funnel_log
order by id desc
limit 10;
```

### What it queries
Latest cycle-level funnel counters and evaluation counters.

### What it means
- `actionable_total` and `proposed_total` should align with top actionable outcomes.
- `alpha_scored_total`, `cost_evaluated_total`, `risk_evaluated_total` confirm stage evaluation happened.

### Healthy shape
- Non-zero `alpha_scored_total` when symbols are processed.
- `actionable_total` should not contradict decision logs.
- reject splits populated as JSON (tradable vs external).

### Red flag shape
- `actionable_total > 0` but `proposed_total = 0` repeatedly.
- `alpha_scored_total = 0` while pipeline is otherwise active.

---

## 2) No quote-currency contamination in candidates

### Query
```sql
select count(*) as bad_rows
from universe_state
where ts >= datetime('now','-1 hour')
  and coalesce(base_ccy, symbol) in ('USDT','USDC','USD','EUR');
```

### What it queries
Checks if quote currencies were mistakenly treated as candidate assets.

### What it means
Should be zero for current cycles.

### Healthy shape
- `bad_rows = 0`

### Red flag shape
- `bad_rows > 0` means identity filtering bug/regression.

---

## 3) One decision per instrument (dedupe sanity)

### Query
```sql
select instrument_symbol, count(*) as rows_per_symbol
from universe_state
where ts >= datetime('now','-1 hour')
  and instrument_symbol is not null
group by instrument_symbol
having count(*) > 12
order by rows_per_symbol desc
limit 20;
```

### What it queries
High-repeat decision rows per instrument in a short window.

### What it means
Some repetition is normal across cycles; extreme duplicates per cycle indicate dedupe/keying issues.

### Healthy shape
- Few/no extreme outliers.

### Red flag shape
- Many symbols with very high repeat count for same cycle period.

---

## 4) Watchlist non-empty when eligible exists

### Query
```sql
select ts, watch_total, eligible_total, actionable_total, sanity_json
from candidate_funnel_log
order by id desc
limit 20;
```

### What it queries
Recent watch/eligible/actionable counts + sanity payload.

### What it means
If eligible exists, watchlist should not be silently empty.

### Healthy shape
- `eligible_total > 0` and watch outputs visible in digest.

### Red flag shape
- `eligible_total > 0` repeatedly but watch list rendered as none.

---

## 5) Listing-watch lane activity

### Query A — watch table status
```sql
select status, count(*) as n
from listing_watch
group by status
order by n desc;
```

### Query B — latest listing events
```sql
select ts, event_type, details_json
from listing_watch_events
order by id desc
limit 20;
```

### What it queries
Bridge-alpha lane internals for DEX→CEX listing front-run logic.

### What it means
- `listing_watch` should accumulate/refresh candidates.
- `listing_watch_events` should log triggers and transitions.

### Healthy shape
- non-zero `watching` rows.
- event rows when new-listing match happens.

### Red flag shape
- always empty despite DEX activity.

---

## 6) Ghost simulator outputs (rejected-signal hindsight)

### Query A — volume of sims
```sql
select count(*) as total_runs
from ghost_sim_runs;
```

### Query B — reject reason performance
```sql
select
  reject_reason,
  round(avg(net_pnl_bps), 2) as avg_net_bps,
  round(min(net_pnl_bps), 2) as min_net_bps,
  round(max(net_pnl_bps), 2) as max_net_bps,
  count(*) as n
from ghost_sim_runs
group by reject_reason
order by n desc;
```

### What it queries
How rejected signals would have performed in shadow hindsight.

### What it means
Helps decide if alpha/cost gates are too strict.

### Healthy shape
- consistent non-zero runs.
- meaningful distribution by reject reason.

### Red flag shape
- zero runs for long periods (simulator not executing).

---

## 7) Regime + pressure diagnostics

### Query
```sql
select ts, regime, pressure_count, sanity_json
from candidate_funnel_log
order by id desc
limit 30;
```

### What it queries
Per-cycle regime classification and pressure signal count.

### What it means
Confirms regime-aware OBI logic is active.

### Healthy shape
- regime present (`QUIET|NORMAL|VOLATILE`)
- pressure_count varies with market conditions.

### Red flag shape
- regime null/blank across active cycles.

---

## 8) Microstructure feed freshness

### Query
```sql
select
  count(*) as symbols,
  max(ts) as latest_ts,
  min(ts) as oldest_ts
from micro_features_latest;
```

### What it queries
Microservice output freshness and symbol coverage.

### What it means
If stale/empty, alpha and cost model quality drops.

### Healthy shape
- non-zero symbols and recent `latest_ts`.

### Red flag shape
- empty table or very old timestamps.

---

## 9) Source health (all services)

### Query
```sql
select source_name, status, count(*) as n, max(ts) as last_seen
from source_health_log
where ts >= datetime('now','-6 hours')
group by source_name, status
order by source_name, n desc;
```

### What it queries
Health/availability of data inputs and services.

### What it means
Shows DEGRADED/DISABLED sources that may explain funnel drops.

### Healthy shape
- major sources mostly `OK`.

### Red flag shape
- prolonged `DEGRADED` for critical inputs.

---

## 10) Quick acceptance bundle (copy/paste)

```sql
-- 1) latest funnel
select ts, watch_total, eligible_total, actionable_total, proposed_total,
       alpha_scored_total, cost_evaluated_total, risk_evaluated_total,
       rejects_tradable_json, rejects_external_json, regime, pressure_count
from candidate_funnel_log
order by id desc
limit 1;

-- 2) quote-currency contamination
select count(*) as bad_rows
from universe_state
where ts >= datetime('now','-1 hour')
  and coalesce(base_ccy, symbol) in ('USDT','USDC','USD','EUR');

-- 3) listing watch signal path
select status, count(*) as n from listing_watch group by status order by n desc;
select ts, event_type from listing_watch_events order by id desc limit 5;

-- 4) ghost simulator alive
select count(*) as total_runs from ghost_sim_runs;
```
