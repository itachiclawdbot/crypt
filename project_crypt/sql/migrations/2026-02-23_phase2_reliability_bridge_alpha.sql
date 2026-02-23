-- Phase-2 Reliability + Bridge-Alpha Upgrade
-- Safe additive migration for SQLite

ALTER TABLE universe_state ADD COLUMN instrument_symbol TEXT;
ALTER TABLE universe_state ADD COLUMN base_ccy TEXT;
ALTER TABLE universe_state ADD COLUMN quote_ccy TEXT;

ALTER TABLE candidate_funnel_log ADD COLUMN watch_total INTEGER;
ALTER TABLE candidate_funnel_log ADD COLUMN actionable_total INTEGER;
ALTER TABLE candidate_funnel_log ADD COLUMN alpha_scored_total INTEGER;
ALTER TABLE candidate_funnel_log ADD COLUMN cost_evaluated_total INTEGER;
ALTER TABLE candidate_funnel_log ADD COLUMN risk_evaluated_total INTEGER;
ALTER TABLE candidate_funnel_log ADD COLUMN rejects_tradable_json TEXT;
ALTER TABLE candidate_funnel_log ADD COLUMN rejects_external_json TEXT;
ALTER TABLE candidate_funnel_log ADD COLUMN sanity_json TEXT;
ALTER TABLE candidate_funnel_log ADD COLUMN regime TEXT;
ALTER TABLE candidate_funnel_log ADD COLUMN pressure_count INTEGER;

CREATE TABLE IF NOT EXISTS listing_watch (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_first TEXT NOT NULL,
  ts_last TEXT NOT NULL,
  symbol_guess TEXT,
  chain TEXT,
  address TEXT,
  priority_score REAL,
  reasons_json TEXT,
  status TEXT
);

CREATE TABLE IF NOT EXISTS listing_watch_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  event_type TEXT NOT NULL,
  details_json TEXT
);

CREATE TABLE IF NOT EXISTS ghost_sim_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  instrument_symbol TEXT NOT NULL,
  entry_ts TEXT,
  exit_ts TEXT,
  horizon TEXT,
  entry_price REAL,
  exit_price REAL,
  pnl_bps REAL,
  costs_bps REAL,
  net_pnl_bps REAL,
  reject_reason TEXT,
  features_ref TEXT
);
