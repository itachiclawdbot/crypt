from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
import queue
from dataclasses import dataclass
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone, timedelta

import requests

from project_crypt.telegram_notify import TelegramNotifier

DB_PATH = os.getenv("CRYPTO_DB_PATH", "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3")
POLL_SECONDS = int(os.getenv("PHASE2_POLL_SECONDS", "300"))
STATUS_PATH = os.getenv("PHASE2_STATUS_PATH", "/home/itachi/.openclaw/workspace/project_crypt/phase2_status.json")

EXECUTION_INTENT_QUEUE: "queue.Queue[dict]" = queue.Queue()
EXECUTION_RESULTS_QUEUE: "queue.Queue[dict]" = queue.Queue()
CYCLE_SEQ = 0
SHADOW_STATE = {"open_positions": {}, "fills_24h": [], "attempts": 0, "fills": 0, "expired": 0, "latencies_sec": [], "min_delay_cycles": None}

@dataclass
class ShadowAttempt:
    attempt_id: str
    symbol: str
    lane: str
    attempt_cycle_id: int
    attempt_ts: float
    maker_time_budget_sec: int
    p_fill_hint: float
    crosses_spread: bool


class ShadowExecutionWorker:
    def __init__(self):
        self.pending: dict[str, ShadowAttempt] = {}

    def process_intents(self, cycle_id: int):
        while True:
            try:
                it = EXECUTION_INTENT_QUEUE.get_nowait()
            except queue.Empty:
                break
            a = ShadowAttempt(
                attempt_id=it["attempt_id"], symbol=it["symbol"], lane=it.get("lane","CORE"),
                attempt_cycle_id=cycle_id, attempt_ts=time.time(), maker_time_budget_sec=int(it.get("maker_time_budget_sec",45)),
                p_fill_hint=float(it.get("p_fill_hint",0.4)), crosses_spread=bool(it.get("crosses_spread",False))
            )
            self.pending[a.attempt_id]=a
            EXECUTION_RESULTS_QUEUE.put({"type":"ATTEMPT_STARTED","attempt_id":a.attempt_id,"symbol":a.symbol,"lane":a.lane,"attempt_cycle_id":a.attempt_cycle_id,"attempt_ts":a.attempt_ts})

    def on_market_snapshot(self, cycle_id: int):
        now=time.time()
        done=[]
        for aid,a in list(self.pending.items()):
            if cycle_id <= a.attempt_cycle_id:
                continue
            if a.crosses_spread:
                EXECUTION_RESULTS_QUEUE.put({"type":"ATTEMPT_FAILED","reason":"CROSSES_SPREAD","attempt_id":aid,"symbol":a.symbol,"lane":a.lane,"attempt_cycle_id":a.attempt_cycle_id,"fill_cycle_id":cycle_id,"attempt_ts":a.attempt_ts,"fill_ts":now})
                done.append(aid); continue
            if (now - a.attempt_ts) >= a.maker_time_budget_sec:
                EXECUTION_RESULTS_QUEUE.put({"type":"EXPIRED_UNFILLED","attempt_id":aid,"symbol":a.symbol,"lane":a.lane,"attempt_cycle_id":a.attempt_cycle_id,"fill_cycle_id":cycle_id,"attempt_ts":a.attempt_ts,"fill_ts":now})
                done.append(aid); continue
            if random.random() < min(0.9,max(0.05,a.p_fill_hint*0.35)):
                fill_id=f"SF-{a.symbol}-{aid}"
                EXECUTION_RESULTS_QUEUE.put({"type":"FILLED_MAKER","attempt_id":aid,"execution_id":fill_id,"symbol":a.symbol,"lane":a.lane,"attempt_cycle_id":a.attempt_cycle_id,"fill_cycle_id":cycle_id,"attempt_ts":a.attempt_ts,"fill_ts":now})
                done.append(aid)
        for aid in done:
            self.pending.pop(aid,None)

SHADOW_WORKER = ShadowExecutionWorker()


COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")
DEX_SCREENER_BASE_URL = os.getenv("DEX_SCREENER_BASE_URL", "https://api.dexscreener.com")
DEX_LATEST_PROFILES_ENDPOINT = os.getenv("DEX_LATEST_PROFILES_ENDPOINT", "/token-profiles/latest/v1")
DEX_BOOSTED_TOKENS_ENDPOINT = os.getenv("DEX_BOOSTED_TOKENS_ENDPOINT", "/token-boosts/latest/v1")
DEX_BOOSTED_TOP_ENDPOINT = os.getenv("DEX_BOOSTED_TOP_ENDPOINT", "/token-boosts/top/v1")
TARGET_NOTIONAL_USDT = float(os.getenv("PHASE2_TARGET_NOTIONAL_USDT", "300"))
MIN_ACTIONABLE_SCORE = float(os.getenv("PHASE2_MIN_ACTIONABLE_SCORE", "70"))
MIN_ELIGIBLE_SCORE = float(os.getenv("PHASE2_MIN_ELIGIBLE_SCORE", "45"))
DISCOVERY_TICK_SECONDS = int(os.getenv("PHASE2_DISCOVERY_TICK_SECONDS", "120"))
MARKET_TICK_SECONDS = int(os.getenv("PHASE2_MARKET_TICK_SECONDS", "60"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "/var/run/cryptobot/STOP")
LISTING_STATE_PATH = os.getenv("PHASE2_LISTING_STATE_PATH", "/home/itachi/.openclaw/workspace/project_crypt/phase2_listing_state.json")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DUNE_QUERY_IDS = [x.strip() for x in (os.getenv("DUNE_QUERY_IDS") or "").split(",") if x.strip()]

BLOODBATH_ENABLED = str(os.getenv("BLOODBATH_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
BLOODBATH_SYMBOL_ALLOWLIST = [x.strip().upper() for x in os.getenv("BLOODBATH_SYMBOL_ALLOWLIST", "BTC_USDT,ETH_USDT,SOL_USDT").split(",") if x.strip()]
BLOODBATH_PRESSURE_CONF_MIN = float(os.getenv("BLOODBATH_PRESSURE_CONF_MIN", "0.60"))
BLOODBATH_SPREAD_MAX_BPS = float(os.getenv("BLOODBATH_SPREAD_MAX_BPS", "35"))
BLOODBATH_MIN_UTILIZATION = float(os.getenv("BLOODBATH_MIN_UTILIZATION", "0.35"))
BLOODBATH_MAX_ATTEMPTS_PER_SYMBOL_HOUR = int(os.getenv("BLOODBATH_MAX_ATTEMPTS_PER_SYMBOL_HOUR", "1"))
BLOODBATH_MAX_ATTEMPTS_PER_HOUR = int(os.getenv("BLOODBATH_MAX_ATTEMPTS_PER_HOUR", "3"))
BLOODBATH_LANE_RISK_FRACTION = float(os.getenv("BLOODBATH_LANE_RISK_FRACTION", "0.001"))
BLOODBATH_LANE_CONSEC_LOSS_LIMIT = int(os.getenv("BLOODBATH_LANE_CONSEC_LOSS_LIMIT", "2"))
BLOODBATH_LANE_COOLDOWN_SEC = int(os.getenv("BLOODBATH_LANE_COOLDOWN_SEC", "7200"))
BLOODBATH_MAKER_TIME_BUDGET_SEC = int(os.getenv("BLOODBATH_MAKER_TIME_BUDGET_SEC", "35"))
BLOODBATH_GHOST_EXIT_HORIZON_SEC = int(os.getenv("BLOODBATH_GHOST_EXIT_HORIZON_SEC", "900"))
BLOODBATH_DD_LIMIT_BPS_DAY = float(os.getenv("BLOODBATH_DD_LIMIT_BPS_DAY", "80"))
BLOODBATH_HYSTERESIS_SEC = int(os.getenv("BLOODBATH_HYSTERESIS_SEC", "3600"))

PHASE2_AUTOTUNER_APPLY = str(os.getenv("PHASE2_AUTOTUNER_APPLY", "false")).lower() in {"1", "true", "yes", "on"}
PHASE2_AUTOTUNER_MAX_DAILY_CHANGE = float(os.getenv("PHASE2_AUTOTUNER_MAX_DAILY_CHANGE", "0.05"))
PHASE2_AUTOTUNER_MIN_N = int(os.getenv("PHASE2_AUTOTUNER_MIN_N", "40"))
PHASE2_AUTOTUNER_MIN_SORTINO = float(os.getenv("PHASE2_AUTOTUNER_MIN_SORTINO", "0.25"))
PHASE2_AUTOTUNER_P5_FLOOR = float(os.getenv("PHASE2_AUTOTUNER_P5_FLOOR", "-80"))

CHURN_SYMBOL_FAIL_LIMIT_5M = int(os.getenv("CHURN_SYMBOL_FAIL_LIMIT_5M", "6"))
CHURN_PENALTY_BOX_SEC = int(os.getenv("CHURN_PENALTY_BOX_SEC", "900"))
CHURN_GLOBAL_PENALTY_SYMBOLS_LIMIT = int(os.getenv("CHURN_GLOBAL_PENALTY_SYMBOLS_LIMIT", "4"))
CHURN_GLOBAL_WINDOW_MIN = int(os.getenv("CHURN_GLOBAL_WINDOW_MIN", "15"))
CHURN_HOT_LOOP_MAX_IDENTICAL_5M = int(os.getenv("CHURN_HOT_LOOP_MAX_IDENTICAL_5M", "8"))
CHURN_RETRY_MAX = int(os.getenv("CHURN_RETRY_MAX", "3"))
CHURN_RETRY_BACKOFF_BASE_SEC = float(os.getenv("CHURN_RETRY_BACKOFF_BASE_SEC", "0.7"))


class Source:
    CRYPTOCOM = "crypto.com"
    COINGECKO = "coingecko"
    CMC = "coinmarketcap"
    DEX = "dexscreener"
    DEFILLAMA = "defillama"
    DUNE = "dune"
    CRYPTOPANIC = "cryptopanic"
    FREE_NEWS = "free-crypto-news"
    X = "x"
    REDDIT = "reddit"
    SYSTEM = "system"


def is_kill_switch_active() -> bool:
    return os.path.exists(KILL_SWITCH_FILE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def write_status(payload: dict) -> None:
    payload["ts"] = utc_now()
    with open(STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _norm_url(endpoint: str) -> str:
    e = (endpoint or "").strip()
    if e.startswith("http://") or e.startswith("https://"):
        return e
    if e.startswith("/"):
        return DEX_SCREENER_BASE_URL.rstrip("/") + e
    return DEX_SCREENER_BASE_URL.rstrip("/") + "/" + e


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, col_type: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")


def init_tables() -> None:
    with db() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS intel_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT,
                source TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                conviction TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                dedupe_hash TEXT NOT NULL UNIQUE
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS source_health_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                source_name TEXT NOT NULL,
                status TEXT NOT NULL,
                latency_ms INTEGER,
                items_ingested INTEGER,
                error_count INTEGER DEFAULT 0,
                last_error TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_funnel_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                discovered_total INTEGER NOT NULL,
                mapped_to_venue_total INTEGER NOT NULL,
                eligible_total INTEGER NOT NULL,
                alpha_pass_total INTEGER NOT NULL,
                risk_pass_total INTEGER NOT NULL,
                cost_pass_total INTEGER NOT NULL,
                proposed_total INTEGER NOT NULL,
                reasons_json TEXT,
                sources_present_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS candidate_reject_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT,
                stage TEXT NOT NULL,
                reason TEXT NOT NULL,
                metrics_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_symbol TEXT,
                base_ccy TEXT,
                quote_ccy TEXT,
                eligibility_score REAL,
                alpha_score REAL,
                confidence REAL,
                status TEXT NOT NULL,
                deny_stage TEXT,
                deny_reason TEXT,
                metrics_json TEXT,
                sources_present_json TEXT
            )
            """
        )
        c.execute(
            """
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
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_watch_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
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
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_state_current (
                id INTEGER PRIMARY KEY CHECK (id=1),
                ts TEXT NOT NULL,
                halted INTEGER NOT NULL,
                halt_reason TEXT,
                churn_basis TEXT,
                churn_count INTEGER NOT NULL,
                consecutive_losses INTEGER NOT NULL,
                cooldown_seconds_remaining INTEGER NOT NULL,
                cooldown_until TEXT,
                window_minutes INTEGER NOT NULL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bloodbath_ghost_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                lane TEXT NOT NULL,
                entry_ts TEXT,
                exit_ts TEXT,
                entry_price REAL,
                exit_price REAL,
                net_pnl_bps REAL,
                execution_style TEXT,
                utilization REAL,
                regime TEXT,
                macro_shock INTEGER,
                p_fill_maker REAL,
                maker_time_budget_sec INTEGER,
                costs_bps REAL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bloodbath_lane_state (
                id INTEGER PRIMARY KEY CHECK (id=1),
                ts TEXT NOT NULL,
                active INTEGER NOT NULL,
                active_reason TEXT,
                active_until TEXT,
                cooldown_until TEXT,
                consecutive_losses INTEGER NOT NULL,
                attempts_hour INTEGER NOT NULL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS macro_state_current (
                id INTEGER PRIMARY KEY CHECK (id=1),
                ts TEXT NOT NULL,
                macro_shock INTEGER NOT NULL,
                entered_at TEXT,
                calm_checks INTEGER NOT NULL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS parameter_governor_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                mode TEXT NOT NULL,
                namespace TEXT NOT NULL,
                recommendation_json TEXT,
                applied INTEGER NOT NULL,
                reason TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS churn_event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                attempt_id TEXT,
                symbol TEXT,
                event_class TEXT NOT NULL,
                event_reason TEXT NOT NULL,
                params_hash TEXT,
                counted INTEGER NOT NULL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_penalty_box (
                symbol TEXT PRIMARY KEY,
                until_ts TEXT NOT NULL,
                reason TEXT,
                updated_ts TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_portfolio_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT,
                lane TEXT,
                fill_id TEXT UNIQUE,
                execution_style_intent TEXT,
                realized_pnl_bps REAL,
                details_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS ghost_stats_meta (
                id INTEGER PRIMARY KEY CHECK (id=1),
                version INTEGER NOT NULL,
                updated_ts TEXT NOT NULL
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_state_change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                entity TEXT,
                entity_id TEXT,
                from_state TEXT,
                to_state TEXT,
                reason TEXT
            )
            """
        )
        _ensure_column(c, "churn_event_log", "lane", "TEXT")
        _ensure_column(c, "churn_event_log", "execution_id", "TEXT")
        _ensure_column(c, "ghost_sim_runs", "macro_shock", "INTEGER")
        _ensure_column(c, "ghost_sim_runs", "estimated_cost_bps", "REAL")
        _ensure_column(c, "ghost_sim_runs", "execution_style", "TEXT")
        _ensure_column(c, "ghost_sim_runs", "invalid_reason", "TEXT")
        _ensure_column(c, "ghost_sim_runs", "extreme_class", "TEXT")
        _ensure_column(c, "ghost_sim_runs", "valid_for_governor", "INTEGER")
        _ensure_column(c, "risk_state_current", "previous_halt_reason", "TEXT")
        _ensure_column(c, "risk_state_current", "attempt_churn_count", "INTEGER")
        _ensure_column(c, "risk_state_current", "execution_churn_count", "INTEGER")
        _ensure_column(c, "risk_state_current", "loss_pressure_24h", "REAL")
        _ensure_column(c, "candidate_funnel_log", "watch_total", "INTEGER")
        _ensure_column(c, "candidate_funnel_log", "actionable_total", "INTEGER")
        _ensure_column(c, "candidate_funnel_log", "alpha_scored_total", "INTEGER")
        _ensure_column(c, "candidate_funnel_log", "cost_evaluated_total", "INTEGER")
        _ensure_column(c, "candidate_funnel_log", "risk_evaluated_total", "INTEGER")
        _ensure_column(c, "candidate_funnel_log", "rejects_tradable_json", "TEXT")
        _ensure_column(c, "candidate_funnel_log", "rejects_external_json", "TEXT")
        _ensure_column(c, "candidate_funnel_log", "sanity_json", "TEXT")
        _ensure_column(c, "candidate_funnel_log", "regime", "TEXT")
        _ensure_column(c, "candidate_funnel_log", "pressure_count", "INTEGER")
        _ensure_column(c, "universe_state", "instrument_symbol", "TEXT")
        _ensure_column(c, "universe_state", "base_ccy", "TEXT")
        _ensure_column(c, "universe_state", "quote_ccy", "TEXT")


def put_signal(symbol: str | None, source: str, signal_type: str, payload: dict, conviction: str = "watch") -> None:
    raw = json.dumps(payload, sort_keys=True, default=str)
    dedupe = hashlib.sha256(f"{symbol}|{source}|{signal_type}|{raw}".encode()).hexdigest()
    with db() as c:
        c.execute(
            "INSERT OR IGNORE INTO intel_cache (ts,symbol,source,signal_type,conviction,payload_json,dedupe_hash) VALUES (?,?,?,?,?,?,?)",
            (utc_now(), symbol, source, signal_type, conviction, raw, dedupe),
        )


def log_source_health(source_name: str, status: str, latency_ms: int | None, items: int, err: str | None = None) -> None:
    with db() as c:
        c.execute(
            "INSERT INTO source_health_log (ts,source_name,status,latency_ms,items_ingested,error_count,last_error) VALUES (?,?,?,?,?,?,?)",
            (utc_now(), source_name, status, latency_ms, items, 0 if not err else 1, err),
        )


def get_json(url: str, headers: dict | None = None) -> dict | list:
    r = requests.get(url, headers=headers or {}, timeout=20)
    r.raise_for_status()
    return r.json()


def get_text(url: str, headers: dict | None = None) -> str:
    r = requests.get(url, headers=headers or {}, timeout=20)
    r.raise_for_status()
    return r.text


def ingest_cryptocom() -> tuple[set[str], dict[str, str]]:
    t0 = time.time()
    invalid_bases = {"USDT", "USDC", "USD", "EUR"}
    bases: set[str] = set()
    base_to_instrument: dict[str, str] = {}
    try:
        instruments = get_json("https://api.crypto.com/exchange/v1/public/get-instruments")
        result = instruments.get("result", {}) if isinstance(instruments, dict) else {}
        rows = result.get("instruments") or result.get("data") or []

        for r in rows[:5000]:
            quote = str(r.get("quote_ccy") or r.get("quote_currency") or "").upper()
            base = str(r.get("base_ccy") or r.get("base_currency") or "").upper()
            name = str(r.get("instrument_name") or r.get("symbol") or "").upper()
            inst_type = str(r.get("inst_type") or "")

            if not base and (name.endswith("_USDT") or name.endswith("_USD") or name.endswith("_USDC") or name.endswith("_EUR")):
                parts = name.split("_")
                if len(parts) >= 2:
                    base, quote = parts[0], parts[1]

            if not base or base in invalid_bases:
                continue
            if quote not in {"USD", "USDT", "USDC", "EUR"}:
                continue
            if inst_type and inst_type != "CCY_PAIR":
                continue

            instrument_symbol = name if "_" in name else f"{base}_{quote}"
            # prefer USDT over USD over USDC over EUR for canonical join key
            prev = base_to_instrument.get(base)
            if prev is None or prev.endswith("_EUR") or (prev.endswith("_USD") and instrument_symbol.endswith("_USDT")) or (prev.endswith("_USDC") and instrument_symbol.endswith("_USDT")):
                base_to_instrument[base] = instrument_symbol
            bases.add(base)

        put_signal(None, Source.CRYPTOCOM, "instruments_snapshot", {"count": len(rows), "mapped_bases": len(bases), "mapped_instruments": len(base_to_instrument)})
        log_source_health(Source.CRYPTOCOM, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CRYPTOCOM, "ingest_error", {"error": str(e)})
        log_source_health(Source.CRYPTOCOM, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return bases, base_to_instrument


def _percentile(values: list[float], p: float, fallback: float) -> float:
    clean = sorted(v for v in values if isinstance(v, (int, float)))
    if not clean:
        return fallback
    idx = int((len(clean) - 1) * max(0.0, min(1.0, p)))
    return float(clean[idx])


def ingest_cryptocom_movers() -> tuple[set[str], dict[str, dict]]:
    """Venue-native discovery (top volume / movers / spread events).

    Returns mover symbols and per-symbol microstructure-like metrics from ticker stream.
    """
    t0 = time.time()
    movers: set[str] = set()
    metrics: dict[str, dict] = {}
    try:
        data = get_json("https://api.crypto.com/exchange/v1/public/get-tickers")
        rows = data.get("result", {}).get("data", []) if isinstance(data, dict) else []

        ranked = []
        for r in rows:
            inst = str(r.get("i", "")).upper()  # e.g. BTC_USDT / BTC_USD
            if "_" not in inst:
                continue
            base, quote = inst.split("_", 1)
            if quote not in {"USD", "USDT"}:
                continue

            vol_quote = float(r.get("vv") or 0.0)
            chg_24h = abs(float(r.get("c") or 0.0))
            bid = float(r.get("b") or 0.0)
            ask = float(r.get("k") or 0.0)
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
            spread_bps = ((ask - bid) / mid * 10000.0) if mid > 0 else 9999.0

            ranked.append((base, vol_quote, chg_24h, spread_bps))
            metrics[base] = {
                "volume_24h_quote": vol_quote,
                "chg_24h_abs": chg_24h,
                "spread_bps": spread_bps,
            }

        ranked_by_vol = sorted(ranked, key=lambda x: x[1], reverse=True)[:80]
        ranked_by_move = sorted(ranked, key=lambda x: x[2], reverse=True)[:80]
        spread_events = [x for x in ranked if x[3] <= 50.0][:80]

        for base, *_ in ranked_by_vol + ranked_by_move + spread_events:
            movers.add(base)

        put_signal(None, Source.CRYPTOCOM, "venue_movers_snapshot", {
            "rows": len(rows),
            "movers": len(movers),
            "top_volume": [x[0] for x in ranked_by_vol[:10]],
            "top_move": [x[0] for x in ranked_by_move[:10]],
        })
        log_source_health(Source.CRYPTOCOM, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CRYPTOCOM, "venue_movers_error", {"error": str(e)})
        log_source_health(Source.CRYPTOCOM, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))

    return movers, metrics


def detect_new_listings(current_symbols: set[str]) -> set[str]:
    prev: set[str] = set()
    try:
        if os.path.exists(LISTING_STATE_PATH):
            prev = set(json.loads(open(LISTING_STATE_PATH, "r", encoding="utf-8").read()).get("symbols", []))
    except Exception:
        prev = set()

    new_symbols = set(current_symbols) - prev if prev else set()
    if new_symbols:
        for sym in sorted(new_symbols)[:100]:
            put_signal(sym, Source.CRYPTOCOM, "new_listing", {"symbol": sym}, conviction="watch")

    try:
        with open(LISTING_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump({"ts": utc_now(), "symbols": sorted(list(current_symbols))}, f)
    except Exception:
        pass

    return new_symbols


def ingest_coingecko() -> set[str]:
    t0 = time.time()
    out: set[str] = set()
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    try:
        trending = get_json("https://api.coingecko.com/api/v3/search/trending", headers=headers)
        coins = trending.get("coins", []) if isinstance(trending, dict) else []
        for item in coins:
            coin = item.get("item", {})
            sym = str(coin.get("symbol", "")).upper()
            if sym:
                out.add(sym)
                put_signal(sym, Source.COINGECKO, "trending", {"coin": coin})
        log_source_health(Source.COINGECKO, "OK", int((time.time() - t0) * 1000), len(coins))
    except Exception as e:
        put_signal(None, Source.COINGECKO, "ingest_error", {"error": str(e)})
        log_source_health(Source.COINGECKO, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return out


def ingest_cmc() -> set[str]:
    t0 = time.time()
    out: set[str] = set()
    if not COINMARKETCAP_API_KEY:
        log_source_health(Source.CMC, "DISABLED", 0, 0, "missing api key")
        return out
    try:
        headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
        data = get_json("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=100&convert=USD", headers=headers)
        rows = data.get("data", []) if isinstance(data, dict) else []
        for row in rows:
            sym = str(row.get("symbol", "")).upper()
            if sym:
                out.add(sym)
                put_signal(sym, Source.CMC, "ranking", {"cmc_rank": row.get("cmc_rank")})
        log_source_health(Source.CMC, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CMC, "ingest_error", {"error": str(e)})
        log_source_health(Source.CMC, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return out


def estimate_cost_bps(spread_bps: float, liquidity_cover: float, volatility_abs: float, depth_usd_20bps: float | None = None) -> float:
    half_spread = max(0.0, spread_bps / 2.0)
    depth = max(1.0, float(depth_usd_20bps or 0.0))
    slippage_depth = (TARGET_NOTIONAL_USDT / depth) * 10000.0 * 0.08
    slippage_proxy = max(2.0, 20.0 / max(1.0, liquidity_cover))
    slippage = max(slippage_proxy, slippage_depth)
    vol_penalty = min(15.0, max(0.0, volatility_abs * 100.0 * 0.3))
    return half_spread + slippage + vol_penalty


def _k_slip(regime: str, liquidity_cover: float, orderbook_slope: float) -> float:
    base = 0.08
    if regime == "VOLATILE":
        base *= 1.30
    elif regime == "QUIET":
        base *= 0.90
    if liquidity_cover >= 30:
        base *= 0.85
    elif liquidity_cover < 8:
        base *= 1.20
    if orderbook_slope > 1.15:
        base *= 1.12
    elif orderbook_slope < 0.85:
        base *= 0.93
    return max(0.04, min(0.18, base))


def _interpolated_impact_bps(notional: float, depth_map: dict[float, float]) -> float:
    points = sorted([(float(k), max(1.0, float(v))) for k, v in depth_map.items()], key=lambda x: x[0])
    if not points:
        return 50.0
    if notional <= points[0][1]:
        return points[0][0]
    for i in range(1, len(points)):
        b0, d0 = points[i - 1]
        b1, d1 = points[i]
        if d0 <= notional <= d1:
            if d1 <= d0:
                return b1
            ratio = (notional - d0) / (d1 - d0)
            return b0 + ratio * (b1 - b0)
    b_last, d_last = points[-1]
    return b_last * max(1.0, notional / max(1.0, d_last))


def _safety_margin_components(regime: str, is_new_listing: bool, liquidity_cover: float, volatility_abs: float, uncertainty: float) -> dict[str, float]:
    base = 10.0 if is_new_listing else 6.0
    regime_component = 1.0 if regime == "NORMAL" else (4.0 if regime == "VOLATILE" else 2.0)
    uncertainty_component = max(0.0, min(8.0, uncertainty * 8.0))
    if regime == "NORMAL" and liquidity_cover >= 25:
        regime_component = min(regime_component, 1.5)
    total = base + regime_component + uncertainty_component
    floor_bps = 5.0
    cap_bps = 12.0 if (regime == "NORMAL" and liquidity_cover >= 25 and not is_new_listing) else 22.0
    total = max(floor_bps, min(cap_bps, total))
    return {
        "base": round(base, 3),
        "regime_component": round(regime_component, 3),
        "uncertainty_component": round(uncertainty_component, 3),
        "floor": floor_bps,
        "cap": cap_bps,
        "total": round(total, 3),
    }


def _maker_fill_probability(spread_bps: float, vol_abs: float, obi_abs: float, spread_stability: float, trade_rate_proxy: float, time_budget_sec: int) -> float:
    p = 0.55
    p -= min(0.25, spread_bps / 500.0)
    p -= min(0.15, obi_abs / 4.0)
    p += min(0.18, vol_abs * 2.0)
    p += 0.15 * max(0.0, min(1.0, spread_stability))
    p += 0.12 * max(0.0, min(1.0, trade_rate_proxy / 1.5))
    p += min(0.08, max(0, time_budget_sec - 30) / 600.0)
    return max(0.05, min(0.95, p))


def _chunked(items: list[str], n: int) -> list[list[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def _resolve_dex_symbols(rows: list[dict]) -> dict[str, str]:
    """Resolve tokenAddress -> symbol via DEX pairs endpoint.

    token-profiles/boost endpoints often omit tokenSymbol. We enrich using:
    /latest/dex/tokens/{addr1,addr2,...} and pick highest-liquidity pair symbol.
    """
    addrs = [str(r.get("tokenAddress") or "").strip() for r in rows if r.get("tokenAddress")]
    addrs = [a for a in addrs if a]
    if not addrs:
        return {}

    resolved: dict[str, tuple[str, float]] = {}
    uniq = list(dict.fromkeys(addrs))
    for batch in _chunked(uniq, 30):
        try:
            u = _norm_url("/latest/dex/tokens/" + ",".join(batch))
            d = get_json(u)
            pairs = d.get("pairs", []) if isinstance(d, dict) else []
            for p in pairs:
                base = p.get("baseToken", {}) or {}
                addr = str(base.get("address") or "").strip()
                sym = str(base.get("symbol") or "").upper().strip()
                liq = float((p.get("liquidity") or {}).get("usd") or 0.0)
                if not addr or not sym:
                    continue
                prev = resolved.get(addr)
                if prev is None or liq > prev[1]:
                    resolved[addr] = (sym, liq)
        except Exception:
            continue

    return {k: v[0] for k, v in resolved.items()}


def ingest_dex() -> tuple[set[str], set[str], dict[str, dict]]:
    t0 = time.time()
    boosted: set[str] = set()
    latest: set[str] = set()
    symbol_metrics: dict[str, dict] = {}
    try:
        l = get_json(_norm_url(DEX_LATEST_PROFILES_ENDPOINT))
        b = get_json(_norm_url(DEX_BOOSTED_TOKENS_ENDPOINT))
        bt = get_json(_norm_url(DEX_BOOSTED_TOP_ENDPOINT))
        l_rows = l if isinstance(l, list) else []
        b_rows = b if isinstance(b, list) else []
        bt_rows = bt if isinstance(bt, list) else []

        addr_to_sym = _resolve_dex_symbols(l_rows + b_rows + bt_rows)

        for row in l_rows[:400]:
            addr = str(row.get("tokenAddress") or "").strip()
            sym = str(row.get("tokenSymbol") or row.get("symbol") or addr_to_sym.get(addr) or "").upper()
            if sym:
                latest.add(sym)
                vol = float(row.get("volume") or row.get("volume24h") or 0)
                symbol_metrics.setdefault(sym, {})["volume24h"] = max(vol, symbol_metrics.get(sym, {}).get("volume24h", 0))
                put_signal(sym, Source.DEX, "latest_profile", {"entry": row}, conviction="watch")

        for row in b_rows[:400]:
            addr = str(row.get("tokenAddress") or "").strip()
            sym = str(row.get("tokenSymbol") or row.get("symbol") or addr_to_sym.get(addr) or "").upper()
            if sym:
                boosted.add(sym)
                put_signal(sym, Source.DEX, "boosted", {"entry": row}, conviction="high")

        for row in bt_rows[:400]:
            addr = str(row.get("tokenAddress") or "").strip()
            sym = str(row.get("tokenSymbol") or row.get("symbol") or addr_to_sym.get(addr) or "").upper()
            if sym:
                boosted.add(sym)
                put_signal(sym, Source.DEX, "boosted_top", {"entry": row}, conviction="high")

        put_signal(None, Source.DEX, "symbol_resolution", {
            "rows_total": len(l_rows) + len(b_rows) + len(bt_rows),
            "resolved_symbols": len(addr_to_sym),
        })
        log_source_health(Source.DEX, "OK", int((time.time() - t0) * 1000), len(l_rows) + len(b_rows) + len(bt_rows))
    except Exception as e:
        put_signal(None, Source.DEX, "ingest_error", {"error": str(e)})
        log_source_health(Source.DEX, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return latest, boosted, symbol_metrics


def ingest_news() -> int:
    total_hits = 0
    for source, url in [
        (Source.CRYPTOPANIC, "https://cryptopanic.com/news/"),
        (Source.FREE_NEWS, "https://freecryptonews.com/"),
    ]:
        t0 = time.time()
        try:
            html = get_text(url, headers={"User-Agent": "Mozilla/5.0"})
            bullish_hits = sum(1 for w in ["surge", "breakout", "bull", "rally", "soar", "uptrend", "listing"] if w in html.lower())
            total_hits += bullish_hits
            put_signal(None, source, "headline_scan", {"bullish_keyword_hits": bullish_hits, "bytes": len(html)})
            log_source_health(source, "OK", int((time.time() - t0) * 1000), 1)
        except Exception as e:
            put_signal(None, source, "ingest_error", {"error": str(e)})
            log_source_health(source, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return total_hits


def ingest_defillama() -> tuple[set[str], int]:
    t0 = time.time()
    symbols: set[str] = set()
    points = 0
    try:
        chains = get_json("https://api.llama.fi/v2/chains")
        protocols = get_json("https://api.llama.fi/protocols")

        chain_rows = chains if isinstance(chains, list) else []
        proto_rows = protocols if isinstance(protocols, list) else []

        top_chains = sorted(chain_rows, key=lambda x: float(x.get("tvl") or 0), reverse=True)[:20]
        for c in top_chains:
            put_signal(None, Source.DEFILLAMA, "chain_tvl", {
                "name": c.get("name"),
                "tvl": c.get("tvl"),
                "tokenSymbol": c.get("tokenSymbol"),
                "change_1d": c.get("change_1d"),
                "change_7d": c.get("change_7d"),
            })

        top_protocols = sorted(proto_rows, key=lambda x: float(x.get("tvl") or 0), reverse=True)[:300]
        for p in top_protocols:
            sym = str(p.get("symbol") or "").upper().strip()
            if sym and sym.replace("$", "").replace("-", "").replace("_", "").isalnum():
                symbols.add(sym)
                put_signal(sym, Source.DEFILLAMA, "protocol_tvl", {
                    "name": p.get("name"),
                    "tvl": p.get("tvl"),
                    "change_1d": p.get("change_1d"),
                    "change_7d": p.get("change_7d"),
                    "category": p.get("category"),
                    "chains": p.get("chains"),
                })

        points = len(top_chains) + len(top_protocols)
        log_source_health(Source.DEFILLAMA, "OK", int((time.time() - t0) * 1000), points)
    except Exception as e:
        put_signal(None, Source.DEFILLAMA, "ingest_error", {"error": str(e)})
        log_source_health(Source.DEFILLAMA, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))

    return symbols, points


def ingest_dune() -> tuple[set[str], int]:
    t0 = time.time()
    symbols: set[str] = set()
    total_rows = 0

    if not DUNE_API_KEY or not DUNE_QUERY_IDS:
        log_source_health(Source.DUNE, "DISABLED", 0, 0, "missing api key/query ids")
        return symbols, total_rows

    headers = {"X-Dune-API-Key": DUNE_API_KEY}
    for qid in DUNE_QUERY_IDS:
        try:
            u = f"https://api.dune.com/api/v1/query/{qid}/results"
            d = get_json(u, headers=headers)
            rows = d.get("result", {}).get("rows", []) if isinstance(d, dict) else []
            total_rows += len(rows)
            for r in rows[:300]:
                sym = str(r.get("symbol") or r.get("token") or "").upper().strip()
                if sym:
                    symbols.add(sym)
                    put_signal(sym, Source.DUNE, "query_signal", {"query_id": qid, "row": r})
            put_signal(None, Source.DUNE, "query_summary", {"query_id": qid, "rows": len(rows)})
        except Exception as e:
            put_signal(None, Source.DUNE, "query_error", {"query_id": qid, "error": str(e)})

    status = "OK" if total_rows > 0 else "DEGRADED"
    log_source_health(Source.DUNE, status, int((time.time() - t0) * 1000), total_rows, None if total_rows > 0 else "no rows")
    return symbols, total_rows


def load_micro_latest() -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        with db() as c:
            cur = c.cursor()
            cur.execute(
                """
                select base_symbol, spread_bps_median_300, spread_bps_p95_300, depth_usd_5bps, depth_usd_10bps, depth_usd_20bps, depth_usd_50bps,
                       obi_10bps, obi_20bps, pressure_flag, pressure_confidence, orderbook_slope, liquidity_score, risk_flags_json, payload_json
                from micro_features_latest
                """
            )
            for r in cur.fetchall():
                payload = {}
                try:
                    payload = json.loads(r[14] or "{}")
                except Exception:
                    payload = {}
                out[str(r[0]).upper()] = {
                    "spread_bps_median_300": float(r[1] or 0.0),
                    "spread_bps_p95_300": float(r[2] or 0.0),
                    "depth_usd_5bps": float(r[3] or 0.0),
                    "depth_usd_10bps": float(r[4] or 0.0),
                    "depth_usd_20bps": float(r[5] or 0.0),
                    "depth_usd_50bps": float(r[6] or 0.0),
                    "obi_10bps": float(r[7] or 0.0),
                    "obi_20bps": float(r[8] or 0.0),
                    "pressure_flag": bool(r[9]),
                    "pressure_confidence": float(r[10] or 0.0),
                    "orderbook_slope": float(r[11] or 0.0),
                    "liquidity_score": float(r[12] or 0.0),
                    "risk_flags": json.loads(r[13] or "[]"),
                    "spread_stability": float(payload.get("spread_stability", 0.0) or 0.0),
                    "trade_rate_proxy": float(payload.get("trade_rate_proxy", 0.0) or 0.0),
                }
    except Exception:
        return {}
    return out


def source_availability(cg: set[str], cmc: set[str], dex_latest: set[str], news_hits: int, venue_movers: set[str], llama_syms: set[str], dune_syms: set[str]) -> dict:
    x_on = bool(os.getenv("X_BEARER_TOKEN") or os.getenv("X_API_KEY"))
    reddit_on = bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET"))
    return {
        Source.COINGECKO: len(cg) > 0,
        Source.CMC: len(cmc) > 0,
        Source.DEX: len(dex_latest) > 0,
        Source.DEFILLAMA: len(llama_syms) > 0,
        Source.DUNE: len(dune_syms) > 0,
        Source.CRYPTOPANIC: news_hits >= 0,
        Source.FREE_NEWS: news_hits >= 0,
        "venue_movers": len(venue_movers) > 0,
        "microstructure": True,
        Source.X: x_on,
        Source.REDDIT: reddit_on,
    }


def weighted_alpha(symbol: str, cg: set[str], cmc: set[str], dex_latest: set[str], dex_boosted: set[str], venue_movers: set[str], sources_present: dict, venue_metrics: dict[str, dict]) -> tuple[float, float, dict]:
    base_weights = {
        "market": 28.0,
        "liquidity": 22.0,
        "microstructure": 25.0,
        "news": 10.0,
        "social": 10.0,
        "attention": 5.0,
    }
    social_available = sources_present.get(Source.X) or sources_present.get(Source.REDDIT)
    if not social_available:
        base_weights["social"] = 0.0
        base_weights["market"] += 5.0
        base_weights["microstructure"] += 10.0
        base_weights["news"] += 5.0

    score = 0.0
    market_points = 0.0
    if symbol in cg:
        market_points += 20.0
    if symbol in cmc:
        market_points += 15.0
    score += min(base_weights["market"], market_points)

    liquidity_points = 10.0 if symbol in dex_latest else 0.0
    liquidity_points += 15.0 if symbol in dex_boosted else 0.0
    score += min(base_weights["liquidity"], liquidity_points)

    vm = venue_metrics.get(symbol, {})
    micro_points = 0.0
    if symbol in venue_movers:
        micro_points += 12.0
    spread_bps = float(vm.get("spread_bps", 9999.0))
    if spread_bps <= 50:
        micro_points += 8.0
    if spread_bps <= 20:
        micro_points += 5.0
    if float(vm.get("chg_24h_abs", 0.0)) >= 0.03:
        micro_points += 5.0
    score += min(base_weights["microstructure"], micro_points)

    news_points = 4.0 if symbol in cg else 0.0
    news_points += 6.0 if symbol in dex_boosted else 0.0
    score += min(base_weights["news"], news_points)

    social_points = 0.0
    if social_available:
        social_points = 8.0 if symbol in cg else 0.0
        social_points += 12.0 if symbol in dex_boosted else 0.0
        score += min(base_weights["social"], social_points)

    # Attention burst score from DEX profile/boost activity (capped by attention weight).
    attention_burst = 0.0
    if symbol in dex_latest:
        attention_burst += 2.0
    if symbol in dex_boosted:
        attention_burst += 5.0
    attention_score = min(base_weights["attention"], attention_burst)
    score += attention_score

    confidence = max(0.05, min(0.99, score / 100.0))

    return score, confidence, {
        "weights": base_weights,
        "market_points": market_points,
        "liquidity_points": liquidity_points,
        "micro_points": micro_points,
        "news_points": news_points,
        "social_points": social_points,
        "attention_score": attention_score,
        "spread_bps": spread_bps,
    }


def log_reject(symbol: str, stage: str, reason: str, metrics: dict) -> None:
    with db() as c:
        c.execute(
            "INSERT INTO candidate_reject_log (ts,symbol,stage,reason,metrics_json) VALUES (?,?,?,?,?)",
            (utc_now(), symbol, stage, reason, json.dumps(metrics, default=str)),
        )


def log_universe_state(symbol: str, instrument_symbol: str | None, base_ccy: str | None, quote_ccy: str | None, eligibility_score: float, alpha_score: float, confidence: float, status: str, deny_stage: str | None, deny_reason: str | None, metrics: dict, sources_present: dict) -> None:
    with db() as c:
        c.execute(
            """
            INSERT INTO universe_state (ts,symbol,instrument_symbol,base_ccy,quote_ccy,eligibility_score,alpha_score,confidence,status,deny_stage,deny_reason,metrics_json,sources_present_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                symbol,
                instrument_symbol,
                base_ccy,
                quote_ccy,
                eligibility_score,
                alpha_score,
                confidence,
                status,
                deny_stage,
                deny_reason,
                json.dumps(metrics, default=str),
                json.dumps(sources_present, default=str),
            ),
        )


def log_funnel(discovered: int, mapped: int, eligible: int, alpha_pass: int, risk_pass: int, cost_pass: int, proposed: int, top_reasons: list[tuple[str, int]], sources_present: dict, extras: dict | None = None) -> None:
    e = extras or {}
    with db() as c:
        c.execute(
            """
            INSERT INTO candidate_funnel_log (
                ts,discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,
                reasons_json,sources_present_json,watch_total,actionable_total,alpha_scored_total,cost_evaluated_total,risk_evaluated_total,
                rejects_tradable_json,rejects_external_json,sanity_json,regime,pressure_count
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                discovered,
                mapped,
                eligible,
                alpha_pass,
                risk_pass,
                cost_pass,
                proposed,
                json.dumps(top_reasons, default=str),
                json.dumps(sources_present, default=str),
                int(e.get("watch_total", 0)),
                int(e.get("actionable_total", proposed)),
                int(e.get("alpha_scored_total", 0)),
                int(e.get("cost_evaluated_total", 0)),
                int(e.get("risk_evaluated_total", 0)),
                json.dumps(e.get("rejects_tradable", {}), default=str),
                json.dumps(e.get("rejects_external", {}), default=str),
                json.dumps(e.get("sanity", {}), default=str),
                e.get("regime"),
                int(e.get("pressure_count", 0)),
            ),
        )


def classify_regime(venue_metrics: dict[str, dict], micro_latest: dict[str, dict]) -> str:
    spreads = [float(v.get("spread_bps_p95_300", 0.0)) for v in micro_latest.values() if v.get("spread_bps_p95_300") is not None]
    vols = [abs(float(v.get("chg_24h_abs", 0.0))) for v in venue_metrics.values()]
    med_spread = _percentile(spreads, 0.5, 20.0)
    med_vol = _percentile(vols, 0.5, 0.02)
    if med_spread <= 15 and med_vol <= 0.02:
        return "QUIET"
    if med_spread >= 35 or med_vol >= 0.06:
        return "VOLATILE"
    return "NORMAL"


def obi_threshold_for_regime(regime: str) -> float:
    return {"QUIET": 0.40, "NORMAL": 0.55, "VOLATILE": 0.70}.get(regime, 0.55)


def build_startup_ping(summary: dict) -> str:
    funnel = (
        f"discovered={summary.get('discovered_total', 0)} "
        f"mapped={summary.get('mapped_to_venue_total', 0)} "
        f"eligible={summary.get('eligible_total', 0)} "
        f"alpha={summary.get('alpha_pass_total', 0)} "
        f"risk={summary.get('risk_pass_total', 0)} "
        f"cost={summary.get('cost_pass_total', 0)} "
        f"proposed={summary.get('proposed_total', 0)}"
    )
    top_actionable = summary.get("actionable_top", [])[:3]
    top_watch = summary.get("watchlist_top", [])[:3]
    action_txt = ", ".join(f"{x.get('symbol')}({x.get('alpha_score')})" for x in top_actionable) or "none"
    watch_txt = ", ".join(f"{x.get('symbol')}({x.get('alpha_score')})" for x in top_watch) or "none"
    return (
        "[Project Crypt][Phase-2] startup complete\n"
        f"Funnel: {funnel}\n"
        f"Top actionable: {action_txt}\n"
        f"Top watchlist: {watch_txt}\n"
        f"Fast ticks: market={MARKET_TICK_SECONDS}s discovery={DISCOVERY_TICK_SECONDS}s full_cycle={POLL_SECONDS}s"
    )


def refresh_listing_watch(dex_boosted: set[str], dex_latest: set[str], llama_syms: set[str], crypto_bases: set[str]) -> None:
    now = utc_now()
    candidates = sorted((dex_boosted.union(dex_latest)).difference(crypto_bases))
    with db() as c:
        for sym in candidates[:500]:
            fundamentals = sym in llama_syms
            priority = 60.0 + (25.0 if sym in dex_boosted else 0.0) + (15.0 if fundamentals else 0.0)
            reasons = {"dex_boosted": sym in dex_boosted, "dex_latest": sym in dex_latest, "fundamentals_spike": fundamentals}
            c.execute(
                """
                INSERT INTO listing_watch (ts_first,ts_last,symbol_guess,chain,address,priority_score,reasons_json,status)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (now, now, sym, None, None, priority, json.dumps(reasons), "watching"),
            )
        c.execute(
            """
            UPDATE listing_watch
            SET ts_last=?, status='watching'
            WHERE symbol_guess IN ({})
            """.format(",".join("?" * len(candidates)) if candidates else "''"),
            ([now] + candidates) if candidates else [now],
        )


def emit_listing_front_run(new_listings: set[str]) -> set[str]:
    triggered: set[str] = set()
    if not new_listings:
        return triggered
    with db() as c:
        cur = c.cursor()
        for sym in sorted(new_listings):
            cur.execute("select id,priority_score,reasons_json from listing_watch where symbol_guess=? and status='watching' order by id desc limit 1", (sym,))
            row = cur.fetchone()
            if not row:
                continue
            triggered.add(sym)
            details = {
                "symbol": sym,
                "priority_score": float(row[1] or 0.0),
                "reasons": json.loads(row[2] or "{}"),
            }
            put_signal(sym, Source.SYSTEM, "listing_front_run", details, conviction="moonshot")
            c.execute("insert into listing_watch_events (ts,event_type,details_json) values (?,?,?)", (utc_now(), "listing_front_run", json.dumps(details)))
            c.execute("update listing_watch set status='triggered', ts_last=? where id=?", (utc_now(), int(row[0])))
    return triggered


def tradable_autotuner_stats(hours: int = 24) -> dict:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    out = {"ghost_runs": 0, "avg_net_pnl_bps": 0.0, "median_net_pnl_bps": 0.0, "p25": 0.0, "p75": 0.0, "p5": 0.0, "hit_rate": 0.0, "by_reject_reason": {}}
    all_vals: list[float] = []
    with db() as c:
        cur = c.cursor()
        cur.execute(
            """
            select reject_reason, net_pnl_bps
            from ghost_sim_runs
            where ts>=? and instrument_symbol like '%_%' and coalesce(valid_for_governor,1)=1
            """,
            (since,),
        )
        rows = cur.fetchall()
        bucket: dict[str, list[float]] = {}
        for rr, val in rows:
            k = str(rr or "unknown")
            v = float(val or 0.0)
            bucket.setdefault(k, []).append(v)
            all_vals.append(v)

        for rr, vals in bucket.items():
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            if n == 0:
                continue
            p25 = vals_sorted[int(max(0, min(n - 1, round(0.25 * (n - 1)))))]
            p75 = vals_sorted[int(max(0, min(n - 1, round(0.75 * (n - 1)))))]
            p5 = vals_sorted[int(max(0, min(n - 1, round(0.05 * (n - 1)))))]
            med = vals_sorted[n // 2]
            hit = sum(1 for x in vals_sorted if x > 0) / max(1, n)
            out["by_reject_reason"][rr] = {
                "n": n,
                "avg_net_pnl_bps": round(sum(vals_sorted) / n, 2),
                "median": round(med, 2),
                "p25": round(p25, 2),
                "p75": round(p75, 2),
                "p5": round(p5, 2),
                "hit_rate": round(hit, 3),
            }

    if all_vals:
        s = sorted(all_vals)
        n = len(s)
        out["ghost_runs"] = n
        out["avg_net_pnl_bps"] = round(sum(s) / n, 2)
        out["median_net_pnl_bps"] = round(s[n // 2], 2)
        out["p25"] = round(s[int(round(0.25 * (n - 1)))], 2)
        out["p75"] = round(s[int(round(0.75 * (n - 1)))], 2)
        out["p5"] = round(s[int(round(0.05 * (n - 1)))], 2)
        out["hit_rate"] = round(sum(1 for x in s if x > 0) / n, 3)
    return out


def _churn_params_hash(symbol: str, reason: str, extras: dict | None = None) -> str:
    raw = json.dumps({"symbol": symbol, "reason": reason, "extras": extras or {}}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def log_churn_event(attempt_id: str, symbol: str, event_class: str, event_reason: str, counted: bool, extras: dict | None = None, lane: str | None = None, execution_id: str | None = None) -> None:
    params_hash = _churn_params_hash(symbol, event_reason, extras)
    with db() as c:
        cur = c.cursor()
        if counted:
            if event_class == "EXECUTION" and execution_id:
                cur.execute("select 1 from churn_event_log where event_class='EXECUTION' and execution_id=? limit 1", (execution_id,))
                if cur.fetchone():
                    return
            else:
                cur.execute("select 1 from churn_event_log where attempt_id=? and event_class=? and counted=1 limit 1", (attempt_id, event_class))
                if cur.fetchone():
                    return
        cur.execute(
            "insert into churn_event_log (ts,attempt_id,symbol,event_class,event_reason,params_hash,counted,details_json,lane,execution_id) values (?,?,?,?,?,?,?,?,?,?)",
            (utc_now(), attempt_id, symbol, event_class, event_reason, params_hash, int(bool(counted)), json.dumps(extras or {}, default=str), lane, execution_id),
        )


def in_penalty_box(symbol: str) -> bool:
    now = datetime.now(timezone.utc)
    with db() as c:
        cur = c.cursor()
        cur.execute("select until_ts from symbol_penalty_box where symbol=?", (symbol,))
        r = cur.fetchone()
        if not r:
            return False
        try:
            return datetime.fromisoformat(str(r[0])) > now
        except Exception:
            return False


def put_penalty_box(symbol: str, reason: str) -> None:
    until = (datetime.now(timezone.utc) + timedelta(seconds=CHURN_PENALTY_BOX_SEC)).isoformat()
    with db() as c:
        c.execute(
            "insert into symbol_penalty_box (symbol,until_ts,reason,updated_ts) values (?,?,?,?) on conflict(symbol) do update set until_ts=excluded.until_ts, reason=excluded.reason, updated_ts=excluded.updated_ts",
            (symbol, until, reason, utc_now()),
        )


def _get_json_retry(url: str, risk_state_fn=None) -> dict | list:
    last_err = None
    for i in range(max(1, CHURN_RETRY_MAX)):
        if risk_state_fn is not None:
            try:
                rs = risk_state_fn()
                if rs.get("halted"):
                    raise RuntimeError("RISK_FLIP_ABORT")
            except Exception as e:
                raise e
        try:
            return get_json(url)
        except Exception as e:
            last_err = e
            if "429" in str(e):
                sleep_s = (CHURN_RETRY_BACKOFF_BASE_SEC * (2 ** i)) + random.uniform(0.05, 0.4)
                time.sleep(min(5.0, sleep_s))
            else:
                sleep_s = (CHURN_RETRY_BACKOFF_BASE_SEC * (2 ** i)) + random.uniform(0.05, 0.3)
                time.sleep(min(4.0, sleep_s))
    if last_err:
        raise last_err
    raise RuntimeError("retry_exhausted")


def _btc_1h_return_abs() -> float:
    try:
        d = get_json("https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name=BTC_USDT&timeframe=1h")
        rows = d.get("result", {}).get("data", []) if isinstance(d, dict) else []
        if len(rows) < 3:
            return 0.0
        prev = float(rows[-2].get("c") or 0.0)
        curr = float(rows[-1].get("c") or 0.0)
        if prev <= 0 or curr <= 0:
            return 0.0
        return abs((curr - prev) / prev)
    except Exception:
        return 0.0


def detect_macro_shock(regime: str, news_hits: int, venue_metrics: dict[str, dict]) -> dict:
    now = datetime.now(timezone.utc)
    btc_ret_1h_abs = _btc_1h_return_abs()
    moves = [abs(float(v.get("chg_24h_abs", 0.0))) for v in venue_metrics.values() if v]
    median_move = _percentile(moves, 0.5, 0.0)

    # entry stricter than exit to avoid sticky/always-on shock
    enter = bool(btc_ret_1h_abs >= 0.02 or median_move >= 0.10 or news_hits >= 20 or (regime == "VOLATILE" and btc_ret_1h_abs >= 0.012))
    calm = bool(btc_ret_1h_abs < 0.010 and median_move < 0.06 and news_hits < 8)

    with db() as c:
        cur = c.cursor()
        cur.execute("select macro_shock, entered_at, calm_checks, details_json from macro_state_current where id=1")
        r = cur.fetchone()
        prev_active = bool(r[0]) if r else False
        entered_at = r[1] if r else None
        calm_checks = int(r[2] or 0) if r else 0

        active = prev_active
        if enter:
            active = True
            calm_checks = 0
            entered_at = entered_at or now.isoformat()
        elif prev_active:
            calm_checks = calm_checks + 1 if calm else 0
            if calm_checks >= 3:
                active = False
                entered_at = None
                calm_checks = 0

        payload = {
            "btc_ret_1h_abs": round(btc_ret_1h_abs, 5),
            "median_abs_move_24h": round(float(median_move), 5),
            "news_velocity": int(news_hits),
            "regime": regime,
            "enter_trigger": enter,
            "calm": calm,
        }
        c.execute(
            """
            insert into macro_state_current (id,ts,macro_shock,entered_at,calm_checks,details_json)
            values (1,?,?,?,?,?)
            on conflict(id) do update set
              ts=excluded.ts, macro_shock=excluded.macro_shock, entered_at=excluded.entered_at,
              calm_checks=excluded.calm_checks, details_json=excluded.details_json
            """,
            (utc_now(), int(active), entered_at, calm_checks, json.dumps(payload, default=str)),
        )

    return {
        "macro_shock": bool(active),
        "btc_ret_1h_abs": round(btc_ret_1h_abs, 5),
        "median_abs_move_24h": round(float(median_move), 5),
        "news_velocity": int(news_hits),
        "regime": regime,
        "enter_trigger": enter,
        "calm": calm,
        "entered_at": entered_at,
    }


def load_risk_state() -> dict:
    with db() as c:
        cur = c.cursor()
        cur.execute("select halted,halt_reason,churn_basis,churn_count,consecutive_losses,cooldown_seconds_remaining,cooldown_until,window_minutes,details_json,previous_halt_reason,attempt_churn_count,execution_churn_count,loss_pressure_24h from risk_state_current where id=1")
        r = cur.fetchone()
        if not r:
            return {
                "halted": False,
                "halt_reason": None,
                "previous_halt_reason": None,
                "churn_basis": "ACTIONABLE_ATTEMPTS",
                "churn_count": 0,
                "attempt_churn_count": 0,
                "execution_churn_count": 0,
                "consecutive_losses": 0,
                "loss_pressure_24h": 0.0,
                "cooldown_seconds_remaining": 0,
                "cooldown_until": None,
                "window_minutes": 60,
                "details": {},
            }
        return {
            "halted": bool(r[0]),
            "halt_reason": r[1],
            "churn_basis": r[2] or "ACTIONABLE_ATTEMPTS",
            "churn_count": int(r[3] or 0),
            "consecutive_losses": int(r[4] or 0),
            "cooldown_seconds_remaining": int(r[5] or 0),
            "cooldown_until": r[6],
            "window_minutes": int(r[7] or 60),
            "details": json.loads(r[8] or "{}"),
            "previous_halt_reason": r[9],
            "attempt_churn_count": int(r[10] or 0),
            "execution_churn_count": int(r[11] or 0),
            "loss_pressure_24h": float(r[12] or 0.0),
        }


def compute_shadow_risk_state(macro_shock: bool = False, regime: str = "NORMAL") -> dict:
    now = datetime.now(timezone.utc)
    window_minutes = int(os.getenv("PHASE2_RISK_WINDOW_MIN", "60"))
    cooldown_sec = int(os.getenv("PHASE2_RISK_COOLDOWN_SEC", "3600"))
    attempt_churn_limit = int(os.getenv("PHASE2_RISK_ATTEMPT_CHURN_LIMIT", "22"))
    execution_churn_limit = int(os.getenv("PHASE2_RISK_EXEC_CHURN_LIMIT", "14"))
    if regime == "VOLATILE":
        attempt_churn_limit = int(attempt_churn_limit * 1.25)
    if macro_shock:
        attempt_churn_limit = int(attempt_churn_limit * 1.5)
        execution_churn_limit = int(execution_churn_limit * 1.2)
    consec_loss_limit = int(os.getenv("PHASE2_RISK_CONSEC_LOSS_LIMIT", "8"))

    since = (now - timedelta(minutes=window_minutes)).isoformat()
    since_5m = (now - timedelta(minutes=5)).isoformat()
    since_24h = (now - timedelta(hours=24)).isoformat()
    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1", (since,))
        attempt_churn = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='EXECUTION' and counted=1", (since,))
        execution_churn = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1", (since_5m,))
        attempt_churn_5m = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from churn_event_log where ts>=? and event_class='EXECUTION' and counted=1", (since_5m,))
        execution_churn_5m = int((cur.fetchone() or [0])[0] or 0)

        cur.execute("select realized_pnl_bps, ts from shadow_portfolio_ledger where ts>=? order by id desc limit 200", (since_24h,))
        closed_rows_24h = [(float(r[0] or 0.0), str(r[1] or "")) for r in cur.fetchall()]

        cur.execute("select symbol, count(*) c from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1 group by symbol having c>=?", (since_5m, CHURN_SYMBOL_FAIL_LIMIT_5M))
        penalty_candidates = [str(r[0]) for r in cur.fetchall() if r[0]]
        for sym in penalty_candidates:
            put_penalty_box(sym, "attempt_churn_5m")

        cur.execute("select count(*) from symbol_penalty_box where until_ts > ?", (now.isoformat(),))
        unique_penalty_symbols = int((cur.fetchone() or [0])[0] or 0)

        cur.execute("select symbol,event_reason,params_hash,count(*) c from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1 group by symbol,event_reason,params_hash order by c desc limit 1", (since_5m,))
        hr = cur.fetchone()
        hot_loop = bool(hr and int(hr[3] or 0) >= CHURN_HOT_LOOP_MAX_IDENTICAL_5M)
        hot_loop_key = f"{hr[0]}:{hr[1]}:{hr[2]}" if hr else None

        cur.execute("select event_reason,count(*) c from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1 group by event_reason order by c desc limit 6", (since,))
        attempt_reasons_60m = {str(r[0]): int(r[1] or 0) for r in cur.fetchall()}
        cur.execute("select event_reason,count(*) c from churn_event_log where ts>=? and event_class='ATTEMPT' and counted=1 group by event_reason order by c desc limit 6", (since_5m,))
        attempt_reasons_5m = {str(r[0]): int(r[1] or 0) for r in cur.fetchall()}
        cur.execute("select symbol,event_reason,params_hash,ts from churn_event_log where event_class='ATTEMPT' order by id desc limit 8")
        last_attempt_events = [
            {"symbol": str(r[0] or ''), "reason": str(r[1] or ''), "params_hash": str(r[2] or ''), "ts": str(r[3] or '')}
            for r in cur.fetchall()
        ]

    # Consecutive losses from CLOSED trades only (pure streak)
    consec_losses = 0
    last_close_ts = None
    for pnl, ts in closed_rows_24h:
        if last_close_ts is None:
            last_close_ts = ts
        if pnl <= 0:
            consec_losses += 1
        else:
            break
    loss_pressure = float(sum(1 for x, _ in closed_rows_24h if x < 0))

    halt_reason = None
    halted = False
    previous_halt_reason = None

    prev = load_risk_state()
    cooldown_until = prev.get("cooldown_until")
    if prev.get("halted"):
        previous_halt_reason = prev.get("halt_reason")

    if hot_loop:
        halted = True
        halt_reason = "RISK_HOT_LOOP"
    elif execution_churn >= execution_churn_limit:
        halted = True
        halt_reason = "RISK_CHURN"
    elif unique_penalty_symbols >= CHURN_GLOBAL_PENALTY_SYMBOLS_LIMIT:
        halted = True
        halt_reason = "RISK_ATTEMPT_CHURN_SYSTEMIC"
    elif consec_losses >= consec_loss_limit and execution_churn > 0:
        halted = True
        halt_reason = "RISK_CONSEC_LOSSES"

    if halted:
        cooldown_until = (now + timedelta(seconds=cooldown_sec)).isoformat()
    elif cooldown_until:
        try:
            remain = int((datetime.fromisoformat(cooldown_until) - now).total_seconds())
            if remain > 0:
                halted = True
                halt_reason = "COOLDOWN"
            else:
                cooldown_until = None
        except Exception:
            cooldown_until = None

    cooldown_remaining = 0
    if cooldown_until:
        try:
            cooldown_remaining = max(0, int((datetime.fromisoformat(cooldown_until) - now).total_seconds()))
        except Exception:
            cooldown_remaining = 0

    # attempt churn = soft throttle only
    soft_throttle = bool(attempt_churn >= attempt_churn_limit)

    state = {
        "halted": bool(halted),
        "halt_reason": halt_reason,
        "previous_halt_reason": previous_halt_reason,
        "churn_basis": "ACTIONABLE_ATTEMPTS+EXECUTIONS",
        "churn_count": int(attempt_churn + execution_churn),
        "attempt_churn_count": int(attempt_churn),
        "execution_churn_count": int(execution_churn),
        "consecutive_losses": int(consec_losses),
        "loss_pressure_24h": round(loss_pressure, 3),
        "cooldown_seconds_remaining": int(cooldown_remaining),
        "cooldown_until": cooldown_until,
        "window_minutes": window_minutes,
        "details": {
            "attempt_churn_limit": attempt_churn_limit,
            "execution_churn_limit": execution_churn_limit,
            "consec_loss_limit": consec_loss_limit,
            "soft_throttle": soft_throttle,
            "macro_shock": bool(macro_shock),
            "attempt_churn_5m": attempt_churn_5m,
            "execution_churn_5m": execution_churn_5m,
            "unique_penalty_symbols": unique_penalty_symbols,
            "hot_loop": hot_loop,
            "hot_loop_key": hot_loop_key,
            "penalty_candidates": penalty_candidates,
            "attempt_reasons_5m": attempt_reasons_5m,
            "attempt_reasons_60m": attempt_reasons_60m,
            "last_attempt_events": last_attempt_events,
            "churn_telemetry_suspect": bool(execution_churn > max(1, attempt_churn * 2)),
            "consecutive_losses_basis": "CLOSED_TRADES",
            "last_close_ts": last_close_ts,
        },
    }

    with db() as c:
        c.execute(
            """
            insert into risk_state_current (id,ts,halted,halt_reason,churn_basis,churn_count,consecutive_losses,cooldown_seconds_remaining,cooldown_until,window_minutes,details_json,previous_halt_reason,attempt_churn_count,execution_churn_count,loss_pressure_24h)
            values (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            on conflict(id) do update set
              ts=excluded.ts, halted=excluded.halted, halt_reason=excluded.halt_reason,
              churn_basis=excluded.churn_basis, churn_count=excluded.churn_count,
              consecutive_losses=excluded.consecutive_losses,
              cooldown_seconds_remaining=excluded.cooldown_seconds_remaining,
              cooldown_until=excluded.cooldown_until,
              window_minutes=excluded.window_minutes,
              details_json=excluded.details_json,
              previous_halt_reason=excluded.previous_halt_reason,
              attempt_churn_count=excluded.attempt_churn_count,
              execution_churn_count=excluded.execution_churn_count,
              loss_pressure_24h=excluded.loss_pressure_24h
            """,
            (
                utc_now(),
                int(state["halted"]),
                state["halt_reason"],
                state["churn_basis"],
                state["churn_count"],
                state["consecutive_losses"],
                state["cooldown_seconds_remaining"],
                state["cooldown_until"],
                state["window_minutes"],
                json.dumps(state["details"], default=str),
                state["previous_halt_reason"],
                state["attempt_churn_count"],
                state["execution_churn_count"],
                state["loss_pressure_24h"],
            ),
        )
    return state


def _ghost_stats_meta() -> dict:
    with db() as c:
        cur = c.cursor()
        cur.execute("select version,updated_ts from ghost_stats_meta where id=1")
        r = cur.fetchone()
        if not r:
            c.execute("insert into ghost_stats_meta (id,version,updated_ts) values (1,1,?)", (utc_now(),))
            return {"version": 1, "updated_ts": utc_now()}
        return {"version": int(r[0] or 1), "updated_ts": str(r[1] or utc_now())}


def parameter_governor_recommend(summary: dict, autotuner_stats: dict, risk_state: dict, macro: dict) -> dict:
    halted = bool(risk_state.get("halted", False))
    shock = bool(macro.get("macro_shock", False))
    n = int(autotuner_stats.get("ghost_runs", 0) or 0)
    mean = float(autotuner_stats.get("avg_net_pnl_bps", 0.0) or 0.0)
    p5 = float(autotuner_stats.get("p5", 0.0) or 0.0)
    hit = float(autotuner_stats.get("hit_rate", 0.0) or 0.0)
    # simple Sortino proxy (only downside semidev)
    downside = []
    for rr in (autotuner_stats.get("by_reject_reason") or {}).values():
        if float(rr.get("avg_net_pnl_bps", 0.0)) < 0:
            downside.append(abs(float(rr.get("avg_net_pnl_bps", 0.0))))
    downside_dev = (sum(x * x for x in downside) / max(1, len(downside))) ** 0.5
    sortino = mean / max(1e-6, downside_dev)

    suspend_reasons = []
    if shock:
        suspend_reasons.append("SHOCK_ACTIVE")
    if halted:
        suspend_reasons.append("RISK_HALT")
    if p5 < PHASE2_AUTOTUNER_P5_FLOOR:
        suspend_reasons.append("TAIL_GATE_FAIL")

    suspend = bool(suspend_reasons)
    stable = bool((summary.get("risk_pass_total", 0) or 0) > 0 and (summary.get("cost_pass_total", 0) or 0) > 0)
    if not stable:
        suspend_reasons.append("WINDOW_UNSTABLE")
    can_loosen = bool((not suspend) and stable and n >= PHASE2_AUTOTUNER_MIN_N and mean > 0 and sortino >= PHASE2_AUTOTUNER_MIN_SORTINO)

    with db() as c:
        cur = c.cursor()
        cur.execute("select count(*), sum(case when coalesce(valid_for_governor,1)=1 then 1 else 0 end) from ghost_sim_runs where ts>=?", ((datetime.now(timezone.utc)-timedelta(hours=24)).isoformat(),))
        total_rows, valid_rows = cur.fetchone()
        valid_rate = float(valid_rows or 0) / max(1, int(total_rows or 0))
        cur.execute("select coalesce(extreme_class,'NORMAL') cls, count(*) c from ghost_sim_runs where ts>=? group by cls", ((datetime.now(timezone.utc)-timedelta(hours=24)).isoformat(),))
        class_counts = {str(r[0]): int(r[1] or 0) for r in cur.fetchall()}
    meta = _ghost_stats_meta()

    rec = {
        "mode": "recommend_only" if not PHASE2_AUTOTUNER_APPLY else "apply_enabled",
        "namespace": "core",
        "suspended": suspend,
        "suspend_reasons": suspend_reasons,
        "suspend_reason": ",".join(suspend_reasons) if suspend_reasons else None,
        "ghost_stats_version": meta.get("version", 1),
        "metrics": {
            "n": n,
            "mean": round(mean, 2),
            "hit_rate": round(hit, 3),
            "p5": round(p5, 2),
            "sortino_proxy": round(sortino, 3),
            "valid_rate": round(valid_rate, 3),
            "class_counts": class_counts,
        },
        "recommendations": [],
        "bounds": {
            "daily_change_cap": PHASE2_AUTOTUNER_MAX_DAILY_CHANGE,
            "apply": PHASE2_AUTOTUNER_APPLY,
        },
    }
    if can_loosen:
        rec["recommendations"].append({"param": "MIN_ACTIONABLE_SCORE", "direction": "down", "max_change_pct": PHASE2_AUTOTUNER_MAX_DAILY_CHANGE, "reason": "stable_positive_expectancy"})
    else:
        rec["recommendations"].append({"param": "MIN_ACTIONABLE_SCORE", "direction": "hold", "reason": "guardrails_not_met"})

    with db() as c:
        c.execute(
            "insert into parameter_governor_log (ts,mode,namespace,recommendation_json,applied,reason) values (?,?,?,?,?,?)",
            (utc_now(), rec["mode"], rec["namespace"], json.dumps(rec, default=str), 0, rec.get("suspend_reason") or "ok"),
        )
    return rec


def load_bloodbath_lane_state() -> dict:
    with db() as c:
        cur = c.cursor()
        cur.execute("select active,active_reason,active_until,cooldown_until,consecutive_losses,attempts_hour,details_json from bloodbath_lane_state where id=1")
        r = cur.fetchone()
        if not r:
            return {
                "active": False,
                "active_reason": "disabled",
                "active_until": None,
                "cooldown_until": None,
                "consecutive_losses": 0,
                "attempts_hour": 0,
                "details": {},
            }
        return {
            "active": bool(r[0]),
            "active_reason": r[1] or "disabled",
            "active_until": r[2],
            "cooldown_until": r[3],
            "consecutive_losses": int(r[4] or 0),
            "attempts_hour": int(r[5] or 0),
            "details": json.loads(r[6] or "{}"),
        }


def save_bloodbath_lane_state(st: dict) -> None:
    with db() as c:
        c.execute(
            """
            insert into bloodbath_lane_state (id,ts,active,active_reason,active_until,cooldown_until,consecutive_losses,attempts_hour,details_json)
            values (1,?,?,?,?,?,?,?,?)
            on conflict(id) do update set
              ts=excluded.ts, active=excluded.active, active_reason=excluded.active_reason,
              active_until=excluded.active_until, cooldown_until=excluded.cooldown_until,
              consecutive_losses=excluded.consecutive_losses, attempts_hour=excluded.attempts_hour,
              details_json=excluded.details_json
            """,
            (
                utc_now(),
                int(bool(st.get("active", False))),
                st.get("active_reason"),
                st.get("active_until"),
                st.get("cooldown_until"),
                int(st.get("consecutive_losses", 0) or 0),
                int(st.get("attempts_hour", 0) or 0),
                json.dumps(st.get("details", {}), default=str),
            ),
        )


def run_bloodbath_lane(regime: str, macro: dict, risk_state: dict, venue_metrics: dict[str, dict], micro_latest: dict[str, dict], base_to_instrument: dict[str, str | None]) -> dict:
    now = datetime.now(timezone.utc)
    lane_state = load_bloodbath_lane_state()
    out = {
        "active": False,
        "reason": "disabled",
        "activation_reasons": [],
        "attempts_hour": 0,
        "attempts_by_symbol": {},
        "majors_evaluated": 0,
        "reject_breakdown": {"pressure_confidence_fail": 0, "spread_stability_fail": 0, "utilization_fail": 0, "depth_curve_fail": 0},
        "candidates_top": [],
        "ghost_1h": {"trades": 0, "net_pnl_bps": 0.0, "winrate": 0.0},
        "ghost_24h": {"trades": 0, "net_pnl_bps": 0.0, "winrate": 0.0},
    }
    if not BLOODBATH_ENABLED:
        out["reason"] = "BLOODBATH_DISABLED"
        return out
    if risk_state.get("halted"):
        out["reason"] = "GLOBAL_RISK_HALTED"
        return out
    if bool((risk_state.get("details") or {}).get("soft_throttle", False)):
        out["reason"] = "SOFT_THROTTLE"
        return out

    activation_reasons = []
    if bool(macro.get("macro_shock", False)):
        activation_reasons.append("MACRO_SHOCK")
    if regime == "VOLATILE":
        activation_reasons.append("VOLATILE")
    is_shock = bool(activation_reasons)
    active_until = lane_state.get("active_until")
    active_hysteresis = False
    if active_until:
        try:
            active_hysteresis = datetime.fromisoformat(active_until) > now
        except Exception:
            active_hysteresis = False

    if not is_shock and not active_hysteresis:
        out["reason"] = "NO_SHOCK"
        save_bloodbath_lane_state({**lane_state, "active": False, "active_reason": out["reason"], "attempts_hour": 0})
        return out

    cooldown_until = lane_state.get("cooldown_until")
    if cooldown_until:
        try:
            if datetime.fromisoformat(cooldown_until) > now:
                out["reason"] = "LANE_COOLDOWN"
                out["active"] = False
                return out
        except Exception:
            pass

    out["active"] = True
    out["activation_reasons"] = activation_reasons if activation_reasons else (["HYSTERESIS"] if active_hysteresis else [])
    out["reason"] = "+".join(out["activation_reasons"]) if out["activation_reasons"] else "HYSTERESIS"

    max_attempts_hour = BLOODBATH_MAX_ATTEMPTS_PER_HOUR
    max_attempts_symbol = BLOODBATH_MAX_ATTEMPTS_PER_SYMBOL_HOUR
    if bool(macro.get("macro_shock", False)):
        max_attempts_hour = max(1, int(max_attempts_hour * 0.7))
        max_attempts_symbol = max(1, int(max_attempts_symbol * 0.7))

    since_1h = (now - timedelta(hours=1)).isoformat()
    since_24h = (now - timedelta(hours=24)).isoformat()
    attempts_by_symbol: dict[str, int] = {}
    total_attempts = 0
    with db() as c:
        cur = c.cursor()
        cur.execute("select symbol,count(*) from bloodbath_ghost_runs where ts>=? group by symbol", (since_1h,))
        for sym, cnt in cur.fetchall():
            attempts_by_symbol[str(sym)] = int(cnt or 0)
            total_attempts += int(cnt or 0)

        cur.execute("select count(*), coalesce(sum(net_pnl_bps),0), coalesce(avg(case when net_pnl_bps>0 then 1.0 else 0.0 end),0) from bloodbath_ghost_runs where ts>=?", (since_1h,))
        t1, n1, w1 = cur.fetchone()
        cur.execute("select count(*), coalesce(sum(net_pnl_bps),0), coalesce(avg(case when net_pnl_bps>0 then 1.0 else 0.0 end),0) from bloodbath_ghost_runs where ts>=?", (since_24h,))
        t24, n24, w24 = cur.fetchone()
        cur.execute("select count(*) from bloodbath_ghost_runs where ts>=? and execution_style='FILLED_MAKER'", (since_1h,))
        filled_1h = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select count(*) from bloodbath_ghost_runs where ts>=? and execution_style='EXPIRED_UNFILLED'", (since_1h,))
        expired_1h = int((cur.fetchone() or [0])[0] or 0)
        cur.execute("select coalesce(avg(net_pnl_bps),0) from bloodbath_ghost_runs where ts>=? and execution_style='FILLED_MAKER'", (since_1h,))
        pnl_cond_fill_1h = float((cur.fetchone() or [0])[0] or 0.0)
    out["attempts_hour"] = total_attempts
    out["attempts_by_symbol"] = attempts_by_symbol
    out["ghost_1h"] = {"trades": int(t1 or 0), "net_pnl_bps": round(float(n1 or 0.0), 2), "winrate": round(float(w1 or 0.0), 3), "fill_rate": round((filled_1h / max(1, int(t1 or 0))), 3), "expired_rate": round((expired_1h / max(1, int(t1 or 0))), 3), "pnl_conditional_on_fill": round(pnl_cond_fill_1h, 2)}
    out["ghost_24h"] = {"trades": int(t24 or 0), "net_pnl_bps": round(float(n24 or 0.0), 2), "winrate": round(float(w24 or 0.0), 3)}

    # daily lane DD guard
    if out["ghost_24h"]["net_pnl_bps"] <= -abs(BLOODBATH_DD_LIMIT_BPS_DAY):
        out["active"] = False
        out["reason"] = "LANE_DD_LIMIT"
        save_bloodbath_lane_state({**lane_state, "active": False, "active_reason": out["reason"], "cooldown_until": (now + timedelta(seconds=BLOODBATH_LANE_COOLDOWN_SEC)).isoformat()})
        return out

    candidates = []
    for inst in BLOODBATH_SYMBOL_ALLOWLIST:
        base = inst.split("_")[0]
        micro = micro_latest.get(base, {})
        venue = venue_metrics.get(base, {})
        out["majors_evaluated"] += 1
        if not micro:
            out["reject_breakdown"]["depth_curve_fail"] += 1
            continue
        spread = float(micro.get("spread_bps_p95_300", venue.get("spread_bps", 9999.0)) or 9999.0)
        spread_stability = float(micro.get("spread_stability", 0.0) or 0.0)
        pressure_conf = float(micro.get("pressure_confidence", 0.0) or 0.0)
        depth20 = float(micro.get("depth_usd_20bps", 0.0) or 0.0)
        utilization = min(1.0, depth20 / max(1.0, TARGET_NOTIONAL_USDT))
        if pressure_conf < BLOODBATH_PRESSURE_CONF_MIN:
            out["reject_breakdown"]["pressure_confidence_fail"] += 1
            continue
        if spread_stability < 0.45 or spread > BLOODBATH_SPREAD_MAX_BPS:
            out["reject_breakdown"]["spread_stability_fail"] += 1
            continue
        if utilization < BLOODBATH_MIN_UTILIZATION:
            out["reject_breakdown"]["utilization_fail"] += 1
            continue
        candidates.append({
            "symbol": base,
            "instrument_symbol": base_to_instrument.get(base) or inst,
            "pressure_confidence": round(pressure_conf, 3),
            "spread_stability": round(spread_stability, 3),
            "spread_bps": round(spread, 2),
            "utilization": round(utilization, 3),
        })

    candidates = sorted(candidates, key=lambda x: (x["pressure_confidence"], x["utilization"]), reverse=True)
    out["candidates_top"] = candidates[:3]

    attempts_made = 0
    consec_losses = int(lane_state.get("consecutive_losses", 0) or 0)
    for cand in candidates:
        sym = cand["symbol"]
        inst = cand["instrument_symbol"]
        attempt_id = f"BB-{sym}-{int(time.time()*1000)}-{random.randint(100,999)}"
        log_churn_event(attempt_id, sym, "ATTEMPT", "ATTEMPT_STARTED", True, {"instrument": inst}, lane="BLOODBATH_LANE")
        if total_attempts >= max_attempts_hour:
            break
        if attempts_by_symbol.get(sym, 0) >= max_attempts_symbol:
            log_churn_event(attempt_id, sym, "ATTEMPT", "SYMBOL_ATTEMPT_CAP", False, {"cap": max_attempts_symbol}, lane="BLOODBATH_LANE")
            continue
        if in_penalty_box(sym):
            log_churn_event(attempt_id, sym, "ATTEMPT", "PENALTY_BOX_BLOCK", False, {}, lane="BLOODBATH_LANE")
            continue
        if consec_losses >= BLOODBATH_LANE_CONSEC_LOSS_LIMIT:
            out["active"] = False
            out["reason"] = "LANE_CONSEC_LOSS_COOLDOWN"
            break
        # retry-loop guard: re-check global risk state before each last-mile attempt
        rs_now = load_risk_state()
        if rs_now.get("halted"):
            log_churn_event(attempt_id, sym, "ATTEMPT", "RISK_FLIP_ABORT", False, {"halt_reason": rs_now.get("halt_reason")}, lane="BLOODBATH_LANE")
            out["active"] = False
            out["reason"] = "GLOBAL_RISK_HALTED"
            break
        try:
            cd = _get_json_retry(
                f"https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name={inst}&timeframe=5m",
                risk_state_fn=load_risk_state,
            )
            rows = cd.get("result", {}).get("data", []) if isinstance(cd, dict) else []
            if len(rows) < 3:
                log_churn_event(attempt_id, sym, "ATTEMPT", "STALE_BOOK_ABORT", False, {"rows": len(rows)}, lane="BLOODBATH_LANE")
                continue
            entry = float(rows[-2].get("o") or 0.0)
            exitp = float(rows[-1].get("c") or 0.0)
            if entry <= 0 or exitp <= 0:
                log_churn_event(attempt_id, sym, "ATTEMPT", "PRICE_CHASE_ABORT", False, {"entry": entry, "exit": exitp}, lane="BLOODBATH_LANE")
                continue
            m = micro_latest.get(sym, {})
            spread = float(m.get("spread_bps_p95_300", 20.0) or 20.0)
            obi_abs = abs(float(m.get("obi_20bps", 0.0) or 0.0))
            p_fill = _maker_fill_probability(spread, float(venue_metrics.get(sym, {}).get("chg_24h_abs", 0.0) or 0.0), obi_abs, float(m.get("spread_stability", 0.0) or 0.0), float(m.get("trade_rate_proxy", 0.0) or 0.0), BLOODBATH_MAKER_TIME_BUDGET_SEC)
            if float(m.get("spread_stability", 0.0) or 0.0) < 0.5:
                p_fill *= 0.5
            maker_cost = max(1.5, spread * 0.25)
            # Bloodbath lane default: no taker fallback, non-fill expires.
            expired_unfilled = p_fill < 0.45
            fallback_cost = 0.0
            exp_cost = (p_fill * maker_cost)
            pnl_bps = ((exitp - entry) / entry) * 10000.0
            net = (p_fill * pnl_bps) - exp_cost
            if expired_unfilled:
                net = 0.0
            exec_style = "EXPIRED_UNFILLED" if expired_unfilled else "FILLED_MAKER"
            with db() as c:
                c.execute(
                    """
                    insert into bloodbath_ghost_runs (ts,symbol,lane,entry_ts,exit_ts,entry_price,exit_price,net_pnl_bps,execution_style,utilization,regime,macro_shock,p_fill_maker,maker_time_budget_sec,costs_bps,details_json)
                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (utc_now(), sym, "BLOODBATH_LANE", utc_now(), utc_now(), entry, exitp, net, exec_style, float(cand["utilization"]), regime, int(bool(macro.get("macro_shock"))), p_fill, BLOODBATH_MAKER_TIME_BUDGET_SEC, exp_cost, json.dumps(cand, default=str)),
                )
            attempts_by_symbol[sym] = attempts_by_symbol.get(sym, 0) + 1
            total_attempts += 1
            attempts_made += 1
            if expired_unfilled:
                log_churn_event(attempt_id, sym, "ATTEMPT", "EXPIRED_UNFILLED", False, {"p_fill": p_fill}, lane="BLOODBATH_LANE")
            else:
                log_churn_event(attempt_id, sym, "ATTEMPT", "FILLED_MAKER", False, {"p_fill": p_fill}, lane="BLOODBATH_LANE")
                execution_id = f"GF-{sym}-{attempt_id}"
                log_churn_event(attempt_id, sym, "EXECUTION", "FILLED_MAKER", True, {"net_pnl_bps": net}, lane="BLOODBATH_LANE", execution_id=execution_id)
                with db() as _lc:
                    _lc.execute(
                        "insert or ignore into shadow_portfolio_ledger (ts,symbol,lane,fill_id,execution_style_intent,realized_pnl_bps,details_json) values (?,?,?,?,?,?,?)",
                        (utc_now(), sym, "BLOODBATH_LANE", execution_id, "MAKER_POST_ONLY", float(net), json.dumps({"attempt_id": attempt_id}, default=str)),
                    )
            if net < 0 and not expired_unfilled:
                consec_losses += 1
            elif not expired_unfilled:
                consec_losses = 0
        except Exception as e:
            es = str(e)
            reason = "VENUE_ERROR"
            if "RISK_FLIP_ABORT" in es:
                reason = "RISK_FLIP_ABORT"
            elif "429" in es:
                reason = "RATE_LIMIT"
            elif "timeout" in es.lower():
                reason = "API_TIMEOUT"
            log_churn_event(attempt_id, sym, "ATTEMPT", reason, False, {"error": es[:160]}, lane="BLOODBATH_LANE")
            continue

    next_state = {
        "active": bool(out["active"]),
        "active_reason": out["reason"],
        "active_until": (now + timedelta(seconds=BLOODBATH_HYSTERESIS_SEC)).isoformat() if is_shock else lane_state.get("active_until"),
        "cooldown_until": (now + timedelta(seconds=BLOODBATH_LANE_COOLDOWN_SEC)).isoformat() if consec_losses >= BLOODBATH_LANE_CONSEC_LOSS_LIMIT else lane_state.get("cooldown_until"),
        "consecutive_losses": consec_losses,
        "attempts_hour": total_attempts,
        "details": {
            "attempts_made_this_cycle": attempts_made,
            "attempts_by_symbol": attempts_by_symbol,
            "lane_risk_fraction": BLOODBATH_LANE_RISK_FRACTION,
            "max_attempts_hour": max_attempts_hour,
            "max_attempts_symbol": max_attempts_symbol,
        },
    }
    save_bloodbath_lane_state(next_state)
    out["attempts_hour"] = total_attempts
    return out


def run_ghost_simulator(macro_shock: bool = False) -> dict:
    since = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    rows_done = 0
    sampled = 0
    by_reason: Counter = Counter()
    with db() as c:
        cur = c.cursor()
        cur.execute(
            """
            select instrument_symbol, ts, deny_reason, alpha_score, metrics_json
            from universe_state
            where ts>=? and status in ('WATCH','ELIGIBLE')
              and deny_reason in ('alpha-score-below-threshold','net-edge-too-low','RISK_CONFIDENCE_LOW')
              and instrument_symbol is not null
            order by id desc
            limit 500
            """,
            (since,),
        )
        raw = cur.fetchall()
        # stratified sample: cap repeats per symbol and reject reason
        per_symbol: Counter = Counter()
        candidates = []
        for r in raw:
            sym = str(r[0] or "")
            reason = str(r[2] or "unknown")
            key = f"{reason}:{sym}"
            if per_symbol[key] >= 2:
                continue
            per_symbol[key] += 1
            candidates.append(r)
            sampled += 1
            if sampled >= 140:
                break

        for r in candidates:
            inst = r[0]
            reason = str(r[2] or "unknown")
            try:
                d = get_json(f"https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name={inst}&timeframe=5m")
                candles = d.get("result", {}).get("data", []) if isinstance(d, dict) else []
                if len(candles) < 4:
                    continue
                entry = float(candles[-2].get("o") or 0.0)
                exitp = float(candles[-1].get("c") or 0.0)
                if entry <= 0 or exitp <= 0:
                    continue
                metrics = {}
                try:
                    metrics = json.loads(r[4] or "{}")
                except Exception:
                    metrics = {}
                est_cost = float(metrics.get("est_cost_bps", 18.0) or 18.0)
                exec_style = str(metrics.get("execution_style", "taker") or "taker")
                pnl_bps = ((exitp - entry) / entry) * 10000.0
                costs_bps = est_cost
                net = pnl_bps - costs_bps
                invalid_reason = None
                extreme_class = "NORMAL"
                valid_for_governor = 1
                if any(v is None for v in [entry, exitp]) or entry <= 0 or exitp <= 0:
                    invalid_reason = "DATA_MISSING"
                    extreme_class = "DATA_MISSING"
                    valid_for_governor = 0
                elif net <= -9000:
                    extreme_class = "UNVERIFIED_EXTREME"
                    valid_for_governor = 0
                elif net <= -1500:
                    extreme_class = "REAL_CRASH"
                elif net <= -700:
                    extreme_class = "LIQUIDITY_DEATH"
                cur.execute(
                    """
                    insert into ghost_sim_runs (ts,instrument_symbol,entry_ts,exit_ts,horizon,entry_price,exit_price,pnl_bps,costs_bps,net_pnl_bps,reject_reason,features_ref,macro_shock,estimated_cost_bps,execution_style,invalid_reason,extreme_class,valid_for_governor)
                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (utc_now(), inst, r[1], utc_now(), "5m", entry, exitp, pnl_bps, costs_bps, net, reason, None, int(bool(macro_shock)), est_cost, exec_style, invalid_reason, extreme_class, valid_for_governor),
                )
                rows_done += 1
                by_reason[reason] += 1
            except Exception:
                continue
    return {"rows": rows_done, "sampled": sampled, "by_reason": dict(by_reason), "macro_shock": bool(macro_shock)}




def _drain_execution_results_and_apply() -> dict:
    drained = {"attempts": 0, "fills": 0, "expired": 0, "latencies": [], "delay_cycles": []}
    now = time.time()
    # expire old open positions after 24h for slot realism
    for sym, ots in list(SHADOW_STATE["open_positions"].items()):
        if now - float(ots) > 24 * 3600:
            SHADOW_STATE["open_positions"].pop(sym, None)

    while True:
        try:
            ev = EXECUTION_RESULTS_QUEUE.get_nowait()
        except queue.Empty:
            break
        et = ev.get("type")
        aid = ev.get("attempt_id", "")
        sym = ev.get("symbol", "")
        lane = ev.get("lane", "CORE")
        if et == "ATTEMPT_STARTED":
            drained["attempts"] += 1
            SHADOW_STATE["attempts"] += 1
            log_churn_event(aid, sym, "ATTEMPT", "ATTEMPT_STARTED", True, {"attempt_cycle_id": ev.get("attempt_cycle_id")}, lane=lane)
        elif et == "FILLED_MAKER":
            drained["fills"] += 1
            SHADOW_STATE["fills"] += 1
            latency = max(0.0, float(ev.get("fill_ts", now)) - float(ev.get("attempt_ts", now)))
            delay_cycles = int(ev.get("fill_cycle_id", 0) or 0) - int(ev.get("attempt_cycle_id", 0) or 0)
            drained["latencies"].append(latency)
            drained["delay_cycles"].append(delay_cycles)
            SHADOW_STATE["latencies_sec"].append(latency)
            SHADOW_STATE["min_delay_cycles"] = delay_cycles if SHADOW_STATE.get("min_delay_cycles") is None else min(SHADOW_STATE["min_delay_cycles"], delay_cycles)
            SHADOW_STATE["open_positions"][sym] = float(ev.get("fill_ts", now))
            execution_id = str(ev.get("execution_id") or f"E-{aid}")
            log_churn_event(aid, sym, "EXECUTION", "FILLED_MAKER", True, {"delay_cycles": delay_cycles}, lane=lane, execution_id=execution_id)
            with db() as c:
                c.execute("insert or ignore into shadow_portfolio_ledger (ts,symbol,lane,fill_id,execution_style_intent,realized_pnl_bps,details_json) values (?,?,?,?,?,?,?)",
                          (utc_now(), sym, lane, execution_id, "MAKER_POST_ONLY", 0.0, json.dumps({"attempt_id": aid}, default=str)))
        elif et in {"EXPIRED_UNFILLED", "ATTEMPT_FAILED"}:
            drained["expired"] += 1
            SHADOW_STATE["expired"] += 1
            reason = ev.get("reason") or et
            log_churn_event(aid, sym, "ATTEMPT", str(reason), False, {"attempt_cycle_id": ev.get("attempt_cycle_id"), "fill_cycle_id": ev.get("fill_cycle_id")}, lane=lane)
    return drained


def _shadow_open_positions_count() -> int:
    return len(SHADOW_STATE.get("open_positions", {}))

def run_cycle() -> dict:
    global CYCLE_SEQ
    CYCLE_SEQ += 1
    cycle_id = CYCLE_SEQ
    drained_exec = _drain_execution_results_and_apply()

    write_status({"state": "running", "job": "ingest_cryptocom"})
    crypto_bases, base_to_instrument = ingest_cryptocom()

    write_status({"state": "running", "job": "ingest_cryptocom_movers"})
    venue_movers, venue_metrics = ingest_cryptocom_movers()
    new_listings = detect_new_listings(crypto_bases)

    write_status({"state": "running", "job": "ingest_coingecko"})
    cg = ingest_coingecko()

    write_status({"state": "running", "job": "ingest_cmc"})
    cmc = ingest_cmc()

    write_status({"state": "running", "job": "ingest_dex"})
    dex_latest, dex_boosted, dex_metrics = ingest_dex()

    write_status({"state": "running", "job": "ingest_news"})
    news_hits = ingest_news()

    write_status({"state": "running", "job": "ingest_defillama"})
    llama_syms, llama_points = ingest_defillama()

    write_status({"state": "running", "job": "ingest_dune"})
    dune_syms, dune_points = ingest_dune()

    write_status({"state": "running", "job": "load_micro_features"})
    micro_latest = load_micro_latest()
    # evaluate only pending attempts from previous cycles with fresh snapshot
    SHADOW_WORKER.on_market_snapshot(cycle_id)

    sources_present = source_availability(cg, cmc, dex_latest, news_hits, venue_movers, llama_syms, dune_syms)
    discovered = sorted(set(cg).union(cmc).union(dex_latest).union(dex_boosted).union(venue_movers).union(new_listings).union(llama_syms).union(dune_syms))

    refresh_listing_watch(dex_boosted, dex_latest, llama_syms, crypto_bases)
    front_run_symbols = emit_listing_front_run(new_listings)

    mapped_count = 0
    eligible_count = 0
    alpha_pass_count = 0
    risk_pass_count = 0
    cost_pass_count = 0
    proposed_count = 0
    shadow_attempts = int(drained_exec.get("attempts", 0) or 0)
    shadow_fills = int(drained_exec.get("fills", 0) or 0)
    shadow_expired = int(drained_exec.get("expired", 0) or 0)
    reject_counter: Counter = Counter()
    rejects_tradable: Counter = Counter()
    rejects_external: Counter = Counter()
    risk_code_counter: Counter = Counter()

    watchlist: list[dict] = []
    eligible_list: list[dict] = []
    actionable_list: list[dict] = []

    regime = classify_regime(venue_metrics, micro_latest)
    obi_threshold = obi_threshold_for_regime(regime)
    pressure_count = 0
    macro = detect_macro_shock(regime, news_hits, venue_metrics)
    risk_state = compute_shadow_risk_state(bool(macro.get("macro_shock", False)), regime)
    bloodbath = run_bloodbath_lane(regime, macro, risk_state, venue_metrics, micro_latest, base_to_instrument)

    max_positions_normal = int(os.getenv("PHASE2_MAX_CONCURRENT_NORMAL", "10"))
    max_positions_volatile = int(os.getenv("PHASE2_MAX_CONCURRENT_VOLATILE", "3"))
    max_positions = max_positions_volatile if regime == "VOLATILE" else max_positions_normal
    open_positions = _shadow_open_positions_count()
    remaining_slots = max(0, max_positions - open_positions)
    capacity_admitted = 0
    capacity_rejected = 0

    alpha_scored_count = 0
    cost_evaluated_count = 0
    risk_evaluated_count = 0
    invalid_assets_filtered = 0
    alpha_floor_override_count = 0
    cost_fail_breakdown: Counter = Counter()
    pre_cost_skip: Counter = Counter()
    notional_util_samples: list[float] = []

    venue_vols = [float(v.get("volume_24h_quote", 0.0)) for v in venue_metrics.values()]
    venue_spreads = [float(v.get("spread_bps", 9999.0)) for v in venue_metrics.values()]
    dynamic_min_volume = max(100000.0, _percentile(venue_vols, 0.25, 100000.0))
    dynamic_max_spread_bps = min(50.0, _percentile(venue_spreads, 0.75, 50.0))

    # Top-K alpha gating fallback when social is unavailable.
    social_missing = (not sources_present.get(Source.X)) and (not sources_present.get(Source.REDDIT))
    topk_alpha_syms: set[str] = set()
    TOPK_ALPHA = int(os.getenv("PHASE2_ALPHA_TOPK", "20"))
    if social_missing:
        pre = []
        for _sym in discovered:
            s = str(_sym).upper().strip()
            if s in {"USDT", "USDC", "USD", "EUR"} or not s.replace("$", "").replace("-", "").replace("_", "").isalnum():
                continue
            if s not in crypto_bases:
                continue
            _venue = venue_metrics.get(s, {})
            _micro = micro_latest.get(s, {})
            _vol = float(_venue.get("volume_24h_quote", dex_metrics.get(s, {}).get("volume24h", 0.0)))
            _spread = float(_micro.get("spread_bps_p95_300", _venue.get("spread_bps", 9999.0)))
            _liq = float(_micro.get("liquidity_score", (_vol / max(1.0, TARGET_NOTIONAL_USDT))))
            _elig = 0.0
            if _vol >= dynamic_min_volume:
                _elig += 30
            if _spread <= dynamic_max_spread_bps:
                _elig += 20
            if _liq >= 10:
                _elig += 20
            elif _liq >= 5:
                _elig += 10
            _elig += 30
            if _elig < MIN_ELIGIBLE_SCORE:
                continue
            _alpha, _, _ = weighted_alpha(s, cg, cmc, dex_latest, dex_boosted, venue_movers, sources_present, venue_metrics)
            pre.append((s, _alpha))
        pre.sort(key=lambda x: x[1], reverse=True)
        topk_alpha_syms = {x[0] for x in pre[:TOPK_ALPHA]}

    for sym in discovered:
        sym_norm = str(sym).upper().strip()
        if sym_norm in {"USDT", "USDC", "USD", "EUR"} or not sym_norm.replace("$", "").replace("-", "").replace("_", "").isalnum():
            invalid_assets_filtered += 1
            continue
        sym = sym_norm

        mapped = sym in crypto_bases
        instrument_symbol = base_to_instrument.get(sym)
        if mapped:
            mapped_count += 1

        venue = venue_metrics.get(sym, {})
        micro = micro_latest.get(sym, {})
        volume_24h = float(venue.get("volume_24h_quote", dex_metrics.get(sym, {}).get("volume24h", 0.0)))
        spread_bps = float(micro.get("spread_bps_p95_300", venue.get("spread_bps", 9999.0)))
        depth_5bps = float(micro.get("depth_usd_5bps", 0.0))
        depth_20bps = float(micro.get("depth_usd_20bps", 0.0))
        depth_50bps = float(micro.get("depth_usd_50bps", depth_20bps))
        liquidity_cover = float(micro.get("liquidity_score", (volume_24h / max(1.0, TARGET_NOTIONAL_USDT))))

        eligibility_score = 0.0
        deny_stage = None
        deny_reason = None

        if mapped:
            eligibility_score += 30
        if volume_24h >= dynamic_min_volume:
            eligibility_score += 30
        if spread_bps <= dynamic_max_spread_bps:
            eligibility_score += 20
        if liquidity_cover >= 10:
            eligibility_score += 20
        elif liquidity_cover >= 5:
            eligibility_score += 10

        risk_flags: list[str] = list(micro.get("risk_flags", []))

        alpha_score, confidence, alpha_dbg = weighted_alpha(
            sym, cg, cmc, dex_latest, dex_boosted, venue_movers, sources_present, venue_metrics
        )
        alpha_scored_count += 1
        obi20 = abs(float(micro.get("obi_20bps", 0.0)))
        pressure_flag = bool(micro.get("pressure_flag", False))
        pressure_confidence = float(micro.get("pressure_confidence", 0.0))
        if pressure_flag:
            pressure_count += 1
            alpha_score += (4.0 + 6.0 * pressure_confidence)
            risk_flags.append("pressure_flag")
        if regime == "VOLATILE" and obi20 >= 0.85:
            alpha_score += 3.0
            risk_flags.append("obi_extreme")
        alpha_score = min(100.0, alpha_score)

        status = "WATCH"
        stage_reached = "discovery"
        is_new_listing = sym in new_listings
        if is_new_listing:
            risk_flags.append("new_listing")
        if spread_bps > dynamic_max_spread_bps:
            risk_flags.append("wide_spread")
        if liquidity_cover < 5:
            risk_flags.append("thin_liquidity")

        expected_edge_bps = alpha_score * 0.45

        # Cost model upgrades:
        # - curve-based slippage using depth bands (5/10/20/50 bps)
        # - maker-first with probabilistic fill model
        # - decomposed safety margin with anti-double-counting guard
        spread_toxic = spread_bps > 80.0
        uncertainty = max(0.0, min(1.0, 1.0 - confidence))
        safety_margin = _safety_margin_components(regime, is_new_listing, liquidity_cover, float(venue.get("chg_24h_abs", 0.0)), uncertainty)
        safety_margin_bps = float(safety_margin["total"])

        depth_curve = {
            5.0: max(1.0, depth_5bps),
            10.0: max(1.0, float(micro.get("depth_usd_10bps", depth_20bps * 0.65))),
            20.0: max(1.0, depth_20bps),
            50.0: max(1.0, depth_50bps),
        }
        k_slip = _k_slip(regime, liquidity_cover, float(micro.get("orderbook_slope", 1.0)))
        impact_multiplier = max(0.75, min(1.6, k_slip / 0.08))
        slippage_cap_bps = 10.0 if regime == "VOLATILE" else (20.0 if regime == "QUIET" else 15.0)

        b_star_full = _interpolated_impact_bps(TARGET_NOTIONAL_USDT, depth_curve)
        full_slippage_bps = b_star_full * impact_multiplier
        max_notional_by_slip = max(1.0, TARGET_NOTIONAL_USDT * (slippage_cap_bps / max(1.0, full_slippage_bps)))
        target_notional_adj = min(TARGET_NOTIONAL_USDT, max_notional_by_slip)
        size_reduced = target_notional_adj < TARGET_NOTIONAL_USDT

        b_star_adj = _interpolated_impact_bps(target_notional_adj, depth_curve)
        slippage_bps = b_star_adj * impact_multiplier

        half_spread_bps = max(0.0, spread_bps / 2.0)
        vol_penalty_bps = min(15.0, max(0.0, float(venue.get("chg_24h_abs", 0.0)) * 100.0 * 0.3))

        est_cost_bps_full_size = half_spread_bps + full_slippage_bps + vol_penalty_bps + (half_spread_bps * max(0.0, safety_margin_bps - 1.0) / 100.0)
        taker_cost_bps = half_spread_bps + slippage_bps + vol_penalty_bps
        maker_cost_bps = (half_spread_bps * 0.2) + (slippage_bps * 0.6) + (vol_penalty_bps * 0.8)

        maker_time_budget_sec = 45 if regime == "NORMAL" else 25
        p_fill_maker = _maker_fill_probability(
            spread_bps=spread_bps,
            vol_abs=float(venue.get("chg_24h_abs", 0.0)),
            obi_abs=obi20,
            spread_stability=float(micro.get("spread_stability", 0.0)),
            trade_rate_proxy=float(micro.get("trade_rate_proxy", 0.0)),
            time_budget_sec=maker_time_budget_sec,
        )
        maker_fallback_cost = taker_cost_bps
        maker_expected_cost_bps = (p_fill_maker * maker_cost_bps) + ((1.0 - p_fill_maker) * maker_fallback_cost)

        chosen_style = "taker"
        est_cost_bps = taker_cost_bps
        if maker_expected_cost_bps < taker_cost_bps:
            est_cost_bps = maker_expected_cost_bps
            chosen_style = "maker-first"

        if spread_toxic:
            est_cost_bps = 9999.0
            cost_edge_bps = -9999.0
        else:
            cost_evaluated_count += 1
            raw_cost = half_spread_bps + slippage_bps
            vol_adjusted_cost = raw_cost + vol_penalty_bps
            margin_addon = raw_cost * (safety_margin_bps / 100.0)
            est_cost_bps = est_cost_bps + margin_addon
            cost_edge_bps = expected_edge_bps - est_cost_bps

        if sym in front_run_symbols and mapped:
            status = "ACTIONABLE"
            stage_reached = "actionable"
            alpha_score = 100.0
            confidence = max(confidence, 0.9)
            risk_flags.extend(["listing_front_run", "dex_hype", "fundamentals_spike"])
            proposed_count += 1
            risk_pass_count += 1
            cost_pass_count += 1
            alpha_pass_count += 1
            put_signal(sym, Source.SYSTEM, "listing_front_run_actionable", {"instrument_symbol": instrument_symbol, "alpha_score": alpha_score}, conviction="moonshot")
        elif not mapped:
            deny_stage = "mapping"
            stage_reached = "mapping"
            deny_reason = "not-tradable-external"
            reject_counter[deny_reason] += 1
            rejects_external["not_tradable"] += 1
            pre_cost_skip["mapping_failure"] += 1
            log_reject(sym, deny_stage, deny_reason, {"mapped": mapped, "instrument_symbol": instrument_symbol})
            status = "WATCH"
        elif eligibility_score < MIN_ELIGIBLE_SCORE:
            deny_stage = "eligibility"
            stage_reached = "eligibility"
            deny_reason = "eligibility-score-too-low"
            reject_counter[deny_reason] += 1
            rejects_tradable["eligibility"] += 1
            pre_cost_skip["eligibility_fail"] += 1
            log_reject(sym, deny_stage, deny_reason, {
                "eligibility_score": eligibility_score,
                "volume_24h": volume_24h,
                "spread_bps": spread_bps,
                "dynamic_min_volume": dynamic_min_volume,
                "dynamic_max_spread_bps": dynamic_max_spread_bps,
                "liquidity_cover": liquidity_cover,
            })
            status = "WATCH"
        else:
            eligible_count += 1
            stage_reached = "eligibility"
            status = "ELIGIBLE"

            # Early/static risk checks (cheap filters)
            max_symbol_concentration = int(os.getenv("PHASE2_MAX_SYMBOL_CONCENTRATION", "1"))
            current_same_symbol = sum(1 for r in actionable_list if r.get("symbol") == sym)
            early_risk_block = None
            if open_positions >= max_positions:
                early_risk_block = "RISK_MAX_POSITIONS"
                log_reject(sym, "risk", early_risk_block, {"max_positions": max_positions, "open_positions": open_positions, "regime": regime, "check_phase": "early"})
            elif current_same_symbol >= max_symbol_concentration:
                early_risk_block = "RISK_CONCENTRATION"
                log_reject(sym, "risk", early_risk_block, {"symbol": sym, "max_symbol_concentration": max_symbol_concentration, "check_phase": "early"})

            alpha_floor_effective = MIN_ACTIONABLE_SCORE
            if (not sources_present.get(Source.X) and not sources_present.get(Source.REDDIT) and sym in venue_movers and obi20 >= 0.5 and liquidity_cover >= 20):
                alpha_floor_effective = 50.0
                alpha_floor_override_count += 1
            alpha_gate_pass = (alpha_score >= alpha_floor_effective)
            if social_missing and sym in topk_alpha_syms:
                alpha_gate_pass = True
            if early_risk_block:
                deny_stage = "risk"
                stage_reached = "risk"
                deny_reason = early_risk_block
                reject_counter[deny_reason] += 1
                rejects_tradable["risk"] += 1
                risk_code_counter[deny_reason] += 1
            elif not alpha_gate_pass:
                deny_stage = "alpha"
                stage_reached = "alpha"
                deny_reason = "alpha-score-below-threshold"
                reject_counter[deny_reason] += 1
                rejects_tradable["alpha"] += 1
                pre_cost_skip["alpha_fail"] += 1
                log_reject(sym, deny_stage, deny_reason, {
                    "alpha_score": alpha_score,
                    "confidence": confidence,
                    "alpha_floor_effective": alpha_floor_effective,
                    "topk_override_active": social_missing,
                    "in_topk": sym in topk_alpha_syms,
                })
            else:
                alpha_pass_count += 1
                stage_reached = "alpha"
                if spread_toxic:
                    deny_stage = "cost"
                    stage_reached = "cost"
                    deny_reason = "watch-spread-toxic"
                    reject_counter[deny_reason] += 1
                    rejects_tradable["cost"] += 1
                    pre_cost_skip["spread_toxic_bypass"] += 1
                    cost_fail_breakdown["spread_cost_dominant"] += 1
                    status = "WATCH"
                    log_reject(sym, deny_stage, deny_reason, {
                        "spread_bps": spread_bps,
                        "spread_limit_bps": 80.0,
                        "bypass_cost_eval": True,
                        "est_cost_bps_full_size": est_cost_bps_full_size,
                        "est_cost_bps_smallcap_lane": taker_cost_bps,
                        "execution_style": chosen_style,
                        "k_slip_used": k_slip,
                        "b_interpolated_full": b_star_full,
                        "p_fill_maker": p_fill_maker,
                    })
                elif cost_edge_bps < 0:
                    deny_stage = "cost"
                    stage_reached = "cost"
                    deny_reason = "net-edge-too-low"
                    reject_counter[deny_reason] += 1
                    rejects_tradable["cost"] += 1
                    dominant = max(
                        [("spread_cost_dominant", half_spread_bps), ("slippage_dominant", slippage_bps), ("vol_penalty_dominant", vol_penalty_bps), ("safety_margin_dominant", safety_margin_bps)],
                        key=lambda x: x[1],
                    )[0]
                    cost_fail_breakdown[dominant] += 1
                    log_reject(sym, deny_stage, deny_reason, {
                        "expected_edge_bps": expected_edge_bps,
                        "est_cost_bps": est_cost_bps,
                        "safety_margin_bps": safety_margin_bps,
                        "cost_edge_bps": cost_edge_bps,
                        "target_notional_adj": target_notional_adj,
                        "size_reduced": size_reduced,
                        "est_cost_bps_full_size": est_cost_bps_full_size,
                        "est_cost_bps_smallcap_lane": taker_cost_bps,
                        "est_cost_bps_maker": maker_cost_bps,
                        "est_cost_bps_maker_expected": maker_expected_cost_bps,
                        "execution_style": chosen_style,
                        "k_slip_used": k_slip,
                        "b_interpolated_full": b_star_full,
                        "b_interpolated_adj": b_star_adj,
                        "impact_multiplier": impact_multiplier,
                        "p_fill_maker": p_fill_maker,
                        "maker_time_budget_sec": maker_time_budget_sec,
                        "safety_margin_breakdown": safety_margin,
                        "cost_components": {
                            "spread": half_spread_bps,
                            "slippage": slippage_bps,
                            "vol_penalty": vol_penalty_bps,
                            "safety_margin": safety_margin_bps,
                            "dominant": dominant,
                        },
                    })
                else:
                    cost_pass_count += 1
                    stage_reached = "cost"
                    min_conf = 0.55 if not is_new_listing else 0.65
                    risk_evaluated_count += 1

                    risk_reason_code = None
                    if risk_state.get("halted"):
                        risk_reason_code = "RISK_GLOBAL_HALT"
                    elif confidence < min_conf:
                        risk_reason_code = "RISK_CONFIDENCE_LOW"
                    else:
                        with db() as _c:
                            _cur = _c.cursor()
                            _cur.execute("select coalesce(sum(realized_pnl_bps),0) from shadow_portfolio_ledger where ts >= datetime('now','-1 hour')")
                            hourly_pnl = float((_cur.fetchone() or [0])[0] or 0.0)
                            _cur.execute("select coalesce(sum(realized_pnl_bps),0) from shadow_portfolio_ledger where ts >= datetime('now','-24 hours')")
                            daily_pnl = float((_cur.fetchone() or [0])[0] or 0.0)
                        if daily_pnl <= -80:
                            risk_reason_code = "RISK_DD_DAILY"
                        elif hourly_pnl <= -30:
                            risk_reason_code = "RISK_DD_HOURLY"

                    if risk_reason_code:
                        deny_stage = "risk"
                        stage_reached = "risk"
                        deny_reason = risk_reason_code
                        reject_counter[deny_reason] += 1
                        rejects_tradable["risk"] += 1
                        risk_code_counter[risk_state.get("halt_reason") if risk_reason_code == "RISK_GLOBAL_HALT" else deny_reason] += 1
                        if risk_reason_code != "RISK_GLOBAL_HALT":
                            log_reject(sym, deny_stage, deny_reason, {"confidence": confidence, "min_conf": min_conf, "regime": regime})
                        status = "BLOCKED_BY_RISK" if risk_reason_code == "RISK_GLOBAL_HALT" else status
                    else:
                        risk_pass_count += 1
                        proposed_count += 1
                        stage_reached = "actionable"
                        status = "ACTIONABLE"
                        notional_util_samples.append(target_notional_adj / max(1.0, TARGET_NOTIONAL_USDT))
                        put_signal(sym, Source.SYSTEM, "actionable_candidate", {
                            "alpha_score": alpha_score,
                            "confidence": confidence,
                            "target_notional": target_notional_adj,
                            "size_reduced": size_reduced,
                            "execution_style": chosen_style,
                            "cost_edge_bps": cost_edge_bps,
                            "risk_flags": risk_flags,
                            "new_listing": is_new_listing,
                        }, conviction="high")

                        # enqueue ACTIONABLE intent; worker will create ATTEMPT now and evaluate fill next cycle+
                        attempt_id = f"CORE-{sym}-{int(time.time()*1000)}-{random.randint(100,999)}"
                        crosses_spread = bool(chosen_style != "maker-first")
                        EXECUTION_INTENT_QUEUE.put({
                            "attempt_id": attempt_id,
                            "symbol": sym,
                            "lane": "CORE",
                            "maker_time_budget_sec": maker_time_budget_sec,
                            "p_fill_hint": p_fill_maker,
                            "crosses_spread": crosses_spread,
                        })

        if sym in {"USDT", "USDC", "USD", "EUR"}:
            invalid_assets_filtered += 1
            continue

        item = {
            "symbol": sym,
            "instrument_symbol": instrument_symbol or f"{sym}_UNMAPPED",
            "base_ccy": sym,
            "quote_ccy": (instrument_symbol.split("_")[1] if instrument_symbol and "_" in instrument_symbol else None),
            "status": status,
            "stage_reached": stage_reached,
            "eligibility_score": round(eligibility_score, 2),
            "alpha_score": round(alpha_score, 2),
            "confidence": round(confidence, 3),
            "uncertainty": round(1.0 - confidence, 3),
            "cost_edge_bps": round(cost_edge_bps, 2),
            "execution_style": chosen_style,
            "size_reduced": size_reduced,
            "target_notional_adj": round(target_notional_adj, 2),
            "risk_flags": risk_flags,
            "liquidity_cover": round(liquidity_cover, 3),
            "spread_bps": round(spread_bps, 2),
            "mapped": mapped,
            "deny_stage": deny_stage,
            "deny_reason": deny_reason,
        }
        watchlist.append(item)
        if status in {"ELIGIBLE", "ACTIONABLE"}:
            eligible_list.append(item)
        if status == "ACTIONABLE":
            actionable_list.append(item)

        log_universe_state(sym, instrument_symbol, sym, (instrument_symbol.split("_")[1] if instrument_symbol and "_" in instrument_symbol else None), eligibility_score, alpha_score, confidence, status, deny_stage, deny_reason, {
            "stage_reached": stage_reached,
            "volume_24h": volume_24h,
            "spread_bps": spread_bps,
            "depth_usd_5bps": depth_5bps,
            "depth_usd_20bps": depth_20bps,
            "depth_usd_50bps": depth_50bps,
            "obi_20bps": float(micro.get("obi_20bps", 0.0)),
            "pressure_flag": bool(micro.get("pressure_flag", False)),
            "pressure_confidence": pressure_confidence,
            "dynamic_min_volume": dynamic_min_volume,
            "dynamic_max_spread_bps": dynamic_max_spread_bps,
            "liquidity_cover": liquidity_cover,
            "is_new_listing": is_new_listing,
            "expected_edge_bps": expected_edge_bps,
            "est_cost_bps": est_cost_bps,
            "est_cost_bps_full_size": est_cost_bps_full_size,
            "est_cost_bps_smallcap_lane": taker_cost_bps,
            "est_cost_bps_maker": maker_cost_bps,
            "est_cost_bps_maker_expected": maker_expected_cost_bps,
            "execution_style": chosen_style,
            "target_notional_adj": target_notional_adj,
            "size_reduced": size_reduced,
            "k_slip_used": k_slip,
            "b_interpolated_full": b_star_full,
            "b_interpolated_adj": b_star_adj,
            "impact_multiplier": impact_multiplier,
            "p_fill_maker": p_fill_maker,
            "maker_time_budget_sec": maker_time_budget_sec,
            "safety_margin_breakdown": safety_margin,
            "cost_edge_bps": cost_edge_bps,
            "uncertainty": (1.0 - confidence),
            "risk_flags": risk_flags,
            **alpha_dbg,
        }, sources_present)

    top_reasons = reject_counter.most_common(8)
    autotuner_stats = tradable_autotuner_stats(24)
    governor = parameter_governor_recommend({"risk_pass_total": risk_pass_count, "cost_pass_total": cost_pass_count}, autotuner_stats, risk_state, macro)

    # dedupe by instrument_symbol
    deduped: dict[str, dict] = {}
    duplicates_removed = 0
    for row in sorted(watchlist, key=lambda x: (x.get("instrument_symbol") or "", x.get("alpha_score", 0.0)), reverse=True):
        k = row.get("instrument_symbol") or ""
        if not k:
            continue
        if k in deduped:
            duplicates_removed += 1
            continue
        deduped[k] = row
    all_rows = list(deduped.values())

    watch_rows = [x for x in all_rows if x["status"] in {"WATCH", "BLOCKED_BY_RISK"}]
    eligible_rows = [x for x in all_rows if x["status"] == "ELIGIBLE"]
    actionable_rows = [x for x in all_rows if x["status"] == "ACTIONABLE"]

    # deterministic capacity routing at batch end
    actionable_rows = sorted(
        actionable_rows,
        key=lambda x: (
            -float(x.get("alpha_score", 0.0) or 0.0),
            -float(x.get("cost_edge_bps", 0.0) or 0.0),
            -float(x.get("liquidity_cover", 0.0) or 0.0),
            float(x.get("spread_bps", 9999.0) or 9999.0),
            str(x.get("symbol", "")),
        ),
    )
    admitted_symbols = set(x["symbol"] for x in actionable_rows[:remaining_slots]) if remaining_slots > 0 else set()
    capacity_admitted = len(admitted_symbols)
    capacity_rejected = max(0, len(actionable_rows) - capacity_admitted)
    if capacity_rejected > 0:
        for r in all_rows:
            if r.get("status") == "ACTIONABLE" and r.get("symbol") not in admitted_symbols:
                r["status"] = "WATCH"
                r["deny_stage"] = "risk"
                r["deny_reason"] = "RISK_CAPACITY_FULL"
                reject_counter["RISK_CAPACITY_FULL"] += 1
                rejects_tradable["risk"] += 1
                risk_code_counter["RISK_CAPACITY_FULL"] += 1

        watch_rows = [x for x in all_rows if x["status"] in {"WATCH", "BLOCKED_BY_RISK"}]
        eligible_rows = [x for x in all_rows if x["status"] == "ELIGIBLE"]
        actionable_rows = [x for x in all_rows if x["status"] == "ACTIONABLE"]

    # watchlist must come from ELIGIBLE universe first
    watch_top = sorted(eligible_rows, key=lambda x: x["alpha_score"], reverse=True)[:10]
    if not watch_top and len(eligible_rows) > 0:
        watch_top = eligible_rows[:10]
    eligible_top = sorted(eligible_rows + actionable_rows, key=lambda x: x["alpha_score"], reverse=True)[:10]
    actionable_top = sorted(actionable_rows, key=lambda x: x["alpha_score"], reverse=True)[:5]

    short_circuit_reason = "NONE"
    stages_evaluated_mode = "FULL"
    if risk_state.get("halted"):
        short_circuit_reason = "GLOBAL_RISK_HALTED"
        stages_evaluated_mode = "PARTIAL"
    elif remaining_slots <= 0:
        short_circuit_reason = "CAPACITY_FULL"
        stages_evaluated_mode = "SKIPPED"
    elif len(discovered) == 0:
        short_circuit_reason = "NO_CANDIDATES"
        stages_evaluated_mode = "SKIPPED"

    eligible_syms = {x["symbol"] for x in eligible_rows + actionable_rows}
    micro_join = len([s for s in eligible_syms if s in micro_latest])
    micro_join_fail = max(0, len(eligible_syms) - micro_join)
    llama_join = len([s for s in eligible_syms if s in llama_syms])
    dex_join = len([s for s in eligible_syms if s in dex_latest or s in dex_boosted])
    denom = max(1, len(eligible_syms))
    # start attempts for intents queued this cycle (fills evaluated in future cycles only)
    SHADOW_WORKER.process_intents(cycle_id)

    micro_target_k = int(os.getenv("MICRO_TOP_SYMBOLS", "150"))
    micro_pinned_total = 0
    micro_interest_total = 0
    try:
        mu = json.loads(Path("/home/itachi/.openclaw/workspace/project_crypt/micro_universe_state.json").read_text())
        micro_pinned_total = len(mu.get("pinned_symbols", []) or [])
        micro_interest_total = len(mu.get("interest_symbols", []) or [])
    except Exception:
        pass
    with db() as _cdd:
        _cur = _cdd.cursor()
        _cur.execute("select coalesce(sum(realized_pnl_bps),0) from shadow_portfolio_ledger where ts >= datetime('now','-1 hour')")
        dd_hourly = float((_cur.fetchone() or [0])[0] or 0.0)
        _cur.execute("select coalesce(sum(realized_pnl_bps),0) from shadow_portfolio_ledger where ts >= datetime('now','-24 hours')")
        dd_daily = float((_cur.fetchone() or [0])[0] or 0.0)
    sanity = {
        "micro_target_k": micro_target_k,
        "pre_cost_skip_breakdown": dict(pre_cost_skip),
        "avg_notional_utilization": round((sum(notional_util_samples) / len(notional_util_samples)) if notional_util_samples else 0.0, 3),
        "eligible_unique": len({x["instrument_symbol"] for x in eligible_rows}),
        "actionable_unique": len({x["instrument_symbol"] for x in actionable_rows}),
        "duplicates_removed": duplicates_removed,
        "invalid_assets_filtered": invalid_assets_filtered,
        "micro_present": len(micro_latest),
        "micro_tracked_total": len(micro_latest),
        "micro_pinned_total": micro_pinned_total,
        "micro_interest_total": micro_interest_total,
        "join_rate_to_eligible": {
            "micro": round(micro_join / denom, 3),
            "defillama": round(llama_join / denom, 3),
            "dex": round(dex_join / denom, 3),
        },
        "micro_join_fail": micro_join_fail,
        "risk_reason_codes": dict(risk_code_counter),
        "open_positions_basis": "shadow_state",
        "open_positions": open_positions,
        "max_positions": max_positions,
        "remaining_slots": remaining_slots,
        "capacity_admitted": capacity_admitted,
        "capacity_rejected": capacity_rejected,
        "shadow_attempts": shadow_attempts,
        "shadow_fills": shadow_fills,
        "shadow_expired": shadow_expired,
        "avg_fill_latency_sec": round((sum(drained_exec.get("latencies", []) or [0.0]) / max(1, len(drained_exec.get("latencies", [])))), 3) if drained_exec.get("latencies") else 0.0,
        "min_fill_delay_cycles": int(min(drained_exec.get("delay_cycles", [1])) if drained_exec.get("delay_cycles") else (SHADOW_STATE.get("min_delay_cycles") or 1)),
        "dd_basis": "shadow_ledger",
        "dd_hourly_bps": round(dd_hourly, 2),
        "dd_daily_bps": round(dd_daily, 2),
        "risk_state": risk_state,
        "macro_shock": macro,
        "autotuner_frozen": bool(macro.get("macro_shock")) or bool(risk_state.get("halted")),
        "short_circuit_reason": short_circuit_reason,
        "stages_evaluated_mode": stages_evaluated_mode,
        "bloodbath_lane": bloodbath,
        "parameter_governor": governor,
    }

    log_funnel(
        discovered=len(discovered),
        mapped=mapped_count,
        eligible=len(eligible_rows) + len(actionable_rows),
        alpha_pass=alpha_pass_count,
        risk_pass=risk_pass_count,
        cost_pass=cost_pass_count,
        proposed=len(actionable_rows),
        top_reasons=top_reasons,
        sources_present=sources_present,
        extras={
            "watch_total": len(watch_rows),
            "actionable_total": len(actionable_rows),
            "alpha_scored_total": alpha_scored_count,
            "cost_evaluated_total": cost_evaluated_count,
            "risk_evaluated_total": risk_evaluated_count,
            "rejects_tradable": dict(rejects_tradable),
            "rejects_external": dict(rejects_external),
            "sanity": sanity,
            "regime": regime,
            "cost_fail_breakdown": dict(cost_fail_breakdown),
            "autotuner_tradable": autotuner_stats,
            "pressure_count": pressure_count,
            "risk_state": risk_state,
            "macro_shock": macro,
            "autotuner_frozen": bool(macro.get("macro_shock")) or bool(risk_state.get("halted")),
            "bloodbath_lane": bloodbath,
            "parameter_governor": governor,
        },
    )

    summary = {
        "discovered_total": len(discovered),
        "watch_total": len(watch_rows),
        "eligible_state_total": len(eligible_rows),
        "actionable_state_total": len(actionable_rows),
        "mapped_to_venue_total": mapped_count,
        "eligible_total": len(eligible_rows) + len(actionable_rows),
        "alpha_pass_total": alpha_pass_count,
        "risk_pass_total": risk_pass_count,
        "cost_pass_total": cost_pass_count,
        "proposed_total": len(actionable_rows),
        "top_reject_reasons": top_reasons,
        "sources_present": sources_present,
        "rejects_tradable": dict(rejects_tradable),
        "rejects_external": dict(rejects_external),
        "alpha_scored_total": alpha_scored_count,
        "cost_evaluated_total": cost_evaluated_count,
        "risk_evaluated_total": risk_evaluated_count,
        "stage_reach": {
            "scored": alpha_scored_count,
            "costed": cost_evaluated_count,
            "cost_pass": cost_pass_count,
            "risked": risk_evaluated_count,
            "sized": risk_pass_count,
            "actionable": len(actionable_rows),
        },
        "short_circuit_reason": short_circuit_reason,
        "stages_evaluated_mode": stages_evaluated_mode,
        "alpha_floor_effective": 50 if alpha_floor_override_count > 0 else MIN_ACTIONABLE_SCORE,
        "alpha_floor_overrides": alpha_floor_override_count,
        "pre_cost_skip_breakdown": dict(pre_cost_skip),
        "avg_notional_utilization": round((sum(notional_util_samples) / len(notional_util_samples)) if notional_util_samples else 0.0, 3),
        "alpha_override_active_count": alpha_floor_override_count,
        "regime": regime,
        "pressure_count": pressure_count,
        "sanity": sanity,
        "cost_fail_breakdown": dict(cost_fail_breakdown),
        "autotuner_tradable": autotuner_stats,
        "autotuner_frozen": bool(macro.get("macro_shock")) or bool(risk_state.get("halted")),
        "risk_state": risk_state,
        "macro_shock": macro,
        "bloodbath_lane": bloodbath,
        "parameter_governor": governor,
        "venue_movers_total": len(venue_movers),
        "new_listings_total": len(new_listings),
        "defillama_symbols_total": len(llama_syms),
        "defillama_points": llama_points,
        "dune_symbols_total": len(dune_syms),
        "dune_points": dune_points,
        "adaptive_thresholds": {
            "min_volume_24h": round(dynamic_min_volume, 2),
            "max_spread_bps": round(dynamic_max_spread_bps, 2),
            "target_notional": TARGET_NOTIONAL_USDT,
        },
        "watchlist_top": watch_top,
        "eligible_top": eligible_top,
        "actionable_top": actionable_top,
    }

    write_status({"state": "idle", "job": "sleeping", "last_cycle_summary": summary})
    return summary


def main() -> None:
    init_tables()
    tg = TelegramNotifier()
    kill_switch_alerted = False
    startup_ping_sent = False
    last_ghost_run = 0.0
    write_status({"state": "starting", "job": "boot", "kill_switch_active": is_kill_switch_active()})
    while True:
        if is_kill_switch_active():
            write_status({
                "state": "halted",
                "job": "kill_switch_hold",
                "kill_switch_active": True,
                "kill_switch_file": KILL_SWITCH_FILE,
            })
            put_signal(None, Source.SYSTEM, "kill_switch_active", {"kill_switch_file": KILL_SWITCH_FILE}, conviction="watch")
            if not kill_switch_alerted:
                msg = f"[Project Crypt][CRITICAL] Kill switch active. All proposals denied. file={KILL_SWITCH_FILE}"
                tg.send(msg)
                kill_switch_alerted = True
            time.sleep(15)
            continue

        if kill_switch_alerted:
            tg.send("[Project Crypt] Kill switch cleared. Phase-2 shadow pipeline resumed.")
            kill_switch_alerted = False

        t0 = time.time()
        try:
            # Full decision cycle: ingest -> score -> funnel logging -> ranked outputs.
            summary = run_cycle()
            if not startup_ping_sent:
                tg.send(build_startup_ping(summary))
                startup_ping_sent = True
            if time.time() - last_ghost_run >= 3600:
                is_shock = bool((summary.get("macro_shock") or {}).get("macro_shock", False))
                ghost = run_ghost_simulator(macro_shock=is_shock)
                put_signal(None, Source.SYSTEM, "ghost_sim_hourly", ghost)
                last_ghost_run = time.time()
            write_status({
                "state": "cycle_complete",
                "job": "sleeping",
                "last_cycle_seconds": round(time.time() - t0, 2),
                "last_cycle_summary": summary,
                "kill_switch_active": False,
            })
        except Exception as e:
            put_signal(None, Source.SYSTEM, "cycle_error", {"error": str(e)})
            write_status({"state": "error", "job": "cycle_error", "error": str(e), "kill_switch_active": is_kill_switch_active()})

        jitter = random.uniform(-0.1 * POLL_SECONDS, 0.1 * POLL_SECONDS)
        cycle_wait = max(30, int(POLL_SECONDS + jitter - (time.time() - t0)))
        next_cycle_at = time.time() + cycle_wait
        next_market_tick = time.time() + max(20, MARKET_TICK_SECONDS)
        next_discovery_tick = time.time() + max(30, DISCOVERY_TICK_SECONDS)

        write_status({
            "state": "sleeping",
            "job": "waiting_next_cycle",
            "sleep_for": cycle_wait,
            "kill_switch_active": False,
            "next_market_tick_s": MARKET_TICK_SECONDS,
            "next_discovery_tick_s": DISCOVERY_TICK_SECONDS,
        })

        while time.time() < next_cycle_at:
            if is_kill_switch_active():
                break

            now = time.time()
            if now >= next_market_tick:
                try:
                    write_status({"state": "running", "job": "market_fast_tick", "kill_switch_active": False})
                    symbols = ingest_cryptocom()
                    put_signal(None, Source.SYSTEM, "market_fast_tick", {"symbols": len(symbols)})
                except Exception as e:
                    put_signal(None, Source.SYSTEM, "market_fast_tick_error", {"error": str(e)})
                next_market_tick = now + max(20, MARKET_TICK_SECONDS)

            if now >= next_discovery_tick:
                try:
                    write_status({"state": "running", "job": "discovery_fast_tick", "kill_switch_active": False})
                    cg = ingest_coingecko()
                    dex_latest, dex_boosted, _ = ingest_dex()
                    news_hits = ingest_news()
                    put_signal(None, Source.SYSTEM, "discovery_fast_tick", {
                        "coingecko": len(cg),
                        "dex_latest": len(dex_latest),
                        "dex_boosted": len(dex_boosted),
                        "news_hits": news_hits,
                    })
                except Exception as e:
                    put_signal(None, Source.SYSTEM, "discovery_fast_tick_error", {"error": str(e)})
                next_discovery_tick = now + max(30, DISCOVERY_TICK_SECONDS)

            time.sleep(5)


if __name__ == "__main__":
    main()
