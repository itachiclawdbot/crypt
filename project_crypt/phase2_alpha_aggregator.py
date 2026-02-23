from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone

import requests

from project_crypt.telegram_notify import TelegramNotifier

DB_PATH = os.getenv("CRYPTO_DB_PATH", "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3")
POLL_SECONDS = int(os.getenv("PHASE2_POLL_SECONDS", "300"))
STATUS_PATH = os.getenv("PHASE2_STATUS_PATH", "/home/itachi/.openclaw/workspace/project_crypt/phase2_status.json")

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


class Source:
    CRYPTOCOM = "crypto.com"
    COINGECKO = "coingecko"
    CMC = "coinmarketcap"
    DEX = "dexscreener"
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


def ingest_cryptocom() -> set[str]:
    t0 = time.time()
    symbols: set[str] = set()
    try:
        instruments = get_json("https://api.crypto.com/exchange/v1/public/get-instruments")
        result = instruments.get("result", {}) if isinstance(instruments, dict) else {}

        # Crypto.com schema drift handling:
        # - old: result.instruments[] with instrument_name like BTC_USDT
        # - new: result.data[] with symbol/base_ccy/quote_ccy and symbol like BTC_USD
        rows = result.get("instruments") or result.get("data") or []

        for r in rows[:5000]:
            quote = str(r.get("quote_ccy") or r.get("quote_currency") or "").upper()
            base = str(r.get("base_ccy") or r.get("base_currency") or "").upper()
            name = str(r.get("instrument_name") or r.get("symbol") or "").upper()

            # Keep USD + USDT spot mappings for broader venue tradability mapping.
            if base and quote in {"USD", "USDT"}:
                symbols.add(base)
                continue

            # Fallback parse for pair strings if fields are missing.
            if name.endswith("_USDT") or name.endswith("_USD"):
                symbols.add(name.split("_")[0].upper())

        put_signal(None, Source.CRYPTOCOM, "instruments_snapshot", {"count": len(rows), "mapped_bases": len(symbols)})
        log_source_health(Source.CRYPTOCOM, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CRYPTOCOM, "ingest_error", {"error": str(e)})
        log_source_health(Source.CRYPTOCOM, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return symbols


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


def load_micro_latest() -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        with db() as c:
            cur = c.cursor()
            cur.execute(
                """
                select base_symbol, spread_bps_median_300, spread_bps_p95_300, depth_usd_10bps, depth_usd_20bps,
                       obi_10bps, obi_20bps, pressure_flag, orderbook_slope, liquidity_score, risk_flags_json
                from micro_features_latest
                """
            )
            for r in cur.fetchall():
                out[str(r[0]).upper()] = {
                    "spread_bps_median_300": float(r[1] or 0.0),
                    "spread_bps_p95_300": float(r[2] or 0.0),
                    "depth_usd_10bps": float(r[3] or 0.0),
                    "depth_usd_20bps": float(r[4] or 0.0),
                    "obi_10bps": float(r[5] or 0.0),
                    "obi_20bps": float(r[6] or 0.0),
                    "pressure_flag": bool(r[7]),
                    "orderbook_slope": float(r[8] or 0.0),
                    "liquidity_score": float(r[9] or 0.0),
                    "risk_flags": json.loads(r[10] or "[]"),
                }
    except Exception:
        return {}
    return out


def source_availability(cg: set[str], cmc: set[str], dex_latest: set[str], news_hits: int, venue_movers: set[str]) -> dict:
    x_on = bool(os.getenv("X_BEARER_TOKEN") or os.getenv("X_API_KEY"))
    reddit_on = bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET"))
    return {
        Source.COINGECKO: len(cg) > 0,
        Source.CMC: len(cmc) > 0,
        Source.DEX: len(dex_latest) > 0,
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


def log_universe_state(symbol: str, eligibility_score: float, alpha_score: float, confidence: float, status: str, deny_stage: str | None, deny_reason: str | None, metrics: dict, sources_present: dict) -> None:
    with db() as c:
        c.execute(
            """
            INSERT INTO universe_state (ts,symbol,eligibility_score,alpha_score,confidence,status,deny_stage,deny_reason,metrics_json,sources_present_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                symbol,
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


def log_funnel(discovered: int, mapped: int, eligible: int, alpha_pass: int, risk_pass: int, cost_pass: int, proposed: int, top_reasons: list[tuple[str, int]], sources_present: dict) -> None:
    with db() as c:
        c.execute(
            """
            INSERT INTO candidate_funnel_log (ts,discovered_total,mapped_to_venue_total,eligible_total,alpha_pass_total,risk_pass_total,cost_pass_total,proposed_total,reasons_json,sources_present_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)
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
            ),
        )


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


def run_cycle() -> dict:
    write_status({"state": "running", "job": "ingest_cryptocom"})
    crypto_symbols = ingest_cryptocom()

    write_status({"state": "running", "job": "ingest_cryptocom_movers"})
    venue_movers, venue_metrics = ingest_cryptocom_movers()
    new_listings = detect_new_listings(crypto_symbols)

    write_status({"state": "running", "job": "ingest_coingecko"})
    cg = ingest_coingecko()

    write_status({"state": "running", "job": "ingest_cmc"})
    cmc = ingest_cmc()

    write_status({"state": "running", "job": "ingest_dex"})
    dex_latest, dex_boosted, dex_metrics = ingest_dex()

    write_status({"state": "running", "job": "ingest_news"})
    news_hits = ingest_news()

    write_status({"state": "running", "job": "load_micro_features"})
    micro_latest = load_micro_latest()

    sources_present = source_availability(cg, cmc, dex_latest, news_hits, venue_movers)
    discovered = sorted(set(cg).union(cmc).union(dex_latest).union(dex_boosted).union(venue_movers).union(new_listings))

    mapped_count = 0
    eligible_count = 0
    alpha_pass_count = 0
    risk_pass_count = 0
    cost_pass_count = 0
    proposed_count = 0
    reject_counter: Counter = Counter()

    watchlist: list[dict] = []
    eligible_list: list[dict] = []
    actionable_list: list[dict] = []

    venue_vols = [float(v.get("volume_24h_quote", 0.0)) for v in venue_metrics.values()]
    venue_spreads = [float(v.get("spread_bps", 9999.0)) for v in venue_metrics.values()]
    dynamic_min_volume = max(100000.0, _percentile(venue_vols, 0.25, 100000.0))
    dynamic_max_spread_bps = min(50.0, _percentile(venue_spreads, 0.75, 50.0))

    for sym in discovered:
        mapped = sym in crypto_symbols
        if mapped:
            mapped_count += 1

        venue = venue_metrics.get(sym, {})
        micro = micro_latest.get(sym, {})
        volume_24h = float(venue.get("volume_24h_quote", dex_metrics.get(sym, {}).get("volume24h", 0.0)))
        spread_bps = float(micro.get("spread_bps_p95_300", venue.get("spread_bps", 9999.0)))
        depth_20bps = float(micro.get("depth_usd_20bps", 0.0))
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
        if micro.get("pressure_flag"):
            alpha_score += 8.0
        if abs(float(micro.get("obi_20bps", 0.0))) >= 0.70:
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
        est_cost_bps = estimate_cost_bps(
            spread_bps,
            liquidity_cover,
            float(venue.get("chg_24h_abs", 0.0)),
            depth_usd_20bps=depth_20bps,
        )
        safety_margin_bps = 12.0 if not is_new_listing else 20.0
        cost_edge_bps = expected_edge_bps - est_cost_bps - safety_margin_bps

        if not mapped:
            deny_stage = "mapping"
            stage_reached = "mapping"
            deny_reason = "not-tradable-on-crypto-com"
            reject_counter[deny_reason] += 1
            log_reject(sym, deny_stage, deny_reason, {"mapped": mapped})
            status = "WATCH"
        elif eligibility_score < MIN_ELIGIBLE_SCORE:
            deny_stage = "eligibility"
            stage_reached = "eligibility"
            deny_reason = "eligibility-score-too-low"
            reject_counter[deny_reason] += 1
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
            if alpha_score < MIN_ACTIONABLE_SCORE:
                deny_stage = "alpha"
                stage_reached = "alpha"
                deny_reason = "alpha-score-below-threshold"
                reject_counter[deny_reason] += 1
                log_reject(sym, deny_stage, deny_reason, {"alpha_score": alpha_score, "confidence": confidence})
            else:
                alpha_pass_count += 1
                stage_reached = "alpha"
                if cost_edge_bps < 0:
                    deny_stage = "cost"
                    stage_reached = "cost"
                    deny_reason = "net-edge-too-low"
                    reject_counter[deny_reason] += 1
                    log_reject(sym, deny_stage, deny_reason, {
                        "expected_edge_bps": expected_edge_bps,
                        "est_cost_bps": est_cost_bps,
                        "safety_margin_bps": safety_margin_bps,
                        "cost_edge_bps": cost_edge_bps,
                    })
                else:
                    cost_pass_count += 1
                    stage_reached = "cost"
                    min_conf = 0.55 if not is_new_listing else 0.65
                    risk_block = confidence < min_conf
                    if risk_block:
                        deny_stage = "risk"
                        stage_reached = "risk"
                        deny_reason = "confidence-too-low"
                        reject_counter[deny_reason] += 1
                        log_reject(sym, deny_stage, deny_reason, {"confidence": confidence, "min_conf": min_conf})
                    else:
                        risk_pass_count += 1
                        proposed_count += 1
                        stage_reached = "actionable"
                        status = "ACTIONABLE"
                        put_signal(sym, Source.SYSTEM, "actionable_candidate", {
                            "alpha_score": alpha_score,
                            "confidence": confidence,
                            "target_notional": TARGET_NOTIONAL_USDT,
                            "cost_edge_bps": cost_edge_bps,
                            "risk_flags": risk_flags,
                            "new_listing": is_new_listing,
                        }, conviction="high")

        item = {
            "symbol": sym,
            "status": status,
            "stage_reached": stage_reached,
            "eligibility_score": round(eligibility_score, 2),
            "alpha_score": round(alpha_score, 2),
            "confidence": round(confidence, 3),
            "uncertainty": round(1.0 - confidence, 3),
            "cost_edge_bps": round(cost_edge_bps, 2),
            "risk_flags": risk_flags,
            "mapped": mapped,
            "deny_stage": deny_stage,
            "deny_reason": deny_reason,
        }
        watchlist.append(item)
        if status in {"ELIGIBLE", "ACTIONABLE"}:
            eligible_list.append(item)
        if status == "ACTIONABLE":
            actionable_list.append(item)

        log_universe_state(sym, eligibility_score, alpha_score, confidence, status, deny_stage, deny_reason, {
            "stage_reached": stage_reached,
            "volume_24h": volume_24h,
            "spread_bps": spread_bps,
            "depth_usd_20bps": depth_20bps,
            "obi_20bps": float(micro.get("obi_20bps", 0.0)),
            "pressure_flag": bool(micro.get("pressure_flag", False)),
            "dynamic_min_volume": dynamic_min_volume,
            "dynamic_max_spread_bps": dynamic_max_spread_bps,
            "liquidity_cover": liquidity_cover,
            "is_new_listing": is_new_listing,
            "expected_edge_bps": expected_edge_bps,
            "est_cost_bps": est_cost_bps,
            "cost_edge_bps": cost_edge_bps,
            "uncertainty": (1.0 - confidence),
            "risk_flags": risk_flags,
            **alpha_dbg,
        }, sources_present)

    top_reasons = reject_counter.most_common(8)
    log_funnel(
        discovered=len(discovered),
        mapped=mapped_count,
        eligible=eligible_count,
        alpha_pass=alpha_pass_count,
        risk_pass=risk_pass_count,
        cost_pass=cost_pass_count,
        proposed=proposed_count,
        top_reasons=top_reasons,
        sources_present=sources_present,
    )

    watch_top = sorted(watchlist, key=lambda x: x["alpha_score"], reverse=True)[:10]
    eligible_top = sorted(eligible_list, key=lambda x: x["alpha_score"], reverse=True)[:10]
    actionable_top = sorted(actionable_list, key=lambda x: x["alpha_score"], reverse=True)[:5]

    summary = {
        "discovered_total": len(discovered),
        "watch_total": len([x for x in watchlist if x["status"] == "WATCH"]),
        "eligible_state_total": len([x for x in watchlist if x["status"] == "ELIGIBLE"]),
        "actionable_state_total": len([x for x in watchlist if x["status"] == "ACTIONABLE"]),
        "mapped_to_venue_total": mapped_count,
        "eligible_total": eligible_count,
        "alpha_pass_total": alpha_pass_count,
        "risk_pass_total": risk_pass_count,
        "cost_pass_total": cost_pass_count,
        "proposed_total": proposed_count,
        "top_reject_reasons": top_reasons,
        "sources_present": sources_present,
        "venue_movers_total": len(venue_movers),
        "new_listings_total": len(new_listings),
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
