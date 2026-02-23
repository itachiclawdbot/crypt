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
TARGET_NOTIONAL_USDT = float(os.getenv("PHASE2_TARGET_NOTIONAL_USDT", "300"))
MIN_ACTIONABLE_SCORE = float(os.getenv("PHASE2_MIN_ACTIONABLE_SCORE", "70"))
MIN_ELIGIBLE_SCORE = float(os.getenv("PHASE2_MIN_ELIGIBLE_SCORE", "45"))
DISCOVERY_TICK_SECONDS = int(os.getenv("PHASE2_DISCOVERY_TICK_SECONDS", "120"))
MARKET_TICK_SECONDS = int(os.getenv("PHASE2_MARKET_TICK_SECONDS", "60"))
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "/var/run/cryptobot/STOP")


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
        rows = instruments.get("result", {}).get("instruments", []) if isinstance(instruments, dict) else []
        for r in rows[:2000]:
            name = str(r.get("instrument_name", ""))
            if name.endswith("_USDT"):
                symbols.add(name.split("_")[0].upper())
        put_signal(None, Source.CRYPTOCOM, "instruments_snapshot", {"count": len(rows)})
        log_source_health(Source.CRYPTOCOM, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CRYPTOCOM, "ingest_error", {"error": str(e)})
        log_source_health(Source.CRYPTOCOM, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return symbols


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


def ingest_dex() -> tuple[set[str], set[str], dict[str, dict]]:
    t0 = time.time()
    boosted: set[str] = set()
    latest: set[str] = set()
    symbol_metrics: dict[str, dict] = {}
    try:
        l = get_json(_norm_url(DEX_LATEST_PROFILES_ENDPOINT))
        b = get_json(_norm_url(DEX_BOOSTED_TOKENS_ENDPOINT))
        l_rows = l if isinstance(l, list) else []
        b_rows = b if isinstance(b, list) else []
        for row in l_rows[:400]:
            sym = str(row.get("tokenSymbol") or row.get("symbol") or "").upper()
            if sym:
                latest.add(sym)
                vol = float(row.get("volume") or row.get("volume24h") or 0)
                symbol_metrics.setdefault(sym, {})["volume24h"] = max(vol, symbol_metrics.get(sym, {}).get("volume24h", 0))
                put_signal(sym, Source.DEX, "latest_profile", {"entry": row})
        for row in b_rows[:400]:
            sym = str(row.get("tokenSymbol") or row.get("symbol") or "").upper()
            if sym:
                boosted.add(sym)
                put_signal(sym, Source.DEX, "boosted", {"entry": row}, conviction="high")
        log_source_health(Source.DEX, "OK", int((time.time() - t0) * 1000), len(l_rows) + len(b_rows))
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


def source_availability(cg: set[str], cmc: set[str], dex_latest: set[str], news_hits: int) -> dict:
    x_on = bool(os.getenv("X_BEARER_TOKEN") or os.getenv("X_API_KEY"))
    reddit_on = bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET"))
    return {
        Source.COINGECKO: len(cg) > 0,
        Source.CMC: len(cmc) > 0,
        Source.DEX: len(dex_latest) > 0,
        Source.CRYPTOPANIC: news_hits >= 0,
        Source.FREE_NEWS: news_hits >= 0,
        Source.X: x_on,
        Source.REDDIT: reddit_on,
    }


def weighted_alpha(symbol: str, cg: set[str], cmc: set[str], dex_latest: set[str], dex_boosted: set[str], mapped: bool, sources_present: dict) -> tuple[float, float, dict]:
    base_weights = {
        "market": 35.0,
        "liquidity": 25.0,
        "news": 20.0,
        "social": 20.0,
    }
    social_available = sources_present.get(Source.X) or sources_present.get(Source.REDDIT)
    if not social_available:
        base_weights["news"] += 10.0
        base_weights["market"] += 10.0
        base_weights["social"] = 0.0

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

    news_points = 8.0 if symbol in cg else 0.0
    news_points += 12.0 if symbol in dex_boosted else 0.0
    score += min(base_weights["news"], news_points)

    social_points = 0.0
    if social_available:
        social_points = 8.0 if symbol in cg else 0.0
        social_points += 12.0 if symbol in dex_boosted else 0.0
        score += min(base_weights["social"], social_points)

    confidence = max(0.05, min(0.99, score / 100.0))
    if not mapped:
        confidence *= 0.7

    return score, confidence, {"weights": base_weights, "market_points": market_points, "liquidity_points": liquidity_points, "news_points": news_points, "social_points": social_points}


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

    write_status({"state": "running", "job": "ingest_coingecko"})
    cg = ingest_coingecko()

    write_status({"state": "running", "job": "ingest_cmc"})
    cmc = ingest_cmc()

    write_status({"state": "running", "job": "ingest_dex"})
    dex_latest, dex_boosted, dex_metrics = ingest_dex()

    write_status({"state": "running", "job": "ingest_news"})
    news_hits = ingest_news()

    sources_present = source_availability(cg, cmc, dex_latest, news_hits)
    discovered = sorted(set(cg).union(cmc).union(dex_latest).union(dex_boosted))

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

    dynamic_min_volume = 100000.0

    for sym in discovered:
        mapped = sym in crypto_symbols
        if mapped:
            mapped_count += 1

        volume_24h = float(dex_metrics.get(sym, {}).get("volume24h", 0.0))
        eligibility_score = 0.0
        deny_stage = None
        deny_reason = None

        if mapped:
            eligibility_score += 40
        if volume_24h >= dynamic_min_volume:
            eligibility_score += 35
        if sym in dex_latest:
            eligibility_score += 25

        alpha_score, confidence, alpha_dbg = weighted_alpha(sym, cg, cmc, dex_latest, dex_boosted, mapped, sources_present)

        status = "WATCH"
        if not mapped:
            deny_stage = "mapping"
            deny_reason = "not-tradable-on-crypto-com"
            reject_counter[deny_reason] += 1
            log_reject(sym, deny_stage, deny_reason, {"mapped": mapped})
            status = "DENIED"
        elif eligibility_score < MIN_ELIGIBLE_SCORE:
            deny_stage = "eligibility"
            deny_reason = "eligibility-score-too-low"
            reject_counter[deny_reason] += 1
            log_reject(sym, deny_stage, deny_reason, {"eligibility_score": eligibility_score, "volume_24h": volume_24h})
            status = "WATCH"
        else:
            eligible_count += 1
            if alpha_score < MIN_ACTIONABLE_SCORE:
                deny_stage = "alpha"
                deny_reason = "alpha-score-below-threshold"
                reject_counter[deny_reason] += 1
                log_reject(sym, deny_stage, deny_reason, {"alpha_score": alpha_score, "confidence": confidence})
                status = "ELIGIBLE"
            else:
                alpha_pass_count += 1
                spread_cost_bps = 20.0 if sym in dex_boosted else 30.0
                expected_edge_bps = alpha_score * 0.45
                if expected_edge_bps - spread_cost_bps < 10.0:
                    deny_stage = "cost"
                    deny_reason = "net-edge-too-low"
                    reject_counter[deny_reason] += 1
                    log_reject(sym, deny_stage, deny_reason, {"expected_edge_bps": expected_edge_bps, "spread_cost_bps": spread_cost_bps})
                    status = "ELIGIBLE"
                else:
                    cost_pass_count += 1
                    risk_block = confidence < 0.55
                    if risk_block:
                        deny_stage = "risk"
                        deny_reason = "confidence-too-low"
                        reject_counter[deny_reason] += 1
                        log_reject(sym, deny_stage, deny_reason, {"confidence": confidence})
                        status = "ELIGIBLE"
                    else:
                        risk_pass_count += 1
                        proposed_count += 1
                        status = "ACTIONABLE"
                        put_signal(sym, Source.SYSTEM, "actionable_candidate", {"alpha_score": alpha_score, "confidence": confidence, "target_notional": TARGET_NOTIONAL_USDT}, conviction="high")

        item = {
            "symbol": sym,
            "status": status,
            "eligibility_score": round(eligibility_score, 2),
            "alpha_score": round(alpha_score, 2),
            "confidence": round(confidence, 3),
            "mapped": mapped,
            "deny_stage": deny_stage,
            "deny_reason": deny_reason,
        }
        watchlist.append(item)
        if status in {"ELIGIBLE", "ACTIONABLE"}:
            eligible_list.append(item)
        if status == "ACTIONABLE":
            actionable_list.append(item)

        log_universe_state(sym, eligibility_score, alpha_score, confidence, status, deny_stage, deny_reason, {"volume_24h": volume_24h, **alpha_dbg}, sources_present)

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
        "mapped_to_venue_total": mapped_count,
        "eligible_total": eligible_count,
        "alpha_pass_total": alpha_pass_count,
        "risk_pass_total": risk_pass_count,
        "cost_pass_total": cost_pass_count,
        "proposed_total": proposed_count,
        "top_reject_reasons": top_reasons,
        "sources_present": sources_present,
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
