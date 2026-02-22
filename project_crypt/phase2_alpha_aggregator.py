from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timezone

import requests

DB_PATH = os.getenv("CRYPTO_DB_PATH", "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3")
POLL_SECONDS = int(os.getenv("PHASE2_POLL_SECONDS", "300"))
STATUS_PATH = os.getenv("PHASE2_STATUS_PATH", "/home/itachi/.openclaw/workspace/project_crypt/phase2_status.json")

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY")
DEX_SCREENER_BASE_URL = os.getenv("DEX_SCREENER_BASE_URL", "https://api.dexscreener.com")
DEX_LATEST_PROFILES_ENDPOINT = os.getenv("DEX_LATEST_PROFILES_ENDPOINT", "/token-profiles/latest/v1")
DEX_BOOSTED_TOKENS_ENDPOINT = os.getenv("DEX_BOOSTED_TOKENS_ENDPOINT", "/token-boosts/latest/v1")


class Source:
    CRYPTOCOM = "crypto.com"
    COINGECKO = "coingecko"
    CMC = "coinmarketcap"
    DEX = "dexscreener"
    CRYPTOPANIC = "cryptopanic"
    FREE_NEWS = "free-crypto-news"
    SYSTEM = "system"


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
                sym = name.split("_")[0].upper()
                symbols.add(sym)
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
                put_signal(sym, Source.CMC, "ranking", {"cmc_rank": row.get("cmc_rank"), "market_cap": row.get("quote", {}).get("USD", {}).get("market_cap")})
        log_source_health(Source.CMC, "OK", int((time.time() - t0) * 1000), len(rows))
    except Exception as e:
        put_signal(None, Source.CMC, "ingest_error", {"error": str(e)})
        log_source_health(Source.CMC, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))
    return out


def ingest_dex() -> tuple[set[str], set[str]]:
    t0 = time.time()
    boosted: set[str] = set()
    latest: set[str] = set()
    try:
        l = get_json(_norm_url(DEX_LATEST_PROFILES_ENDPOINT))
        b = get_json(_norm_url(DEX_BOOSTED_TOKENS_ENDPOINT))
        l_rows = l if isinstance(l, list) else []
        b_rows = b if isinstance(b, list) else []
        for row in l_rows[:400]:
            sym = str(row.get("tokenSymbol") or row.get("symbol") or "").upper()
            if sym:
                latest.add(sym)
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
    return latest, boosted


def ingest_news() -> None:
    for source, url in [
        (Source.CRYPTOPANIC, "https://cryptopanic.com/news/"),
        (Source.FREE_NEWS, "https://freecryptonews.com/"),
    ]:
        t0 = time.time()
        try:
            html = get_text(url, headers={"User-Agent": "Mozilla/5.0"})
            bullish_hits = sum(1 for w in ["surge", "breakout", "bull", "rally", "soar", "uptrend", "listing"] if w in html.lower())
            put_signal(None, source, "headline_scan", {"bullish_keyword_hits": bullish_hits, "bytes": len(html)})
            log_source_health(source, "OK", int((time.time() - t0) * 1000), 1)
        except Exception as e:
            put_signal(None, source, "ingest_error", {"error": str(e)})
            log_source_health(source, "DEGRADED", int((time.time() - t0) * 1000), 0, str(e))


def fuse_signals(cg: set[str], dex_latest: set[str], dex_boosted: set[str], crypto_symbols: set[str]) -> None:
    triangulated = cg.intersection(dex_latest)
    for sym in sorted(triangulated):
        listed = sym in crypto_symbols
        conviction = "high" if listed else "watchlist"
        if sym in dex_boosted:
            conviction = "moonshot" if listed else "watchlist"
        put_signal(sym, Source.SYSTEM, "triangulated_alpha", {
            "coingecko_trending": True,
            "dex_volume_profile": True,
            "dex_boosted": sym in dex_boosted,
            "crypto_com_listed": listed,
        }, conviction=conviction)


def run_cycle() -> dict:
    write_status({"state": "running", "job": "ingest_cryptocom"})
    crypto_symbols = ingest_cryptocom()

    write_status({"state": "running", "job": "ingest_coingecko"})
    cg = ingest_coingecko()

    write_status({"state": "running", "job": "ingest_cmc"})
    cmc = ingest_cmc()

    write_status({"state": "running", "job": "ingest_dex"})
    dex_latest, dex_boosted = ingest_dex()

    write_status({"state": "running", "job": "ingest_news"})
    ingest_news()

    write_status({"state": "running", "job": "fuse_signals"})
    fuse_signals(cg, dex_latest, dex_boosted, crypto_symbols)

    summary = {
        "crypto_symbols": len(crypto_symbols),
        "coingecko_trending": len(cg),
        "cmc_ranked": len(cmc),
        "dex_latest": len(dex_latest),
        "dex_boosted": len(dex_boosted),
        "triangulated": len(cg.intersection(dex_latest)),
    }
    write_status({"state": "idle", "job": "sleeping", "last_cycle_summary": summary})
    return summary


def main() -> None:
    init_tables()
    write_status({"state": "starting", "job": "boot"})
    while True:
        t0 = time.time()
        try:
            summary = run_cycle()
            write_status({"state": "cycle_complete", "job": "sleeping", "last_cycle_seconds": round(time.time() - t0, 2), "last_cycle_summary": summary})
        except Exception as e:
            put_signal(None, Source.SYSTEM, "cycle_error", {"error": str(e)})
            write_status({"state": "error", "job": "cycle_error", "error": str(e)})
        jitter = random.uniform(-0.1 * POLL_SECONDS, 0.1 * POLL_SECONDS)
        sleep_for = max(30, int(POLL_SECONDS + jitter - (time.time() - t0)))
        write_status({"state": "sleeping", "job": "waiting_next_cycle", "sleep_for": sleep_for})
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
