from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timezone

import requests

DB_PATH = "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3"

POLL_SECONDS = int(os.getenv("PHASE1B_POLL_SECONDS", "300"))
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
def _norm_url(value: str | None, fallback: str) -> str:
    v = (value or "").strip()
    if not v:
        return fallback
    if v.startswith("http://") or v.startswith("https://"):
        return v
    if v.startswith("/"):
        return "https://api.dexscreener.com" + v
    return fallback


DEX_LATEST_PROFILES_ENDPOINT = _norm_url(
    os.getenv("DEX_LATEST_PROFILES_ENDPOINT"),
    "https://api.dexscreener.com/token-profiles/latest/v1",
)
DEX_BOOSTED_TOKENS_ENDPOINT = _norm_url(
    os.getenv("DEX_BOOSTED_TOKENS_ENDPOINT"),
    "https://api.dexscreener.com/token-boosts/latest/v1",
)


class Source:
    COINGECKO = "coingecko"
    DEX = "dexscreener"
    COINPAPRIKA = "coinpaprika"
    NEWS = "free-crypto-news"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_tables() -> None:
    with conn() as c:
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


def fetch_json(url: str, headers: dict | None = None) -> dict | list:
    r = requests.get(url, headers=headers or {}, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_text(url: str, headers: dict | None = None) -> str:
    r = requests.get(url, headers=headers or {}, timeout=20)
    r.raise_for_status()
    return r.text


def put_signal(symbol: str | None, source: str, signal_type: str, payload: dict, conviction: str = "watch") -> None:
    raw = json.dumps(payload, sort_keys=True, default=str)
    h = hashlib.sha256(f"{symbol}|{source}|{signal_type}|{raw}".encode()).hexdigest()
    with conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO intel_cache (ts,symbol,source,signal_type,conviction,payload_json,dedupe_hash) VALUES (?,?,?,?,?,?,?)",
            (now_iso(), symbol, source, signal_type, conviction, raw, h),
        )


def step_coingecko() -> list[str]:
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
    trending = fetch_json("https://api.coingecko.com/api/v3/search/trending", headers=headers)
    symbols = []
    for item in trending.get("coins", []):
        coin = item.get("item", {})
        sym = str(coin.get("symbol", "")).upper()
        if sym:
            symbols.append(sym)
            put_signal(sym, Source.COINGECKO, "trending", {"coin": coin}, conviction="watch")

    # Requires API key on many plans; tolerate failure.
    try:
        new_list = fetch_json("https://api.coingecko.com/api/v3/coins/list/new", headers=headers)
        for coin in new_list[:100]:
            sym = str(coin.get("symbol", "")).upper()
            put_signal(sym, Source.COINGECKO, "new_listing", {"coin": coin}, conviction="watch")
    except Exception as e:
        put_signal(None, Source.COINGECKO, "new_listing_unavailable", {"error": str(e)}, conviction="watch")

    return symbols


def step_dex() -> dict:
    latest = fetch_json(DEX_LATEST_PROFILES_ENDPOINT)
    boosted = fetch_json(DEX_BOOSTED_TOKENS_ENDPOINT)

    boosted_symbols = set()
    for x in boosted[:200] if isinstance(boosted, list) else []:
        sym = str(x.get("tokenSymbol") or x.get("symbol") or "").upper()
        if sym:
            boosted_symbols.add(sym)
            put_signal(sym, Source.DEX, "boosted", {"entry": x}, conviction="watch")

    spike_symbols = set()
    for x in latest[:300] if isinstance(latest, list) else []:
        sym = str(x.get("tokenSymbol") or x.get("symbol") or "").upper()
        if not sym:
            continue
        vol = x.get("volume") or x.get("volume24h") or 0
        if isinstance(vol, (int, float)) and vol > 0:
            spike_symbols.add(sym)
        put_signal(sym, Source.DEX, "latest_profile", {"entry": x}, conviction="watch")

    return {"boosted": boosted_symbols, "spike": spike_symbols}


def step_coinpaprika() -> None:
    g = fetch_json("https://api.coinpaprika.com/v1/global")
    t = fetch_json("https://api.coinpaprika.com/v1/tickers?limit=100")
    put_signal(None, Source.COINPAPRIKA, "global", {"global": g}, conviction="watch")
    put_signal(None, Source.COINPAPRIKA, "tickers", {"tickers": t}, conviction="watch")


def step_news() -> None:
    html = fetch_text("https://cryptopanic.com/news/", headers={"User-Agent": "Mozilla/5.0"})
    bullish_words = ["surge", "breakout", "bull", "rally", "soar", "uptrend"]
    score = sum(1 for w in bullish_words if w in html.lower())
    put_signal(None, Source.NEWS, "headline_sentiment", {"bullish_keyword_hits": score}, conviction="watch")


def triangulate(trending: list[str], dex: dict) -> None:
    high = set(trending).intersection(dex.get("spike", set()))
    for sym in sorted(high):
        conviction = "high"
        if sym in dex.get("boosted", set()):
            conviction = "moonshot"
        put_signal(sym, "fusion", "triangulated_alpha", {"in_trending": True, "dex_spike": True, "boosted": sym in dex.get("boosted", set())}, conviction=conviction)


def run_cycle() -> None:
    trending = step_coingecko()
    dex = step_dex()
    step_coinpaprika()
    step_news()
    triangulate(trending, dex)


def main() -> None:
    init_tables()
    while True:
        started = time.time()
        try:
            run_cycle()
        except Exception as e:
            put_signal(None, "system", "cycle_error", {"error": str(e)}, conviction="watch")
        # 300s with ±10% jitter
        base = POLL_SECONDS
        jitter = random.uniform(-0.1 * base, 0.1 * base)
        sleep_s = max(30, int(base + jitter - (time.time() - started)))
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
