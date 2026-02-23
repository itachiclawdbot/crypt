from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
import time
from datetime import datetime, timezone, timedelta

import requests

DB_PATH = os.getenv("CRYPTO_DB_PATH", "/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3")
POLL_SECONDS = int(os.getenv("MICRO_POLL_SECONDS", "30"))
TOP_SYMBOLS = int(os.getenv("MICRO_TOP_SYMBOLS", "150"))
TARGET_NOTIONAL_USDT = float(os.getenv("PHASE2_TARGET_NOTIONAL_USDT", "300"))
MICRO_STATE_PATH = os.getenv("MICRO_STATE_PATH", "/home/itachi/.openclaw/workspace/project_crypt/micro_universe_state.json")
MICRO_MAX_REPLACEMENTS = int(os.getenv("MICRO_MAX_REPLACEMENTS", "20"))
MICRO_MIN_RESIDENCY_SEC = int(os.getenv("MICRO_MIN_RESIDENCY_SEC", "1800"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def db() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_tables() -> None:
    with db() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS micro_features_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                base_symbol TEXT NOT NULL,
                spread_bps REAL,
                spread_bps_median_60 REAL,
                spread_bps_median_300 REAL,
                spread_bps_p95_300 REAL,
                depth_usd_10bps REAL,
                depth_usd_20bps REAL,
                obi_10bps REAL,
                obi_20bps REAL,
                pressure_flag INTEGER,
                orderbook_slope REAL,
                liquidity_score REAL,
                risk_flags_json TEXT,
                payload_json TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS micro_features_latest (
                symbol TEXT PRIMARY KEY,
                base_symbol TEXT NOT NULL,
                ts TEXT NOT NULL,
                spread_bps REAL,
                spread_bps_median_60 REAL,
                spread_bps_median_300 REAL,
                spread_bps_p95_300 REAL,
                depth_usd_10bps REAL,
                depth_usd_20bps REAL,
                obi_10bps REAL,
                obi_20bps REAL,
                pressure_flag INTEGER,
                orderbook_slope REAL,
                liquidity_score REAL,
                risk_flags_json TEXT,
                payload_json TEXT
            )
            """
        )


def get_json(url: str) -> dict | list:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def _active_universe_symbols(limit: int) -> list[str]:
    with db() as c:
        cur = c.cursor()
        cur.execute(
            """
            select coalesce(instrument_symbol,'') inst
            from universe_state
            where ts >= datetime('now','-2 hours')
              and status in ('WATCH','ELIGIBLE','ACTIONABLE')
              and instrument_symbol is not null
              and quote_ccy in ('USDT','USD','USDC','EUR')
            group by instrument_symbol
            order by max(alpha_score) desc
            limit ?
            """,
            (limit,),
        )
        return [str(r[0]).upper() for r in cur.fetchall() if r[0]]


def _load_micro_state() -> dict:
    try:
        if os.path.exists(MICRO_STATE_PATH):
            with open(MICRO_STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"tracked": {}, "updated_ts": utc_now()}


def _save_micro_state(state: dict) -> None:
    try:
        with open(MICRO_STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


def top_symbols() -> list[str]:
    d = get_json("https://api.crypto.com/exchange/v1/public/get-tickers")
    rows = d.get("result", {}).get("data", []) if isinstance(d, dict) else []
    pairs: list[tuple[str, float]] = []
    for r in rows:
        inst = str(r.get("i", "")).upper()
        if "_" not in inst:
            continue
        base, quote = inst.split("_", 1)
        if quote not in {"USD", "USDT", "USDC", "EUR"}:
            continue
        if base in {"USDT", "USDC", "USD", "EUR"}:
            continue
        vol_quote = float(r.get("vv") or 0.0)
        if vol_quote <= 0:
            continue
        pairs.append((inst, vol_quote))
    pairs.sort(key=lambda x: x[1], reverse=True)

    # Issue-1 fix: expand to 120 interest-based books by default.
    top_vol = [p[0] for p in pairs[: max(40, TOP_SYMBOLS // 2)]]
    active = _active_universe_symbols(max(60, TOP_SYMBOLS))
    pinned = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "SUI_USDT"]

    candidates = []
    seen = set()
    for s in active + top_vol + pinned:
        if s not in seen:
            seen.add(s)
            candidates.append(s)

    state = _load_micro_state()
    tracked: dict[str, str] = dict(state.get("tracked", {}))  # instrument -> first_seen_ts
    now = datetime.now(timezone.utc)

    keep: list[str] = []
    for inst, first_ts in tracked.items():
        try:
            age = (now - datetime.fromisoformat(first_ts)).total_seconds()
        except Exception:
            age = MICRO_MIN_RESIDENCY_SEC + 1
        if inst in candidates or age < MICRO_MIN_RESIDENCY_SEC:
            keep.append(inst)

    desired = candidates[:TOP_SYMBOLS]
    additions = [s for s in desired if s not in keep]
    additions = additions[: max(1, MICRO_MAX_REPLACEMENTS)]

    final = []
    for s in keep + additions:
        if s not in final:
            final.append(s)
        if len(final) >= TOP_SYMBOLS:
            break

    # backfill if needed
    for s in desired:
        if len(final) >= TOP_SYMBOLS:
            break
        if s not in final:
            final.append(s)

    new_tracked = {}
    for s in final:
        new_tracked[s] = tracked.get(s) or utc_now()
    state["tracked"] = new_tracked
    state["updated_ts"] = utc_now()
    _save_micro_state(state)

    return final


def depth_usd_in_band(levels: list[list[str]], low: float, high: float) -> float:
    total = 0.0
    for px_s, qty_s, *_ in levels:
        px = float(px_s)
        qty = float(qty_s)
        if low <= px <= high:
            total += px * qty
    return total


def compute_snapshot_features(symbol: str) -> dict | None:
    d = get_json(f"https://api.crypto.com/exchange/v1/public/get-book?instrument_name={symbol}&depth=50")
    rows = d.get("result", {}).get("data", []) if isinstance(d, dict) else []
    if not rows:
        return None
    b = rows[0].get("bids", [])
    a = rows[0].get("asks", [])
    if not b or not a:
        return None

    best_bid = float(b[0][0])
    best_ask = float(a[0][0])
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None

    spread_bps = ((best_ask - best_bid) / mid) * 10000.0

    b10_low = mid * (1 - 10 / 10000)
    a10_high = mid * (1 + 10 / 10000)
    b20_low = mid * (1 - 20 / 10000)
    a20_high = mid * (1 + 20 / 10000)

    bid10 = depth_usd_in_band(b, b10_low, mid)
    ask10 = depth_usd_in_band(a, mid, a10_high)
    bid20 = depth_usd_in_band(b, b20_low, mid)
    ask20 = depth_usd_in_band(a, mid, a20_high)

    depth10 = bid10 + ask10
    depth20 = bid20 + ask20
    eps = 1e-9
    obi10 = (bid10 - ask10) / (depth10 + eps)
    obi20 = (bid20 - ask20) / (depth20 + eps)

    pressure_raw = bid20 > 3.0 * max(1.0, ask20)

    bands = [5, 10, 20, 50]
    depths = []
    for bp in bands:
        bl = mid * (1 - bp / 10000)
        ah = mid * (1 + bp / 10000)
        d_band = depth_usd_in_band(b, bl, mid) + depth_usd_in_band(a, mid, ah)
        depths.append(max(1.0, d_band))
    xs = [math.log(x) for x in bands]
    ys = [math.log(y) for y in depths]
    xbar = sum(xs) / len(xs)
    ybar = sum(ys) / len(ys)
    num = sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys))
    den = sum((x - xbar) ** 2 for x in xs) or 1.0
    slope = num / den

    liq_score = depth20 / max(1.0, TARGET_NOTIONAL_USDT)

    # Issue-2 fix: pressure requires both OBI condition AND adequate liquidity, plus non-toxic spread.
    pressure = bool(pressure_raw and liq_score >= 10.0 and spread_bps <= 80.0)

    flags = []
    if abs(obi20) >= 0.70 and liq_score >= 10.0:
        flags.append("obi_extreme")
    if liq_score < 5:
        flags.append("thin_liquidity")
    if spread_bps > 80:
        flags.append("wide_spread")

    return {
        "symbol": symbol,
        "base_symbol": symbol.split("_")[0].upper(),
        "spread_bps": spread_bps,
        "depth_usd_10bps": depth10,
        "depth_usd_20bps": depth20,
        "bid_depth_usd_20bps": bid20,
        "ask_depth_usd_20bps": ask20,
        "obi_10bps": obi10,
        "obi_20bps": obi20,
        "pressure_raw": pressure_raw,
        "pressure_flag": pressure,
        "orderbook_slope": slope,
        "liquidity_score": liq_score,
        "risk_flags": flags,
    }


def enrich_rolling(f: dict) -> dict:
    base = f["base_symbol"]
    since_15m = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    since_5m = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    with db() as c:
        cur = c.cursor()
        cur.execute(
            "select spread_bps from micro_features_log where base_symbol=? and ts>=? order by id desc limit 200",
            (base, since_15m),
        )
        spreads = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
        cur.execute(
            "select spread_bps from micro_features_log where base_symbol=? and ts>=? order by id desc limit 80",
            (base, since_5m),
        )
        spreads_5m = [float(r[0]) for r in cur.fetchall() if r[0] is not None]

    if spreads:
        f["spread_bps_median_300"] = statistics.median(spreads)
        f["spread_bps_p95_300"] = statistics.quantiles(spreads, n=20)[-1] if len(spreads) >= 20 else max(spreads)
    else:
        f["spread_bps_median_300"] = f["spread_bps"]
        f["spread_bps_p95_300"] = f["spread_bps"]

    f["spread_bps_median_60"] = statistics.median(spreads_5m[-12:]) if spreads_5m else f["spread_bps"]
    return f


def persist_feature(f: dict) -> None:
    payload = json.dumps(f, default=str)
    with db() as c:
        c.execute(
            """
            INSERT INTO micro_features_log (
                ts,symbol,base_symbol,spread_bps,spread_bps_median_60,spread_bps_median_300,spread_bps_p95_300,
                depth_usd_10bps,depth_usd_20bps,obi_10bps,obi_20bps,pressure_flag,orderbook_slope,liquidity_score,risk_flags_json,payload_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                f["symbol"],
                f["base_symbol"],
                f["spread_bps"],
                f["spread_bps_median_60"],
                f["spread_bps_median_300"],
                f["spread_bps_p95_300"],
                f["depth_usd_10bps"],
                f["depth_usd_20bps"],
                f["obi_10bps"],
                f["obi_20bps"],
                int(bool(f["pressure_flag"])),
                f["orderbook_slope"],
                f["liquidity_score"],
                json.dumps(f.get("risk_flags", [])),
                payload,
            ),
        )
        c.execute(
            """
            INSERT INTO micro_features_latest (
                symbol,base_symbol,ts,spread_bps,spread_bps_median_60,spread_bps_median_300,spread_bps_p95_300,
                depth_usd_10bps,depth_usd_20bps,obi_10bps,obi_20bps,pressure_flag,orderbook_slope,liquidity_score,risk_flags_json,payload_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                base_symbol=excluded.base_symbol, ts=excluded.ts, spread_bps=excluded.spread_bps,
                spread_bps_median_60=excluded.spread_bps_median_60, spread_bps_median_300=excluded.spread_bps_median_300,
                spread_bps_p95_300=excluded.spread_bps_p95_300, depth_usd_10bps=excluded.depth_usd_10bps,
                depth_usd_20bps=excluded.depth_usd_20bps, obi_10bps=excluded.obi_10bps, obi_20bps=excluded.obi_20bps,
                pressure_flag=excluded.pressure_flag, orderbook_slope=excluded.orderbook_slope,
                liquidity_score=excluded.liquidity_score, risk_flags_json=excluded.risk_flags_json, payload_json=excluded.payload_json
            """,
            (
                f["symbol"],
                f["base_symbol"],
                utc_now(),
                f["spread_bps"],
                f["spread_bps_median_60"],
                f["spread_bps_median_300"],
                f["spread_bps_p95_300"],
                f["depth_usd_10bps"],
                f["depth_usd_20bps"],
                f["obi_10bps"],
                f["obi_20bps"],
                int(bool(f["pressure_flag"])),
                f["orderbook_slope"],
                f["liquidity_score"],
                json.dumps(f.get("risk_flags", [])),
                payload,
            ),
        )


def main() -> None:
    init_tables()
    while True:
        try:
            state = _load_micro_state()
            pressure_meta: dict = dict(state.get("pressure_meta", {}))
            syms = top_symbols()
            now_iso = utc_now()
            now_dt = datetime.fromisoformat(now_iso)

            for s in syms:
                try:
                    f = compute_snapshot_features(s)
                    if not f:
                        continue

                    # Issue-4 fix: require pressure persistence (>=2 consecutive updates OR >=30s)
                    # and guard against near-zero ask depth artifacts for extreme OBI cases.
                    meta = pressure_meta.get(s, {})
                    prev_count = int(meta.get("count", 0))
                    prev_first = meta.get("first_ts")
                    try:
                        prev_first_dt = datetime.fromisoformat(prev_first) if prev_first else now_dt
                    except Exception:
                        prev_first_dt = now_dt

                    extreme_obi = abs(float(f.get("obi_20bps", 0.0))) > 0.9
                    ask20 = float(f.get("ask_depth_usd_20bps", 0.0))
                    bid20 = float(f.get("bid_depth_usd_20bps", 0.0))
                    artifact = bool(extreme_obi and ask20 < max(50.0, 0.05 * max(1.0, bid20)))

                    if bool(f.get("pressure_flag")) and (not artifact):
                        count = prev_count + 1
                        first_ts = prev_first or now_iso
                    else:
                        count = 0
                        first_ts = None

                    held_secs = (now_dt - prev_first_dt).total_seconds() if prev_first else 0.0
                    persisted = bool((count >= 2) or (held_secs >= 30.0))
                    f["pressure_flag"] = bool(f.get("pressure_flag") and persisted and (not artifact))
                    f["pressure_persistence_count"] = count
                    f["pressure_held_seconds"] = round(held_secs, 2)
                    f["obi_artifact_guard"] = artifact

                    if extreme_obi:
                        f.setdefault("risk_flags", []).append("obi_extreme_debug")
                        f["obi_debug"] = {
                            "bid_depth_usd_20bps": bid20,
                            "ask_depth_usd_20bps": ask20,
                            "artifact": artifact,
                        }

                    pressure_meta[s] = {"count": count, "first_ts": first_ts}

                    f = enrich_rolling(f)
                    persist_feature(f)
                except Exception:
                    continue

            state["pressure_meta"] = pressure_meta
            state["updated_ts"] = utc_now()
            _save_micro_state(state)
        except Exception:
            pass
        time.sleep(max(10, POLL_SECONDS))


if __name__ == "__main__":
    main()
