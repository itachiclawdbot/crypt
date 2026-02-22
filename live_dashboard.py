from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path("/home/itachi/.openclaw/workspace/project_crypt/cryptobot.sqlite3")
LOG_PATH = Path("/home/itachi/.openclaw/workspace/project_crypt/phase1_activity_monitor.log")
MONITOR_PATTERN = "python3 -m project_crypt.phase1_activity_monitor"


class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    BLUE = "\033[94m"
    GRAY = "\033[90m"


def ansi(s: str, color: str) -> str:
    return f"{color}{s}{C.RESET}"


def terminal_width(default: int = 110) -> int:
    try:
        return os.get_terminal_size().columns
    except OSError:
        return default


def hr(char: str = "═") -> str:
    return char * max(40, terminal_width() - 2)


def get_monitor_status() -> tuple[bool, str]:
    cmd = "ps -eo pid,etimes,cmd | grep -F \"python3 -m project_crypt.phase1_activity_monitor\" | grep -v grep"
    proc = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    out = proc.stdout.strip()
    if not out:
        return False, "down"
    line = out.splitlines()[0]
    parts = line.split(maxsplit=2)
    if len(parts) < 3:
        return True, "up"
    pid = parts[0]
    etimes = parts[1]
    return True, f"up • pid={pid} • alive={etimes}s"


def db_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    cur = conn.cursor()
    cur.execute("select count(*) from risk_log")
    risk = int(cur.fetchone()[0])
    cur.execute("select count(*) from trade_log")
    trade = int(cur.fetchone()[0])
    return risk, trade


def latest_risk(conn: sqlite3.Connection):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "select id, ts, event_type, approved, reason, payload_json from risk_log order by id desc limit 1"
    )
    return cur.fetchone()


def latest_trade(conn: sqlite3.Connection):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "select id, ts, symbol, side, pnl, meta_json from trade_log order by id desc limit 1"
    )
    return cur.fetchone()


def parse_payload(payload_json: str | None) -> tuple[str, str, str]:
    if not payload_json:
        return "-", "-", "-"
    try:
        obj = json.loads(payload_json)
        p = obj.get("payload", {})
        symbol = str(p.get("symbol", "-"))
        side = str(p.get("side", "-"))
        size = p.get("size_notional", "-")
        if isinstance(size, (float, int)):
            size = f"{size:,.2f}"
        return symbol, side, str(size)
    except Exception:
        return "-", "-", "-"


def last_log_line() -> str:
    if not LOG_PATH.exists():
        return "(no monitor log yet)"
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
        return lines[-1] if lines else "(log empty)"
    except Exception as e:
        return f"(log read error: {e})"


def fmt_ts(ts: str | None) -> str:
    if not ts:
        return "-"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        local = dt.astimezone()
        return local.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return ts


def render() -> str:
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    up, mon = get_monitor_status()
    mon_badge = ansi("● RUNNING", C.GREEN) if up else ansi("● STOPPED", C.RED)

    if not DB_PATH.exists():
        return f"{ansi('Project Crypt Live Dashboard', C.BOLD + C.CYAN)}\nDB missing: {DB_PATH}"

    conn = sqlite3.connect(DB_PATH)
    try:
        risk_count, trade_count = db_counts(conn)
        r = latest_risk(conn)
        t = latest_trade(conn)
    finally:
        conn.close()

    r_symbol = r_side = r_size = "-"
    r_approved = "-"
    r_reason = "-"
    r_when = "-"
    if r is not None:
        r_symbol, r_side, r_size = parse_payload(r["payload_json"])
        r_approved = "YES" if int(r["approved"]) == 1 else "NO"
        r_reason = r["reason"]
        r_when = fmt_ts(r["ts"])

    t_when = t_symbol = t_side = t_pnl = "-"
    if t is not None:
        t_when = fmt_ts(t["ts"])
        t_symbol = str(t["symbol"])
        t_side = str(t["side"])
        pnl = t["pnl"]
        t_pnl = "-" if pnl is None else f"{float(pnl):,.2f}"

    lines = []
    lines.append(ansi("╔" + hr("═") + "╗", C.BLUE))
    lines.append(ansi(f"║ {C.BOLD}PROJECT CRYPT • PHASE-1 LIVE DASHBOARD{C.RESET}{' ' * max(1, terminal_width()-44)}║", C.BLUE))
    lines.append(ansi("╚" + hr("═") + "╝", C.BLUE))
    lines.append(f"{ansi('Now:', C.GRAY)} {now}")
    lines.append(f"{ansi('Monitor:', C.GRAY)} {mon_badge}  {ansi(mon, C.DIM)}")
    lines.append("")
    lines.append(f"{ansi('Totals', C.MAGENTA)}  risk_log={ansi(str(risk_count), C.CYAN)}  trade_log={ansi(str(trade_count), C.CYAN)}")
    lines.append(ansi("─" * max(40, terminal_width()-2), C.GRAY))
    lines.append(ansi("Latest Risk Decision", C.YELLOW))
    lines.append(f"  approved: {ansi(r_approved, C.GREEN if r_approved=='YES' else C.RED)}   reason: {r_reason}")
    lines.append(f"  symbol: {r_symbol}   side: {r_side}   size_notional: {r_size}")
    lines.append(f"  ts: {r_when}")
    lines.append("")
    lines.append(ansi("Latest Trade", C.YELLOW))
    lines.append(f"  symbol: {t_symbol}   side: {t_side}   pnl: {t_pnl}")
    lines.append(f"  ts: {t_when}")
    lines.append("")
    lines.append(ansi("Last Monitor Activity", C.YELLOW))
    lines.append(f"  {last_log_line()}")
    lines.append("")
    lines.append(ansi("Press Ctrl+C to exit", C.DIM))
    return "\n".join(lines)


def main() -> None:
    refresh = 2.0
    if len(sys.argv) > 1:
        try:
            refresh = float(sys.argv[1])
        except Exception:
            pass
    try:
        while True:
            print("\033[2J\033[H", end="")
            print(render())
            time.sleep(refresh)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
