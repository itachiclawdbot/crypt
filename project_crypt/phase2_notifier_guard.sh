#!/usr/bin/env bash
set -euo pipefail
cd /home/itachi/.openclaw/workspace
HB="/home/itachi/.openclaw/workspace/project_crypt/phase2_notifier_heartbeat.json"
LOG="/home/itachi/.openclaw/workspace/project_crypt/phase2_notifier_guard.log"

# Ensure notifier process exists
if ! pgrep -f "python3 -m project_crypt.phase2_monitor_notifier" >/dev/null 2>&1; then
  nohup python3 -m project_crypt.phase2_monitor_notifier >> /home/itachi/.openclaw/workspace/project_crypt/phase2_monitor_notifier.log 2>&1 < /dev/null &
  disown || true
  echo "$(date -Is) restarted_notifier_process" >> "$LOG"
fi

# Backup send when heartbeat stale (>75 min) or missing
python3 - <<'PY'
import json, os, time
from pathlib import Path
from project_crypt.phase2_monitor_notifier import hourly_summary
from project_crypt.telegram_notify import TelegramNotifier
hb=Path('/home/itachi/.openclaw/workspace/project_crypt/phase2_notifier_heartbeat.json')
stale=True
if hb.exists():
    try:
        d=json.loads(hb.read_text())
        ts=d.get('ts','')
        from datetime import datetime, timezone
        t=datetime.fromisoformat(ts)
        age=(datetime.now(timezone.utc)-t).total_seconds()
        stale = age > 75*60
    except Exception:
        stale=True
if stale:
    ok = TelegramNotifier().send('[GUARD][forced-hourly-backup]\n' + hourly_summary())
    hb.write_text(json.dumps({'ts': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(), 'ok': bool(ok), 'error': None if ok else 'guard_send_false'}))
    with open('/home/itachi/.openclaw/workspace/project_crypt/phase2_notifier_guard.log','a',encoding='utf-8') as f:
        f.write(f"{__import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()} guard_send_ok={bool(ok)}\n")
PY
