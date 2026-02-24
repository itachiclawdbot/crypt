#!/usr/bin/env bash
set -euo pipefail
cd /home/itachi/.openclaw/workspace
LOG="/home/itachi/.openclaw/workspace/project_crypt/phase2_watchdog.log"

restart_if_missing () {
  local pattern="$1"
  local name="$2"
  shift 2
  if pgrep -f "$pattern" >/dev/null 2>&1; then
    echo "$(date -Is) ok $name" >> "$LOG"
  else
    echo "$(date -Is) restart $name" >> "$LOG"
    nohup "$@" >> "/home/itachi/.openclaw/workspace/project_crypt/${name}.log" 2>&1 < /dev/null &
    disown || true
  fi
}

restart_if_missing "python3 -m project_crypt.microstructure_service" "microstructure_service" python3 -m project_crypt.microstructure_service
restart_if_missing "python3 -m project_crypt.phase2_alpha_aggregator" "phase2_alpha_aggregator" python3 -m project_crypt.phase2_alpha_aggregator
restart_if_missing "python3 -m project_crypt.phase2_monitor_notifier" "phase2_monitor_notifier" python3 -m project_crypt.phase2_monitor_notifier
