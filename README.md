# Project Crypt — Phase-1 Baseline

This branch contains the stable **Phase-1** implementation:

- Risk gate (`RiskManager`) with hard denials and sizing logic
- SQLite audit logs (`risk_log`, `trade_log`)
- Phase-1 smoke runner (`phase1_run.py`)
- Telegram notifier integration
- 5-minute activity monitor (`phase1_activity_monitor.py`)
- Live terminal dashboard (`live_dashboard.py`)

## Status
- Paper/shadow mode only
- No live order execution

## Run
```bash
python3 -m project_crypt.phase1_run
python3 -m project_crypt.phase1_activity_monitor
python3 -m project_crypt.live_dashboard
```
